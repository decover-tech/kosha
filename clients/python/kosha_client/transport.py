"""Transport layer for KoshaClient.

Handles API key injection, HTTP requests, retry, and JSON serialization.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

try:
    import numpy as np
except ImportError:
    np = None

logger = logging.getLogger(__name__)


def json_default(data: Any) -> Any:
    """Serialize non-JSON-native types the way opensearchpy would.

    This is the same serializer used by opensearchpy.serializer.JSONSerializer
    so that documents carrying datetimes, UUIDs, Decimals or numpy scalars
    serialize exactly as they would on the OpenSearch path.
    """
    if isinstance(data, (date, datetime)):
        return data.isoformat()
    if isinstance(data, UUID):
        return str(data)
    if isinstance(data, Decimal):
        return float(data)
    if np is not None:
        if isinstance(data, np.integer):
            return int(data)
        if isinstance(data, (np.floating, np.bool_)):
            return data.item()
        if isinstance(data, np.datetime64):
            return data.item().isoformat()
    raise TypeError(f"Unable to serialize {data!r} (type: {type(data)})")


class Transport:
    """Low-level HTTP transport for Kosha API requests.

    Handles URL construction, API key auth, request/response serialization,
    timeouts, and retries.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout: int = 60,
        max_retries: int = 3,
        retry_on_timeout: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout

    def request(
        self, method: str, path: str, body: Any = None
    ) -> Any:
        """Make an HTTP request, returning the parsed JSON response.

        Uses the v1 proto-defined paths (eg. ``/v1/namespaces/{ns}/search``).
        The caller is responsible for constructing the full path.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        data = json.dumps(body, default=json_default).encode() if body is not None else None

        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.api_key:
            req.add_header("Authorization", f"Bearer {self.api_key}")

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = urllib.request.urlopen(req, timeout=self.timeout)
                return json.loads(resp.read().decode())
            except urllib.error.HTTPError as e:
                body_bytes = e.read()
                try:
                    err = json.loads(body_bytes.decode())
                except json.JSONDecodeError:
                    err = {"error": body_bytes.decode()}
                logger.warning(
                    "Kosha request failed: %s %s → %s %s",
                    method, path, e.code, err,
                )
                raise KoshaRequestError(e.code, err.get("error", str(e)), err) from e
            except urllib.error.URLError as e:
                last_error = e
                if isinstance(e, urllib.error.URLError) and not self.retry_on_timeout:
                    break
                if attempt < self.max_retries:
                    import time
                    time.sleep(0.1 * (2 ** attempt))
                continue

        raise KoshaRequestError(0, f"request failed after {self.max_retries} retries: {last_error}") from last_error

    def request_with_ns(
        self,
        method: str,
        path_template: str,
        namespace: str,
        body: Any = None,
    ) -> Any:
        """Convenience: build a v1 path with namespace substitution.

        ``path_template`` should contain ``{namespace}``, e.g.
        ``/v1/namespaces/{namespace}/search``.
        """
        path = path_template.replace("{namespace}", namespace)
        return self.request(method, path, body)


class KoshaRequestError(Exception):
    """Raised when a Kosha HTTP request fails."""

    def __init__(self, status_code: int, error: str, info: dict | None = None):
        self.status_code = status_code
        self.error = error
        self.info = info or {}
        super().__init__(f"KoshaRequestError({status_code}): {error}")
