"""Transport layer for KoshaClient.

Handles API key injection, HTTP requests, retry, and JSON serialization.
"""

from __future__ import annotations

import json
import logging
import math
import random
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
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


# A 429 is retried for every request. A 429'd write is safe to repeat: the
# server refuses most before any work (tenant limits, write backpressure),
# and a sharded write refused on one shard after another committed repeats
# the committed part as the same upsert by document id. A 429'd search may
# have scored before it was shed, but a search only reads.
# 502/503/504 are retried only for reads: a 503 can also come back after a
# write was forwarded to the ingest pod, which may have applied it, and is
# left to the caller.
_RETRY_ANY = frozenset({429})
_RETRY_READ = frozenset({502, 503, 504})


def _is_read(method: str, path: str) -> bool:
    """Whether a request only reads (search, count, get, stats, schema)."""
    last_segment = path.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    if method in ("GET", "HEAD"):
        return True
    return method == "POST" and last_segment == "search"


def _retry_after_secs(value: str | None) -> float | None:
    """A ``Retry-After`` header (delta-seconds or HTTP-date), in seconds."""
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        pass
    else:
        # "nan"/"inf" parse as floats but are no usable hint.
        return max(0.0, seconds) if math.isfinite(seconds) else None
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


class Transport:
    """Low-level HTTP transport for Kosha API requests.

    Handles URL construction, API key auth, request/response serialization,
    timeouts, and retries.

    ``max_retries`` is the number of attempts in total. Connection errors are
    retried as before. A 429 (any request) or a 502/503/504 (reads only) is
    retried after the server's ``Retry-After`` plus jitter, or after a full
    jitter exponential backoff when there is none, as long as the retry
    starts within ``retry_budget`` seconds of the first attempt. A
    ``Retry-After`` that does not fit the budget (a 60 s quota refusal) is
    not waited for: the error is raised at once. ``retry_budget=0`` turns
    status retries off.
    """

    # Full-jitter backoff without a Retry-After: uniform(0, min(cap, base * 2**n)).
    backoff_base = 0.5
    backoff_cap = 4.0

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout: int = 60,
        max_retries: int = 3,
        retry_on_timeout: bool = True,
        retry_budget: float = 20.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout
        self.retry_budget = retry_budget

    def _status_retry_delay(
        self, method: str, path: str, status: int, retry_after: str | None,
        attempt: int, started: float,
    ) -> float | None:
        """Seconds to sleep before retrying this status, or ``None`` to raise."""
        if attempt >= self.max_retries or self.retry_budget <= 0:
            return None
        if status not in _RETRY_ANY and not (
            status in _RETRY_READ and _is_read(method, path)
        ):
            return None
        jitter = random.uniform(
            0, min(self.backoff_cap, self.backoff_base * 2**attempt)
        )
        hinted = _retry_after_secs(retry_after)
        # The hint is the floor; jitter spreads the callers it shed together.
        delay = jitter if hinted is None else hinted + jitter / 2
        if time.monotonic() - started + delay > self.retry_budget:
            return None
        return delay

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
        started = time.monotonic()
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = urllib.request.urlopen(req, timeout=self.timeout)
                return json.loads(resp.read().decode())
            except urllib.error.HTTPError as e:
                body_bytes = e.read()
                try:
                    err = json.loads(body_bytes.decode())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    err = None
                if not isinstance(err, dict):
                    # A non-JSON or non-object body (a proxy's HTML page).
                    err = {"error": body_bytes.decode(errors="replace")}
                retry_after = e.headers.get("Retry-After") if e.headers else None
                delay = self._status_retry_delay(
                    method, path, e.code, retry_after, attempt, started
                )
                if delay is not None:
                    logger.info(
                        "Kosha request retrying: %s %s → %s (attempt %d/%d, in %.2fs) %s",
                        method, path, e.code, attempt, self.max_retries, delay,
                        err.get("reason", ""),
                    )
                    time.sleep(delay)
                    continue
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
