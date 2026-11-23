"""Transport layer for KoshaClient.

Handles API key injection, HTTP requests, retry, and JSON serialization.
"""

from __future__ import annotations

import email.utils
import http.client
import json
import logging
import random
import socket
import time
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


def encode_json(data: Any) -> bytes:
    """The request-body encoding: compact JSON in UTF-8, as the TypeScript
    (``JSON.stringify``) and Go (``json.Marshal``) SDKs send it.

    ``bulk_index`` sizes batches with this same function, so the bytes it
    counts are the bytes sent. A lone surrogate (not encodable in UTF-8)
    becomes its ``\\uXXXX`` JSON escape, character by character, so the
    encoding of a list is still the sum of its items' encodings.
    """
    text = json.dumps(data, default=json_default, separators=(",", ":"), ensure_ascii=False)
    return text.encode("utf-8", "backslashreplace")


# The retry policy (clients/conformance/RETRY.md), the same in the Python,
# TypeScript and Go SDKs.
DEFAULT_BASE_DELAY_MS = 250
DEFAULT_MAX_DELAY_MS = 8000
DEFAULT_MAX_RETRY_AFTER_MS = 30000
DEFAULT_TOTAL_BUDGET_MS = 60000

# POST actions under /v1/namespaces/{ns}/ that are safe to replay: upserts
# are keyed by the required id, the rest read or converge.
_IDEMPOTENT_ACTIONS = frozenset({"search", "exists", "documents", "replace", "delete", "flush"})


def is_idempotent(method: str, path: str) -> bool:
    """Whether replaying ``method path`` cannot change the outcome.

    Idempotent: every GET, search, exists, upsert, merge, delete_by_query,
    flush and namespace delete. Not: namespace create, schema put, branch
    create and delete, shards, and any route this table does not know.

    The deprecated ``POST /replace`` counts as merge: the client still sends
    merges there against servers older than API 0.9.0 (``client._request``).
    """
    m = method.upper()
    if m in ("GET", "HEAD"):
        return True
    parts = [p for p in path.split("?", 1)[0].split("/") if p]
    if m == "POST" and parts == ["replace"]:
        return True
    if parts[:2] != ["v1", "namespaces"]:
        return False
    if m == "DELETE":
        return len(parts) == 3
    return m == "POST" and len(parts) == 4 and parts[3] in _IDEMPOTENT_ACTIONS


def retryable_status(status: int, body: Any, idempotent: bool) -> bool:
    """RETRY.md rules 1 and 2: the body's ``retryable`` decides when present."""
    if isinstance(body, dict) and isinstance(body.get("retryable"), bool):
        return body["retryable"]
    if status in (429, 503):
        return True
    if status in (502, 504):
        return idempotent
    return False


def parse_retry_after(value: str | None, now: float | None = None) -> float | None:
    """``Retry-After`` (delta-seconds or an HTTP-date) in milliseconds."""
    if value is None:
        return None
    value = value.strip()
    if value.isdigit():
        return int(value) * 1000.0
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None
    current = time.time() if now is None else now
    return max(0.0, (when.timestamp() - current) * 1000.0)


def server_delay_ms(body: Any, headers: Any) -> float | None:
    """The delay the server asked for: ``retry_after_ms``, else ``Retry-After``."""
    if isinstance(body, dict):
        ms = body.get("retry_after_ms")
        if isinstance(ms, (int, float)) and not isinstance(ms, bool) and ms >= 0:
            return float(ms)
    return parse_retry_after(headers.get("Retry-After") if headers is not None else None)


def _before_write(reason: Any) -> bool:
    """A failure before the request was sent (DNS, refused): safe to replay."""
    return isinstance(reason, (ConnectionRefusedError, socket.gaierror))


class Transport:
    """Low-level HTTP transport for Kosha API requests.

    Handles URL construction, API key auth, request/response serialization,
    timeouts, and retries under the shared policy in
    ``clients/conformance/RETRY.md``:

    * a response whose body says ``"retryable": true`` is retried (and
      ``false`` never is); without that field, 429 and 503 are retried for
      every operation and 502/504 for idempotent ones;
    * a connection that could not be opened is retried for every operation;
      a timeout or reset after the request was sent only for idempotent ones
      (or with ``retry_non_idempotent``). ``retry_on_timeout=False`` turns
      off both, as in 0.2.x (no transport error is retried; HTTP responses
      still follow the first rule);
    * the delay is the server's (``retry_after_ms``, then ``Retry-After``),
      else full-jitter backoff. A server delay over ``max_retry_after_ms``, a
      sleep past ``total_budget_ms``, or ``max_retries`` attempts ends it.

    ``max_retries`` counts attempts, including the first: 1 is fail-fast.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout: int = 60,
        max_retries: int = 3,
        retry_on_timeout: bool = True,
        retry_non_idempotent: bool = False,
        base_delay_ms: float = DEFAULT_BASE_DELAY_MS,
        max_delay_ms: float = DEFAULT_MAX_DELAY_MS,
        max_retry_after_ms: float = DEFAULT_MAX_RETRY_AFTER_MS,
        total_budget_ms: float = DEFAULT_TOTAL_BUDGET_MS,
        headers: dict | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout
        self.retry_non_idempotent = retry_non_idempotent
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self.max_retry_after_ms = max_retry_after_ms
        self.total_budget_ms = total_budget_ms
        self.headers = dict(headers or {})

    def _backoff_ms(self, retry: int) -> float:
        return random.uniform(0, min(self.max_delay_ms, self.base_delay_ms * (2 ** retry)))

    def request(
        self, method: str, path: str, body: Any = None, headers: dict | None = None
    ) -> Any:
        """Make an HTTP request, returning the parsed JSON response.

        Uses the v1 proto-defined paths (eg. ``/v1/namespaces/{ns}/search``).
        The caller is responsible for constructing the full path.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        # Serialized once, so every attempt replays the same bytes.
        data = encode_json(body) if body is not None else None
        idempotent = is_idempotent(method, "/" + path.lstrip("/"))
        attempts = max(1, int(self.max_retries))
        started = time.monotonic()

        attempt = 0
        while True:
            attempt += 1
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header("Content-Type", "application/json")
            if self.api_key:
                req.add_header("Authorization", f"Bearer {self.api_key}")
            for name, value in {**self.headers, **(headers or {})}.items():
                req.add_header(name, value)

            delay: float | None = None
            cause: Exception
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    raw = resp.read()
                return json.loads(raw.decode()) if raw.strip() else {}
            except urllib.error.HTTPError as e:
                body_bytes = e.read()
                try:
                    err = json.loads(body_bytes.decode())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    err = {"error": body_bytes.decode(errors="replace")}
                if not isinstance(err, dict):
                    err = {"error": str(err)}
                delay = server_delay_ms(err, e.headers)
                request_id = e.headers.get("X-Request-Id") if e.headers is not None else None
                if request_id and not err.get("request_id"):
                    err = {**err, "request_id": request_id}
                error = KoshaRequestError(e.code, err.get("error", str(e)), err, retry_after_ms=delay)
                retry = retryable_status(e.code, err, idempotent)
                cause = e
            except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
                reason = e.reason if isinstance(e, urllib.error.URLError) else e
                # retry_on_timeout=False is 0.2.x's fail-fast on every
                # transport error, before or after the write.
                retry = self.retry_on_timeout and (
                    _before_write(reason) or idempotent or self.retry_non_idempotent
                )
                error = KoshaRequestError(0, f"{method} {path} failed: {reason}")
                cause = e

            error.attempts = attempt
            give_up = not retry or attempt >= attempts
            if not give_up:
                if delay is not None:
                    give_up = delay > self.max_retry_after_ms
                    sleep_ms = delay + random.uniform(0, 100)
                else:
                    sleep_ms = self._backoff_ms(attempt - 1)
                elapsed_ms = (time.monotonic() - started) * 1000
                give_up = give_up or elapsed_ms + sleep_ms > self.total_budget_ms
            if give_up:
                logger.warning(
                    "Kosha request failed: %s %s → %s %s (attempts=%d)",
                    method, path, error.status_code, error.info or error.error, attempt,
                )
                raise error from cause
            logger.info(
                "Kosha request %s %s: retrying in %.0f ms (attempt %d of %d)",
                method, path, sleep_ms, attempt, attempts,
            )
            time.sleep(sleep_ms / 1000.0)

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
    """Raised when a Kosha HTTP request fails.

    ``code``, ``retryable``, ``retry_after_ms`` and ``request_id`` come from
    the server's error body (docs/api/errors.md); branch on ``code``, never
    on the human-readable ``error``. ``code`` falls back to the deprecated
    ``reason`` of a pre-0.10 tenant-limit rejection. They are ``None`` /
    ``False`` for an error that did not come from a Kosha server
    (``status_code`` 0 means no response arrived). ``attempts`` is how many
    attempts were made before giving up.
    """

    def __init__(
        self,
        status_code: int,
        error: str,
        info: dict | None = None,
        retry_after_ms: float | None = None,
    ):
        self.status_code = status_code
        self.error = error
        self.info = info or {}
        self.code: str | None = self.info.get("code") or self.info.get("reason")
        self.retryable: bool = bool(self.info.get("retryable", False))
        body_delay = self.info.get("retry_after_ms")
        self.retry_after_ms: float | None = body_delay if body_delay is not None else retry_after_ms
        self.request_id: str | None = self.info.get("request_id")
        self.attempts: int = 1
        detail = f" [{self.code}]" if self.code else ""
        super().__init__(f"KoshaRequestError({status_code}){detail}: {error}")
