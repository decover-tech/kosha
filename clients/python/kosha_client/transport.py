"""Transport layer for KoshaClient.

Handles API key injection, HTTP requests, retry, and JSON serialization.
"""

from __future__ import annotations

import collections
import email.utils
import http.client
import json
import logging
import os
import random
import select
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit
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

    Explicit calls to the deprecated ``POST /replace`` through this transport
    also count as merges.
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


def retryable_status(
    status: int, body: Any, idempotent: bool, replay_safe: bool | None = None
) -> bool:
    """RETRY.md rules 1 and 2: the body's ``retryable`` decides when present.

    ``replay_safe`` is ``idempotent`` or the caller's ``retry_non_idempotent``
    opt-in. A body with ``details.outcome == "unknown"`` (a forwarded write
    that may have been applied) is retried only when replay is safe,
    whatever ``retryable`` says.
    """
    if replay_safe is None:
        replay_safe = idempotent
    details = body.get("details") if isinstance(body, dict) else None
    if isinstance(details, dict) and details.get("outcome") == "unknown":
        return replay_safe
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


def _is_read(method: str, path: str) -> bool:
    """Whether a request only reads (search, count, get, stats, schema): the
    requests resent once when a reused connection turns out closed."""
    last_segment = path.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    if method in ("GET", "HEAD"):
        return True
    return method == "POST" and last_segment == "search"


# Kept-alive connections, shared by every Transport in the process (the
# backend builds a new client per call) and reused per (scheme, host, port),
# so a request pays DNS and the TCP/TLS handshake only when it opens a new
# connection, not every time. At most _POOL_MAXSIZE idle connections are
# kept per host; a request that finds none opens one, so concurrency is not
# capped. An idle connection is dropped after _POOL_IDLE_SECS, below the
# server's 30 s keep-alive idle timeout (KOSHA_HTTP_IDLE_TIMEOUT_SECS). A
# connection is retired after _POOL_MAX_AGE_SECS: a Service balances per
# connection, so this is how load reaches pods added since (autoscaling) and
# leaves pods being replaced.
_POOL_MAXSIZE = 10
_POOL_IDLE_SECS = 15.0
_POOL_MAX_AGE_SECS = 60.0
_USER_AGENT = f"Python-urllib/{sys.version_info[0]}.{sys.version_info[1]}"


def _alive(sock: socket.socket | None) -> bool:
    """Whether an idle connection's socket is still open with nothing unread.

    A peer that closed it (idle timeout, restart) makes it readable: EOF, a
    reset, or stray bytes. None of those can carry another request. Polled
    rather than peeked so it works on TLS sockets too.
    """
    if sock is None:
        return False
    try:
        if hasattr(select, "poll"):  # select() fails on fds above FD_SETSIZE
            poller = select.poll()
            poller.register(sock, select.POLLIN)
            return not poller.poll(0)
        return not select.select([sock], [], [], 0)[0]
    except (OSError, ValueError):
        return False


class _Pool:
    """Idle kept-alive connections to one host, newest last."""

    def __init__(self, scheme: str, host: str, port: int | None):
        self.scheme = scheme
        self.host = host
        self.port = port
        self._lock = threading.Lock()
        self._idle: collections.deque[tuple[http.client.HTTPConnection, float]] = (
            collections.deque()
        )

    def get(self, timeout: float) -> tuple[http.client.HTTPConnection, bool]:
        """A connection and whether it was reused; a new one is not connected yet."""
        while True:
            with self._lock:
                conn, since = self._idle.pop() if self._idle else (None, 0.0)
            if conn is None:
                break
            if time.monotonic() - since <= _POOL_IDLE_SECS and _alive(conn.sock):
                conn.timeout = timeout
                conn.sock.settimeout(timeout)
                return conn, True
            conn.close()
        cls = (
            http.client.HTTPSConnection
            if self.scheme == "https"
            else http.client.HTTPConnection
        )
        conn = cls(self.host, self.port, timeout=timeout)
        conn._kosha_opened = time.monotonic()  # type: ignore[attr-defined]
        return conn, False

    def put(self, conn: http.client.HTTPConnection) -> None:
        opened = getattr(conn, "_kosha_opened", 0.0)
        if time.monotonic() - opened > _POOL_MAX_AGE_SECS:
            conn.close()
            return
        with self._lock:
            self._idle.append((conn, time.monotonic()))
            extra = self._idle.popleft()[0] if len(self._idle) > _POOL_MAXSIZE else None
        if extra is not None:
            extra.close()

    def clear(self) -> None:
        """Close every idle connection (their peer has likely closed them all)."""
        with self._lock:
            idle, self._idle = self._idle, collections.deque()
        for conn, _ in idle:
            conn.close()


_POOLS: dict[tuple[str, str, int | None], _Pool] = {}
_POOLS_LOCK = threading.Lock()


def _pool_for(scheme: str, host: str, port: int | None) -> _Pool:
    key = (scheme, host, port)
    pool = _POOLS.get(key)
    if pool is None:
        with _POOLS_LOCK:
            pool = _POOLS.setdefault(key, _Pool(scheme, host, port))
    return pool


def _reset_pools_after_fork() -> None:
    # A forked child must not share its parent's sockets, nor inherit a lock
    # another thread held at fork time.
    global _POOLS, _POOLS_LOCK
    _POOLS = {}
    _POOLS_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_pools_after_fork)


def _uses_proxy(url: str) -> bool:
    """Whether urllib would send this URL through a proxy (``*_proxy`` env)."""
    parts = urlsplit(url)
    return parts.scheme in urllib.request.getproxies() and not (
        urllib.request.proxy_bypass(parts.hostname or "")
    )


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

    Connections are kept alive and reused (see ``_Pool``). A read that fails
    on a reused connection before any response (its peer closed it while it
    sat idle) is resent once on a new connection without counting as an
    attempt; any other request takes the transport-error rule above. A base
    URL that the ``*_proxy`` environment routes through a proxy uses
    ``urlopen`` per request.

    ``retry_budget`` (seconds, 0.2.17) is kept for compatibility: when given,
    it sets ``total_budget_ms``.
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
        retry_budget: float | None = None,
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
        self.total_budget_ms = (
            total_budget_ms if retry_budget is None else retry_budget * 1000.0
        )
        self.headers = dict(headers or {})
        self._proxied = _uses_proxy(self.base_url)

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

        # Header names are capitalized as urllib's Request.add_header does,
        # so a caller's header replaces ours instead of duplicating it.
        req_headers = {"Content-type": "application/json"}
        if self.api_key:
            req_headers["Authorization"] = f"Bearer {self.api_key}"
        for name, value in {**self.headers, **(headers or {})}.items():
            req_headers[name.capitalize()] = value

        attempt = 0
        while True:
            attempt += 1
            delay: float | None = None
            cause: Exception | None = None
            try:
                status, reason, resp_headers, body_bytes = self._send(
                    method, path, url, data, req_headers
                )
            except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
                reason = e.reason if isinstance(e, urllib.error.URLError) else e
                # retry_on_timeout=False is 0.2.x's fail-fast on every
                # transport error, before or after the write.
                retry = self.retry_on_timeout and (
                    _before_write(reason) or idempotent or self.retry_non_idempotent
                )
                error = KoshaRequestError(0, f"{method} {path} failed: {reason}")
                cause = e
            else:
                if 200 <= status < 300:
                    return json.loads(body_bytes.decode()) if body_bytes.strip() else {}
                try:
                    err = json.loads(body_bytes.decode())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    err = None
                if not isinstance(err, dict):
                    # A non-JSON or non-object body (a proxy's HTML page).
                    err = {"error": body_bytes.decode(errors="replace")}
                delay = server_delay_ms(err, resp_headers)
                request_id = resp_headers.get("X-Request-Id") if resp_headers is not None else None
                if request_id and not err.get("request_id"):
                    err = {**err, "request_id": request_id}
                error = KoshaRequestError(
                    status, err.get("error", f"HTTP Error {status}: {reason}"), err,
                    retry_after_ms=delay,
                )
                retry = retryable_status(
                    status, err, idempotent, idempotent or self.retry_non_idempotent
                )

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

    def _send(
        self, method: str, path: str, url: str, data: bytes | None, headers: dict
    ) -> tuple[int, str, Any, bytes]:
        """One HTTP exchange: ``(status, reason, headers, body)`` for any status.

        Errors are those of ``urlopen``: a failure to connect or send raises
        ``urllib.error.URLError``; a failure while waiting for or reading the
        response (a timeout included) propagates unchanged. New here: a
        reused connection that turns out closed (any error but a timeout)
        raises ``URLError``, so it takes the connection-error retry.
        """
        if self._proxied:
            req = urllib.request.Request(url, data=data, method=method)
            for name, value in headers.items():
                req.add_header(name, value)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return resp.status, resp.reason, resp.headers, resp.read()
            except urllib.error.HTTPError as e:
                return e.code, e.reason, e.headers, e.read()

        parts = urlsplit(url)
        target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        headers = {"User-agent": _USER_AGENT, **headers}
        pool = _pool_for(parts.scheme, parts.hostname or "", parts.port)
        resend_stale = _is_read(method, path)
        while True:
            conn, reused = pool.get(self.timeout)
            sent = False
            try:
                conn.request(method, target, body=data, headers=headers)
                sent = True
                resp = conn.getresponse()
            except OSError as e:
                conn.close()
                # A timeout is a slow server, not a closed connection: it is
                # neither resent here nor a reason to drop the idle ones.
                stale = reused and not isinstance(e, TimeoutError)
                if stale:
                    pool.clear()
                    if resend_stale:
                        resend_stale = False
                        continue
                if stale or not sent:
                    raise urllib.error.URLError(e) from e
                raise
            except BaseException:
                conn.close()
                raise
            try:
                body = resp.read()
            except BaseException:
                conn.close()
                raise
            if resp.will_close:
                conn.close()
            else:
                pool.put(conn)
            return resp.status, resp.reason, resp.headers, body

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
