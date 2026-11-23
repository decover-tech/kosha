"""Transport layer for KoshaClient.

Handles API key injection, HTTP requests, retry, and JSON serialization.
"""

from __future__ import annotations

import collections
import http.client
import json
import logging
import math
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
    timeouts, and retries.

    ``max_retries`` is the number of attempts in total. Connection errors are
    retried as before. A 429 (any request) or a 502/503/504 (reads only) is
    retried after the server's ``Retry-After`` plus jitter, or after a full
    jitter exponential backoff when there is none, as long as the retry
    starts within ``retry_budget`` seconds of the first attempt. A
    ``Retry-After`` that does not fit the budget (a 60 s quota refusal) is
    not waited for: the error is raised at once. ``retry_budget=0`` turns
    status retries off.

    Connections are kept alive and reused (see ``_Pool``). A read that fails
    on a reused connection before any response (its peer closed it while it
    sat idle) is resent once on a new connection without counting as an
    attempt; a write takes the connection-error retry above. A base URL that
    the ``*_proxy`` environment routes through a proxy uses ``urlopen`` per
    request, as before.
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
        self._proxied = _uses_proxy(self.base_url)

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

        # Header names are capitalized as urllib's Request.add_header does.
        req_headers = {"Content-type": "application/json"}
        if self.api_key:
            req_headers["Authorization"] = f"Bearer {self.api_key}"

        last_error: Exception | None = None
        started = time.monotonic()
        for attempt in range(1, self.max_retries + 1):
            try:
                status, reason, resp_headers, body_bytes = self._send(
                    method, path, url, data, req_headers
                )
            except urllib.error.URLError as e:
                last_error = e
                if isinstance(e, urllib.error.URLError) and not self.retry_on_timeout:
                    break
                if attempt < self.max_retries:
                    time.sleep(0.1 * (2 ** attempt))
                continue
            if 200 <= status < 300:
                return json.loads(body_bytes.decode())
            try:
                err = json.loads(body_bytes.decode())
            except (json.JSONDecodeError, UnicodeDecodeError):
                err = None
            if not isinstance(err, dict):
                # A non-JSON or non-object body (a proxy's HTML page).
                err = {"error": body_bytes.decode(errors="replace")}
            retry_after = resp_headers.get("Retry-After") if resp_headers else None
            delay = self._status_retry_delay(
                method, path, status, retry_after, attempt, started
            )
            if delay is not None:
                logger.info(
                    "Kosha request retrying: %s %s → %s (attempt %d/%d, in %.2fs) %s",
                    method, path, status, attempt, self.max_retries, delay,
                    err.get("reason", ""),
                )
                time.sleep(delay)
                continue
            logger.warning(
                "Kosha request failed: %s %s → %s %s",
                method, path, status, err,
            )
            raise KoshaRequestError(
                status, err.get("error", f"HTTP Error {status}: {reason}"), err
            )

        raise KoshaRequestError(0, f"request failed after {self.max_retries} retries: {last_error}") from last_error

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
    """Raised when a Kosha HTTP request fails."""

    def __init__(self, status_code: int, error: str, info: dict | None = None):
        self.status_code = status_code
        self.error = error
        self.info = info or {}
        super().__init__(f"KoshaRequestError({status_code}): {error}")
