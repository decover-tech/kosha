"""Native /v1 helpers shared with the TypeScript and Go SDKs (#351).

``search_all`` pages a search with ``search_after``; ``bulk_index`` indexes
many documents in batches with bounded concurrency. Both take Kosha-native
bodies (``{"query_text": ..., "filter": ...}``, ``{"id": ..., "fields":
[...]}``), not OpenSearch DSL, and behave exactly like ``searchAll`` /
``bulkIndex`` in the other SDKs (clients/conformance pins all three).
"""

from __future__ import annotations

import json
import threading
import urllib.parse
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Iterable, Iterator

from .transport import KoshaRequestError, json_default

DEFAULT_PAGE_SIZE = 500
_ID_ASC = {"_id": {"order": "asc"}}


class KoshaConfigError(ValueError):
    """Bad arguments for a helper; nothing was sent."""


class BulkError(KoshaRequestError):
    """A ``bulk_index`` that stopped on a failed batch.

    ``result`` is what was done: ``{"indexed", "batches", "failures"}``.
    """

    def __init__(self, result: dict):
        first = result["failures"][0]
        cause = first["error"]
        info = getattr(cause, "info", None) or {}
        super().__init__(
            getattr(cause, "status_code", 0),
            f"bulk index stopped: batch {first['batch_index']} failed: {cause}",
            info,
        )
        self.attempts = getattr(cause, "attempts", 1)
        self.result = result


def ns_path(namespace: str, action: str | None = None) -> str:
    path = "v1/namespaces/" + urllib.parse.quote(str(namespace), safe="")
    return f"{path}/{action}" if action else path


def _sort_key(entry: Any) -> str:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict) and len(entry) == 1:
        return next(iter(entry))
    raise KoshaConfigError(f"search_all: unsupported sort entry {entry!r}")


def paged_request(body: dict, page_size: int) -> tuple[dict, list[str]]:
    """The body search_all sends for each page, and its sort keys."""
    if "from" in body:
        raise KoshaConfigError("search_all pages with search_after; do not set from")
    if "search_after" in body:
        raise KoshaConfigError("search_all sets search_after itself")
    if "rerank" in body:
        raise KoshaConfigError("search_all cannot page a reranked search")
    if "knn" in body or "rank" in body or body.get("mode") in ("semantic", "hybrid"):
        raise KoshaConfigError("search_all cannot page a kNN or hybrid search")
    if not isinstance(page_size, int) or page_size < 1:
        raise KoshaConfigError("page_size must be a positive integer")
    sort = [{e: {"order": "asc"}} if isinstance(e, str) else dict(e) for e in body.get("sort") or []]
    keys = [_sort_key(e) for e in sort]
    if not keys or keys[-1] != "_id":
        sort.append(dict(_ID_ASC))
        keys.append("_id")
    out = {**body, "sort": sort, "max_results": page_size}
    if body.get("source_includes") is not None:
        include = list(body["source_includes"])
        for k in keys:
            if k not in ("_id", "_score") and k not in include:
                include.append(k)
        out["source_includes"] = include
    return out, keys


def _score_string(score: Any) -> str:
    if isinstance(score, float) and score.is_integer():
        return str(int(score))
    return repr(score) if isinstance(score, float) else str(score)


def cursor_for(hit: dict, keys: list[str]) -> list[str]:
    """The search_after cursor after ``hit`` (the kosha-cli rule)."""
    sort_values = hit.get("sort")
    if isinstance(sort_values, list) and len(sort_values) == len(keys):
        return [str(v) for v in sort_values]
    out = []
    for k in keys:
        if k == "_id":
            out.append(hit.get("doc_id") or "")
        elif k == "_score":
            out.append(_score_string(hit.get("score") or 0))
        else:
            out.append(next((f.get("value", "") for f in hit.get("fields") or [] if f.get("name") == k), ""))
    return out


def search_all(transport: Any, namespace: str, body: dict, page_size: int | None = None) -> Iterator[dict]:
    """Yield every hit of ``body`` page by page; see ``KoshaClient.search_all``."""
    if page_size is None:
        page_size = int(body["max_results"]) if body.get("max_results") is not None else DEFAULT_PAGE_SIZE
    request, keys = paged_request(body, page_size)
    path = ns_path(namespace, "search")
    cursor: list[str] | None = None
    while True:
        page = transport.request("POST", path, {**request, "search_after": cursor} if cursor else request)
        hits = page.get("results") or []
        yield from hits
        if len(hits) < page_size:
            return
        nxt = cursor_for(hits[-1], keys)
        if cursor is not None and nxt == cursor:
            raise KoshaConfigError("search_all: the cursor did not advance (is a sort field stored and returned?)")
        cursor = nxt


def bulk_index(
    transport: Any,
    namespace: str,
    documents: Iterable[dict],
    max_docs_per_batch: int = 500,
    max_bytes_per_batch: int = 8 * 1024 * 1024,
    concurrency: int = 2,
    on_error: str = "stop",
) -> dict:
    """Index ``documents`` in batches; see ``KoshaClient.bulk_index``."""
    if on_error not in ("stop", "continue"):
        raise KoshaConfigError("on_error must be 'stop' or 'continue'")
    path = ns_path(namespace, "documents")
    result: dict = {"indexed": 0, "batches": 0, "failures": []}
    lock = threading.Lock()
    state = {"stopped": False}
    inflight: dict[Future, set] = {}
    current: dict[str, tuple[dict, int]] = {}  # insertion-ordered; last occurrence wins
    current_bytes = 0

    def send(batch_index: int, docs: list[dict], ids: list[str]) -> None:
        try:
            res = transport.request("POST", path, {"documents": docs})
        except Exception as e:  # noqa: BLE001 - reported per batch
            with lock:
                result["failures"].append({"batch_index": batch_index, "ids": ids, "error": e})
                if on_error == "stop":
                    state["stopped"] = True
            return
        count = res.get("indexed_count") if isinstance(res, dict) else None
        with lock:
            result["indexed"] += count if isinstance(count, int) else len(docs)

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:

        def dispatch() -> None:
            nonlocal current, current_bytes
            batch, current, current_bytes = current, {}, 0
            while len(inflight) >= max(1, concurrency):
                done, _ = wait(list(inflight), return_when=FIRST_COMPLETED)
                for f in done:
                    inflight.pop(f, None)
            with lock:
                if state["stopped"] or not batch:
                    return
                index = result["batches"]
                result["batches"] += 1
            ids = list(batch)
            fut = pool.submit(send, index, [d for d, _ in batch.values()], ids)
            inflight[fut] = set(ids)

        for doc in documents:
            with lock:
                if state["stopped"]:
                    break
            doc_id = doc["id"]
            size = len(json.dumps(doc, default=json_default, separators=(",", ":"), ensure_ascii=False).encode()) + 1
            if doc_id in current:
                current_bytes += size - current[doc_id][1]
                current[doc_id] = (doc, size)
                continue
            if current and (len(current) >= max_docs_per_batch or current_bytes + size > max_bytes_per_batch):
                dispatch()
            # A later write to an id still in flight waits for it, so the
            # later occurrence always wins.
            blockers = [f for f, ids in inflight.items() if doc_id in ids and not f.done()]
            if blockers:
                if current:
                    dispatch()
                wait(blockers)
                with lock:
                    if state["stopped"]:
                        break
            current[doc_id] = (doc, size)
            current_bytes += size
        with lock:
            stopped = state["stopped"]
        if not stopped and current:
            dispatch()
        wait(list(inflight))

    result["failures"].sort(key=lambda f: f["batch_index"])
    if result["failures"] and on_error == "stop":
        raise BulkError(result)
    return result
