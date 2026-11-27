"""Agent tools over the native /v1 API (#584).

``AgentTools`` is the thin SDK behind the ``kosha-mcp`` server: one method
per tool an agent calls (``search``, ``semantic_search``, ``filter``,
``count``, ``aggregate``, ``branch``, ``list_branches``, ``index_documents``),
each taking plain JSON-able arguments and returning plain dicts sized for a
model's context window. Every hit carries its stable ``id`` and its ``path``
(when the document has one), so the agent can filter or read it next.
"""

from __future__ import annotations

import json
import os
from typing import Any

from .native import KoshaConfigError, ns_path

DEFAULT_MAX_RESULTS = 10
MAX_RESULTS_CAP = 100
DEFAULT_SNIPPET_CHARS = 500
DEFAULT_PATH_FIELD = "path"


class AgentTools:
    """Budgeted agent-facing calls against one Kosha server.

    ``client`` is a ``KoshaClient`` (built from ``KOSHA_HOST`` /
    ``KOSHA_API_KEY`` when omitted). ``namespace`` is used when a call names
    none (default ``KOSHA_NAMESPACE``). Hits are capped at
    ``max_results_cap`` per call and each stored string at ``snippet_chars``
    characters; vectors are never returned.
    """

    def __init__(
        self,
        client: Any = None,
        namespace: str | None = None,
        max_results_cap: int = MAX_RESULTS_CAP,
        snippet_chars: int = DEFAULT_SNIPPET_CHARS,
        path_field: str = DEFAULT_PATH_FIELD,
    ) -> None:
        if client is None:
            from .client import KoshaClient

            client = KoshaClient(api_key=os.environ.get("KOSHA_API_KEY"))
        self._transport = client._transport
        self.namespace = namespace or os.environ.get("KOSHA_NAMESPACE") or None
        self.max_results_cap = max_results_cap
        self.snippet_chars = snippet_chars
        self.path_field = path_field

    # ── Retrieval ─────────────────────────────────────────────────────────

    def search(
        self,
        query: str,
        namespace: str | None = None,
        filter: dict | None = None,
        fields: list[str] | None = None,
        max_results: int = DEFAULT_MAX_RESULTS,
        include_fields: list[str] | None = None,
    ) -> dict:
        """BM25 search for ``query``, optionally restricted by ``filter`` and
        to the Text ``fields`` named."""
        if not query:
            raise KoshaConfigError("search: `query` must be non-empty; use filter() for no query")
        body: dict[str, Any] = {"query_text": query}
        if fields:
            body["fields"] = list(fields)
        return self._hits(namespace, body, filter, max_results, include_fields)

    def semantic_search(
        self,
        field: str,
        query: str | None = None,
        vector: list[float] | None = None,
        namespace: str | None = None,
        filter: dict | None = None,
        k: int = DEFAULT_MAX_RESULTS,
        hybrid: bool = False,
        include_fields: list[str] | None = None,
    ) -> dict:
        """Nearest neighbours on the Vector ``field``, for ``vector`` or for
        ``query`` embedded by the field's server-side embedder. ``hybrid``
        also runs ``query`` as BM25 and fuses both lists with RRF."""
        if hybrid and not query:
            raise KoshaConfigError("semantic_search: `hybrid` needs `query`")
        if not hybrid and (query is None) == (vector is None):
            raise KoshaConfigError("semantic_search: set exactly one of `query` and `vector`")
        k = self._cap(k)
        knn: dict[str, Any] = {"field": field, "k": k}
        if vector is not None:
            knn["vector"] = [float(x) for x in vector]
        else:
            knn["query_text"] = query
        body: dict[str, Any] = {
            "query_text": query if hybrid else "",
            "knn": knn,
            "mode": "hybrid" if hybrid else "semantic",
        }
        return self._hits(namespace, body, filter, k, include_fields)

    def filter(
        self,
        filter: dict,
        namespace: str | None = None,
        sort: list | None = None,
        max_results: int = DEFAULT_MAX_RESULTS,
        search_after: list | None = None,
        include_fields: list[str] | None = None,
    ) -> dict:
        """Documents matching ``filter`` (term/terms/range/prefix/glob/bool/
        exists/...), with no scoring. ``sort`` and ``search_after`` page."""
        if not isinstance(filter, dict) or not filter:
            raise KoshaConfigError("filter: `filter` must be a non-empty object")
        body: dict[str, Any] = {"query_text": ""}
        if sort:
            body["sort"] = list(sort)
        if search_after:
            body["search_after"] = [str(v) for v in search_after]
        return self._hits(namespace, body, filter, max_results, include_fields)

    def count(
        self,
        filter: dict | None = None,
        query: str = "",
        namespace: str | None = None,
    ) -> dict:
        """The exact number of documents matching ``query`` and ``filter``
        (every live document when both are empty)."""
        body = self._body(query, filter or _match_all(query), 0)
        body["exact_total_hits"] = True
        res = self._post(namespace, "search", body)
        return {"count": res.get("total_hits", 0), "relation": res.get("total_hits_relation", "eq")}

    def aggregate(
        self,
        aggs: dict,
        filter: dict | None = None,
        query: str = "",
        namespace: str | None = None,
    ) -> dict:
        """Aggregations (``terms``, ``range``, ``date_histogram``, ``stats``,
        ...) over the documents matching ``query`` and ``filter``."""
        if not isinstance(aggs, dict) or not aggs:
            raise KoshaConfigError("aggregate: `aggs` must be a non-empty object")
        body = self._body(query, filter or _match_all(query), 0)
        body["aggs"] = aggs
        res = self._post(namespace, "search", body)
        return {
            "aggregations": res.get("aggregations") or {},
            "total_hits": res.get("total_hits", 0),
            "total_hits_relation": res.get("total_hits_relation", "eq"),
        }

    # ── Branches and writes ───────────────────────────────────────────────

    def branch(self, name: str, namespace: str | None = None) -> dict:
        """A copy-on-write branch of the namespace, named ``name``: a new
        namespace to write to without changing the parent (needs an
        ``admin`` key)."""
        return self._post(namespace, "branch", {"name": name})

    def list_branches(self, namespace: str | None = None) -> dict:
        """The namespace's branches."""
        return self._transport.request("GET", ns_path(self._ns(namespace), "branches"))

    def index_documents(
        self,
        documents: list[dict],
        namespace: str | None = None,
        keyword_fields: list[str] | None = None,
        flush: bool = True,
    ) -> dict:
        """Upsert ``documents``, each ``{"id": ..., "fields": {name: value}}``.
        Strings are Text unless named in ``keyword_fields`` (the path field
        always is); numbers, booleans and numeric lists (vectors) are typed
        from their value. ``flush`` makes them searchable before returning."""
        if not documents:
            raise KoshaConfigError("index_documents: `documents` must be non-empty")
        keywords = set(keyword_fields or ()) | {self.path_field}
        docs = []
        for doc in documents:
            if not isinstance(doc, dict) or not doc.get("id") or not isinstance(doc.get("fields"), dict):
                raise KoshaConfigError(
                    'index_documents: every document is {"id": "...", "fields": {...}}'
                )
            docs.append(
                {
                    "id": str(doc["id"]),
                    "fields": [_encode_field(k, v, k in keywords) for k, v in doc["fields"].items()],
                }
            )
        ns = self._ns(namespace)
        res = self._post(ns, "documents", {"documents": docs})
        if flush:
            self._post(ns, "flush", {})
        return {"indexed": res.get("indexed_count", len(docs)), "flushed": flush}

    # ── Internals ─────────────────────────────────────────────────────────

    def _ns(self, namespace: str | None) -> str:
        ns = namespace or self.namespace
        if not ns:
            raise KoshaConfigError("no namespace: pass `namespace` or set KOSHA_NAMESPACE")
        return ns

    def _post(self, namespace: str | None, action: str, body: dict) -> dict:
        return self._transport.request("POST", ns_path(self._ns(namespace), action), body)

    def _cap(self, n: int) -> int:
        return max(1, min(int(n), self.max_results_cap))

    def _body(self, query: str, filter: dict | None, max_results: int) -> dict:
        body: dict[str, Any] = {"query_text": query or "", "max_results": max_results}
        if filter:
            body["filter"] = _normalize_filter(filter)
        return body

    def _hits(
        self,
        namespace: str | None,
        body: dict,
        filter: dict | None,
        max_results: int,
        include_fields: list[str] | None,
    ) -> dict:
        body = {**self._body(body.get("query_text", ""), filter, self._cap(max_results)), **body}
        if include_fields:
            body["source_includes"] = sorted(set(include_fields) | {self.path_field})
        res = self._post(namespace, "search", body)
        return {
            "total_hits": res.get("total_hits", 0),
            "total_hits_relation": res.get("total_hits_relation", "eq"),
            "hits": [self._hit(h) for h in res.get("results", [])],
        }

    def _hit(self, raw: dict) -> dict:
        hit: dict[str, Any] = {"id": raw.get("doc_id", ""), "score": raw.get("score", 0.0)}
        fields: dict[str, Any] = {}
        truncated: list[str] = []
        for f in raw.get("fields", []):
            name, kind, value = f.get("name"), f.get("field_type"), f.get("value", "")
            if kind == "Vector":
                continue
            if name == self.path_field:
                hit["path"] = value
                continue
            if isinstance(value, str) and len(value) > self.snippet_chars:
                value = value[: self.snippet_chars]
                truncated.append(name)
            fields[name] = _decode(value, kind)
        hit["fields"] = fields
        if truncated:
            hit["truncated_fields"] = truncated
        if raw.get("highlight"):
            hit["highlight"] = raw["highlight"]
        return hit


def _match_all(query: str) -> dict | None:
    # With no query text, a search matches only what a filter selects.
    return None if query else {"match_all": {}}


_VALUE_CLAUSES = ("term", "terms", "range", "prefix")


def _normalize_filter(clause: Any) -> Any:
    """Kosha filter values are strings; models write ``{"range": {"year":
    {"gte": 2024}}}``. Numbers inside value clauses become strings."""
    if isinstance(clause, list):
        return [_normalize_filter(c) for c in clause]
    if not isinstance(clause, dict):
        return clause
    return {
        k: _stringify_numbers(v) if k in _VALUE_CLAUSES else _normalize_filter(v)
        for k, v in clause.items()
    }


def _stringify_numbers(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return [_stringify_numbers(v) for v in value]
    if isinstance(value, dict):
        return {k: _stringify_numbers(v) for k, v in value.items()}
    return value


def _encode_field(name: str, value: Any, keyword: bool) -> dict:
    if isinstance(value, bool):
        kind, text = "Boolean", "true" if value else "false"
    elif isinstance(value, int):
        kind, text = "Integer", str(value)
    elif isinstance(value, float):
        kind, text = "Float", repr(value)
    elif isinstance(value, str):
        kind, text = ("Keyword" if keyword else "Text"), value
    elif isinstance(value, (list, tuple)) and value and all(
        isinstance(x, (int, float)) and not isinstance(x, bool) for x in value
    ):
        kind, text = "Vector", json.dumps([float(x) for x in value])
    else:
        raise KoshaConfigError(
            f"index_documents: field {name!r} must be a string, number, boolean or numeric vector"
        )
    return {"name": name, "field_type": kind, "value": text}


def _decode(value: str, kind: str | None) -> Any:
    try:
        if kind == "Integer":
            return int(value)
        if kind == "Float":
            return float(value)
    except ValueError:
        return value
    if kind == "Boolean":
        return value == "true"
    return value
