"""KoshaClient — an OpenSearch-compatible client that talks to Kosha."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Sequence

from .compat import Transport as CompatTransport
from .transport import KoshaRequestError, Transport, json_default

logger = logging.getLogger(__name__)


# ─── Public interface ──────────────────────────────────────────────────────


class KoshaClient:
    """Drop-in replacement for ``opensearchpy.OpenSearch``.

    Translates the most common OpenSearch / Elasticsearch operations into
    Kosha HTTP API calls and returns response dicts in the same shape as
    the OpenSearch JSON API so that ``opensearch_dsl`` wrappers
    (``Search``, ``Q``, ``Document.save``, ``helpers.bulk``) work unchanged.

    Phase 1 focus — BM25 lexical search only:

    * ``search()``  — ``{"query": {"match": ...}}``,
                      ``{"query": {"bool": {"must": [...], "filter": [...]}}}``
    * ``index()``   — single-document index
    * ``bulk()``    — multi-document index via ``opensearchpy.helpers.bulk``
    * ``count()``   — document count
    * ``update()``  — update by id
    * ``update_by_query()`` — synchronous or task-polled scripted updates
    * ``delete_by_query()``  — delete matching documents
    """

    def __init__(
        self,
        hosts: Any = None,
        http_auth: Any = None,
        api_key: str | None = None,
        timeout: int = 60,
        max_retries: int = 3,
        retry_on_timeout: bool = True,
        pool_maxsize: int = 20,
        **kwargs: Any,
    ) -> None:
        # Normalise the Kosha base URL.  Resolution order:
        #   1. ``hosts`` arg (opensearchpy-compatible)
        #   2. ``KOSHA_HOST`` env var (customer-friendly)
        #   3. Fallback to localhost:8080 (dev)
        import os as _os

        if hosts is None:
            hosts = _os.environ.get("KOSHA_HOST") or kwargs.get("kosha_url")
        if isinstance(hosts, str):
            kosha_url = hosts.rstrip("/")
        elif isinstance(hosts, (list, tuple)) and len(hosts) > 0:
            h = hosts[0]
            if isinstance(h, str):
                kosha_url = h.rstrip("/")
            elif isinstance(h, dict):
                scheme = h.get("scheme", "http")
                host = h.get("host", "localhost")
                port = h.get("port", 8080)
                kosha_url = f"{scheme}://{host}:{port}"
            else:
                kosha_url = "http://localhost:8080"
        else:
            kosha_url = kwargs.get("kosha_url", "http://localhost:8080")

        # Resolve API key: explicit arg > http_auth tuple > env var.
        resolved_api_key = api_key
        if resolved_api_key is None and http_auth is not None:
            if isinstance(http_auth, tuple) and len(http_auth) == 2:
                resolved_api_key = http_auth[1]  # (user, key) backward compat
            elif isinstance(http_auth, str):
                resolved_api_key = http_auth
        if resolved_api_key is None:
            resolved_api_key = _os.environ.get("KOSHA_API_KEY")

        # Use the v1 proto-defined path template for all requests.
        self._transport = Transport(
            base_url=kosha_url,
            api_key=resolved_api_key,
            timeout=timeout,
            max_retries=max_retries,
            retry_on_timeout=retry_on_timeout,
        )

        self._kosha_url = kosha_url
        self._auth = http_auth
        self._timeout = timeout

        # Kosha namespace → index name mapping.
        # In Phase 1, index name is used directly as the namespace.
        self._namespace = kwargs.get("namespace", "default")

        # Duck-typed transport so opensearchpy.helpers.bulk() can chunk
        # actions before delegating to self.bulk().
        self.transport = CompatTransport()
        self._tasks: dict[str, dict] = {}
        self._tasks_lock = threading.Lock()

        logger.info(
            "KoshaClient targeting %s namespace=%s api_key=%s",
            kosha_url,
            self._namespace,
            bool(resolved_api_key),
        )

    # ── Low-level request helpers ──────────────────────────────────────────

    def _request(self, method: str, path: str, body: Any = None) -> Any:
        """Legacy request helper — delegates to the Transport layer.

        Paths here are relative (no ``/v1/namespaces/...`` prefix); they are
        appended directly to the base URL.  Used by the legacy Phase 1 routes.
        """
        return self._transport.request(method, path, body)

    def _v1_request(
        self, method: str, namespace: str, action: str, body: Any = None
    ) -> Any:
        """Request against a v1 proto-defined path.

        Builds ``/v1/namespaces/{namespace}/{action}`` automatically.
        """
        path = f"v1/namespaces/{namespace}/{action}"
        return self._transport.request(method, path, body)

    # ── Search ─────────────────────────────────────────────────────────────

    def _resolve_ns(self, index):
        """Resolve namespace from opensearch_dsl index parameter (can be list)."""
        ns = index or self._namespace
        if isinstance(ns, (list, tuple)):
            ns = ns[0] if ns else self._namespace
        if ns is None:
            ns = "default"
        return str(ns)

    def search(
        self, index: str | None = None, body: dict | None = None, **params: Any
    ) -> dict:
        ns = self._resolve_ns(index)

        query_text = self._extract_query_text(body) if body else ""
        size = body.get("size", 10) if body else 10
        from_ = body.get("from", 0) if body else 0
        filter_clause = self._extract_filter(body)
        aggs = self._extract_aggs(body)
        wildcard = self._extract_wildcard(body)
        match_phrase = self._extract_match_phrase(body)
        knn = self._extract_knn(body)
        sort = self._extract_sort(body)
        search_after = body.get("search_after") if body else None
        if search_after is not None:
            search_after = [str(v) for v in search_after]

        # Determine if we need POST (agg/wildcard/phrase/filter/knn/sort/cursor).
        needs_post = bool(
            filter_clause
            or aggs
            or wildcard
            or match_phrase
            or knn
            or sort
            or search_after
        )

        if not needs_post:
            bm25_params = {}
            q = body and (body.get("query") or {})
            if q:
                bm25_params = self._extract_bm25_params(q)
            query_params = {"ns": ns, "q": query_text, "max_results": str(size + from_)}
            if bm25_params:
                query_params.update(bm25_params)
            url = f"{self._kosha_url}/search?{urllib.parse.urlencode(query_params)}"
            try:
                req = urllib.request.Request(url)
                if self._transport.api_key:
                    req.add_header("Authorization", f"Bearer {self._transport.api_key}")
                elif self._auth:
                    import base64

                    user, pwd = self._auth
                    token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
                    req.add_header("Authorization", f"Basic {token}")
                start = time.monotonic()
                resp = urllib.request.urlopen(req, timeout=self._timeout)
                took_ms = int((time.monotonic() - start) * 1000)
                result = json.loads(resp.read().decode())
            except urllib.error.HTTPError as e:
                took_ms = 0
                if e.code == 404:
                    return self._build_search_response([], from_, size, took_ms)
                body_bytes = e.read()
                try:
                    err = json.loads(body_bytes.decode())
                except json.JSONDecodeError:
                    err = {"error": body_bytes.decode()}
                raise KoshaRequestError(e.code, err.get("error", str(e)), err) from e
            kosha_hits = result.get("results", [])
            total = result.get("total_hits", 0)
            return self._build_search_response(kosha_hits, from_, size, took_ms, total)

        # search_after replaces from (OpenSearch semantics).
        kosha_body = {
            "namespace": ns,
            "query_text": query_text,
            "max_results": size if search_after else size + from_,
            "from": 0 if search_after else from_,
        }
        if filter_clause:
            kosha_body["filter"] = filter_clause
        if aggs:
            kosha_body["aggs"] = aggs
        if wildcard:
            kosha_body["wildcard"] = wildcard
        if match_phrase:
            kosha_body["match_phrase"] = match_phrase
        if knn:
            kosha_body["knn"] = knn
        if sort:
            kosha_body["sort"] = sort
        if search_after:
            kosha_body["search_after"] = search_after

        try:
            result = self._request("POST", "search", body=kosha_body)
        except KoshaRequestError as e:
            if e.status_code == 404:
                return self._build_search_response([], 0, size, 0)
            raise
        kosha_hits = result.get("results", [])
        total = result.get("total_hits", 0)
        kosha_aggs = result.get("aggregations")

        # Server already applied from/search_after/size — do not re-slice.
        response = self._build_search_response(
            kosha_hits, 0, len(kosha_hits) or size, 0, total
        )
        if kosha_aggs:
            response["aggregations"] = kosha_aggs
        return response

    def _build_search_response(
        self,
        kosha_hits: list[dict],
        from_: int,
        size: int,
        took_ms: int,
        total: int | None = None,
    ) -> dict:
        if total is None:
            total = len(kosha_hits)

        hits = []
        for k_hit in kosha_hits:
            doc_id = k_hit.get("doc_id", "")
            score = k_hit.get("score", 0.0)
            source = {}
            for field in k_hit.get("fields", []):
                source[field["name"]] = self._decode_field_value(
                    field.get("value", ""), field.get("field_type")
                )
                # Store field type for filter-aware operations.
                if field.get("field_type") not in ("Text", None):
                    source[f"__type__{field['name']}"] = field["field_type"]
            hits.append(
                {
                    "_index": self._namespace,
                    "_id": doc_id,
                    "_score": score,
                    "_source": source,
                }
            )

        # Apply offset/pagination in Python (Kosha returns flat top-N).
        page = hits[from_ : from_ + size]

        return {
            "took": took_ms,
            "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": total, "relation": "eq"},
                "max_score": max((h["_score"] for h in page), default=0.0),
                "hits": page,
            },
        }

    @staticmethod
    def _extract_query_text(body: dict | None) -> str:
        """Extract the user's query text from an OpenSearch query body."""
        if not body:
            return ""
        query = body.get("query") or {}
        return KoshaClient._extract_from_query_dsl(query)

    @staticmethod
    def _extract_from_query_dsl(query: dict) -> str:
        """Recursively extract search terms from query DSL."""
        if not query:
            return ""

        # match: {"match": {"field": "text"}} or {"match": {"field": {"query": "text"}}}
        match = query.get("match")
        if match is not None:
            for field, val in match.items():
                if isinstance(val, str):
                    return val
                if isinstance(val, dict) and "query" in val:
                    return val["query"]
            return ""

        # multi_match: {"multi_match": {"query": "text", "fields": [...]}}
        multi_match = query.get("multi_match")
        if multi_match is not None:
            return multi_match.get("query", "")

        # bool: {"bool": {"must": [...], "should": [...], "filter": [...]}}
        # Only extract text from must/should (full-text clauses).
        # Filter clauses (term, terms, range) are not query text.
        bool_q = query.get("bool")
        if bool_q is not None:
            texts = []
            for clause_key in ("must", "should"):
                for clause in bool_q.get(clause_key, []):
                    if not KoshaClient._is_filter_only_clause(clause):
                        t = KoshaClient._extract_from_query_dsl(clause)
                        if t:
                            texts.append(t)
            return " ".join(texts)

        # term: {"term": {"field": "value"}} — exact match, not full-text.
        # We return empty because Kosha does BM25, not term queries (Phase 1).
        term = query.get("term")
        if term is not None:
            for field, val in term.items():
                if isinstance(val, str):
                    return val
            return ""

        # match_all
        if query.get("match_all") is not None:
            return ""

        # function_score: {"function_score": {"query": {...}, ...}}
        fn_score = query.get("function_score")
        if fn_score is not None:
            return KoshaClient._extract_from_query_dsl(fn_score.get("query", {}))

        return ""

    @staticmethod
    def _extract_bm25_params(query: dict) -> dict:
        """Extract BM25 tuning parameters if present in the query body."""
        params = {}
        if "settings" in query:
            sim = (
                query["settings"]
                .get("index", {})
                .get("similarity", {})
                .get("default", {})
            )
            if sim.get("type") == "BM25":
                k1 = sim.get("k1", 1.2)
                b = sim.get("b", 0.75)
                params["k1"] = str(k1)
                params["b"] = str(b)
        return params

    @staticmethod
    def _extract_filter(body: dict | None) -> dict | None:
        """Extract a filter clause from an OpenSearch query body.

        Handles:
        - ``body["query"]["bool"]["filter"]`` — ES bool filter clauses
        - ``body["post_filter"]`` — ES post_filter
        - ``body["query"]["bool"]["must_not"]`` — ES bool must_not
        """
        if not body:
            return None

        # Collect filter clauses from various locations.
        must_clauses: list = []
        must_not_clauses: list = []
        should_clauses: list = []
        has_clauses = False

        query = body.get("query") or {}

        # A filter-only clause may be the query itself, as in
        # {"query": {"terms": {"documentId": [...]}}}.
        if KoshaClient._is_filter_only_clause(query):
            translated = KoshaClient._translate_es_clause(query)
            if translated:
                return translated

        # bool.filter clauses
        bool_q = query.get("bool")
        if bool_q:
            for clause in bool_q.get("filter", []):
                translated = KoshaClient._translate_es_clause(clause)
                if translated:
                    must_clauses.append(translated)
                    has_clauses = True
            for clause in bool_q.get("must_not", []):
                translated = KoshaClient._translate_es_clause(clause)
                if translated:
                    must_not_clauses.append(translated)
                    has_clauses = True
            for clause in bool_q.get("must", []):
                # Only translate term/terms/range in must clauses
                # (match clauses are handled by _extract_query_text).
                translated = KoshaClient._translate_es_clause(clause)
                if translated and KoshaClient._is_filter_only_clause(clause):
                    must_clauses.append(translated)
                    has_clauses = True
            for clause in bool_q.get("should", []):
                translated = KoshaClient._translate_es_clause(clause)
                if translated and KoshaClient._is_filter_only_clause(clause):
                    must_clauses.append(translated)
                    has_clauses = True

        # post_filter
        post_filter = body.get("post_filter")
        if post_filter:
            translated = KoshaClient._translate_es_clause(post_filter)
            if translated:
                must_clauses.append(translated)
                has_clauses = True

        if not has_clauses:
            return None

        result: dict = {}
        if must_clauses:
            result["bool"] = result.get("bool", {})
            result["bool"]["must"] = must_clauses
        if must_not_clauses:
            result["bool"] = result.get("bool", {})
            result["bool"]["must_not"] = must_not_clauses

        return result if result else None

    @staticmethod
    def _is_filter_only_clause(clause: dict) -> bool:
        """Check if a clause is a filter-only clause (not a match query)."""
        if not clause:
            return False
        if "term" in clause or "terms" in clause or "range" in clause:
            return True
        if "exists" in clause or "prefix" in clause or "wildcard" in clause:
            return True
        if "match_all" in clause:
            return True
        return False

    @staticmethod
    def _translate_es_clause(clause: dict) -> dict | None:
        """Translate a single ES filter clause to Kosha format."""
        if not clause:
            return None

        # term: {"term": {"matterId": "value"}}
        if "term" in clause:
            return clause  # Kosha accepts the same format

        # terms: {"terms": {"documentId": ["v1", "v2"]}}
        if "terms" in clause:
            return clause  # Kosha accepts the same format

        # range: {"range": {"sentAt": {"gte": "...", "lte": "..."}}}
        if "range" in clause:
            # Normalize range values to strings for Kosha.
            bounds = {}
            for field, val in clause["range"].items():
                if isinstance(val, dict):
                    bounds[field] = {k: str(v) for k, v in val.items()}
                else:
                    bounds[field] = val
            return {"range": bounds}

        # bool: nested bool (recursive)
        if "bool" in clause:
            return clause  # Kosha accepts the same format

        # match_all: {"match_all": {}}
        if "match_all" in clause:
            return clause

        # exists: {"exists": {"field": "..."}}
        if "exists" in clause:
            # Phase 1: skip exists (not supported yet).
            return None

        return None

    # ── Index ──────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_aggs(body: dict | None) -> dict | None:
        """Extract aggregations from an ES query body."""
        if not body:
            return None
        aggs = body.get("aggs") or body.get("aggregations")
        if aggs:
            return aggs  # Kosha accepts the same format as ES
        return None

    @staticmethod
    def _extract_wildcard(body: dict | None) -> dict | None:
        """Extract wildcard query from an ES query body.
        Converts ES format to Kosha format.
        """
        if not body:
            return None
        query = body.get("query") or {}
        wc = query.get("wildcard")
        if not wc:
            return None
        # ES: {"wildcard": {"field": {"value": "*Smith*", "case_insensitive": true}}}
        for field, spec in wc.items():
            if isinstance(spec, dict):
                return {
                    "field": field,
                    "pattern": spec.get("value", spec.get("wildcard", "")),
                    "case_insensitive": spec.get("case_insensitive", True),
                }
            return {
                "field": field,
                "pattern": spec,
                "case_insensitive": True,
            }
        return None

    @staticmethod
    def _extract_match_phrase(body: dict | None) -> dict | None:
        """Extract match_phrase query from an ES query body.
        Converts ES format to Kosha format.
        """
        if not body:
            return None
        query = body.get("query") or {}
        mp = query.get("match_phrase")
        if not mp:
            return None
        # ES: {"match_phrase": {"field": {"query": "phrase text", "slop": 2}}}
        for field, spec in mp.items():
            if isinstance(spec, dict):
                return {
                    "field": field,
                    "phrase": spec.get("query", ""),
                    "slop": spec.get("slop", 0),
                }
            return {
                "field": field,
                "phrase": spec,
                "slop": 0,
            }
        return None

    @staticmethod
    def _extract_sort(body: dict | None) -> list[dict] | None:
        """Translate OpenSearch ``sort`` into Kosha ``SortSpec`` objects.

        Accepts ``[{"_id": "asc"}]`` or ``[{"_id": {"order": "asc"}}]``.
        """
        if not body:
            return None
        raw = body.get("sort")
        if not raw:
            return None
        out: list[dict] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            spec: dict[str, dict[str, str]] = {}
            for field, val in entry.items():
                if isinstance(val, str):
                    spec[field] = {"order": val}
                elif isinstance(val, dict):
                    spec[field] = {"order": str(val.get("order", "asc"))}
                else:
                    spec[field] = {"order": "asc"}
            if spec:
                out.append(spec)
        return out or None

    @staticmethod
    def _extract_knn(body: dict | None) -> dict | None:
        """Extract kNN query from an ES query body."""
        if not body:
            return None
        # ES: {"knn": {"field": {"vector": [...], "k": N}}}
        # or embedded: {"query": {"knn": {...}}}
        knn = body.get("knn") or (body.get("query") or {}).get("knn")
        if not knn:
            return None
        for field, spec in knn.items():
            if isinstance(spec, dict):
                return {
                    "field": field,
                    "vector": spec.get("vector", []),
                    "k": spec.get("k", 10),
                    "num_candidates": spec.get("num_candidates", 100),
                }
        return None

    @staticmethod
    def _field_to_kosha(name: str, value: str, field_type: str = "Text") -> dict:
        return {"name": name, "field_type": field_type, "value": value}

    # Below this length, a numeric list is treated as an incidental tuple
    # (coordinates, RGB, lat/long, ...) rather than a semantic embedding —
    # shape-based, so no caller's field names need to be known here. Every
    # embedding model in production use (OpenAI, Azure OpenAI, Cohere,
    # Mistral, Gemini) outputs at least a few hundred dimensions; incidental
    # numeric tuples are never more than a handful. Kosha's Vector storage
    # (``vector.idx``) is shared across every Vector field in a segment, so
    # mixing a real embedding with a low-dim tuple corrupts it (dimension
    # mismatch -> read_f32_le panics on retrieval) and forces lexical-only
    # queries to rebuild an HNSW graph they never needed.
    _MIN_VECTOR_DIM = 8

    @staticmethod
    def _decode_field_value(value: str, field_type: str | None) -> Any:
        """Reverse of ``_field_to_kosha``.

        Kosha always returns field values as strings on the wire
        (server-side ``Field.value: String``), regardless of field_type.
        Callers index straight into these (``hit.bottom_left[0]``,
        ``if hit.isHot``), so they must come back as the original Python
        type, not the raw string. ``Keyword`` is used exclusively (by this
        client) for JSON-encoded lists that aren't Vector-worthy, so it
        decodes the same way as Vector.
        """
        if field_type == "Float":
            try:
                return float(value)
            except (TypeError, ValueError):
                return value
        if field_type == "Boolean":
            return value == "true"
        if field_type in ("Vector", "Keyword"):
            try:
                return json.loads(value)
            except (TypeError, ValueError, json.JSONDecodeError):
                return value
        return value

    @classmethod
    def _source_to_fields(cls, source: dict | None) -> list[dict]:
        """Convert an OpenSearch ``_source`` / ``doc`` body into Kosha fields.

        Handles scalar types and float/int vectors (``contentEmbedding``).
        Unwraps ``{"doc": {...}}`` update payloads from ``helpers.bulk``.
        """
        if not source:
            return []
        if "doc" in source and isinstance(source.get("doc"), dict):
            # Update actions wrap the partial doc; ignore meta keys.
            source = source["doc"]

        fields: list[dict] = []
        for k, v in source.items():
            if k.startswith("__type__"):
                continue
            declared_type = source.get(f"__type__{k}")
            if isinstance(v, str):
                field_type = (
                    declared_type if declared_type in ("Text", "Keyword") else "Text"
                )
                fields.append(cls._field_to_kosha(k, v, field_type))
            elif isinstance(v, bool):
                fields.append(cls._field_to_kosha(k, str(v).lower(), "Boolean"))
            elif isinstance(v, (int, float)):
                fields.append(cls._field_to_kosha(k, str(v), "Float"))
            elif (
                isinstance(v, (list, tuple))
                and len(v) >= cls._MIN_VECTOR_DIM
                and all(
                    isinstance(x, (int, float)) and not isinstance(x, bool) for x in v
                )
            ):
                fields.append(cls._field_to_kosha(k, json.dumps(list(v)), "Vector"))
            elif isinstance(v, (list, tuple)):
                # Everything else list-shaped: short numeric tuples (below
                # _MIN_VECTOR_DIM), empty lists, string lists (recipients),
                # mixed content. JSON-encode as Keyword — stored verbatim
                # and filterable by exact match, but never tokenized and
                # never inserted into the shared Vector store. Decoded back
                # to a Python list by _decode_field_value.
                fields.append(
                    cls._field_to_kosha(
                        k, json.dumps(list(v), default=json_default), "Keyword"
                    )
                )
            # else: unsupported type — skip silently (matches prior behavior)
        return fields

    def index(
        self,
        index: str | None = None,
        id: str | None = None,
        body: dict | None = None,
        **params: Any,
    ) -> dict:
        """Index a single document."""
        ns = self._resolve_ns(index)
        fields = self._source_to_fields(body or {})
        doc = {
            "id": id or "",
            "fields": fields,
        }
        payload = {"namespace": ns, "documents": [doc]}
        result = self._request("POST", "index", body=payload)
        return {
            "_index": ns,
            "_id": id or "",
            "_version": 1,
            "result": "created",
            "_shards": {"total": 1, "successful": 1, "failed": 0},
            "_seq_no": result.get("indexed_count", 1),
        }

    # ── Bulk ───────────────────────────────────────────────────────────────

    def bulk(
        self, body: Sequence | str, index: str | None = None, **params: Any
    ) -> dict:
        """Bulk-index documents.

        Accepts the standard OpenSearch bulk body format (action+source lines
        as a list or newline-delimited string) and indexes each document via
        Kosha.
        """
        default_ns = self._resolve_ns(index)
        # (namespace, op, doc) triples in original order — response items must
        # stay aligned with the request actions for opensearchpy parsing.
        actions: list[tuple[str, str, dict]] = []
        errors: list[dict] = []

        # Parse the bulk body.
        lines = body if isinstance(body, list) else body.strip().split("\n")
        i = 0
        while i < len(lines):
            action_line = lines[i]
            try:
                action = (
                    json.loads(action_line)
                    if isinstance(action_line, str)
                    else action_line
                )
            except json.JSONDecodeError:
                i += 1
                continue
            op_type = None
            doc_id = None
            doc_index = None
            for key in ("index", "create", "update", "delete"):
                if key in action:
                    op_type = key
                    meta = action[key] or {}
                    doc_id = meta.get("_id")
                    doc_index = meta.get("_index")
                    break

            if op_type == "delete":
                errors.append(
                    {
                        "delete": {
                            "_id": doc_id,
                            "status": 501,
                            "error": "not implemented",
                        }
                    }
                )
                i += 1
                continue

            if i + 1 >= len(lines):
                break
            source_line = lines[i + 1]
            try:
                source = (
                    json.loads(source_line)
                    if isinstance(source_line, str)
                    else source_line
                )
            except json.JSONDecodeError:
                source = {}
            i += 2

            # helpers.bulk update actions send {"doc": {...}} — unwrap + keep
            # Vector fields (contentEmbedding) that index() already supported.
            # Partial updates are merged server-side by POST /replace.
            fields = self._source_to_fields(source if isinstance(source, dict) else {})
            doc = {
                "id": doc_id or "",
                "fields": fields,
            }
            actions.append((doc_index or default_ns, op_type or "index", doc))

        # Route each document to the namespace named by its action's _index.
        # `index` upserts full docs via /index; partial `update` uses /replace.
        docs_by_ns: dict[str, list[dict]] = {}
        updates_by_ns: dict[str, list[dict]] = {}
        for doc_ns, op_type, doc in actions:
            if op_type == "update":
                updates_by_ns.setdefault(doc_ns, []).append(doc)
            else:
                docs_by_ns.setdefault(doc_ns, []).append(doc)
        for doc_ns, docs in docs_by_ns.items():
            self._request(
                "POST", "index", body={"namespace": doc_ns, "documents": docs}
            )
            self._request("POST", "flush", {"namespace": doc_ns})
        for doc_ns, docs in updates_by_ns.items():
            self._request(
                "POST", "replace", body={"namespace": doc_ns, "documents": docs}
            )

        return {
            "errors": bool(errors),
            "items": (
                [
                    {
                        ("update" if op_type == "update" else "index"): {
                            "_index": doc_ns,
                            "_id": d["id"],
                            "status": 200 if op_type == "update" else 201,
                        }
                    }
                    for doc_ns, op_type, d in actions
                ]
                + errors
            ),
        }

    # ── Count ──────────────────────────────────────────────────────────────

    def count(
        self, index: str | None = None, body: dict | None = None, **params: Any
    ) -> dict:
        """Return document count from a broad search.

        Kosha does not have a dedicated count API, so we search with a
        non-empty placeholder (``*`` rendered as ``match``) and report the
        total_hits.
        """
        ns = self._resolve_ns(index)
        try:
            result = self.search(
                index=ns, body={"query": {"match": {"_all": "*"}}, "size": 0}
            )
            count = result["hits"]["total"]["value"]
        except KoshaRequestError:
            count = 0
        return {"count": count, "_shards": {"total": 1, "successful": 1, "failed": 0}}

    # ── Update ─────────────────────────────────────────────────────────────

    def update(
        self,
        index: str | None = None,
        id: str | None = None,
        body: dict | None = None,
        **params: Any,
    ) -> dict:
        """Update a document by durable replace (merges partial ``doc`` patches)."""
        ns = self._resolve_ns(index)
        fields = self._source_to_fields(body or {})
        payload = {"namespace": ns, "documents": [{"id": id or "", "fields": fields}]}
        self._request("POST", "replace", body=payload)
        return {
            "_index": ns,
            "_id": id or "",
            "_version": 2,
            "result": "updated",
        }

    # ── Delete by query ────────────────────────────────────────────────────

    def delete_by_query(
        self, index: str | None = None, body: dict | None = None, **params: Any
    ) -> dict:
        """Delete documents matching a filter query."""
        ns = self._resolve_ns(index)
        query = body.get("query", {}) if body else {}
        filter_clause = self._extract_filter(body) or query
        kosha_body = {"namespace": ns, "filter": filter_clause}
        result = self._request("POST", "delete", body=kosha_body)
        deleted = result.get("deleted", 0)
        return {
            "deleted": deleted,
            "total": deleted,
            "failures": [],
        }

    # ── Update by query ────────────────────────────────────────────────────

    def update_by_query(
        self, index: str | None = None, body: dict | None = None, **params: Any
    ) -> dict:
        """Update documents matching a query.

        Supports simple field-copy/literal scripts and parameter-map scripts
        used by metadata backfills. ``wait_for_completion=False`` returns an
        OpenSearch-compatible task id that can be polled through
        ``client.tasks.get(task_id=...)``.
        """
        ns = self._resolve_ns(index)
        request_body = body or {}
        if params.get("wait_for_completion", True) is False:
            task_id = f"kosha:{uuid.uuid4().hex}"
            with self._tasks_lock:
                self._tasks[task_id] = {"completed": False, "task": {"id": task_id}}

            def run() -> None:
                try:
                    response = self._execute_update_by_query(ns, request_body)
                    status = {"completed": True, "response": response}
                except Exception as exc:  # surfaced through the tasks API
                    status = {
                        "completed": True,
                        "error": {
                            "type": type(exc).__name__,
                            "reason": str(exc),
                        },
                    }
                with self._tasks_lock:
                    self._tasks[task_id] = status

            threading.Thread(
                target=run,
                name=f"kosha-update-by-query-{task_id[-8:]}",
                daemon=True,
            ).start()
            return {"task": task_id}

        return self._execute_update_by_query(ns, request_body)

    @staticmethod
    def _compile_update_script(
        script: dict,
    ) -> tuple[str | None, list[tuple[str, str]]]:
        """Compile the supported Painless subset into assignments."""
        source = script.get("source", "").strip()
        lookup_match = re.search(
            r"(?:String|def)\s+_did\s*=\s*ctx\._source\.(\w+)", source
        )
        lookup_field = lookup_match.group(1) if lookup_match else None
        map_assignments = re.findall(
            r"ctx\._source\.(\w+)\s*=\s*params\.(\w+)\s*\[\s*_did\s*\]",
            source,
        )
        if map_assignments:
            if not lookup_field:
                raise NotImplementedError(
                    "Kosha parameter-map scripts must define "
                    "'_did = ctx._source.<field>'"
                )
            return lookup_field, map_assignments

        simple = re.fullmatch(r"\s*ctx\._source\.(\w+)\s*=\s*(.+?)\s*;?\s*", source)
        if simple:
            expression = simple.group(2).strip().strip("'").strip('"')
            if expression.startswith("ctx._source."):
                expression = expression.removeprefix("ctx._source.")
            return None, [(simple.group(1), expression)]

        raise NotImplementedError(
            f"Kosha does not support script: {source!r}. "
            "Supported forms are simple assignments and params.<map>[_did] lookups."
        )

    def _execute_update_by_query(self, ns: str, body: dict) -> dict:
        query = body.get("query", {})
        script = body.get("script", {})
        lookup_field, assignments = self._compile_update_script(script)
        script_params = script.get("params", {})

        # Snapshot every match before replacing any document. This keeps
        # pagination stable and preserves all fields on immutable segments.
        page_size = 100
        from_ = 0
        hits: list[dict] = []
        while True:
            search_body = {
                "query": query,
                "size": page_size,
                "from": from_,
                "_source": True,
            }
            result = self.search(index=ns, body=search_body)
            page = result.get("hits", {}).get("hits", [])
            if not page:
                break
            hits.extend(page)
            from_ += page_size

        updated = 0
        noops = 0
        documents: list[dict] = []
        for hit in hits:
            source_fields = dict(hit.get("_source", {}))
            changed = False
            for target_field, expression in assignments:
                if lookup_field is not None:
                    lookup_value = source_fields.get(lookup_field)
                    values = script_params.get(expression, {})
                    if lookup_value not in values:
                        continue
                    new_value = values[lookup_value]
                elif expression in source_fields:
                    new_value = source_fields[expression]
                else:
                    new_value = expression

                if source_fields.get(target_field) != new_value:
                    source_fields[target_field] = new_value
                    changed = True

            if changed:
                updated += 1
            else:
                noops += 1
            documents.append(
                {
                    "id": hit["_id"],
                    "fields": self._source_to_fields(source_fields),
                }
            )

        if updated:
            # The replace route rewrites affected immutable segments, so old
            # versions cannot reappear after a server restart. Include no-op
            # snapshots because the query is replaced as one consistent set.
            self._request(
                "POST",
                "replace",
                {"namespace": ns, "documents": documents},
            )

        return {
            "updated": updated,
            "noop": noops,
            "total": len(hits),
            "version_conflicts": 0,
            "failures": [],
        }

    # ── Scan / Scroll ──────────────────────────────────────────────────────

    def scroll(self, scroll_id: str = None, scroll: str = "5m", **params):
        """Compatibility stub — Kosha uses pagination, not scroll cursors."""
        raise NotImplementedError(
            "Kosha does not support scroll cursors. "
            "Use paginated search (from/size) instead."
        )

    def clear_scroll(self, scroll_id: str, **params):
        """No-op — Kosha has no scroll state to clear."""

    # ── kNN no-op (Phase 2) ────────────────────────────────────────────────

    # ── Index exists / create ──────────────────────────────────────────────

    @property
    def indices(self) -> "IndexOps":
        """Return an object that mimics ``opensearchpy.client.IndicesClient``."""
        return IndexOps(self)

    @property
    def tasks(self) -> "TasksOps":
        """Return an object that mimics ``opensearchpy.client.TasksClient``."""
        return TasksOps(self)

    def ping(self, **params: Any) -> bool:
        """Check if Kosha is reachable."""
        try:
            self._request("GET", "healthz")
            return True
        except Exception:
            return False

    def close(self) -> None:
        pass


# ─── Tasks / indices operations ─────────────────────────────────────────────


class TasksOps:
    """Minimal task polling API for asynchronous compatibility operations."""

    def __init__(self, client: KoshaClient) -> None:
        self._client = client

    def get(self, task_id: str, **params: Any) -> dict:
        with self._client._tasks_lock:
            task = self._client._tasks.get(task_id)
            if task is None:
                raise KoshaRequestError(404, f"task {task_id!r} not found")
            return dict(task)


class IndexOps:
    """Mimics ``opensearchpy.client.IndicesClient`` for compatibility.

    Phase 1 is schema-less — Kosha creates namespaces on first write, so
    ``create()`` and ``exists()`` are mostly no-ops.
    """

    def __init__(self, client: KoshaClient) -> None:
        self._client = client

    def create(self, index: str, body: dict | None = None, **params: Any) -> dict:
        logger.info("IndexOps.create(%s) — no-op (Kosha is schema-less)", index)
        return {"acknowledged": True, "shards_acknowledged": True, "index": index}

    def exists(self, index: str, **params: Any) -> bool:
        try:
            self._client.count(index=index)
            return True
        except KoshaRequestError:
            return False

    def delete(self, index: str, **params: Any) -> dict:
        logger.info(
            "IndexOps.delete(%s) — no-op (Kosha does not support index deletion yet)",
            index,
        )
        return {"acknowledged": True}

    def put_mapping(self, index: str, body: dict, **params: Any) -> dict:
        logger.info("IndexOps.put_mapping — no-op (Kosha is schema-less)")
        return {"acknowledged": True}

    def put_settings(self, index: str, body: dict, **params: Any) -> dict:
        logger.info("IndexOps.put_settings — no-op")
        return {"acknowledged": True}

    def refresh(self, index: str | None = None, **params: Any) -> dict:
        logger.info("IndexOps.refresh — no-op (Kosha has no refresh cycle)")
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}

    def get(self, index: str, **params: Any) -> dict:
        return {index: {"aliases": {}, "mappings": {}, "settings": {}}}

    def get_mapping(self, index: str | None = None, **params: Any) -> dict:
        idx = index or "default"
        return {
            idx: {
                "mappings": {
                    "properties": {
                        "documentId": {"type": "keyword"},
                        "content": {"type": "text"},
                        "title": {"type": "text"},
                        "sender": {"type": "keyword"},
                        "recipients": {"type": "keyword"},
                        "sentAt": {"type": "date"},
                        "matterId": {"type": "keyword"},
                        "orgId": {"type": "keyword"},
                    }
                }
            }
        }

    def get_settings(self, index: str | None = None, **params: Any) -> dict:
        idx = index or "default"
        return {
            idx: {
                "settings": {
                    "index": {
                        "knn": "true",
                        "refresh_interval": "-1",
                        "number_of_shards": "1",
                        "number_of_replicas": "0",
                    }
                }
            }
        }

    def flush(self, index: str | None = None, **params: Any) -> dict:
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}

    def close(self, index: str, **params: Any) -> dict:
        logger.info(
            "IndexOps.close(%s) — no-op (Kosha has no close-index concept)", index
        )
        return {
            "acknowledged": True,
            "shards_acknowledged": True,
            "indices": {index: {"closed": True}},
        }
