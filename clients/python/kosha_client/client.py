"""KoshaClient — an OpenSearch-compatible client that talks to Kosha."""

from __future__ import annotations

import json
import logging
import time
import urllib.parse
import urllib.request
from typing import Any, Sequence

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
    * ``delete_by_query()``  — delete matching documents
    """

    def __init__(
        self,
        hosts: Any = None,
        http_auth: Any = None,
        timeout: int = 60,
        max_retries: int = 3,
        retry_on_timeout: bool = True,
        pool_maxsize: int = 20,
        **kwargs: Any,
    ) -> None:
        # Normalise the Kosha base URL from the same ``hosts`` format
        # that ``opensearchpy`` accepts.
        if isinstance(hosts, str):
            self._kosha_url = hosts.rstrip("/")
        elif isinstance(hosts, (list, tuple)) and len(hosts) > 0:
            h = hosts[0]
            if isinstance(h, str):
                self._kosha_url = h.rstrip("/")
            elif isinstance(h, dict):
                scheme = h.get("scheme", "http")
                host = h.get("host", "localhost")
                port = h.get("port", 8080)
                self._kosha_url = f"{scheme}://{host}:{port}"
            else:
                self._kosha_url = "http://localhost:8080"
        else:
            self._kosha_url = kwargs.get("kosha_url", "http://localhost:8080")

        self._timeout = timeout
        self._auth = http_auth

        # Kosha namespace → index name mapping.
        # In Phase 1, index name is used directly as the namespace.
        self._namespace = kwargs.get("namespace", "default")

        logger.info("KoshaClient targeting %s namespace=%s", self._kosha_url, self._namespace)

    # ── Low-level request helpers ──────────────────────────────────────────

    def _request(self, method: str, path: str, body: Any = None) -> Any:
        url = f"{self._kosha_url}/{path.lstrip('/')}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self._auth:
            import base64
            user, pwd = self._auth
            token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            req.add_header("Authorization", f"Basic {token}")

        try:
            resp = urllib.request.urlopen(req, timeout=self._timeout)
            return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            body_bytes = e.read()
            try:
                err = json.loads(body_bytes.decode())
            except json.JSONDecodeError:
                err = {"error": body_bytes.decode()}
            logger.warning("Kosha request failed: %s %s → %s %s", method, path, e.code, err)
            raise KoshaRequestError(e.code, err.get("error", str(e)), err) from e

    # ── Search ─────────────────────────────────────────────────────────────

    def search(self, index: str | None = None, body: dict | None = None, **params: Any) -> dict:
        ns = index or self._namespace

        # kNN/semantic not supported in Phase 1 — return empty.
        if body and ("knn" in body or "knn" in (body.get("query") or {})):
            return self._build_search_response([], 0, body.get("size", 10), 0, 0)

        query_text = self._extract_query_text(body) if body else ""
        size = body.get("size", 10) if body else 10
        from_ = body.get("from", 0) if body else 0
        filter_clause = self._extract_filter(body)
        aggs = self._extract_aggs(body)
        wildcard = self._extract_wildcard(body)
        match_phrase = self._extract_match_phrase(body)

        # Determine if we need POST (agg/wildcard/phrase/filter present).
        needs_post = bool(filter_clause or aggs or wildcard or match_phrase)

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
                if self._auth:
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

        kosha_body = {
            "namespace": ns,
            "query_text": query_text,
            "max_results": size + from_,
            "from": from_,
        }
        if filter_clause:
            kosha_body["filter"] = filter_clause
        if aggs:
            kosha_body["aggs"] = aggs
        if wildcard:
            kosha_body["wildcard"] = wildcard
        if match_phrase:
            kosha_body["match_phrase"] = match_phrase

        result = self._request("POST", "search", body=kosha_body)
        kosha_hits = result.get("results", [])
        total = result.get("total_hits", 0)
        kosha_aggs = result.get("aggregations")

        response = self._build_search_response(kosha_hits, 0, size, 0, total)
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
                source[field["name"]] = field.get("value", "")
                # Store field type for filter-aware operations.
                if field.get("field_type") not in ("Text", None):
                    source[f"__type__{field['name']}"] = field["field_type"]
            hits.append({
                "_index": self._namespace,
                "_id": doc_id,
                "_score": score,
                "_source": source,
            })

        # Apply offset/pagination in Python (Kosha returns flat top-N).
        page = hits[from_: from_ + size]

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
            sim = query["settings"].get("index", {}).get("similarity", {}).get("default", {})
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
    def _field_to_kosha(name: str, value: str, field_type: str = "Text") -> dict:
        return {"name": name, "field_type": field_type, "value": value}

    def index(self, index: str | None = None, id: str | None = None,
              body: dict | None = None, **params: Any) -> dict:
        """Index a single document."""
        ns = index or self._namespace
        fields = []
        for k, v in (body or {}).items():
            if isinstance(v, str):
                fields.append(self._field_to_kosha(k, v, "Text"))
            elif isinstance(v, bool):
                fields.append(self._field_to_kosha(k, str(v).lower(), "Boolean"))
            elif isinstance(v, (int, float)):
                fields.append(self._field_to_kosha(k, str(v), "Float"))
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

    def bulk(self, body: Sequence | str, index: str | None = None,
             **params: Any) -> dict:
        """Bulk-index documents.

        Accepts the standard OpenSearch bulk body format (action+source lines
        as a list or newline-delimited string) and indexes each document via
        Kosha.
        """
        ns = index or self._namespace
        documents: list[dict] = []
        errors: list[dict] = []

        # Parse the bulk body.
        lines = body if isinstance(body, list) else body.strip().split("\n")
        i = 0
        while i < len(lines):
            action_line = lines[i]
            try:
                action = json.loads(action_line) if isinstance(action_line, str) else action_line
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
                    doc_index = meta.get("_index", ns)
                    break

            if op_type == "delete":
                errors.append({"delete": {"_id": doc_id, "status": 501, "error": "not implemented"}})
                i += 1
                continue

            if i + 1 >= len(lines):
                break
            source_line = lines[i + 1]
            try:
                source = json.loads(source_line) if isinstance(source_line, str) else source_line
            except json.JSONDecodeError:
                source = {}
            i += 2

            fields = []
            for k, v in source.items():
                if isinstance(v, str):
                    fields.append(self._field_to_kosha(k, v, "Text"))
                elif isinstance(v, bool):
                    fields.append(self._field_to_kosha(k, str(v).lower(), "Boolean"))
                elif isinstance(v, (int, float)):
                    fields.append(self._field_to_kosha(k, str(v), "Float"))
            doc = {
                "id": doc_id or "",
                "fields": fields,
            }
            documents.append(doc)

        if documents:
            payload = {"namespace": ns, "documents": documents}
            self._request("POST", "index", body=payload)

        return {
            "errors": bool(errors),
            "items": (
                [{"index": {"_index": ns, "_id": d["id"], "status": 201}}
                 for d in documents]
                + errors
            ),
        }

    # ── Count ──────────────────────────────────────────────────────────────

    def count(self, index: str | None = None, body: dict | None = None, **params: Any) -> dict:
        """Return document count from a broad search.

        Kosha does not have a dedicated count API, so we search with a
        non-empty placeholder (``*`` rendered as ``match``) and report the
        total_hits.
        """
        ns = index or self._namespace
        try:
            result = self.search(index=ns, body={"query": {"match": {"_all": "*"}}, "size": 0})
            count = result["hits"]["total"]["value"]
        except KoshaRequestError:
            count = 0
        return {"count": count, "_shards": {"total": 1, "successful": 1, "failed": 0}}

    # ── Update ─────────────────────────────────────────────────────────────

    def update(self, index: str | None = None, id: str | None = None,
               body: dict | None = None, **params: Any) -> dict:
        """Update a document by re-indexing (tombstone-based in Phase 1)."""
        ns = index or self._namespace
        doc_body = (body or {}).get("doc", body or {})
        fields = []
        for k, v in doc_body.items():
            if isinstance(v, str):
                fields.append(self._field_to_kosha(k, v, "Text"))
            elif isinstance(v, bool):
                fields.append(self._field_to_kosha(k, str(v).lower(), "Boolean"))
            elif isinstance(v, (int, float)):
                fields.append(self._field_to_kosha(k, str(v), "Float"))
        payload = {"namespace": ns, "documents": [{"id": id or "", "fields": fields}]}
        self._request("POST", "index", body=payload)
        return {
            "_index": ns,
            "_id": id or "",
            "_version": 2,
            "result": "updated",
        }

    # ── Delete by query ────────────────────────────────────────────────────

    def delete_by_query(self, index: str | None = None,
                        body: dict | None = None, **params: Any) -> dict:
        """Delete documents matching a filter query."""
        ns = index or self._namespace
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

    def update_by_query(self, index: str | None = None,
                        body: dict | None = None, **params: Any) -> dict:
        """Update documents matching a query.

        Supports simple ``ctx._source.X = ctx._source.Y`` (field copy)
        and ``ctx._source.X = 'value'`` (literal set) scripts.
        """
        ns = index or self._namespace
        query = (body or {}).get("query", {})
        script = (body or {}).get("script", {})
        source = script.get("source", "").strip()

        # Parse the script to extract target field and source expression.
        target_field = None
        source_expr = None
        import re
        m = re.match(r"ctx\._source\.(\w+)\s*=\s*(.+)", source)
        if m:
            target_field = m.group(1)
            source_expr = m.group(2).strip().strip("'").strip('"')

        if not target_field:
            raise NotImplementedError(
                f"Kosha does not support script: {source!r}. "
                "Only simple 'ctx._source.X = ...' patterns work."
            )

        # Search for matching docs in pages.
        page_size = 100
        from_ = 0
        total_updated = 0
        while True:
            search_body = {
                "query": query,
                "size": page_size,
                "from": from_,
                "_source": True,
            }
            try:
                result = self.search(index=ns, body=search_body)
            except KoshaRequestError:
                break

            hits = result.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                doc_id = hit["_id"]
                source_fields = hit.get("_source", {})

                # Evaluate the source expression.
                if source_expr in source_fields:
                    new_value = source_fields[source_expr]
                else:
                    new_value = source_expr

                # Re-index with updated field.
                source_fields[target_field] = new_value
                self.index(index=ns, id=doc_id, body=source_fields)
                total_updated += 1

            from_ += page_size

        # Flush to persist updated segments.
        self._request("POST", "flush", {"namespace": ns})

        return {
            "updated": total_updated,
            "total": total_updated,
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

    def indices(self) -> "IndexOps":
        """Return an object that mimics ``opensearchpy.client.IndicesClient``."""
        return IndexOps(self)

    def ping(self, **params: Any) -> bool:
        """Check if Kosha is reachable."""
        try:
            self._request("GET", "healthz")
            return True
        except Exception:
            return False

    def close(self) -> None:
        pass


# ─── Indices operations (stub) ──────────────────────────────────────────────

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
        logger.info("IndexOps.delete(%s) — no-op (Kosha does not support index deletion yet)", index)
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
        return {}

    def get_settings(self, index: str | None = None, **params: Any) -> dict:
        return {}

    def flush(self, index: str | None = None, **params: Any) -> dict:
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}


# ─── Error type ─────────────────────────────────────────────────────────────

class KoshaRequestError(Exception):
    """Raised when a Kosha HTTP request fails."""

    def __init__(self, status_code: int, error: str, info: dict | None = None):
        self.status_code = status_code
        self.error = error
        self.info = info or {}
        super().__init__(f"KoshaRequestError({status_code}): {error}")
