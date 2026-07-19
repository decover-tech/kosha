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
        """Execute a search against Kosha.

        Translates an OpenSearch-shaped ``body`` dict into a Kosha search
        and returns an OpenSearch-shaped response dict.
        """
        ns = index or self._namespace
        query_text = self._extract_query_text(body) if body else ""
        size = body.get("size", 10) if body else 10
        from_ = body.get("from", 0) if body else 0

        # Parse optional BM25 params from the request body.
        bm25_params = {}
        q = body and (body.get("query") or {})
        if q:
            bm25_params = self._extract_bm25_params(q)

        # Build Kosha search URL.
        query_params = {
            "ns": ns,
            "q": query_text,
            "max_results": str(size + from_),
        }
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
                # Namespace not found → empty result set.
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
                source[field["name"]] = field["text"]
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
        bool_q = query.get("bool")
        if bool_q is not None:
            texts = []
            for clause_key in ("must", "should", "filter"):
                for clause in bool_q.get(clause_key, []):
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

    # ── Index ──────────────────────────────────────────────────────────────

    def index(self, index: str | None = None, id: str | None = None,
              body: dict | None = None, **params: Any) -> dict:
        """Index a single document."""
        ns = index or self._namespace
        doc = {
            "id": id or body.pop("_id", None) or "",
            "fields": [{"name": k, "text": v} for k, v in (body or {}).items()
                       if isinstance(v, str)],
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

            doc = {
                "id": doc_id or source.pop("_id", None) or "",
                "fields": [{"name": k, "text": v} for k, v in source.items()
                           if isinstance(v, str)],
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
        fields = [{"name": k, "text": v} for k, v in doc_body.items()
                  if isinstance(v, str)]
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
        """Delete documents matching a query (not yet implemented in Kosha)."""
        raise NotImplementedError(
            "Kosha does not yet support delete_by_query. "
            "Phase 1 is append-only; deletes/updates are tombstone-based."
        )

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
