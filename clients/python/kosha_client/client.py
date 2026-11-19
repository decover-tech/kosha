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

    @staticmethod
    def _wants_refresh(params: dict) -> bool:
        """Whether a write asked to be visible on return (ES ``refresh``).

        ``True``/``"true"``/``""`` (bare ``?refresh``) and ``"wait_for"`` all
        mean "searchable when this call returns" — Kosha has no async
        refresh to wait on, so both are an explicit ``/flush``. Anything else
        (the default) leaves visibility to the server's idle-buffer flush
        (``KOSHA_FLUSH_MAX_AGE_MS``, ~1 s), like ES's ``refresh_interval``.
        """
        refresh = params.get("refresh", False)
        if isinstance(refresh, bool):
            return refresh
        return str(refresh).strip().lower() in ("", "true", "wait_for")

    def _flush(self, namespace: str) -> None:
        self._request("POST", "flush", {"namespace": namespace})

    def _resolve_ns(self, index):
        """Resolve namespace from opensearch_dsl index parameter (can be list)."""
        ns = index or self._namespace
        if isinstance(ns, (list, tuple)):
            ns = ns[0] if ns else self._namespace
        if ns is None:
            ns = "default"
        return str(ns)

    def search(
        self,
        index: str | None = None,
        body: dict | None = None,
        rerank: dict | None = None,
        **params: Any,
    ) -> dict:
        """Run an OpenSearch search body against Kosha.

        The body is translated to Kosha's native query (see
        ``_translate_search``). A clause Kosha cannot express raises
        ``KoshaRequestError(400)`` naming it. It is never dropped: dropping
        a filter silently returns documents the caller excluded.

        ``rerank`` (or a ``"rerank"`` key in the body) asks the server to
        rerank the top hits with an external model; see ``rerank_spec``.
        """
        ns = self._resolve_ns(index)
        body = body or {}
        if rerank is not None:
            body = {**body, "rerank": rerank}
        kosha_body, context = self._translate_search(body, params)
        kosha_body["namespace"] = ns

        start = time.monotonic()
        try:
            result = self._request("POST", "search", body=kosha_body)
        except KoshaRequestError as e:
            if e.status_code == 404:
                # An index nothing has been written to yet: no hits.
                result = {"results": [], "total_hits": 0}
            else:
                raise
        took_ms = int((time.monotonic() - start) * 1000)
        return self._build_search_response(result, took_ms, context)

    # ── Rerank ─────────────────────────────────────────────────────────────

    _RERANK_KEYS = frozenset({"provider", "model", "field", "top_n", "timeout_ms", "query"})

    @staticmethod
    def rerank_spec(
        field: str,
        *,
        model: str | None = "rerank-v3.5",
        top_n: int | None = None,
        provider: str = "cohere",
        timeout_ms: int | None = None,
        query: str | None = None,
    ) -> dict:
        """The ``rerank`` clause equivalent to a Cohere rerank call.

        ``co.rerank(model=m, query=q, documents=[hit[field] ...], top_n=n)``
        over the top ``n`` search hits becomes
        ``client.search(index, body, rerank=KoshaClient.rerank_spec(field,
        model=m, top_n=n))``. The server sends the same request to the
        provider it has configured under ``provider`` (the API key stays on
        the server). ``query`` defaults to the search's query text; set it
        for a kNN-only search. ``top_n`` defaults to ``from + size`` and
        must be at least that. If the provider times out or fails, hits
        come back in search order and the response has
        ``rerank.degraded = True``.
        """
        spec: dict = {"provider": provider, "field": field}
        for key, value in (
            ("model", model),
            ("top_n", top_n),
            ("timeout_ms", timeout_ms),
            ("query", query),
        ):
            if value is not None:
                spec[key] = value
        return spec

    @classmethod
    def _rerank(cls, spec: Any) -> dict:
        """Validate a ``rerank`` clause; Cohere's ``rank_fields`` is accepted
        for ``field`` (one field only)."""
        if not isinstance(spec, dict):
            raise cls._unsupported(f"rerank {spec!r}")
        spec = dict(spec)
        rank_fields = spec.pop("rank_fields", None)
        if rank_fields is not None:
            fields = cls._as_list(rank_fields)
            if len(fields) != 1 or "field" in spec:
                raise cls._unsupported("rerank over more than one field")
            spec["field"] = fields[0]
        unknown = set(spec) - cls._RERANK_KEYS
        if unknown:
            raise cls._unsupported(f"rerank option(s) {sorted(unknown)}")
        if not spec.get("provider") or not spec.get("field"):
            raise cls._unsupported("rerank without provider and field")
        return spec

    # ── Query translation ──────────────────────────────────────────────────

    # Options that change how a clause scores or parses but not which
    # documents it matches, so they can be ignored without changing results.
    _IGNORED_RANGE_KEYS = frozenset({"format", "time_zone", "boost", "relation"})
    _BOUND_KEYS = ("gte", "gt", "lte", "lt")

    @staticmethod
    def _unsupported(what: str) -> KoshaRequestError:
        message = (
            f"unsupported by Kosha: {what}. The clause was not sent; "
            "dropping it would change which documents match"
        )
        return KoshaRequestError(400, message, {"error": message})

    @staticmethod
    def _as_list(value: Any) -> list:
        if value is None:
            return []
        return list(value) if isinstance(value, (list, tuple)) else [value]

    @staticmethod
    def _scalar(value: Any) -> str:
        """A filter value as Kosha compares it: always a string.

        Booleans are stored as ``"true"``/``"false"`` (see
        ``_source_to_fields``), so ``True`` must not become ``"True"``.
        """
        if isinstance(value, bool):
            return "true" if value else "false"
        if value is None:
            raise KoshaClient._unsupported("a null filter value")
        if isinstance(value, (dict, list, tuple)):
            raise KoshaClient._unsupported(f"a non-scalar filter value {value!r}")
        return str(value)

    @classmethod
    def _to_filter(cls, clause: Any) -> dict:
        """Translate an OpenSearch filter clause into a Kosha filter.

        Handles ``term``, ``terms``, ``range``, ``exists``, ``match_all``
        and ``bool`` at any depth. Anything else raises.

        ``bool`` follows OpenSearch's rules for ``should``: with no
        ``minimum_should_match``, the clauses are required (at least one)
        when the bool has no ``must``/``filter``, and optional when it has.
        Optional clauses cannot change which documents a filter matches, so
        they are left out. Kosha's own rule is that a non-empty ``should``
        is always required, so the difference has to be resolved here.
        """
        if not isinstance(clause, dict) or len(clause) != 1:
            raise cls._unsupported(f"filter clause {clause!r}")
        kind, spec = next(iter(clause.items()))

        if kind == "match_all":
            return {"match_all": {}}

        if kind == "term":
            out = {}
            for field, value in (spec or {}).items():
                if field == "boost":
                    continue
                if field == "_id":
                    raise cls._unsupported("a term filter on _id")
                if isinstance(value, dict):
                    value = value.get("value")
                out[field] = cls._scalar(value)
            if not out:
                raise cls._unsupported(f"empty term clause {clause!r}")
            return {"term": out}

        if kind == "terms":
            out = {}
            for field, values in (spec or {}).items():
                if field == "boost":
                    continue
                if field == "_id":
                    raise cls._unsupported("a terms filter on _id")
                out[field] = [cls._scalar(v) for v in cls._as_list(values)]
            if not out:
                raise cls._unsupported(f"empty terms clause {clause!r}")
            return {"terms": out}

        if kind == "range":
            out = {}
            for field, bounds in (spec or {}).items():
                if not isinstance(bounds, dict):
                    raise cls._unsupported(f"range clause {clause!r}")
                unknown = set(bounds) - set(cls._BOUND_KEYS) - cls._IGNORED_RANGE_KEYS
                if unknown:
                    raise cls._unsupported(f"range option(s) {sorted(unknown)}")
                out[field] = {
                    k: cls._scalar(v)
                    for k, v in bounds.items()
                    if k in cls._BOUND_KEYS and v is not None
                }
            return {"range": out}

        if kind == "exists":
            field = (spec or {}).get("field")
            if not field:
                raise cls._unsupported(f"exists clause {clause!r}")
            return {"exists": {"field": field}}

        if kind == "bool":
            spec = spec or {}
            unknown = set(spec) - {
                "must", "filter", "must_not", "should", "minimum_should_match", "boost",
            }
            if unknown:
                raise cls._unsupported(f"bool option(s) {sorted(unknown)}")
            must = [
                cls._to_filter(c)
                for key in ("must", "filter")
                for c in cls._as_list(spec.get(key))
            ]
            must_not = [cls._to_filter(c) for c in cls._as_list(spec.get("must_not"))]
            should_raw = cls._as_list(spec.get("should"))
            need = cls._should_need(spec, has_required=bool(must))
            out: dict = {}
            if must:
                out["must"] = must
            if must_not:
                out["must_not"] = must_not
            if should_raw and need > 0:
                out["should"] = [cls._to_filter(c) for c in should_raw]
                out["minimum_should_match"] = need
            if not out:
                return {"match_all": {}}
            return {"bool": out}

        raise cls._unsupported(f"'{kind}' as a filter")

    @classmethod
    def _should_need(cls, bool_spec: dict, has_required: bool) -> int:
        """How many ``should`` clauses must match (OpenSearch's default)."""
        raw = bool_spec.get("minimum_should_match")
        if raw is None:
            return 0 if has_required else 1
        try:
            return int(raw)
        except (TypeError, ValueError):
            raise cls._unsupported(f"minimum_should_match={raw!r}") from None

    @classmethod
    def _is_filter(cls, clause: Any) -> bool:
        try:
            cls._to_filter(clause)
            return True
        except KoshaRequestError:
            return False

    @classmethod
    def _text_leaf(cls, clause: dict) -> tuple[str, Any] | None:
        """``("text"|"phrase"|"wildcard", payload)`` for a full-text leaf.

        Kosha has one text index covering every text field of a document,
        so the field a clause names selects nothing: ``match`` on ``title``
        and on ``content`` are the same query.
        """
        if not isinstance(clause, dict) or len(clause) != 1:
            return None
        kind, spec = next(iter(clause.items()))
        if kind == "match":
            for _field, value in (spec or {}).items():
                if isinstance(value, dict):
                    extra = set(value) - {"query", "operator", "boost"}
                    if extra:
                        raise cls._unsupported(f"match option(s) {sorted(extra)}")
                    text = str(value.get("query", ""))
                    operator = str(value.get("operator", "or")).lower()
                else:
                    text, operator = str(value), "or"
                return "text", (text, operator)
            return None
        if kind == "multi_match":
            spec = spec or {}
            kind_of = spec.get("type", "best_fields")
            if kind_of not in ("best_fields", "most_fields", "cross_fields"):
                raise cls._unsupported(f"multi_match type {kind_of!r}")
            return "text", (
                str(spec.get("query", "")),
                str(spec.get("operator", "or")).lower(),
            )
        if kind == "match_phrase":
            for field, value in (spec or {}).items():
                if isinstance(value, dict):
                    return "phrase", {
                        "field": field,
                        "phrase": str(value.get("query", "")),
                        "slop": int(value.get("slop", 0) or 0),
                    }
                return "phrase", {"field": field, "phrase": str(value), "slop": 0}
            return None
        if kind == "wildcard":
            for field, value in (spec or {}).items():
                if isinstance(value, dict):
                    return "wildcard", {
                        "field": field,
                        "pattern": value.get("value", value.get("wildcard", "")),
                        "case_insensitive": bool(value.get("case_insensitive", True)),
                    }
                return "wildcard", {
                    "field": field, "pattern": value, "case_insensitive": True,
                }
            return None
        return None

    @classmethod
    def _plan_query(cls, query: dict | None) -> dict:
        """Split an OpenSearch query into the parts Kosha's query has.

        Kosha's query is one of each: a text query (terms joined by a single
        AND or OR), a phrase, a wildcard, a vector search, and a filter
        tree. This walks the OpenSearch query and assigns each clause to one
        of those, raising when the query needs something that shape cannot
        hold (two different phrases, text OR-ed with a filter, a clause
        type Kosha does not have).
        """
        plan: dict = {
            "required_text": [],   # [(text, operator)] every one must match
            "any_text": [],        # [(text, operator)] at least one must match
            "phrase": None,
            "wildcard": None,
            "knn": None,
            "must": [],
            "must_not": [],
        }
        cls._absorb(query or {}, plan, required=True)
        return plan

    @classmethod
    def _set_once(cls, plan: dict, key: str, value: dict) -> None:
        if plan[key] is not None and plan[key] != value:
            raise cls._unsupported(f"more than one {key} clause in a query")
        plan[key] = value

    @classmethod
    def _absorb(cls, clause: dict, plan: dict, required: bool) -> None:
        """Place one clause that has to match (``required``) into ``plan``."""
        if not clause or "match_all" in clause:
            return
        if not isinstance(clause, dict) or len(clause) != 1:
            raise cls._unsupported(f"query clause {clause!r}")
        kind, spec = next(iter(clause.items()))

        if kind == "function_score":
            cls._absorb((spec or {}).get("query") or {}, plan, required)
            return

        if kind == "knn":
            cls._set_once(plan, "knn", cls._knn(spec))
            return

        leaf = cls._text_leaf(clause)
        if leaf is not None:
            what, payload = leaf
            if what == "text":
                if payload[0].strip():
                    plan["required_text"].append(payload)
            else:
                cls._set_once(plan, what, payload)
            return

        if kind == "bool":
            cls._absorb_bool(spec or {}, plan)
            return

        # Everything else is either a filter or not expressible.
        plan["must"].append(cls._to_filter(clause))

    @classmethod
    def _absorb_bool(cls, spec: dict, plan: dict) -> None:
        if cls._is_filter({"bool": spec}):
            translated = cls._to_filter({"bool": spec})
            if translated != {"match_all": {}}:
                plan["must"].append(translated)
            return

        unknown = set(spec) - {
            "must", "filter", "must_not", "should", "minimum_should_match", "boost",
        }
        if unknown:
            raise cls._unsupported(f"bool option(s) {sorted(unknown)}")

        required = cls._as_list(spec.get("must")) + cls._as_list(spec.get("filter"))
        for child in required:
            cls._absorb(child, plan, required=True)
        for child in cls._as_list(spec.get("must_not")):
            # Only a filter can be negated: Kosha has no "text must not
            # match".
            plan["must_not"].append(cls._to_filter(child))

        should = cls._as_list(spec.get("should"))
        if not should:
            return
        need = cls._should_need(spec, has_required=bool(required))
        if need == 0:
            # Optional clauses only adjust the score of documents that
            # already match. Kosha cannot apply the boost; the set of
            # matching documents is unchanged.
            return
        if need > 1:
            raise cls._unsupported("minimum_should_match > 1 over text clauses")

        if all(cls._is_filter(c) for c in should):
            plan["must"].append(
                {
                    "bool": {
                        "should": [cls._to_filter(c) for c in should],
                        "minimum_should_match": 1,
                    }
                }
            )
            return

        # At least one of several text clauses. The keyword compiler emits
        # this shape for one term searched across several fields: the same
        # clause repeated per field, which is a single clause here.
        leaves = []
        for child in should:
            leaf = cls._text_leaf(child)
            if leaf is None:
                raise cls._unsupported(
                    "a should group mixing text with filters or nested queries"
                )
            leaves.append(leaf)
        kinds = {what for what, _ in leaves}
        if len(kinds) > 1:
            raise cls._unsupported("a should group mixing text, phrase and wildcard")
        what = kinds.pop()
        if what == "text":
            distinct = list(dict.fromkeys(payload for _, payload in leaves))
            if len(distinct) == 1:
                plan["required_text"].append(distinct[0])
            else:
                plan["any_text"].extend(distinct)
        else:
            same = {
                (p.get("phrase", p.get("pattern")), p.get("slop", 0)) for _, p in leaves
            }
            if len(same) > 1:
                raise cls._unsupported(f"a should group of different {what} clauses")
            cls._set_once(plan, what, leaves[0][1])

    @classmethod
    def _knn(cls, spec: dict) -> dict:
        """``{"knn": {field: {vector, k, filter}}}`` → Kosha's kNN.

        The filter is carried into the vector search itself. It used to be
        dropped here, so a matter-scoped semantic search returned the
        nearest chunks of every matter in the index.
        """
        for field, options in (spec or {}).items():
            if not isinstance(options, dict):
                continue
            unknown = set(options) - {
                "vector", "k", "filter", "num_candidates", "boost",
            }
            if unknown:
                raise cls._unsupported(f"knn option(s) {sorted(unknown)}")
            k = int(options.get("k", 10))
            knn = {
                "field": field,
                "vector": options.get("vector", []),
                "k": k,
                "num_candidates": int(options.get("num_candidates", max(100, k))),
            }
            if options.get("filter"):
                translated = cls._to_filter(options["filter"])
                if translated != {"match_all": {}}:
                    knn["filter"] = translated
            return knn
        raise cls._unsupported(f"knn clause {spec!r}")

    @classmethod
    def _resolve_text(cls, plan: dict) -> tuple[str, str | None]:
        """The plan's text clauses as one ``(query_text, operator)``."""
        required = list(dict.fromkeys(plan["required_text"]))
        any_of = list(dict.fromkeys(plan["any_text"]))
        if required and any_of:
            raise cls._unsupported("text that must match combined with an OR group of text")
        if any_of:
            if any(op == "and" and len(text.split()) > 1 for text, op in any_of):
                raise cls._unsupported("an OR group of multi-word AND clauses")
            return " ".join(text for text, _ in any_of), "or"
        if not required:
            return "", None
        if len(required) == 1:
            text, operator = required[0]
            return text, operator
        # Several clauses, all required: every term of every clause, which
        # is only the same thing if no clause is itself an OR of words.
        if any(op != "and" and len(text.split()) > 1 for text, op in required):
            raise cls._unsupported(
                "several required text clauses where one is a multi-word OR"
            )
        return " ".join(text for text, _ in required), "and"

    @classmethod
    def _translate_search(cls, body: dict, params: dict | None = None) -> tuple[dict, dict]:
        """OpenSearch search body → ``(kosha request, response context)``."""
        params = params or {}
        size = int(body.get("size", params.get("size", 10)))
        from_ = int(body.get("from", params.get("from_", 0)) or 0)

        query = body.get("query") or {}
        plan = cls._plan_query(query)
        if body.get("knn"):
            # Legacy top-level form: {"knn": {field: {...}}}.
            cls._set_once(plan, "knn", cls._knn(body["knn"]))
        if body.get("post_filter"):
            plan["must"].append(cls._to_filter(body["post_filter"]))

        text, operator = cls._resolve_text(plan)
        if plan["phrase"] and text:
            raise cls._unsupported("a phrase and separate query text in one query")

        search_after = body.get("search_after")
        kosha: dict = {
            "query_text": text,
            "max_results": size,
            "from": 0 if search_after else from_,
        }
        if operator and text:
            kosha["operator"] = operator

        must, must_not = plan["must"], plan["must_not"]
        if must_not or len(must) > 1:
            node: dict = {}
            if must:
                node["must"] = must
            if must_not:
                node["must_not"] = must_not
            kosha["filter"] = {"bool": node}
        elif must:
            kosha["filter"] = must[0]

        if plan["phrase"]:
            kosha["match_phrase"] = plan["phrase"]
        if plan["wildcard"]:
            kosha["wildcard"] = plan["wildcard"]
        if plan["knn"]:
            kosha["knn"] = plan["knn"]

        aggs = cls._extract_aggs(body)
        if aggs:
            kosha["aggs"] = aggs
        sort = cls._extract_sort(body)
        if sort:
            kosha["sort"] = sort
        if search_after:
            kosha["search_after"] = [
                "" if v is None else cls._scalar(v) for v in search_after
            ]

        track = body.get("track_total_hits", params.get("track_total_hits"))
        if track is True:
            kosha["exact_total_hits"] = True
        elif isinstance(track, int) and not isinstance(track, bool) and track > 0:
            kosha["total_hits_cap"] = track

        if body.get("rerank") is not None:
            kosha["rerank"] = cls._rerank(body["rerank"])

        collapse = body.get("collapse")
        if collapse:
            unknown = set(collapse) - {"field"}
            if unknown:
                raise cls._unsupported(f"collapse option(s) {sorted(unknown)}")
            kosha["collapse"] = {"field": collapse["field"]}

        source = body.get("_source", params.get("_source"))
        if source is False:
            kosha["source_includes"] = []
        elif isinstance(source, (list, tuple)):
            kosha["source_includes"] = list(source)
        elif isinstance(source, str):
            kosha["source_includes"] = [source]
        elif isinstance(source, dict):
            includes = cls._as_list(source.get("includes", source.get("include")))
            excludes = cls._as_list(source.get("excludes", source.get("exclude")))
            if includes:
                kosha["source_includes"] = includes
            if excludes:
                kosha["source_excludes"] = excludes

        highlight_field = None
        highlight = body.get("highlight")
        if highlight and isinstance(highlight.get("fields"), dict):
            highlight_field, options = next(iter(highlight["fields"].items()))
            options = options or {}
            spec = {"field": highlight_field}
            for key in ("pre_tags", "post_tags"):
                tags = options.get(key, highlight.get(key))
                if tags:
                    spec[key] = cls._as_list(tags)
            kosha["highlight"] = spec

        context = {
            "size": size,
            "sort": sort or [],
            "collapse_field": (collapse or {}).get("field"),
            "highlight_field": highlight_field,
        }
        return kosha, context

    # Aggregation types Kosha computes. A sub-aggregation is refused rather
    # than ignored: the caller would read buckets that are not there.
    _AGG_TYPES = ("terms", "cardinality", "composite")

    @classmethod
    def _extract_aggs(cls, body: dict | None) -> dict | None:
        """Validate and pass through the aggregations of a search body."""
        if not body:
            return None
        aggs = body.get("aggs") or body.get("aggregations")
        if not aggs:
            return None
        out = {}
        for name, spec in aggs.items():
            if not isinstance(spec, dict):
                raise cls._unsupported(f"aggregation {name!r}: {spec!r}")
            if "aggs" in spec or "aggregations" in spec:
                raise cls._unsupported(f"sub-aggregations (in {name!r})")
            kinds = [k for k in spec if k != "meta"]
            if len(kinds) != 1 or kinds[0] not in cls._AGG_TYPES:
                raise cls._unsupported(f"aggregation type(s) {kinds} (in {name!r})")
            out[name] = {kinds[0]: spec[kinds[0]]}
        return out

    @staticmethod
    def _extract_sort(body: dict | None) -> list[dict] | None:
        """Translate OpenSearch ``sort`` into Kosha ``SortSpec`` objects.

        Accepts ``"field"``, ``{"field": "asc"}`` and
        ``{"field": {"order": "asc", ...}}`` entries. ``_doc`` (index
        order, used to walk a result set) becomes ``_id`` ascending, which
        is equally stable. Other per-field options (``missing``, ``mode``,
        ``unmapped_type``) are not applied: a document without the field
        sorts first.
        """
        if not body:
            return None
        raw = body.get("sort")
        if not raw:
            return None
        if isinstance(raw, (str, dict)):
            raw = [raw]
        out: list[dict] = []
        for entry in raw:
            if isinstance(entry, str):
                entry = {entry: "asc"}
            if not isinstance(entry, dict):
                continue
            spec: dict[str, dict[str, str]] = {}
            for field, val in entry.items():
                if field == "_doc":
                    field = "_id"
                if isinstance(val, str):
                    spec[field] = {"order": val}
                elif isinstance(val, dict):
                    spec[field] = {"order": str(val.get("order", "asc"))}
                else:
                    spec[field] = {"order": "asc"}
            if spec:
                out.append(spec)
        return out or None

    def _build_search_response(
        self, result: dict, took_ms: int = 0, context: dict | None = None
    ) -> dict:
        """Kosha search result → OpenSearch response dict."""
        context = context or {}
        sort = context.get("sort") or []
        collapse_field = context.get("collapse_field")
        highlight_field = context.get("highlight_field")

        hits = []
        for k_hit in result.get("results", []):
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
            hit = {
                "_index": self._namespace,
                "_id": doc_id,
                "_score": score,
                "_source": source,
            }
            # Reranked requests: the search-stage score (RRF for hybrid)
            # and the reranker's score; ``_score`` is the latter unless
            # the rerank degraded.
            if "fused_score" in k_hit:
                hit["_fused_score"] = k_hit["fused_score"]
            if "rerank_score" in k_hit:
                hit["_rerank_score"] = k_hit["rerank_score"]
            if highlight_field and k_hit.get("highlights"):
                hit["highlight"] = {highlight_field: k_hit["highlights"]}
            if sort:
                # The values this hit sorted by, in sort-key order: what a
                # caller passes back as ``search_after`` for the next page.
                values = []
                for spec in sort:
                    for field in spec:
                        if field == "_id":
                            values.append(doc_id)
                        elif field == "_score":
                            values.append(score)
                        else:
                            values.append(source.get(field))
                hit["sort"] = values
            if collapse_field:
                base = collapse_field
                for suffix in (".keyword", ".raw"):
                    if base not in source and base.endswith(suffix):
                        base = base[: -len(suffix)]
                hit["fields"] = {collapse_field: [source.get(base)]}
            hits.append(hit)

        response = {
            "took": took_ms,
            "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {
                "total": {
                    "value": result.get("total_hits", len(hits)),
                    # "gte" when the count stopped at the cap: the true
                    # total is at least this. Always reported as "eq"
                    # before, which presented a lower bound as exact.
                    "relation": result.get("total_hits_relation", "eq"),
                },
                "max_score": max((h["_score"] for h in hits), default=0.0),
                "hits": hits,
            },
        }
        if result.get("aggregations") is not None:
            response["aggregations"] = result["aggregations"]
        if "rerank_degraded" in result:
            response["rerank"] = {"degraded": bool(result["rerank_degraded"])}
            if result.get("rerank_degraded_reason"):
                response["rerank"]["reason"] = result["rerank_degraded_reason"]
        return response

    # ── Older helper names, kept for callers and tests that use them ──────

    @classmethod
    def _extract_query_text(cls, body: dict | None) -> str:
        """The query text Kosha will search for, given a search body."""
        return cls._resolve_text(cls._plan_query((body or {}).get("query") or {}))[0]

    @classmethod
    def _extract_filter(cls, body: dict | None) -> dict | None:
        """The Kosha filter for a search body, or ``None`` if it has none."""
        if not body:
            return None
        kosha, _ = cls._translate_search(body)
        return kosha.get("filter")

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

    # ── Index ──────────────────────────────────────────────────────────────

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
        if self._wants_refresh(params):
            self._flush(ns)
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

        Documents are visible on return only with ``refresh=True`` or
        ``refresh="wait_for"`` (ES semantics); otherwise the server publishes
        them within ~1 s (``KOSHA_FLUSH_MAX_AGE_MS``) or as soon as its
        buffer reaches ``KOSHA_FLUSH_THRESHOLD``. Flushing after every batch
        unconditionally — as this method used to — cut one segment per
        ≤500-doc chunk (1,529 segments and a 12 MB manifest on one staging
        namespace, issue #241).
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
        for doc_ns, docs in updates_by_ns.items():
            self._request(
                "POST", "replace", body={"namespace": doc_ns, "documents": docs}
            )
        if self._wants_refresh(params):
            for doc_ns in dict.fromkeys([*docs_by_ns, *updates_by_ns]):
                self._flush(doc_ns)

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
        """Count the documents matching ``body["query"]`` (all, if none).

        A search for no hits with an exact total. The query in ``body`` was
        ignored before, so every count was of the whole index, and capped.
        """
        ns = self._resolve_ns(index)
        query = (body or {}).get("query") or {"match_all": {}}
        result = self.search(
            index=ns, body={"query": query, "size": 0, "track_total_hits": True}
        )
        return {
            "count": result["hits"]["total"]["value"],
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        }

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
        if not query:
            raise self._unsupported("delete_by_query without a query")
        # The whole query has to translate as a filter. A clause that cannot
        # be expressed raises: leaving it out would delete documents the
        # caller meant to keep.
        filter_clause = self._to_filter(query)
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
        """Make every write to ``index`` searchable now (one ``/flush``)."""
        self._client._flush(self._client._resolve_ns(index))
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
