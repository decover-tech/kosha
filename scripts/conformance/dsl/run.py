#!/usr/bin/env python3
"""OpenSearch / Elasticsearch DSL compatibility matrix, checked live.

Kosha has no server-side OpenSearch endpoint. OpenSearch and Elasticsearch
request bodies are translated on the client by kosha-client (Python,
``clients/python``) into the native ``/v1`` API. This runner checks every
case in ``cases.yaml`` against that path and, when all of them hold, writes
the compatibility matrix:

  docs/compat/opensearch.md     the matrix, for people
  docs/compat/opensearch.json   the same rows, for tools

For each DSL case it translates ``es_body`` with
``KoshaClient._translate_search`` and then:

  supported  the translation succeeds, ``POST /v1/namespaces/{ns}/search``
             answers 200, the case's ``expect`` checks hold and, when the
             translated body is one the reference model covers (text without
             ``fields``, term/terms/range/bool filters, sort, paging), the
             ids, totals and BM25 scores equal ``oracle.py``'s exactly
  partial    the same, and the ``expect`` checks pin the stated divergence
  rejected   the client raises ``KoshaRequestError(400, "unsupported by
             Kosha: ...")`` and sends nothing, or (``rejected_by: server``)
             the server answers 4xx

Index-API cases (``category: index_api``) drive the named KoshaClient method
against the same server; each has a check function in ``API_CHECKS``.

A case whose outcome does not hold fails the run, and the matrix is not
written. The matrix depends only on ``cases.yaml``, never on timings or ids
from the run, so CI regenerates it and fails on any diff
(``.github/workflows/conformance.yml``).

Usage:

  cargo build --release -p kosha-server
  python3 scripts/conformance/dsl/run.py --spawn target/release/kosha-server

  # Against a running server (the docker compose stack, as in CI):
  python3 scripts/conformance/dsl/run.py --host http://localhost:8080 \\
      --api-key sk-kosha-dev

  # Translation checks only, no server (fast; what `make compat-matrix` runs
  # when no server is given):
  python3 scripts/conformance/dsl/run.py --offline

Needs PyYAML (``pip install pyyaml``) and kosha-client, which is imported
from ``clients/python`` in this checkout.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import random
import sys
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
CONFORMANCE = os.path.dirname(HERE)
ROOT = os.path.dirname(os.path.dirname(CONFORMANCE))
CASES_PATH = os.path.join(HERE, "cases.yaml")
DEFAULT_MATRIX = os.path.join(ROOT, "docs", "compat", "opensearch.md")

sys.path.insert(0, CONFORMANCE)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

import corpus  # noqa: E402
from oracle import Oracle, cursor_for  # noqa: E402

from kosha_client import KoshaClient, KoshaRequestError  # noqa: E402


def _load_conformance_run():
    """``scripts/conformance/run.py`` (its HTTP helper and spawned server).

    Loaded by path: this file is also called ``run.py``.
    """
    spec = importlib.util.spec_from_file_location("conformance_run", os.path.join(CONFORMANCE, "run.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CATEGORIES = ("query", "filter", "aggregation", "sort", "paging", "body_key", "index_api")
CATEGORY_TITLES = {
    "query": "Query DSL",
    "filter": "Filters (bool filter context)",
    "aggregation": "Aggregations",
    "sort": "Sort",
    "paging": "Paging and totals",
    "body_key": "Top-level search body keys",
    "index_api": "Index and document APIs",
}
OUTCOMES = ("supported", "partial", "rejected")
CASE_KEYS = {
    "id", "category", "construct", "kinds", "outcome", "note", "divergence",
    "es_body", "api", "rejected_by", "live", "live_reason", "expect",
}
EXPECT_KEYS = {
    "translated", "translated_absent", "hits", "min_hits", "max_hits",
    "response_nonempty", "response_len", "hits_where", "distinct", "oracle",
}

SEED = 1
DOCS = 400
VECTOR_DIMS = 8
SCORE_REL_TOL = 1e-6

# Translated native bodies the reference model can answer exactly.
ORACLE_KEYS = {
    "query_text", "operator", "max_results", "from", "filter", "sort",
    "search_after", "exact_total_hits", "no_cache",
}
ORACLE_FILTERS = {"term", "terms", "range", "bool", "match_all"}


# ─── cases ──────────────────────────────────────────────────────────────────


def load_cases(path: str = CASES_PATH) -> List[Dict]:
    """Read and validate ``cases.yaml``. Raises ``ValueError`` on a bad case."""
    try:
        import yaml
    except ImportError:
        raise SystemExit("PyYAML is required: pip install pyyaml") from None

    with open(path, encoding="utf-8") as f:
        cases = yaml.safe_load(f)
    if not isinstance(cases, list):
        raise ValueError("%s: expected a list of cases" % path)
    seen = set()
    for case in cases:
        cid = case.get("id")
        problems = []
        if not cid or cid in seen:
            problems.append("missing or duplicate id")
        seen.add(cid)
        unknown = set(case) - CASE_KEYS
        if unknown:
            problems.append("unknown keys %s" % sorted(unknown))
        if case.get("category") not in CATEGORIES:
            problems.append("category must be one of %s" % (CATEGORIES,))
        if case.get("outcome") not in OUTCOMES:
            problems.append("outcome must be one of %s" % (OUTCOMES,))
        for key in ("construct", "note"):
            if not isinstance(case.get(key), str) or not case[key].strip():
                problems.append("missing %s" % key)
        if case.get("outcome") == "partial" and not case.get("divergence"):
            problems.append("a partial case needs a divergence")
        if case.get("outcome") != "partial" and case.get("divergence"):
            problems.append("only a partial case has a divergence")
        if case.get("category") == "index_api":
            if not case.get("api") or "es_body" in case:
                problems.append("an index_api case has `api` and no `es_body`")
        elif not isinstance(case.get("es_body"), dict) or "api" in case:
            problems.append("a DSL case has an `es_body` object and no `api`")
        if case.get("rejected_by", "client") not in ("client", "server"):
            problems.append("rejected_by must be client or server")
        if "rejected_by" in case and case.get("outcome") != "rejected":
            problems.append("rejected_by only applies to a rejected case")
        if case.get("live", True) is False and not case.get("live_reason"):
            problems.append("live: false needs a live_reason")
        unknown = set(case.get("expect") or {}) - EXPECT_KEYS
        if unknown:
            problems.append("unknown expect keys %s" % sorted(unknown))
        kinds = case.get("kinds") or []
        if not isinstance(kinds, list):
            problems.append("kinds must be a list")
        elif "es_body" in case:
            keys = _all_keys(case["es_body"])
            missing = [k for k in kinds if k not in keys]
            if missing:
                problems.append("kinds %s do not appear in es_body" % missing)
        if problems:
            raise ValueError("%s: case %r: %s" % (path, cid, "; ".join(problems)))
    return cases


def _all_keys(value: Any) -> set:
    out: set = set()
    if isinstance(value, dict):
        for k, v in value.items():
            out.add(k)
            out |= _all_keys(v)
    elif isinstance(value, list):
        for v in value:
            out |= _all_keys(v)
    return out


_MISSING = object()


def _get(value: Any, path: str) -> Any:
    """``value`` at a dotted path (``a.b.0.c``), or ``_MISSING``."""
    for part in path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return _MISSING
    return value


# ─── checks ─────────────────────────────────────────────────────────────────


class CaseFailed(Exception):
    pass


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise CaseFailed(message)


def check_translation(case: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """Translate the case and check what translation alone can show.

    Returns ``(translated body, None)``, or ``(None, error message)`` for a
    case the client rejects.
    """
    body = case["es_body"]
    try:
        translated, _ = KoshaClient._translate_search(json.loads(json.dumps(body)))
    except KoshaRequestError as e:
        _require(
            case["outcome"] == "rejected" and case.get("rejected_by", "client") == "client",
            "the client refused it: %s" % e.error,
        )
        _require(e.status_code == 400 and "unsupported by Kosha" in str(e.error), "refused with %s %s" % (e.status_code, e.error))
        return None, str(e.error)
    _require(
        not (case["outcome"] == "rejected" and case.get("rejected_by", "client") == "client"),
        "expected the client to refuse it, but it translated to %s" % json.dumps(translated, sort_keys=True),
    )
    expect = case.get("expect") or {}
    for path, want in (expect.get("translated") or {}).items():
        got = _get(translated, path)
        _require(got == want, "translated %s is %s, want %s" % (path, "missing" if got is _MISSING else json.dumps(got), json.dumps(want)))
    for path in expect.get("translated_absent") or []:
        _require(_get(translated, path) is _MISSING, "translated %s should be absent: %s" % (path, json.dumps(translated)))
    return translated, None


def _filter_kinds(flt: Any) -> set:
    if not isinstance(flt, dict) or len(flt) != 1:
        return {"?"}
    (kind, spec), = flt.items()
    out = {kind}
    if kind == "bool":
        for key in ("must", "must_not", "should"):
            for child in spec.get(key) or []:
                out |= _filter_kinds(child)
    return out


def oracle_able(case: Dict, translated: Dict) -> bool:
    if (case.get("expect") or {}).get("oracle") is False:
        return False
    if set(translated) - ORACLE_KEYS:
        return False
    if "filter" in translated and not _filter_kinds(translated["filter"]) <= ORACLE_FILTERS:
        return False
    return bool(translated.get("query_text") or translated.get("filter"))


def oracle_page(oracle: Oracle, body: Dict) -> Tuple[List, List]:
    """``(every hit, the requested page)`` from the reference model."""
    hits = oracle.hits(body)
    after = body.get("search_after")
    if after:
        sort = body.get("sort") or []
        # String-valued sort keys only (_id, keywords), which is all the
        # cases use.
        hits_after = [h for h in hits if (cursor_for(h, sort) or []) > list(after)]
        start = 0
    else:
        hits_after = hits
        start = body.get("from", 0)
    size = body.get("max_results", 10)
    return hits, hits_after[start : start + size]


class Live:
    """The server side of the run: a seeded namespace and a KoshaClient."""

    def __init__(self, kosha, host: str, api_key: str):
        self.kosha = kosha
        self.host = host
        self.api_key = api_key
        self.oracle = Oracle()

    def seed(self) -> None:
        docs = corpus.batch(SEED, 0, DOCS)
        for doc in docs:
            rng = random.Random("emb/%s" % doc["id"])
            vector = [round(rng.uniform(-1, 1), 4) for _ in range(VECTOR_DIMS)]
            doc["fields"].append({"name": "emb", "field_type": "Vector", "value": json.dumps(vector)})
        self.kosha.index(docs)
        self.kosha.flush()
        self.oracle.index(docs)
        self.oracle.flush()

    def search(self, translated: Dict, exact: bool):
        body = dict(translated)
        body.setdefault("query_text", "")
        body["no_cache"] = True
        # Everything is flushed; ask for the indexed view the oracle models.
        body.setdefault("consistency", "eventual")
        if exact:
            body.setdefault("exact_total_hits", True)
        return self.kosha.call("POST", self.kosha.ns_path("search"), body)

    def client(self, **kwargs) -> KoshaClient:
        return KoshaClient(hosts=self.host, api_key=self.api_key, **kwargs)


def check_live(case: Dict, translated: Dict, live: Live) -> str:
    use_oracle = oracle_able(case, translated)
    status, out = live.search(translated, exact=use_oracle)
    if case["outcome"] == "rejected":
        _require(400 <= status < 500, "expected the server to refuse it, got HTTP %s" % status)
        return "server refused it: HTTP %s %s" % (status, json.dumps(out)[:200])
    _require(status == 200, "HTTP %s %s" % (status, json.dumps(out)[:300]))
    expect = case.get("expect") or {}
    hits = out.get("results") or []
    total = out.get("total_hits")
    if "hits" in expect:
        _require(total == expect["hits"], "total_hits %s, want %s" % (total, expect["hits"]))
    if "min_hits" in expect:
        _require((total or 0) >= expect["min_hits"], "total_hits %s, want at least %s" % (total, expect["min_hits"]))
    if "max_hits" in expect:
        _require((total or 0) <= expect["max_hits"], "total_hits %s, want at most %s" % (total, expect["max_hits"]))
    for path in expect.get("response_nonempty") or []:
        got = _get(out, path)
        _require(got is not _MISSING and got not in (None, [], {}), "response %s is empty or missing" % path)
    for path, n in (expect.get("response_len") or {}).items():
        got = _get(out, path)
        _require(isinstance(got, list) and len(got) == n, "response %s has %s entries, want %d" % (path, "no" if got is _MISSING else len(got), n))
    for field, want in (expect.get("hits_where") or {}).items():
        for h in hits:
            values = [f["value"] for f in h.get("fields", []) if f["name"] == field]
            _require(want in values, "hit %s has %s=%s, want %s" % (h.get("doc_id"), field, values, want))
    if expect.get("distinct"):
        field = expect["distinct"]
        values = [tuple(f["value"] for f in h.get("fields", []) if f["name"] == field) for h in hits]
        _require(len(values) == len(set(values)), "hits repeat a %s value: %s" % (field, values))
    if not use_oracle:
        return "200, %s hits" % total
    every, page = oracle_page(live.oracle, translated)
    _require(total == len(every), "total_hits %s, reference model %d" % (total, len(every)))
    got_ids = [h["doc_id"] for h in hits]
    want_ids = [h[0] for h in page]
    _require(got_ids == want_ids, "ids %s, reference model %s" % (got_ids[:5], want_ids[:5]))
    by_id = {h[0]: h[1] for h in every}
    for h in hits:
        want = by_id[h["doc_id"]]
        _require(
            abs(h["score"] - want) <= SCORE_REL_TOL * max(1.0, abs(h["score"]), abs(want)),
            "score of %s is %.9g, reference model %.9g" % (h["doc_id"], h["score"], want),
        )
    return "200, %s hits, equal to the reference model" % total


# ─── index API checks ───────────────────────────────────────────────────────


class ApiContext:
    def __init__(self, live: Live, prefix: str):
        self.live = live
        self.prefix = prefix
        self.client = live.client(consistency="strong")

    def ns(self, name: str) -> str:
        return "%s-%s" % (self.prefix, name)


def _fresh(ctx: ApiContext, name: str, mappings: Optional[Dict] = None) -> str:
    ns = ctx.ns(name)
    ctx.client.indices.delete(ns, ignore_unavailable=True)
    body = {"mappings": mappings} if mappings else None
    ctx.client.indices.create(ns, body=body)
    return ns


MAPPINGS = {
    "properties": {
        "title": {"type": "text"},
        "status": {"type": "keyword"},
        "priority": {"type": "long"},
        "author": {"properties": {"name": {"type": "text"}}},
    }
}


def api_indices_create(ctx: ApiContext) -> str:
    ns = ctx.ns("create")
    ctx.client.indices.delete(ns, ignore_unavailable=True)
    out = ctx.client.indices.create(ns, body={"settings": {"number_of_shards": 3}, "mappings": MAPPINGS})
    _require(out.get("acknowledged") is True, "create returned %s" % out)
    props = ctx.client.indices.get_mapping(ns)[ns]["mappings"]["properties"]
    want = {"title": {"type": "text"}, "status": {"type": "keyword"}, "priority": {"type": "long"}}
    _require(props == want, "mapping after create is %s, want %s (the object field left out)" % (props, want))
    try:
        ctx.client.indices.create(ns)
        raise CaseFailed("creating an existing index did not fail")
    except KoshaRequestError as e:
        _require(e.status_code == 409, "creating an existing index answered %s" % e.status_code)
    return "schema from mappings; object field and settings left out; 409 on re-create"


def api_indices_exists(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "exists")
    _require(ctx.client.indices.exists(ns) is True, "exists is not True for a created index")
    _require(ctx.client.indices.exists(ctx.ns("never-created")) is False, "exists is not False for a missing index")
    return "True / False"


def api_indices_get_mapping(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "mapping", MAPPINGS)
    got = ctx.client.indices.get(ns)[ns]["mappings"]["properties"]
    _require(got.get("status") == {"type": "keyword"}, "indices.get mapping is %s" % got)
    bare = _fresh(ctx, "mapping-bare")
    props = ctx.client.indices.get_mapping(bare)[bare]["mappings"]["properties"]
    _require(props == {}, "a namespace without a schema has properties %s" % props)
    return "round trip; empty without a schema"


def api_indices_get_settings(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "settings")
    got = ctx.client.indices.get_settings(ns)
    _require(got == {ns: {"settings": {"index": {"provided_name": ns}}}}, "get_settings returned %s" % got)
    return "index name only"


def api_indices_put_mapping(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "put-mapping", MAPPINGS)
    before = ctx.client.indices.get_mapping(ns)
    ack = ctx.client.indices.put_mapping(ns, body={"properties": {"extra": {"type": "keyword"}}})
    _require(ack == {"acknowledged": True}, "put_mapping returned %s" % ack)
    _require(ctx.client.indices.put_settings(ns, body={"index": {"refresh_interval": "1s"}}) == {"acknowledged": True}, "put_settings not acknowledged")
    _require(ctx.client.indices.get_mapping(ns) == before, "put_mapping changed the mapping")
    return "acknowledged, mapping unchanged"


def _count(client: KoshaClient, ns: str, query: Optional[Dict] = None) -> int:
    return client.count(index=ns, body={"query": query} if query else {"query": {"exists": {"field": "status"}}})["count"]


def api_indices_refresh(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "refresh")
    eventual = ctx.live.client(consistency="eventual")
    eventual.index(index=ns, id="r1", body={"title": "refresh me", "status": "open"})
    out = eventual.indices.refresh(ns)
    _require(out["_shards"]["failed"] == 0, "refresh returned %s" % out)
    _require(_count(eventual, ns) == 1, "the document is not searchable (eventual) after refresh")
    return "visible to an eventual search after refresh"


def api_indices_flush(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "flush")
    out = ctx.client.indices.flush(ns)
    _require(out["_shards"]["failed"] == 0, "flush returned %s" % out)
    closed = ctx.client.indices.close(ns)
    _require(closed.get("acknowledged") is True, "close returned %s" % closed)
    _require(ctx.client.indices.exists(ns), "close removed the index")
    return "acknowledged no-ops"


def api_indices_delete(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "delete")
    _require(ctx.client.indices.delete(ns) == {"acknowledged": True}, "delete not acknowledged")
    _require(ctx.client.indices.exists(ns) is False, "index still exists after delete")
    try:
        ctx.client.indices.delete(ns)
        raise CaseFailed("deleting a missing index did not fail")
    except KoshaRequestError as e:
        _require(e.status_code == 404, "deleting a missing index answered %s" % e.status_code)
    ctx.client.indices.delete(ns, ignore_unavailable=True)
    return "deleted; 404 when missing unless ignore_unavailable"


def api_index(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "index")
    out = ctx.client.index(index=ns, id="a", body={"title": "first", "status": "open", "priority": 2}, refresh=True)
    _require(out["_id"] == "a", "index returned %s" % out)
    ctx.client.index(index=ns, id="a", body={"title": "second", "status": "closed", "priority": 3}, refresh=True)
    got = ctx.client.get(index=ns, id="a")
    _require(got["found"] and got["_source"].get("title") == "second", "get after upsert returned %s" % got)
    _require(_count(ctx.client, ns) == 1, "upsert left two documents")
    return "upsert by id, visible on return"


def api_bulk(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "bulk")
    body = []
    for i in range(5):
        body += [{"index": {"_id": "b%d" % i}}, {"title": "bulk %d" % i, "status": "open"}]
    body += [{"update": {"_id": "b0"}}, {"doc": {"status": "closed"}}]
    body += [{"delete": {"_id": "b1"}}]
    out = ctx.client.bulk(body=body, index=ns, refresh=True)
    errors = [item for item in out["items"] if "delete" in item]
    _require(out["errors"] is True and len(errors) == 1 and errors[0]["delete"]["status"] == 501, "bulk items %s" % out["items"])
    _require(_count(ctx.client, ns) == 5, "bulk did not index 5 documents (the delete must not apply)")
    _require(_count(ctx.client, ns, {"term": {"status": "closed"}}) == 1, "bulk update did not merge")
    return "index + update applied; delete item has status 501"


def api_get(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "get")
    ctx.client.index(index=ns, id="g1", body={"title": "get me", "status": "open"}, refresh=True)
    got = ctx.client.get(index=ns, id="g1")
    _require(got["found"] is True and got["_source"]["status"] == "open", "get returned %s" % got)
    missing = ctx.client.get(index=ns, id="nope")
    _require(missing["found"] is False, "get of a missing id returned %s" % missing)
    many = ctx.client.mget(index=ns, body={"ids": ["g1", "nope"]})
    _require([d["found"] for d in many["docs"]] == [True, False], "mget returned %s" % many)
    return "found / found:false (no NotFoundError)"


def api_count(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "count")
    ctx.client.bulk(
        body=[x for i in range(6) for x in ({"index": {"_id": "c%d" % i}}, {"status": "open" if i % 2 else "closed"})],
        index=ns,
        refresh=True,
    )
    _require(_count(ctx.client, ns) == 6, "count of all is not 6")
    _require(_count(ctx.client, ns, {"term": {"status": "open"}}) == 3, "count with a query is not 3")
    bare = ctx.client.count(index=ns)["count"]
    _require(bare == 0, "count without a query is %s; the divergence no longer holds, update the case" % bare)
    every = _count(ctx.client, ns, {"bool": {"filter": [{"match_all": {}}]}})
    _require(every == 6, "count with a match_all filter is %s, want 6" % every)
    return "exact with a query; 0 without one"


def api_msearch(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "msearch")
    ctx.client.index(index=ns, id="m1", body={"title": "alpha", "status": "open"}, refresh=True)
    out = ctx.client.msearch(
        index=ns,
        body=[{}, {"query": {"match": {"title": "alpha"}}}, {}, {"query": {"fuzzy": {"title": "alpha"}}}],
    )
    statuses = [r["status"] for r in out["responses"]]
    _require(statuses == [200, 400], "msearch statuses %s" % statuses)
    _require(out["responses"][0]["hits"]["total"]["value"] == 1, "msearch first response %s" % out["responses"][0])
    return "one response per search; a refused one is an error item"


def api_search(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "search")
    ctx.client.index(index=ns, id="s1", body={"title": "breach of contract", "status": "open"}, refresh=True)
    out = ctx.client.search(index=ns, body={"query": {"bool": {"must": [{"match": {"title": "breach"}}], "filter": [{"term": {"status": "open"}}]}}})
    hits = out["hits"]["hits"]
    _require([h["_id"] for h in hits] == ["s1"] and hits[0]["_source"]["status"] == "open", "search returned %s" % out["hits"])
    return "OpenSearch-shaped response"


def api_update(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "update")
    ctx.client.index(index=ns, id="u1", body={"title": "keep me", "status": "open"}, refresh=True)
    ctx.client.update(index=ns, id="u1", body={"doc": {"status": "closed"}})
    ctx.client.indices.refresh(ns)
    got = ctx.client.get(index=ns, id="u1")["_source"]
    _require(got.get("status") == "closed" and got.get("title") == "keep me", "after update: %s" % got)
    return "partial doc merged"


def api_delete_by_query(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "dbq")
    ctx.client.bulk(
        body=[x for i in range(4) for x in ({"index": {"_id": "x%d" % i}}, {"title": "w %d" % i, "status": "open" if i < 3 else "closed"})],
        index=ns,
        refresh=True,
    )
    out = ctx.client.delete_by_query(index=ns, body={"query": {"term": {"status": "open"}}})
    _require(out["deleted"] == 3, "delete_by_query returned %s" % out)
    _require(_count(ctx.client, ns) == 1, "documents left after delete_by_query")
    try:
        ctx.client.delete_by_query(index=ns, body={"query": {"match": {"title": "w"}}})
        raise CaseFailed("a text query was not refused")
    except KoshaRequestError as e:
        _require(e.status_code == 400, "a text query answered %s" % e.status_code)
    return "filter queries delete; text queries refused"


def api_update_by_query(ctx: ApiContext) -> str:
    ns = _fresh(ctx, "ubq")
    ctx.client.bulk(
        body=[x for i in range(3) for x in ({"index": {"_id": "y%d" % i}}, {"title": "t %d" % i, "status": "open"})],
        index=ns,
        refresh=True,
    )
    out = ctx.client.update_by_query(
        index=ns,
        body={"query": {"term": {"status": "open"}}, "script": {"source": "ctx._source.status = 'closed'"}},
    )
    _require(out.get("updated") == 3, "update_by_query returned %s" % out)
    task = ctx.client.update_by_query(
        index=ns,
        body={"query": {"term": {"status": "closed"}}, "script": {"source": "ctx._source.status = 'archived'"}},
        wait_for_completion=False,
    )["task"]
    deadline = time.time() + 30
    status = ctx.client.tasks.get(task_id=task)
    while not status.get("completed") and time.time() < deadline:
        time.sleep(0.2)
        status = ctx.client.tasks.get(task_id=task)
    _require(status.get("completed") and "error" not in status, "task ended as %s" % status)
    try:
        ctx.client.update_by_query(index=ns, body={"query": {"term": {"status": "x"}}, "script": {"source": "ctx._source.n += 1"}})
        raise CaseFailed("an unsupported script was not refused")
    except NotImplementedError:
        pass
    return "simple assignment, task polling; other scripts refused"


def api_scroll(ctx: ApiContext) -> str:
    try:
        ctx.client.scroll(scroll_id="x")
        raise CaseFailed("scroll did not raise")
    except NotImplementedError:
        pass
    ctx.client.clear_scroll(scroll_id="x")
    return "NotImplementedError; clear_scroll no-op"


def api_ping(ctx: ApiContext) -> str:
    _require(ctx.client.ping() is True, "ping is not True")
    _require(KoshaClient(hosts="http://127.0.0.1:9", timeout=2).ping() is False, "ping of a closed port is not False")
    return "True / False"


# ES client methods that KoshaClient does not have.
NOT_IMPLEMENTED = (
    "delete", "exists", "create", "reindex", "search_template", "open_point_in_time",
    "explain", "termvectors", "field_caps",
)
NOT_IMPLEMENTED_INDICES = ("analyze", "put_alias", "get_alias", "update_aliases", "open", "forcemerge", "stats")


def api_not_implemented(ctx: Optional[ApiContext]) -> str:
    from kosha_client.client import IndexOps

    present = [m for m in NOT_IMPLEMENTED if hasattr(KoshaClient, m)]
    present += ["indices." + m for m in NOT_IMPLEMENTED_INDICES if hasattr(IndexOps, m)]
    _require(not present, "now implemented, update the case: %s" % present)
    return "absent"


API_CHECKS: Dict[str, Callable[[ApiContext], str]] = {
    "indices.create": api_indices_create,
    "indices.exists": api_indices_exists,
    "indices.get_mapping": api_indices_get_mapping,
    "indices.get_settings": api_indices_get_settings,
    "indices.put_mapping": api_indices_put_mapping,
    "indices.refresh": api_indices_refresh,
    "indices.flush": api_indices_flush,
    "indices.delete": api_indices_delete,
    "index": api_index,
    "bulk": api_bulk,
    "get": api_get,
    "count": api_count,
    "msearch": api_msearch,
    "search": api_search,
    "update": api_update,
    "delete_by_query": api_delete_by_query,
    "update_by_query": api_update_by_query,
    "scroll": api_scroll,
    "ping": api_ping,
    "not_implemented": api_not_implemented,
}


# ─── running ────────────────────────────────────────────────────────────────


def run_case(case: Dict, live: Optional[Live], api_ctx: Optional[ApiContext]) -> Tuple[str, str]:
    """``(result, detail)``; result is pass, fail or translation-only."""
    try:
        if case["category"] == "index_api":
            check = API_CHECKS.get(case["api"])
            _require(check is not None, "no API_CHECKS entry for %r" % case["api"])
            if case["api"] == "not_implemented":
                return "pass", check(api_ctx)
            if api_ctx is None:
                return "translation-only", "needs a server"
            return "pass", check(api_ctx)
        translated, refused = check_translation(case)
        if refused is not None:
            return "pass", "client refused it: %s" % refused
        if live is None or case.get("live", True) is False:
            return "translation-only", case.get("live_reason") or "offline"
        return "pass", check_live(case, translated, live)
    except CaseFailed as e:
        return "fail", str(e)
    except Exception as e:  # a crash in a check is a failure, with the trace
        return "fail", "%s: %s\n%s" % (type(e).__name__, e, traceback.format_exc(limit=3))


def run(cases: List[Dict], live: Optional[Live], api_ctx: Optional[ApiContext]) -> List[Dict]:
    rows = []
    for case in cases:
        result, detail = run_case(case, live, api_ctx)
        rows.append({"id": case["id"], "result": result, "detail": detail})
        mark = {"pass": "ok  ", "fail": "FAIL", "translation-only": "xlat"}[result]
        print("  %s %-38s %-9s %s" % (mark, case["id"], case["outcome"], detail.splitlines()[0][:110] if detail else ""))
    return rows


# ─── rendering ──────────────────────────────────────────────────────────────

DESCRIBES = (
    "kosha-client (Python): its translation of OpenSearch / Elasticsearch "
    "request bodies into Kosha's native /v1 API, and how that API answers "
    "them. Kosha has no server-side OpenSearch endpoint (_search, _bulk). "
    "The TypeScript and Go SDKs send native /v1 bodies only and do not "
    "translate the DSL."
)


def verification(case: Dict) -> str:
    """How the run checks a case; a function of the case alone, so the
    rendered matrix does not depend on the run.

    exact   live, and ids, totals and BM25 scores equal the reference model
    live    live, with the case's own checks
    client  in the client only: refusals, rows not run live, absent methods
    """
    if case["category"] == "index_api":
        return "client" if case["api"] == "not_implemented" else "live"
    if case.get("live", True) is False:
        return "client"
    try:
        translated, _ = KoshaClient._translate_search(json.loads(json.dumps(case["es_body"])))
    except KoshaRequestError:
        return "client"
    return "exact" if oracle_able(case, translated) else "live"


def _cell(text: str) -> str:
    return " ".join(str(text).split()).replace("|", "\\|")


def render_json(cases: List[Dict]) -> str:
    out = {
        "describes": DESCRIBES,
        "generated_from": "scripts/conformance/dsl/cases.yaml",
        "generated_by": "scripts/conformance/dsl/run.py",
        "summary": _summary(cases),
        "cases": [],
    }
    for case in cases:
        row = {
            "id": case["id"],
            "category": case["category"],
            "construct": case["construct"],
            "outcome": case["outcome"],
            "note": " ".join(case["note"].split()),
        }
        if case.get("divergence"):
            row["divergence"] = " ".join(case["divergence"].split())
        if case["outcome"] == "rejected":
            row["rejected_by"] = case.get("rejected_by", "client")
        row["verified"] = verification(case)
        if "es_body" in case:
            row["es_body"] = case["es_body"]
        else:
            row["api"] = case["api"]
        out["cases"].append(row)
    return json.dumps(out, indent=2, sort_keys=False, ensure_ascii=False) + "\n"


def _summary(cases: List[Dict]) -> Dict[str, Dict[str, int]]:
    summary: Dict[str, Dict[str, int]] = {}
    for category in CATEGORIES:
        mine = [c for c in cases if c["category"] == category]
        summary[category] = {o: sum(1 for c in mine if c["outcome"] == o) for o in OUTCOMES}
    return summary


def render_markdown(cases: List[Dict]) -> str:
    lines = [
        "# OpenSearch / Elasticsearch compatibility",
        "",
        "<!-- GENERATED FILE: do not edit by hand. Source: scripts/conformance/dsl/cases.yaml.",
        "     Regenerate with `make compat-matrix`; CI fails when this file is stale. -->",
        "",
        "**What this describes:** kosha-client (Python) and the native `/v1` API. Kosha has no",
        "server-side OpenSearch endpoint (`_search`, `_bulk`). `KoshaClient` (`clients/python`)",
        "translates OpenSearch and Elasticsearch request bodies into native `/v1` requests, and this",
        "matrix says what that translation does with each construct. The TypeScript and Go SDKs send",
        "native `/v1` bodies only; they do not translate the DSL, so this matrix does not apply to them.",
        "",
        "Every row is a case in [`scripts/conformance/dsl/cases.yaml`](../../scripts/conformance/dsl/cases.yaml).",
        "The conformance workflow translates each case with the client, sends it to a live server",
        "built from the same commit, checks the outcome below (and, where the reference model covers",
        "it, the exact ids, totals and BM25 scores), and fails if this page is out of date.",
        "Machine-readable copy: [`opensearch.json`](opensearch.json).",
        "",
        "| Status | Meaning |",
        "| --- | --- |",
        "| supported | Translated, and answered as Elasticsearch answers it: the same matching documents, with Kosha's BM25 ranking. |",
        "| partial | Runs, with the divergence stated in the row. |",
        "| rejected | Refused with HTTP 400 `unsupported by Kosha: ...` before anything is sent (*client*), or refused by the server (*server*). A clause is never dropped silently except where a partial row says so. |",
        "",
        "| Checked | How CI checks the row |",
        "| --- | --- |",
        "| exact | Run against a live server; ids, total hits and BM25 scores equal the reference model (`scripts/conformance/oracle.py`) exactly. |",
        "| live | Run against a live server, with the row's own checks (the translated body, hit counts, response fields, the divergence where it can be observed). |",
        "| client | Checked in the client only: refusals happen before anything is sent; other rows need something CI does not run, such as an embedding model. |",
        "",
        "Migrating from Elasticsearch or OpenSearch: see [the migration guide](../migration/from-elasticsearch.md).",
        "",
        "## Summary",
        "",
        "| Area | Supported | Partial | Rejected |",
        "| --- | ---: | ---: | ---: |",
    ]
    summary = _summary(cases)
    for category in CATEGORIES:
        s = summary[category]
        lines.append("| [%s](#%s) | %d | %d | %d |" % (CATEGORY_TITLES[category], _anchor(CATEGORY_TITLES[category]), s["supported"], s["partial"], s["rejected"]))
    for category in CATEGORIES:
        lines += ["", "## %s" % CATEGORY_TITLES[category], "", "| Construct | Status | Notes | Checked | Case |", "| --- | --- | --- | --- | --- |"]
        for case in (c for c in cases if c["category"] == category):
            status = case["outcome"]
            if status == "rejected":
                status += " (%s)" % case.get("rejected_by", "client")
            notes = _cell(case["note"])
            if case.get("divergence"):
                notes += " **Divergence:** " + _cell(case["divergence"])
            if case.get("live", True) is False:
                notes += " *Not run live: %s.*" % _cell(case["live_reason"])
            lines.append("| %s | %s | %s | %s | `%s` |" % (_cell(case["construct"]), status, notes, verification(case), case["id"]))
    return "\n".join(lines) + "\n"


def _anchor(title: str) -> str:
    return "".join(ch for ch in title.lower().replace(" ", "-") if ch.isalnum() or ch == "-")


def write_outputs(cases: List[Dict], matrix_path: str, json_path: str) -> None:
    for path, text in ((matrix_path, render_markdown(cases)), (json_path, render_json(cases))):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        print("wrote %s" % os.path.relpath(path, ROOT))


# ─── main ───────────────────────────────────────────────────────────────────


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--spawn", metavar="BINARY", help="start this kosha-server binary with a throwaway data dir")
    target.add_argument("--host", help="URL of an already-running server")
    target.add_argument("--offline", action="store_true", help="translation checks only; no server")
    p.add_argument("--api-key", default=os.environ.get("KOSHA_API_KEY", "sk-conformance"))
    p.add_argument("--admin-key", default=os.environ.get("KOSHA_ADMIN_API_KEY", "sk-conformance-admin"))
    p.add_argument("--port", type=int, default=18081, help="port for --spawn")
    p.add_argument("--namespace", help="default: dsl-<unix time>")
    p.add_argument("--matrix-out", default=DEFAULT_MATRIX, help="Markdown matrix (default docs/compat/opensearch.md)")
    p.add_argument("--json-out", help="JSON matrix (default: --matrix-out with .json)")
    p.add_argument("--no-write", action="store_true", help="check the cases, write nothing")
    p.add_argument("--out", help="write a JSON report of the run here")
    p.add_argument("--keep", action="store_true", help="keep the --spawn data dir and log")
    args = p.parse_args(argv)
    json_out = args.json_out or os.path.splitext(args.matrix_out)[0] + ".json"

    # The index-API checks provoke 404s and 409s on purpose; the client logs
    # each one as a warning.
    logging.getLogger("kosha_client").setLevel(logging.ERROR)
    cases = load_cases()
    print("%d cases from %s" % (len(cases), os.path.relpath(CASES_PATH, ROOT)))
    spawned = None
    live = api_ctx = None
    try:
        if not args.offline:
            helpers = _load_conformance_run()
            ns = args.namespace or "dsl-%d" % int(time.time())
            if args.spawn:
                spawned = helpers.Spawned(args.spawn, args.port, args.api_key, args.admin_key, None)
                spawned.start()
                host = "http://127.0.0.1:%d" % args.port
            else:
                host = args.host.rstrip("/")
            kosha = helpers.Kosha(host, args.api_key, ns, args.admin_key, "default")
            kosha.wait_healthy()
            live = Live(kosha, host, args.api_key)
            live.seed()
            api_ctx = ApiContext(live, ns)
            print("namespace %s: %d documents, one segment" % (ns, DOCS))
        rows = run(cases, live, api_ctx)
    finally:
        if spawned:
            spawned.cleanup(args.keep)

    failed = [r for r in rows if r["result"] == "fail"]
    counts = {k: sum(1 for r in rows if r["result"] == k) for k in ("pass", "fail", "translation-only")}
    print("\nTOTAL %(pass)d pass, %(fail)d fail, %(translation-only)d translation only" % counts)
    if args.out:
        with open(args.out, "w") as f:
            json.dump({"summary": counts, "cases": rows}, f, indent=1)
    if failed:
        print("\nFAILED (the matrix was not written):")
        for r in failed:
            print("  %s: %s" % (r["id"], r["detail"]))
        return 1
    if not args.no_write:
        write_outputs(cases, args.matrix_out, json_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
