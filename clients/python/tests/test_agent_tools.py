"""AgentTools (#584): request bodies and budgeted, agent-shaped results."""

import pytest

from kosha_client import AgentTools, KoshaClient, KoshaConfigError


class FakeTransport:
    def __init__(self, response=None):
        self.calls = []
        self.response = response or {}

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        self.calls.append((method, path, body))
        return self.response


def tools(response=None, **kw):
    c = KoshaClient("http://kosha.test", api_key="k")
    c._transport = FakeTransport(response)
    return AgentTools(c, namespace="docs", **kw), c._transport


HIT = {
    "doc_id": "d1",
    "score": 1.5,
    "fields": [
        {"name": "path", "field_type": "Keyword", "value": "/contracts/2024/a.txt"},
        {"name": "body", "field_type": "Text", "value": "x" * 50},
        {"name": "year", "field_type": "Integer", "value": "2024"},
        {"name": "final", "field_type": "Boolean", "value": "true"},
        {"name": "emb", "field_type": "Vector", "value": "[0.1, 0.2]"},
    ],
}


def test_search_body_and_hit_shape():
    t, tr = tools({"results": [HIT], "total_hits": 7, "total_hits_relation": "eq"}, snippet_chars=10)
    out = t.search("indemnity", filter={"term": {"type": "nda"}}, fields=["body"], max_results=5)
    method, path, body = tr.calls[-1]
    assert (method, path) == ("POST", "v1/namespaces/docs/search")
    assert body == {
        "query_text": "indemnity",
        "max_results": 5,
        "filter": {"term": {"type": "nda"}},
        "fields": ["body"],
    }
    assert out == {
        "total_hits": 7,
        "total_hits_relation": "eq",
        "hits": [
            {
                "id": "d1",
                "score": 1.5,
                "path": "/contracts/2024/a.txt",
                "fields": {"body": "x" * 10, "year": 2024, "final": True},
                "truncated_fields": ["body"],
            }
        ],
    }


def test_max_results_is_capped_and_include_fields_keeps_the_path():
    t, tr = tools(max_results_cap=20)
    t.search("q", max_results=1000, include_fields=["title"])
    body = tr.calls[-1][2]
    assert body["max_results"] == 20
    assert body["source_includes"] == ["path", "title"]
    t.filter({"exists": {"field": "x"}}, max_results=0)
    assert tr.calls[-1][2]["max_results"] == 1


def test_semantic_and_hybrid_search():
    t, tr = tools()
    t.semantic_search("emb", vector=[1, 2], k=3, namespace="other")
    method, path, body = tr.calls[-1]
    assert path == "v1/namespaces/other/search"
    assert body == {
        "query_text": "",
        "max_results": 3,
        "knn": {"field": "emb", "k": 3, "vector": [1.0, 2.0]},
        "mode": "semantic",
    }
    t.semantic_search("emb", query="late fees", hybrid=True)
    body = tr.calls[-1][2]
    assert body["query_text"] == "late fees"
    assert body["knn"] == {"field": "emb", "k": 10, "query_text": "late fees"}
    assert body["mode"] == "hybrid"
    with pytest.raises(KoshaConfigError):
        t.semantic_search("emb")
    with pytest.raises(KoshaConfigError):
        t.semantic_search("emb", query="a", vector=[1.0])
    with pytest.raises(KoshaConfigError):
        t.semantic_search("emb", vector=[1.0], hybrid=True)


def test_filter_sort_and_search_after():
    t, tr = tools()
    t.filter(
        {"prefix": {"path": "/contracts/"}},
        sort=[{"date": {"order": "desc"}}, {"_id": {"order": "asc"}}],
        search_after=["2024-01-01", "d9"],
    )
    body = tr.calls[-1][2]
    assert body["query_text"] == ""
    assert body["filter"] == {"prefix": {"path": "/contracts/"}}
    assert body["sort"][0] == {"date": {"order": "desc"}}
    assert body["search_after"] == ["2024-01-01", "d9"]
    with pytest.raises(KoshaConfigError):
        t.filter({})


def test_count_and_aggregate():
    t, tr = tools({"results": [], "total_hits": 42, "total_hits_relation": "eq", "aggregations": {"a": 1}})
    assert t.count({"term": {"type": "nda"}}) == {"count": 42, "relation": "eq"}
    assert tr.calls[-1][2] == {
        "query_text": "",
        "max_results": 0,
        "filter": {"term": {"type": "nda"}},
        "exact_total_hits": True,
    }
    out = t.aggregate({"a": {"terms": {"field": "type"}}}, query="nda")
    assert tr.calls[-1][2] == {"query_text": "nda", "max_results": 0, "aggs": {"a": {"terms": {"field": "type"}}}}
    assert out == {"aggregations": {"a": 1}, "total_hits": 42, "total_hits_relation": "eq"}


def test_branches():
    t, tr = tools()
    t.branch("experiment")
    assert tr.calls[-1] == ("POST", "v1/namespaces/docs/branch", {"name": "experiment"})
    t.list_branches("a/b")
    assert tr.calls[-1] == ("GET", "v1/namespaces/a%2Fb/branches", None)


def test_index_documents_types_fields_and_flushes():
    t, tr = tools({"indexed_count": 1})
    out = t.index_documents(
        [{"id": 1, "fields": {"path": "/a.txt", "body": "hi", "tag": "x", "n": 3, "f": 0.5, "ok": False, "v": [1, 2]}}],
        keyword_fields=["tag"],
    )
    assert out == {"indexed": 1, "flushed": True}
    (_, path, body), (_, flush_path, _) = tr.calls
    assert path == "v1/namespaces/docs/documents"
    assert flush_path == "v1/namespaces/docs/flush"
    assert body["documents"][0]["id"] == "1"
    assert {f["name"]: (f["field_type"], f["value"]) for f in body["documents"][0]["fields"]} == {
        "path": ("Keyword", "/a.txt"),
        "body": ("Text", "hi"),
        "tag": ("Keyword", "x"),
        "n": ("Integer", "3"),
        "f": ("Float", "0.5"),
        "ok": ("Boolean", "false"),
        "v": ("Vector", "[1.0, 2.0]"),
    }
    with pytest.raises(KoshaConfigError):
        t.index_documents([{"id": "x", "fields": {"bad": {"nested": 1}}}])
    with pytest.raises(KoshaConfigError):
        t.index_documents([{"fields": {}}])


def test_namespace_is_required():
    c = KoshaClient("http://kosha.test", api_key="k")
    c._transport = FakeTransport()
    t = AgentTools(c, namespace=None)
    t.namespace = None
    with pytest.raises(KoshaConfigError):
        t.count()


def test_numeric_filter_values_become_strings():
    t, tr = tools()
    t.count(
        {
            "bool": {
                "must": [{"range": {"year": {"gte": 2024, "lt": 2025.5}}}, {"term": {"final": True}}],
                "should": [{"terms": {"n": [1, 2]}}],
                "minimum_should_match": 1,
            }
        }
    )
    assert tr.calls[-1][2]["filter"] == {
        "bool": {
            "must": [{"range": {"year": {"gte": "2024", "lt": "2025.5"}}}, {"term": {"final": True}}],
            "should": [{"terms": {"n": ["1", "2"]}}],
            "minimum_should_match": 1,
        }
    }


def test_count_and_aggregate_everything():
    t, tr = tools({"total_hits": 5})
    assert t.count()["count"] == 5
    assert tr.calls[-1][2]["filter"] == {"match_all": {}}
    t.aggregate({"a": {"terms": {"field": "type"}}})
    assert tr.calls[-1][2]["filter"] == {"match_all": {}}
    t.count(query="nda")
    assert "filter" not in tr.calls[-1][2]


def test_semantic_search_filters_inside_the_knn_probe():
    t, tr = tools({"results": []})
    t.semantic_search("emb", vector=[1.0], filter={"term": {"year": 2024}})
    body = tr.calls[-1][2]
    assert body["knn"]["filter"] == {"term": {"year": "2024"}}
    assert "filter" not in body
    t.semantic_search("emb", query="fees", hybrid=True, filter={"term": {"type": "nda"}})
    body = tr.calls[-1][2]
    assert body["knn"]["filter"] == {"term": {"type": "nda"}}
    assert body["filter"] == {"term": {"type": "nda"}}


def test_filter_pages_with_a_unique_tie_breaker():
    page = {
        "results": [
            {"doc_id": "d1", "fields": [{"name": "date", "field_type": "Keyword", "value": "2024"}]},
            {"doc_id": "d2", "fields": [{"name": "date", "field_type": "Keyword", "value": "2024"}]},
        ]
    }
    t, tr = tools(page)
    out = t.filter(
        {"exists": {"field": "date"}},
        sort=[{"date": {"order": "desc"}}],
        max_results=2,
        include_fields=["title"],
    )
    body = tr.calls[-1][2]
    assert body["sort"] == [{"date": {"order": "desc"}}, {"_id": {"order": "asc"}}]
    assert body["source_includes"] == ["date", "path", "title"]
    assert out["next_search_after"] == ["2024", "d2"]
    # A short page is the last one.
    assert "next_search_after" not in t.filter({"exists": {"field": "date"}}, sort=["date"])


def test_filter_cursor_for_missing_fields_score_and_caller_cursors():
    page = {"results": [{"doc_id": "d7", "score": 1.5, "fields": []}]}
    t, tr = tools(page)
    out = t.filter({"exists": {"field": "x"}}, sort=["date", "_score"], max_results=1)
    assert out["next_search_after"] == ["", "1.5", "d7"]
    # A caller cursor of one value per requested key keeps its sort as is.
    t.filter({"exists": {"field": "x"}}, sort=["date"], search_after=["2024"], max_results=1)
    assert tr.calls[-1][2]["sort"] == ["date"]
    t.filter({"exists": {"field": "x"}}, sort=["date"], search_after=["2024", "d7"], max_results=1)
    assert tr.calls[-1][2]["sort"] == ["date", {"_id": {"order": "asc"}}]
