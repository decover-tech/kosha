"""Issue #251: the ``rerank`` clause and its response fields."""

import pytest

from kosha_client import KoshaClient, KoshaRequestError


def capture(monkeypatch, result):
    captured = {}

    def fake_request(self, method, path, body=None):  # noqa: ANN001
        captured["body"] = body
        return result

    monkeypatch.setattr(KoshaClient, "_request", fake_request)
    return captured


def test_cohere_rerank_call_maps_to_rerank_clause(monkeypatch):
    # Sage today: search, then co.rerank(model="rerank-v3.5", query=q,
    # documents=[h["_source"]["content"] for h in hits], top_n=50).
    captured = capture(
        monkeypatch,
        {
            "results": [
                {"doc_id": "b", "score": 0.91, "fields": [], "fused_score": 0.016,
                 "rerank_score": 0.91},
                {"doc_id": "a", "score": 0.12, "fields": [], "fused_score": 0.032,
                 "rerank_score": 0.12},
            ],
            "total_hits": 7,
            "rerank_degraded": False,
        },
    )
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    response = client.search(
        index="paragraphs",
        body={"query": {"match": {"content": "termination for breach"}}, "size": 10},
        rerank=KoshaClient.rerank_spec("content", model="rerank-v3.5", top_n=50),
    )
    sent = captured["body"]
    assert sent["query_text"] == "termination for breach"
    assert sent["max_results"] == 10
    assert sent["rerank"] == {
        "provider": "cohere",
        "field": "content",
        "model": "rerank-v3.5",
        "top_n": 50,
    }
    hits = response["hits"]["hits"]
    assert [h["_id"] for h in hits] == ["b", "a"]
    assert hits[0]["_score"] == 0.91
    assert hits[0]["_rerank_score"] == 0.91
    assert hits[0]["_fused_score"] == 0.016
    assert response["hits"]["max_score"] == 0.91
    assert response["rerank"] == {"degraded": False}


def test_degraded_rerank_is_reported(monkeypatch):
    capture(
        monkeypatch,
        {
            "results": [{"doc_id": "a", "score": 0.03, "fields": [], "fused_score": 0.03}],
            "total_hits": 1,
            "rerank_degraded": True,
            "rerank_degraded_reason": "timeout after 500 ms",
        },
    )
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    response = client.search(
        index="i",
        body={"query": {"match": {"content": "x"}},
              "rerank": {"provider": "cohere", "rank_fields": ["content"], "timeout_ms": 500}},
    )
    hit = response["hits"]["hits"][0]
    assert hit["_score"] == hit["_fused_score"] == 0.03
    assert "_rerank_score" not in hit
    assert response["rerank"] == {"degraded": True, "reason": "timeout after 500 ms"}


def test_plain_search_has_no_rerank_fields(monkeypatch):
    captured = capture(
        monkeypatch, {"results": [{"doc_id": "a", "score": 1.0, "fields": []}], "total_hits": 1}
    )
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    response = client.search(index="i", body={"query": {"match": {"content": "x"}}})
    assert "rerank" not in captured["body"]
    assert "rerank" not in response
    assert "_fused_score" not in response["hits"]["hits"][0]


def test_rank_fields_alias_and_query_override():
    kosha, _ = KoshaClient._translate_search(
        {
            "knn": {"emb": {"vector": [1.0, 0.0], "k": 20}},
            "rerank": {"provider": "voyage", "model": "rerank-2", "rank_fields": ["text"],
                       "query": "breach notice"},
        }
    )
    assert kosha["rerank"] == {
        "provider": "voyage",
        "model": "rerank-2",
        "field": "text",
        "query": "breach notice",
    }


@pytest.mark.parametrize(
    "spec",
    [
        {"provider": "cohere", "field": "t", "return_documents": True},
        {"provider": "cohere", "rank_fields": ["a", "b"]},
        {"field": "t"},
        "cohere",
    ],
)
def test_unsupported_rerank_options_are_refused(spec):
    with pytest.raises(KoshaRequestError) as raised:
        KoshaClient._translate_search({"query": {"match": {"t": "x"}}, "rerank": spec})
    assert raised.value.status_code == 400
