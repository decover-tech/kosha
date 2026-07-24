"""Unit tests for KoshaClient field / sort translation helpers."""

from kosha_client.client import KoshaClient


def test_source_to_fields_keeps_vectors():
    fields = KoshaClient._source_to_fields(
        {
            "content": "hello",
            "pageNumber": 3,
            "isHot": False,
            "contentEmbedding": [0.1, 0.2, 0.3],
            "__type__pageNumber": "Float",
        }
    )
    by_name = {f["name"]: f for f in fields}
    assert "contentEmbedding" in by_name
    assert by_name["contentEmbedding"]["field_type"] == "Vector"
    assert by_name["contentEmbedding"]["value"] == "[0.1, 0.2, 0.3]"
    assert "__type__pageNumber" not in by_name
    assert by_name["pageNumber"]["field_type"] == "Float"
    assert by_name["isHot"]["field_type"] == "Boolean"


def test_source_to_fields_unwraps_update_doc():
    fields = KoshaClient._source_to_fields(
        {"doc": {"content": "x", "contentEmbedding": [1.0, 2.0]}}
    )
    by_name = {f["name"]: f for f in fields}
    assert set(by_name) == {"content", "contentEmbedding"}
    assert by_name["contentEmbedding"]["field_type"] == "Vector"


def test_extract_sort_and_search_after_shape():
    sort = KoshaClient._extract_sort({"sort": [{"_id": "asc"}, {"score": {"order": "desc"}}]})
    assert sort == [{"_id": {"order": "asc"}}, {"score": {"order": "desc"}}]


def test_search_body_includes_search_after(monkeypatch):
    captured = {}

    def fake_request(self, method, path, body=None):  # noqa: ANN001
        captured["method"] = method
        captured["path"] = path
        captured["body"] = body
        return {"results": [], "total_hits": 0}

    monkeypatch.setattr(KoshaClient, "_request", fake_request)
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    client.search(
        index="paragraph_index_hnsw",
        body={
            "query": {"bool": {"should": [{"terms": {"documentId": ["d1"]}}]}},
            "size": 100,
            "sort": [{"_id": "asc"}],
            "search_after": ["d1:0:1"],
        },
    )
    assert captured["method"] == "POST"
    assert captured["body"]["search_after"] == ["d1:0:1"]
    assert captured["body"]["sort"] == [{"_id": {"order": "asc"}}]
    assert captured["body"]["from"] == 0
    assert captured["body"]["max_results"] == 100
