"""Unit tests for KoshaClient field / sort translation helpers."""

import json

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


def test_source_to_fields_empty_lists_are_text_not_vector():
    """Empty multi-value fields must not become zero-dim Vectors.

    Mixed dim-0 + dim-N vectors in one segment corrupt vector.idx and
    panic the server on read (read_f32_le bounds error).
    """
    fields = KoshaClient._source_to_fields(
        {
            "content": "hello",
            "system_tags": [],
            "user_defined_tags": [],
            "recipients": [],
            "contentEmbedding": [0.1, 0.2],
        }
    )
    by_name = {f["name"]: f for f in fields}
    assert by_name["contentEmbedding"]["field_type"] == "Vector"
    for name in ("system_tags", "user_defined_tags", "recipients"):
        assert by_name[name]["field_type"] == "Text"
        assert by_name[name]["value"] == "[]"


def test_source_to_fields_bbox_pairs_are_text_not_vector():
    """bottom_left/top_right are 2-float bbox pairs, not embeddings.

    Kosha's Vector storage is shared per-segment across every Vector field;
    a 2-dim bbox pair next to a 1536-dim contentEmbedding corrupts vector.idx
    (dimension mismatch -> read_f32_le panics on retrieval) and forces
    lexical-only queries to rebuild HNSW for no reason.
    """
    fields = KoshaClient._source_to_fields(
        {
            "content": "hello",
            "bottom_left": [12.5, 640.2],
            "top_right": [812.5, 700.2],
            "contentEmbedding": [0.1, 0.2, 0.3],
        }
    )
    by_name = {f["name"]: f for f in fields}
    assert by_name["contentEmbedding"]["field_type"] == "Vector"
    for name in ("bottom_left", "top_right"):
        assert by_name[name]["field_type"] == "Text"
        assert by_name[name]["value"] == json.dumps(
            [12.5, 640.2] if name == "bottom_left" else [812.5, 700.2]
        )


def test_decode_field_value_restores_python_types():
    """The server always returns field values as strings on the wire;
    _decode_field_value is the reverse of _field_to_kosha and must hand
    callers back the original Python type, not the raw string — this is
    the exact bug behind sage's ``TypeError: must be real number, not str``
    when building a protobuf Coordinates from a search hit.
    """
    assert KoshaClient._decode_field_value("pageNumber", "3", "Float") == 3.0
    assert KoshaClient._decode_field_value("isHot", "true", "Boolean") is True
    assert KoshaClient._decode_field_value("isHot", "false", "Boolean") is False
    assert KoshaClient._decode_field_value(
        "contentEmbedding", "[0.1, 0.2, 0.3]", "Vector"
    ) == [0.1, 0.2, 0.3]
    assert KoshaClient._decode_field_value(
        "bottom_left", "[12.5, 640.2]", "Text"
    ) == [12.5, 640.2]
    assert KoshaClient._decode_field_value("content", "hello", "Text") == "hello"


def test_build_search_response_decodes_bbox_and_vector_fields():
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    response = client._build_search_response(
        kosha_hits=[
            {
                "doc_id": "d1",
                "score": 1.5,
                "fields": [
                    {"name": "content", "field_type": "Text", "value": "hello"},
                    {"name": "bottom_left", "field_type": "Text", "value": "[12.5, 640.2]"},
                    {"name": "contentEmbedding", "field_type": "Vector", "value": "[0.1, 0.2]"},
                ],
            }
        ],
        from_=0,
        size=10,
        took_ms=1,
    )
    hit = response["hits"]["hits"][0]["_source"]
    assert hit["bottom_left"] == [12.5, 640.2]
    assert hit["bottom_left"][0] == 12.5
    assert hit["contentEmbedding"] == [0.1, 0.2]


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
