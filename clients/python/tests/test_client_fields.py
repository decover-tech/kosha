"""Unit tests for KoshaClient field / sort translation helpers."""

import json

from kosha_client.client import KoshaClient

# Stand-in for a real embedding: length must clear _MIN_VECTOR_DIM. Real
# providers (OpenAI, Azure OpenAI, Cohere, Mistral, Gemini) output at least
# a few hundred dimensions; this just needs to be unambiguously "not a
# short incidental tuple" for the shape-based classifier under test.
_EMBEDDING = [0.1 * i for i in range(12)]


def test_source_to_fields_keeps_vectors():
    fields = KoshaClient._source_to_fields(
        {
            "content": "hello",
            "pageNumber": 3,
            "isHot": False,
            "contentEmbedding": _EMBEDDING,
            "__type__pageNumber": "Float",
        }
    )
    by_name = {f["name"]: f for f in fields}
    assert "contentEmbedding" in by_name
    assert by_name["contentEmbedding"]["field_type"] == "Vector"
    assert by_name["contentEmbedding"]["value"] == json.dumps(_EMBEDDING)
    assert "__type__pageNumber" not in by_name
    assert by_name["pageNumber"]["field_type"] == "Float"
    assert by_name["isHot"]["field_type"] == "Boolean"


def test_source_to_fields_empty_lists_are_keyword_not_vector():
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
            "contentEmbedding": _EMBEDDING,
        }
    )
    by_name = {f["name"]: f for f in fields}
    assert by_name["contentEmbedding"]["field_type"] == "Vector"
    for name in ("system_tags", "user_defined_tags", "recipients"):
        assert by_name[name]["field_type"] == "Keyword"
        assert by_name[name]["value"] == "[]"


def test_source_to_fields_short_numeric_lists_are_keyword_not_vector():
    """Classification is shape-based (dimension), not tied to any field name.

    Kosha's Vector storage is shared per-segment across every Vector field;
    a low-dim numeric tuple next to a real embedding corrupts vector.idx
    (dimension mismatch -> read_f32_le panics on retrieval) and forces
    lexical-only queries to rebuild HNSW for no reason. Any short numeric
    list is affected — this deliberately uses field names unrelated to any
    known schema to prove there's no name-based special-casing.
    """
    fields = KoshaClient._source_to_fields(
        {
            "content": "hello",
            "some_bbox_pair": [12.5, 640.2],
            "rgb_triplet": [255, 128, 0],
            "contentEmbedding": _EMBEDDING,
        }
    )
    by_name = {f["name"]: f for f in fields}
    assert by_name["contentEmbedding"]["field_type"] == "Vector"
    assert by_name["some_bbox_pair"]["field_type"] == "Keyword"
    assert by_name["some_bbox_pair"]["value"] == json.dumps([12.5, 640.2])
    assert by_name["rgb_triplet"]["field_type"] == "Keyword"
    assert by_name["rgb_triplet"]["value"] == json.dumps([255, 128, 0])


def test_decode_field_value_restores_python_types():
    """The server always returns field values as strings on the wire;
    _decode_field_value is the reverse of _field_to_kosha and must hand
    callers back the original Python type, not the raw string — this is
    the exact bug behind sage's ``TypeError: must be real number, not str``
    when building a protobuf Coordinates from a search hit.
    """
    assert KoshaClient._decode_field_value("3", "Float") == 3.0
    assert KoshaClient._decode_field_value("true", "Boolean") is True
    assert KoshaClient._decode_field_value("false", "Boolean") is False
    assert KoshaClient._decode_field_value(
        json.dumps(_EMBEDDING), "Vector"
    ) == _EMBEDDING
    assert KoshaClient._decode_field_value("[12.5, 640.2]", "Keyword") == [12.5, 640.2]
    assert KoshaClient._decode_field_value("hello", "Text") == "hello"


def test_build_search_response_decodes_short_list_and_vector_fields():
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    response = client._build_search_response(
        kosha_hits=[
            {
                "doc_id": "d1",
                "score": 1.5,
                "fields": [
                    {"name": "content", "field_type": "Text", "value": "hello"},
                    {
                        "name": "some_bbox_pair",
                        "field_type": "Keyword",
                        "value": "[12.5, 640.2]",
                    },
                    {
                        "name": "contentEmbedding",
                        "field_type": "Vector",
                        "value": json.dumps(_EMBEDDING),
                    },
                ],
            }
        ],
        from_=0,
        size=10,
        took_ms=1,
    )
    hit = response["hits"]["hits"][0]["_source"]
    assert hit["some_bbox_pair"] == [12.5, 640.2]
    assert hit["some_bbox_pair"][0] == 12.5
    assert hit["contentEmbedding"] == _EMBEDDING


def test_source_to_fields_unwraps_update_doc():
    fields = KoshaClient._source_to_fields(
        {"doc": {"content": "x", "contentEmbedding": _EMBEDDING}}
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
