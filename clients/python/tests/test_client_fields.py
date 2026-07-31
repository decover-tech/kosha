"""Unit tests for KoshaClient field / sort translation helpers."""

import json
import time

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
    assert (
        KoshaClient._decode_field_value(json.dumps(_EMBEDDING), "Vector") == _EMBEDDING
    )
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


def test_source_to_fields_preserves_string_field_type_metadata():
    fields = KoshaClient._source_to_fields(
        {
            "documentId": "file-1",
            "__type__documentId": "Keyword",
            "content": "hello",
        }
    )

    by_name = {field["name"]: field for field in fields}
    assert by_name["documentId"]["field_type"] == "Keyword"
    assert by_name["content"]["field_type"] == "Text"


def test_extract_sort_and_search_after_shape():
    sort = KoshaClient._extract_sort(
        {"sort": [{"_id": "asc"}, {"score": {"order": "desc"}}]}
    )
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


def test_extract_filter_accepts_top_level_terms_query():
    body = {"query": {"terms": {"documentId": ["d1", "d2"]}}}

    assert KoshaClient._extract_filter(body) == body["query"]


def test_update_by_query_supports_parameter_maps(monkeypatch):
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    pages = [
        {
            "hits": {
                "hits": [
                    {
                        "_id": "chunk-1",
                        "_source": {
                            "documentId": "file-1",
                            "__type__documentId": "Keyword",
                            "content": "hello",
                            "sentAt": "old",
                        },
                    },
                    {
                        "_id": "chunk-2",
                        "_source": {
                            "documentId": "file-2",
                            "__type__documentId": "Keyword",
                            "content": "unchanged",
                            "sentAt": "2026-07-30",
                        },
                    },
                ]
            }
        },
        {"hits": {"hits": []}},
    ]
    requests = []

    monkeypatch.setattr(client, "search", lambda **kwargs: pages.pop(0))
    monkeypatch.setattr(
        client,
        "_request",
        lambda method, path, body=None: requests.append((method, path, body)) or {},
    )

    response = client.update_by_query(
        index="paragraph_index_hnsw",
        body={
            "query": {"terms": {"documentId": ["file-1", "file-2"]}},
            "script": {
                "source": (
                    "String _did = ctx._source.documentId; "
                    "boolean _changed = false; "
                    "if (params.map_sentAt.containsKey(_did) && "
                    "ctx._source.sentAt != params.map_sentAt[_did]) "
                    "{ ctx._source.sentAt = params.map_sentAt[_did]; "
                    "_changed = true; } "
                    "if (!_changed) { ctx.op = 'noop'; }"
                ),
                "params": {
                    "map_sentAt": {
                        "file-1": "2026-07-30",
                        "file-2": "2026-07-30",
                    }
                },
            },
        },
    )

    assert response == {
        "updated": 1,
        "noop": 1,
        "total": 2,
        "version_conflicts": 0,
        "failures": [],
    }
    assert requests[0][:2] == ("POST", "replace")
    indexed = requests[0][2]["documents"]
    by_id = {
        doc["id"]: {field["name"]: field["value"] for field in doc["fields"]}
        for doc in indexed
    }
    assert by_id["chunk-1"]["content"] == "hello"
    assert by_id["chunk-1"]["sentAt"] == "2026-07-30"
    assert (
        next(
            field["field_type"]
            for field in indexed[0]["fields"]
            if field["name"] == "documentId"
        )
        == "Keyword"
    )
    assert by_id["chunk-2"]["content"] == "unchanged"
    assert len(requests) == 1


def test_async_update_by_query_exposes_completed_task(monkeypatch):
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    expected = {
        "updated": 3,
        "noop": 2,
        "total": 5,
        "version_conflicts": 0,
        "failures": [],
    }
    monkeypatch.setattr(
        client,
        "_execute_update_by_query",
        lambda namespace, body: expected,
    )

    submitted = client.update_by_query(
        index="paragraph_index_hnsw",
        body={},
        wait_for_completion=False,
    )
    for _ in range(100):
        task = client.tasks.get(task_id=submitted["task"])
        if task["completed"]:
            break
        time.sleep(0.001)

    assert task == {"completed": True, "response": expected}


def test_update_routes_through_replace(monkeypatch):
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    requests = []
    monkeypatch.setattr(
        client,
        "_request",
        lambda method, path, body=None: requests.append((method, path, body)) or {},
    )

    response = client.update(
        index="paragraph_index_hnsw",
        id="chunk-1",
        body={"doc": {"sentAt": "2026-07-30"}},
    )

    assert response["result"] == "updated"
    assert requests == [
        (
            "POST",
            "replace",
            {
                "namespace": "paragraph_index_hnsw",
                "documents": [
                    {
                        "id": "chunk-1",
                        "fields": [
                            {
                                "name": "sentAt",
                                "field_type": "Text",
                                "value": "2026-07-30",
                            }
                        ],
                    }
                ],
            },
        )
    ]


def test_bulk_update_routes_through_replace(monkeypatch):
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    requests = []
    monkeypatch.setattr(
        client,
        "_request",
        lambda method, path, body=None: requests.append((method, path, body)) or {},
    )

    response = client.bulk(
        body=[
            {"index": {"_index": "paragraph_index_hnsw", "_id": "chunk-1"}},
            {"content": "hello"},
            {"update": {"_index": "paragraph_index_hnsw", "_id": "chunk-2"}},
            {"doc": {"sentAt": "2026-07-30"}},
        ]
    )

    assert response["errors"] is False
    assert [item for item in response["items"]] == [
        {"index": {"_index": "paragraph_index_hnsw", "_id": "chunk-1", "status": 201}},
        {"update": {"_index": "paragraph_index_hnsw", "_id": "chunk-2", "status": 200}},
    ]
    assert requests[0][:2] == ("POST", "index")
    assert requests[1][:2] == ("POST", "flush")
    assert requests[2][:2] == ("POST", "replace")
    assert requests[2][2]["documents"][0]["id"] == "chunk-2"


def test_index_with_id_uses_put_document(monkeypatch):
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    calls = []

    def fake_v1(method, namespace, action, body=None):
        calls.append((method, namespace, action, body))
        return {"result": "updated", "indexed_count": 1}

    monkeypatch.setattr(client, "_v1_request", fake_v1)
    response = client.index(index="paragraphs", id="doc-1", body={"title": "hello"})

    assert response["result"] == "updated"
    assert response["_id"] == "doc-1"
    assert calls == [
        (
            "PUT",
            "paragraphs",
            "documents/doc-1",
            {
                "fields": [
                    {"name": "title", "field_type": "Text", "value": "hello"},
                ]
            },
        )
    ]
