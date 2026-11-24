"""Unit tests for KoshaClient field / sort translation helpers."""

import json
import time

import pytest

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
            "isFeatured": False,
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
    assert by_name["isFeatured"]["field_type"] == "Boolean"


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
            "labels": [],
            "contentEmbedding": _EMBEDDING,
        }
    )
    by_name = {f["name"]: f for f in fields}
    assert by_name["contentEmbedding"]["field_type"] == "Vector"
    for name in ("system_tags", "user_defined_tags", "labels"):
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


def test_source_to_fields_maps_a_token_bag_to_a_vector_field():
    """Late interaction (#205): equal-length numeric token vectors, each at
    least _MIN_VECTOR_DIM long, are one Vector field holding the JSON bag."""
    bag = [[0.01 * (i + t) for i in range(12)] for t in range(5)]
    fields = KoshaClient._source_to_fields(
        {"content": "hello", "tokens": bag, "as_tuples": tuple(tuple(t) for t in bag)}
    )
    by_name = {f["name"]: f for f in fields}
    for name in ("tokens", "as_tuples"):
        assert by_name[name]["field_type"] == "Vector", name
        assert json.loads(by_name[name]["value"]) == bag
    # A one-token bag is still a bag.
    one = KoshaClient._source_to_fields({"tokens": [_EMBEDDING]})
    assert one[0]["field_type"] == "Vector"
    assert json.loads(one[0]["value"]) == [_EMBEDDING]
    # And it reads back as the nested list it was.
    assert KoshaClient._decode_field_value(by_name["tokens"]["value"], "Vector") == bag


def test_source_to_fields_ragged_or_short_nested_lists_stay_keyword():
    """Anything not a clean bag keeps the pre-#205 Keyword mapping."""
    long = [0.5] * 12
    cases = {
        "ragged": [long, long[:10]],
        "short_tokens": [[1.0, 2.0], [3.0, 4.0]],  # polygon / bbox pairs
        "one_short": [long, [1.0, 2.0]],
        "mixed": [long, "x"],
        "nested_strings": [["a"] * 9, ["b"] * 9],
        "bools": [[True] * 9, [False] * 9],
        "empty_tokens": [[], []],
        "deeper": [[long]],
    }
    fields = KoshaClient._source_to_fields(cases)
    by_name = {f["name"]: f for f in fields}
    for name, value in cases.items():
        assert by_name[name]["field_type"] == "Keyword", name
        assert json.loads(by_name[name]["value"]) == value, name

def test_decode_field_value_restores_python_types():
    """The server always returns field values as strings on the wire;
    _decode_field_value is the reverse of _field_to_kosha and must hand
    callers back the original Python type, not the raw string — this is
    the exact bug behind a caller's ``TypeError: must be real number, not str``
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
        {
            "total_hits": 1,
            "results": [
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
        },
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
        index="articles_index",
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


def test_extract_query_text_ignores_bare_top_level_term_value():
    """A bare `{"query": {"term": {...}}}` must not leak its value into
    BM25 query text.

    `term` is an exact-match filter, not free text — `_extract_filter`
    already carries it through to Kosha's native `filter` field unchanged.
    Previously the term's own value (e.g. a project/document id) was
    returned as query text whenever it happened to be a string, turning an
    intended filter-only lookup into a bogus BM25 search for that literal
    id — a search that finds nothing (the id almost never appears in any
    document's indexed free text) and silently collapses correct results
    to zero, even though `_extract_filter` translated the filter correctly.
    """
    body = {"query": {"term": {"project_id": "6642a1b2-c3d4-e5f6-a7b8-c9d0e1f2a3b4"}}}

    assert KoshaClient._extract_query_text(body) == ""
    # The filter side must still carry the clause through unchanged.
    assert KoshaClient._extract_filter(body) == body["query"]


def test_search_bare_term_query_sends_empty_query_text_and_native_filter(monkeypatch):
    """End-to-end: `.search(body={"query": {"term": {...}}})` must hit
    Kosha's filter-only path (empty `query_text`, native `filter`), not a
    bogus BM25 search on the term's value. Regression test for the exact
    failure mode: a bare top-level `term` query silently returning zero
    hits because its value leaked into `query_text`.
    """
    captured = {}

    def fake_request(self, method, path, body=None):  # noqa: ANN001
        captured["method"] = method
        captured["path"] = path
        captured["body"] = body
        return {"results": [{"doc_id": "a", "score": 1.0, "fields": []}], "total_hits": 1}

    monkeypatch.setattr(KoshaClient, "_request", fake_request)
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    result = client.search(
        index="articles_index",
        body={"query": {"term": {"project_id": "m1"}}},
    )

    assert captured["body"]["query_text"] == ""
    assert captured["body"]["filter"] == {"term": {"project_id": "m1"}}
    assert result["hits"]["total"]["value"] == 1


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
                            "publishedAt": "old",
                        },
                    },
                    {
                        "_id": "chunk-2",
                        "_source": {
                            "documentId": "file-2",
                            "__type__documentId": "Keyword",
                            "content": "unchanged",
                            "publishedAt": "2026-07-30",
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
        index="articles_index",
        body={
            "query": {"terms": {"documentId": ["file-1", "file-2"]}},
            "script": {
                "source": (
                    "String _did = ctx._source.documentId; "
                    "boolean _changed = false; "
                    "if (params.map_publishedAt.containsKey(_did) && "
                    "ctx._source.publishedAt != params.map_publishedAt[_did]) "
                    "{ ctx._source.publishedAt = params.map_publishedAt[_did]; "
                    "_changed = true; } "
                    "if (!_changed) { ctx.op = 'noop'; }"
                ),
                "params": {
                    "map_publishedAt": {
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
    assert by_id["chunk-1"]["publishedAt"] == "2026-07-30"
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
        index="articles_index",
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
        index="articles_index",
        id="chunk-1",
        body={"doc": {"publishedAt": "2026-07-30"}},
    )

    assert response["result"] == "updated"
    assert requests == [
        (
            "POST",
            "replace",
            {
                "namespace": "articles_index",
                "documents": [
                    {
                        "id": "chunk-1",
                        "fields": [
                            {
                                "name": "publishedAt",
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
            {"index": {"_index": "articles_index", "_id": "chunk-1"}},
            {"content": "hello"},
            {"update": {"_index": "articles_index", "_id": "chunk-2"}},
            {"doc": {"publishedAt": "2026-07-30"}},
        ]
    )

    assert response["errors"] is False
    assert [item for item in response["items"]] == [
        {"index": {"_index": "articles_index", "_id": "chunk-1", "status": 201}},
        {"update": {"_index": "articles_index", "_id": "chunk-2", "status": 200}},
    ]
    assert [r[:2] for r in requests] == [("POST", "index"), ("POST", "replace")]
    assert requests[1][2]["documents"][0]["id"] == "chunk-2"


def _recording_client(monkeypatch):
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    requests = []
    monkeypatch.setattr(
        client,
        "_request",
        lambda method, path, body=None: requests.append((method, path, body)) or {},
    )
    return client, requests


_TWO_NS_BULK = [
    {"index": {"_index": "a", "_id": "1"}},
    {"content": "x"},
    {"index": {"_index": "b", "_id": "2"}},
    {"content": "y"},
    {"update": {"_index": "a", "_id": "3"}},
    {"doc": {"content": "z"}},
]


def test_bulk_does_not_flush_per_batch_by_default(monkeypatch):
    # #241: a /flush after every bulk chunk cut one segment per chunk.
    client, requests = _recording_client(monkeypatch)
    client.bulk(body=_TWO_NS_BULK)
    client.bulk(body=_TWO_NS_BULK, refresh=False)
    client.bulk(body=_TWO_NS_BULK, refresh="false")
    assert all(path != "flush" for _, path, _ in requests)


@pytest.mark.parametrize("refresh", [True, "true", "wait_for", ""])
def test_bulk_refresh_flushes_each_touched_namespace_once(monkeypatch, refresh):
    client, requests = _recording_client(monkeypatch)
    client.bulk(body=_TWO_NS_BULK, refresh=refresh)
    flushes = [body["namespace"] for _, path, body in requests if path == "flush"]
    assert flushes == ["a", "b"]
    # Flushes come after every write, so they cover the replace too.
    assert [path for _, path, _ in requests][-2:] == ["flush", "flush"]


def test_index_refresh_flushes(monkeypatch):
    client, requests = _recording_client(monkeypatch)
    client.index(index="a", id="1", body={"content": "x"})
    assert [path for _, path, _ in requests] == ["index"]
    client.index(index="a", id="2", body={"content": "y"}, refresh="wait_for")
    assert [path for _, path, _ in requests] == ["index", "index", "flush"]
    assert requests[-1][2] == {"namespace": "a"}


def test_indices_refresh_flushes_namespace(monkeypatch):
    client, requests = _recording_client(monkeypatch)
    client.indices.refresh(index="a")
    assert requests == [("POST", "flush", {"namespace": "a"})]


def test_nested_object_is_sent_as_keyword_json_and_round_trips():
    # #206: a dict used to be dropped. It is now sent whole as Keyword JSON
    # text, which the server flattens into dotted paths for indexing.
    author = {"name": "Ada", "org": "acme", "tags": ["x"], "age": 36}
    authors = [{"name": "Ada"}, {"name": "Bob"}]
    fields = KoshaClient._source_to_fields({"author": author, "authors": authors})
    by_name = {f["name"]: f for f in fields}
    assert by_name["author"]["field_type"] == "Keyword"
    assert json.loads(by_name["author"]["value"]) == author
    assert by_name["authors"]["field_type"] == "Keyword"
    assert json.loads(by_name["authors"]["value"]) == authors

    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    response = client._build_search_response(
        {"total_hits": 1, "results": [{"doc_id": "d1", "score": 1.0, "fields": fields}]}
    )
    source = response["hits"]["hits"][0]["_source"]
    assert source["author"] == author
    assert source["authors"] == authors


# ─── Read-your-writes (#255) ─────────────────────────────────────────────


def test_consistency_is_sent_with_every_search(monkeypatch):
    client = KoshaClient(hosts="http://kosha.invalid", api_key="k", consistency="strong")
    requests = []
    monkeypatch.setattr(
        client,
        "_request",
        lambda method, path, body=None: requests.append((method, path, body))
        or {"results": [], "total_hits": 0},
    )
    client.search(index="a", body={"query": {"match": {"content": "x"}}})
    client.get(index="a", id="1")
    searches = [body for _, path, body in requests if path == "search"]
    assert len(searches) == 2
    assert all(body["consistency"] == "strong" for body in searches)


def test_consistency_defaults_to_the_server(monkeypatch):
    client, requests = _recording_client(monkeypatch)
    client.search(index="a", body={"query": {"match": {"content": "x"}}})
    assert "consistency" not in requests[0][2]


def test_strong_consistency_makes_wait_for_flush_free(monkeypatch):
    client = KoshaClient(hosts="http://kosha.invalid", api_key="k", consistency="strong")
    requests = []
    monkeypatch.setattr(
        client,
        "_request",
        lambda method, path, body=None: requests.append((method, path, body)) or {},
    )
    client.index(index="a", id="1", body={"content": "x"}, refresh="wait_for")
    assert [path for _, path, _ in requests] == ["index"]
    # An explicit refresh still flushes.
    client.index(index="a", id="2", body={"content": "y"}, refresh=True)
    assert [path for _, path, _ in requests] == ["index", "index", "flush"]


def test_consistency_is_validated():
    with pytest.raises(ValueError):
        KoshaClient(hosts="http://kosha.invalid", api_key="k", consistency="sometimes")


def test_source_to_fields_keeps_a_stored_keyword_matrix_keyword():
    """A matrix stored as Keyword before #205 and read back with its type
    (update_by_query rewrites) is not silently re-typed as a Vector bag."""
    bag = [[0.01 * (i + t) for i in range(12)] for t in range(3)]
    fields = KoshaClient._source_to_fields(
        {"m": bag, "__type__m": "Keyword", "fresh": bag}
    )
    by_name = {f["name"]: f for f in fields}
    assert by_name["m"]["field_type"] == "Keyword"
    assert json.loads(by_name["m"]["value"]) == bag
    assert by_name["fresh"]["field_type"] == "Vector"
