"""get / mget / msearch (issue #198).

The server is faked by ``FakeKosha``, which answers ``POST /search`` the way
Kosha does for the shapes these methods send: a ``match_all`` filter, a
``_id`` sort, a ``search_after`` cursor, ``source_includes`` /
``source_excludes``, and a text query matched by substring.
"""

import json

import pytest

from kosha_client import KoshaClient, KoshaRequestError

DOCS = {
    "products": {
        "a": [
            {"name": "title", "field_type": "Text", "value": "red shoe"},
            {"name": "price", "field_type": "Float", "value": "12.5"},
            {"name": "active", "field_type": "Boolean", "value": "true"},
            {"name": "tags", "field_type": "Keyword", "value": '["new", "sale"]'},
        ],
        "ab": [{"name": "title", "field_type": "Text", "value": "blue shoe"}],
        "b": [{"name": "title", "field_type": "Text", "value": "green hat"}],
        "doc-10": [{"name": "title", "field_type": "Text", "value": "red hat"}],
    },
    "orders": {
        "a": [{"name": "title", "field_type": "Text", "value": "order a"}],
        "o-1": [{"name": "title", "field_type": "Text", "value": "order one"}],
    },
}


class FakeKosha:
    def __init__(self, docs=DOCS, fail_namespaces=()):
        self.docs = docs
        self.fail_namespaces = set(fail_namespaces)
        self.calls = []

    def __call__(self, method, path, body=None):
        self.calls.append((method, path, body))
        if path == "flush":
            return {}
        assert (method, path) == ("POST", "search"), (method, path)
        ns = body["namespace"]
        if ns in self.fail_namespaces:
            raise KoshaRequestError(500, f"segment unavailable in {ns}")
        if ns not in self.docs:
            raise KoshaRequestError(404, f"namespace '{ns}' not found")
        items = sorted(self.docs[ns].items())
        text = body.get("query_text", "")
        if text:
            items = [
                (i, f) for i, f in items
                if any(text in x["value"] for x in f if x["field_type"] == "Text")
            ]
        elif body.get("filter") != {"match_all": {}}:
            items = []  # Kosha: no text, no filter -> no hits
        sort = body.get("sort") or []
        if sort == [{"_id": {"order": "desc"}}]:
            items.reverse()
            if body.get("search_after"):
                cursor = body["search_after"][0]
                items = [(i, f) for i, f in items if i < cursor]
        else:
            assert "search_after" not in body
        items = items[: body["max_results"]]

        includes = body.get("source_includes")
        excludes = body.get("source_excludes") or []
        results = []
        for doc_id, fields in items:
            kept = [
                f for f in fields
                if (includes is None or f["name"] in includes)
                and f["name"] not in excludes
            ]
            results.append({"doc_id": doc_id, "score": 1.0, "fields": kept})
        return {"results": results, "total_hits": len(results)}


@pytest.fixture
def fake(monkeypatch):
    server = FakeKosha()
    client = KoshaClient(hosts="http://kosha.invalid", max_retries=1)
    monkeypatch.setattr(client, "_request", server)
    return client, server


# ── get ───────────────────────────────────────────────────────────────────


def test_get_finds_a_document_and_decodes_fields_like_search(fake):
    client, server = fake
    doc = client.get(index="products", id="a")
    assert doc == {
        "_index": "products",
        "_id": "a",
        "_version": 1,
        "found": True,
        "_source": {
            "title": "red shoe",
            "price": 12.5,
            "__type__price": "Float",
            "active": True,
            "__type__active": "Boolean",
            "tags": ["new", "sale"],
            "__type__tags": "Keyword",
        },
    }
    # The _source of a search hit for the same document is identical.
    hit = client.search(index="products", body={"query": {"match": {"title": "red shoe"}}})
    assert hit["hits"]["hits"][0]["_source"] == doc["_source"]

    (_, _, body) = server.calls[0]
    assert body["namespace"] == "products"
    assert body["max_results"] == 1
    assert body["filter"] == {"match_all": {}}
    assert body["sort"] == [{"_id": {"order": "desc"}}]
    assert body["search_after"] == ["a\x00"]


def test_get_is_exact_not_a_prefix_or_neighbour(fake):
    client, _ = fake
    # "ab" sorts right after "a", "aa" sits between "a" and "ab": neither
    # may be mistaken for another document.
    assert client.get(index="products", id="ab")["_source"]["title"] == "blue shoe"
    assert client.get(index="products", id="aa") == {
        "_index": "products", "_id": "aa", "found": False,
    }
    assert client.get(index="products", id="doc-1")["found"] is False
    assert client.get(index="products", id="0")["found"] is False  # below every id
    assert client.get(index="products", id="zzz")["found"] is False  # above every id


def test_get_misses_return_found_false_not_an_exception(fake):
    client, _ = fake
    assert client.get(index="products", id="missing") == {
        "_index": "products", "_id": "missing", "found": False,
    }
    # An index nothing was written to is a miss too, as in search.
    assert client.get(index="nowhere", id="a") == {
        "_index": "nowhere", "_id": "a", "found": False,
    }


def test_get_other_server_errors_raise(monkeypatch):
    client = KoshaClient(hosts="http://kosha.invalid", max_retries=1)
    monkeypatch.setattr(client, "_request", FakeKosha(fail_namespaces={"products"}))
    with pytest.raises(KoshaRequestError) as err:
        client.get(index="products", id="a")
    assert err.value.status_code == 500


def test_get_requires_an_id(fake):
    client, _ = fake
    with pytest.raises(KoshaRequestError):
        client.get(index="products", id="")


@pytest.mark.parametrize(
    "kwargs, expected_keys",
    [
        ({"_source": ["title"]}, {"title"}),
        ({"_source": "title,price"}, {"title", "price", "__type__price"}),
        ({"_source_includes": "title"}, {"title"}),
        ({"_source_excludes": ["tags", "active"]}, {"title", "price", "__type__price"}),
        ({"_source": {"includes": ["title", "tags"], "excludes": ["tags"]}}, {"title"}),
        ({"_source": True}, {
            "title", "price", "__type__price", "active", "__type__active",
            "tags", "__type__tags",
        }),
    ],
)
def test_get_source_filtering(fake, kwargs, expected_keys):
    client, _ = fake
    doc = client.get(index="products", id="a", **kwargs)
    assert set(doc["_source"]) == expected_keys


def test_get_source_false_omits_source(fake):
    client, server = fake
    doc = client.get(index="products", id="a", _source=False)
    assert doc == {"_index": "products", "_id": "a", "_version": 1, "found": True}
    assert server.calls[0][2]["source_includes"] == []


def test_get_refresh_flushes_first(fake):
    client, server = fake
    client.get(index="products", id="a", refresh=True)
    assert server.calls[0] == ("POST", "flush", {"namespace": "products"})


# ── mget ──────────────────────────────────────────────────────────────────


def test_mget_preserves_request_order_across_mixed_indices(fake):
    client, _ = fake
    result = client.mget(
        body={
            "docs": [
                {"_index": "orders", "_id": "o-1"},
                {"_id": "b"},
                {"_index": "orders", "_id": "missing"},
                {"_id": "a"},
                {"_index": "orders", "_id": "a"},
                {"_index": "nowhere", "_id": "a"},
            ]
        },
        index="products",
    )
    assert [(d["_index"], d["_id"], d["found"]) for d in result["docs"]] == [
        ("orders", "o-1", True),
        ("products", "b", True),
        ("orders", "missing", False),
        ("products", "a", True),
        ("orders", "a", True),
        ("nowhere", "a", False),
    ]
    # Same id in two indices: each document comes from its own index.
    assert result["docs"][3]["_source"]["title"] == "red shoe"
    assert result["docs"][4]["_source"]["title"] == "order a"
    assert "_source" not in result["docs"][2]


def test_mget_ids_form(fake):
    client, _ = fake
    result = client.mget(body={"ids": ["doc-10", "nope", "a"]}, index="products")
    assert [(d["_id"], d["found"]) for d in result["docs"]] == [
        ("doc-10", True), ("nope", False), ("a", True),
    ]
    assert all(d["_index"] == "products" for d in result["docs"])


def test_mget_source_per_doc_and_default(fake):
    client, _ = fake
    result = client.mget(
        body={"docs": [{"_id": "a"}, {"_id": "a", "_source": ["price"]}]},
        index="products",
        _source_includes=["title"],
    )
    assert result["docs"][0]["_source"] == {"title": "red shoe"}
    assert result["docs"][1]["_source"] == {"price": 12.5, "__type__price": "Float"}


def test_mget_repeated_documents_are_fetched_once(fake):
    client, server = fake
    result = client.mget(body={"ids": ["a", "a", "b"]}, index="products")
    assert [d["_id"] for d in result["docs"]] == ["a", "a", "b"]
    assert len(server.calls) == 2
    result["docs"][0]["_id"] = "changed"
    assert result["docs"][1]["_id"] == "a"  # entries are independent


def test_mget_per_doc_error_does_not_fail_the_call(monkeypatch):
    client = KoshaClient(hosts="http://kosha.invalid", max_retries=1)
    monkeypatch.setattr(client, "_request", FakeKosha(fail_namespaces={"orders"}))
    result = client.mget(
        body={"docs": [{"_index": "orders", "_id": "a"}, {"_id": "a"}]},
        index="products",
    )
    first, second = result["docs"]
    assert first["_index"] == "orders" and first["_id"] == "a"
    assert "segment unavailable" in first["error"]["reason"]
    assert second["found"] is True


@pytest.mark.parametrize("body", [{}, {"docs": [{"_index": "x"}]}, {"docs": ["a"]}])
def test_mget_malformed_body_raises(fake, body):
    client, _ = fake
    with pytest.raises(KoshaRequestError):
        client.mget(body=body)


# ── msearch ───────────────────────────────────────────────────────────────


def _ids(response):
    return [h["_id"] for h in response["hits"]["hits"]]


def test_msearch_ndjson_str_and_bytes(fake):
    client, server = fake
    lines = [
        {"index": "products"},
        {"query": {"match": {"title": "shoe"}}},
        {},
        {"query": {"match": {"title": "order"}}},
    ]
    ndjson = "\n".join(json.dumps(line) for line in lines) + "\n"
    for body in (ndjson, ndjson.encode()):
        server.calls.clear()
        result = client.msearch(body=body, index="orders")
        assert [r["status"] for r in result["responses"]] == [200, 200]
        assert _ids(result["responses"][0]) == ["a", "ab"]
        assert _ids(result["responses"][1]) == ["a", "o-1"]
        # One request per search: N round trips, not one batch.
        assert [c[2]["namespace"] for c in server.calls] == ["products", "orders"]


def test_msearch_list_of_dicts_and_json_strings(fake):
    client, _ = fake
    result = client.msearch(
        body=[
            {"index": "products"},
            {"query": {"match": {"title": "hat"}}, "size": 1},
            '{"index": "orders"}',
            '{"query": {"match": {"title": "one"}}}',
        ]
    )
    assert _ids(result["responses"][0]) == ["b"]
    assert _ids(result["responses"][1]) == ["o-1"]
    assert "took" in result


def test_msearch_per_item_errors_do_not_fail_the_call(monkeypatch):
    client = KoshaClient(hosts="http://kosha.invalid", max_retries=1)
    monkeypatch.setattr(client, "_request", FakeKosha(fail_namespaces={"orders"}))
    result = client.msearch(
        body=[
            {"index": "products"},
            {"query": {"fuzzy": {"title": "shoe"}}},  # Kosha cannot express it
            {"index": "orders"},
            {"query": {"match": {"title": "order"}}},  # server error
            {"index": "products"},
            {"query": {"match": {"title": "hat"}}, "size": "ten"},  # malformed
            {"index": "products"},
            {"query": {"match": {"title": "hat"}}},
            {"index": "nowhere"},
            {"query": {"match": {"title": "hat"}}},  # empty index: no hits
        ]
    )
    statuses = [r["status"] for r in result["responses"]]
    assert statuses == [400, 500, 400, 200, 200]
    assert "fuzzy" in result["responses"][0]["error"]["reason"]
    assert result["responses"][0]["error"]["type"] == "kosha_request_error"
    assert "segment unavailable" in result["responses"][1]["error"]["reason"]
    assert result["responses"][2]["error"]["type"] == "illegal_argument_exception"
    assert _ids(result["responses"][3]) == ["b", "doc-10"]
    assert _ids(result["responses"][4]) == []


@pytest.mark.parametrize(
    "body",
    [
        '{"index": "products"}\n',  # header without a search body
        '{"index": "products"}\nnot json\n',
        [{"index": "products"}, ["not", "an", "object"]],
        42,
    ],
)
def test_msearch_malformed_body_raises_before_running(fake, body):
    client, server = fake
    with pytest.raises(KoshaRequestError) as err:
        client.msearch(body=body)
    assert err.value.status_code == 400
    assert server.calls == []


def test_methods_exist_on_the_client():
    for name in ("get", "mget", "msearch"):
        assert callable(getattr(KoshaClient, name))
