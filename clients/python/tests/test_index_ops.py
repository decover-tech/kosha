"""IndexOps.get_mapping / get_settings report only what the server knows.

They used to return a fixed, made-up mapping for every index. A caller
that introspects the mapping must get the namespace's real schema or
nothing at all.
"""

import pytest

from kosha_client import KoshaClient, KoshaRequestError


def client_with(monkeypatch, responder):
    """A client whose v1 requests go to ``responder(method, ns, action)``."""
    client = KoshaClient(hosts="http://kosha.invalid", max_retries=1)
    calls = []

    def fake_v1(self, method, namespace, action, body=None):  # noqa: ANN001
        calls.append((method, namespace, action))
        return responder(method, namespace, action)

    monkeypatch.setattr(KoshaClient, "_v1_request", fake_v1)
    return client, calls


SCHEMA = {
    "fields": {
        "body": {"type": "FIELD_TYPE_TEXT", "indexed": True, "analyzer": ["standard"]},
        "sku": {"type": "FIELD_TYPE_KEYWORD"},
        "qty": {"type": "FIELD_TYPE_INTEGER"},
        "price": {"type": "FIELD_TYPE_FLOAT"},
        "created": {"type": "FIELD_TYPE_DATE"},
        "active": {"type": "FIELD_TYPE_BOOLEAN"},
        "embedding": {"type": "FIELD_TYPE_VECTOR"},
        "location": {"type": "FIELD_TYPE_GEO_POINT"},
    }
}

EXPECTED = {
    "body": {"type": "text"},
    "sku": {"type": "keyword"},
    "qty": {"type": "long"},
    "price": {"type": "double"},
    "created": {"type": "date"},
    "active": {"type": "boolean"},
    "embedding": {"type": "knn_vector"},
    "location": {"type": "geo_point"},
}


@pytest.mark.parametrize(
    "response",
    [
        {"namespace": "products", "schema": SCHEMA},  # PR #283 route shape
        SCHEMA,  # bare NamespaceSchema
    ],
)
def test_mapping_is_exactly_the_schema_fields(monkeypatch, response):
    client, calls = client_with(monkeypatch, lambda *_: response)
    mapping = client.indices.get_mapping(index="products")
    assert mapping == {"products": {"mappings": {"properties": EXPECTED}}}
    assert calls == [("GET", "products", "schema")]


def test_type_spellings_are_translated():
    ops_type = KoshaClient(hosts="http://kosha.invalid").indices._es_field_type
    assert ops_type({"type": "Text"}) == "text"
    assert ops_type({"type": "keyword"}) == "keyword"
    assert ops_type({"type": 3}) == "long"
    assert ops_type({"type": "FIELD_TYPE_VECTOR"}) == "knn_vector"
    # GeoPoint (#212): every spelling, including the server's `GeoPoint`.
    assert ops_type({"type": "GeoPoint"}) == "geo_point"
    assert ops_type({"type": 8}) == "geo_point"
    # Only Text fields are analyzed: an analyzer with no type means text.
    assert ops_type({"analyzer": ["standard", "stem"]}) == "text"
    assert ops_type({"type": "FIELD_TYPE_UNSPECIFIED"}) is None
    assert ops_type({}) is None


def test_untyped_schema_field_is_listed_without_a_guessed_type(monkeypatch):
    schema = {"schema": {"fields": {"note": {"stored": True}}}}
    client, _ = client_with(monkeypatch, lambda *_: schema)
    assert client.indices.get_mapping(index="ns") == {
        "ns": {"mappings": {"properties": {"note": {}}}}
    }


@pytest.mark.parametrize(
    "response",
    [
        {"namespace": "ns", "schema": {}},
        {"namespace": "ns", "schema": {"fields": {}}},
        {"namespace": "ns", "schema": None},
        {},
    ],
)
def test_no_schema_means_empty_properties(monkeypatch, response):
    client, _ = client_with(monkeypatch, lambda *_: response)
    assert client.indices.get_mapping(index="ns") == {
        "ns": {"mappings": {"properties": {}}}
    }


@pytest.mark.parametrize(
    "error",
    [
        KoshaRequestError(404, "not found"),  # server older than the schema route
        KoshaRequestError(404, "namespace 'ns' not found"),
        KoshaRequestError(405, "method not allowed"),
        KoshaRequestError(400, "route not found"),
    ],
)
def test_older_server_or_unknown_namespace_means_empty_properties(monkeypatch, error):
    def raise_(*_):
        raise error

    client, _ = client_with(monkeypatch, raise_)
    assert client.indices.get_mapping(index="ns") == {
        "ns": {"mappings": {"properties": {}}}
    }


def test_real_errors_are_not_hidden(monkeypatch):
    def raise_(*_):
        raise KoshaRequestError(401, "unauthorized")

    client, _ = client_with(monkeypatch, raise_)
    with pytest.raises(KoshaRequestError):
        client.indices.get_mapping(index="ns")


def test_mapping_never_contains_placeholder_fields(monkeypatch):
    client, _ = client_with(monkeypatch, lambda *_: {"schema": {}})
    properties = client.indices.get_mapping(index="ns")["ns"]["mappings"]["properties"]
    for name in ("documentId", "content", "title", "sentAt"):
        assert name not in properties


def test_mapping_for_several_indices_and_default(monkeypatch):
    schemas = {"a": {"schema": {"fields": {"x": {"type": "FIELD_TYPE_DATE"}}}}}

    def responder(method, namespace, action):
        if namespace in schemas:
            return schemas[namespace]
        raise KoshaRequestError(404, "not found")

    client, calls = client_with(monkeypatch, responder)
    assert client.indices.get_mapping(index="a,b") == {
        "a": {"mappings": {"properties": {"x": {"type": "date"}}}},
        "b": {"mappings": {"properties": {}}},
    }
    assert client.indices.get_mapping(index=["b"]) == {
        "b": {"mappings": {"properties": {}}}
    }
    assert list(client.indices.get_mapping()) == ["default"]


def test_namespace_is_url_quoted(monkeypatch):
    client, calls = client_with(monkeypatch, lambda *_: {})
    client.indices.get_mapping(index="a/b")
    assert calls == [("GET", "a%2Fb", "schema")]


def test_settings_report_only_the_index_name():
    client = KoshaClient(hosts="http://kosha.invalid")
    settings = client.indices.get_settings(index="ns")
    assert settings == {"ns": {"settings": {"index": {"provided_name": "ns"}}}}
    for invented in ("refresh_interval", "number_of_shards", "number_of_replicas", "knn"):
        assert invented not in settings["ns"]["settings"]["index"]


def test_get_combines_real_mapping_and_settings(monkeypatch):
    schema = {"schema": {"fields": {"sku": {"type": "FIELD_TYPE_KEYWORD"}}}}
    client, _ = client_with(monkeypatch, lambda *_: schema)
    assert client.indices.get(index="ns") == {
        "ns": {
            "aliases": {},
            "mappings": {"properties": {"sku": {"type": "keyword"}}},
            "settings": {"index": {"provided_name": "ns"}},
        }
    }
