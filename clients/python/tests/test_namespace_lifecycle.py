"""indices.create / delete / exists call the real namespace routes.

``indices.delete`` used to log and return ``{"acknowledged": True}`` without
deleting anything, and ``indices.create`` was a no-op.
"""

import urllib.parse

import pytest

from kosha_client import KoshaClient, KoshaRequestError
from kosha_client.transport import Transport


class FakeServer:
    """Records every request; answers from ``namespaces`` like the server."""

    def __init__(self):
        self.namespaces = set()
        self.calls = []

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "POST" and path == "v1/namespaces":
            ns = body["namespace"]
            if ns in self.namespaces:
                raise KoshaRequestError(
                    409,
                    f"namespace '{ns}' already exists",
                    {"error": f"namespace '{ns}' already exists"},
                )
            self.namespaces.add(ns)
            return {"id": ns, "schema": body.get("schema", {"fields": {}})}
        parts = [urllib.parse.unquote(p) for p in path.split("/")]
        if method == "DELETE" and len(parts) == 3:
            ns = parts[2]
            if ns not in self.namespaces:
                raise KoshaRequestError(404, f"namespace '{ns}' not found")
            self.namespaces.remove(ns)
            return {"namespace": ns, "acknowledged": True}
        if method == "GET" and path.endswith("/stats"):
            if parts[2] not in self.namespaces:
                raise KoshaRequestError(404, f"namespace '{parts[2]}' not found")
            return {"namespace": parts[2], "documents": 0}
        raise AssertionError(f"unexpected request {method} {path}")


@pytest.fixture
def server(monkeypatch):
    fake = FakeServer()
    monkeypatch.setattr(
        Transport, "request", lambda self, method, path, body=None: fake.request(method, path, body)
    )
    return fake


@pytest.fixture
def client():
    return KoshaClient(hosts="http://kosha.invalid", max_retries=1)


def test_create_posts_the_namespace(server, client):
    resp = client.indices.create(index="products")
    assert resp == {"acknowledged": True, "shards_acknowledged": True, "index": "products"}
    assert server.calls == [("POST", "v1/namespaces", {"namespace": "products"})]
    assert "products" in server.namespaces


def test_create_translates_plain_mappings_and_ignores_settings(server, client):
    client.indices.create(
        index="products",
        body={
            "settings": {"number_of_shards": 3},
            "mappings": {
                "properties": {
                    "title": {"type": "text", "analyzer": "english"},
                    "sku": {"type": "keyword"},
                    "qty": {"type": "integer"},
                    "price": {"type": "scaled_float", "scaling_factor": 100},
                    "created": {"type": "date"},
                    "active": {"type": "boolean"},
                    "embedding": {"type": "knn_vector", "dimension": 4},
                    "internal": {"type": "keyword", "index": False},
                    "location": {"type": "geo_point"},
                    "author": {"properties": {"name": {"type": "text"}}},
                }
            },
        },
    )
    (_, _, body), = server.calls
    assert body["namespace"] == "products"
    assert "settings" not in body
    fields = body["schema"]["fields"]
    assert fields["title"] == {
        "type": "FIELD_TYPE_TEXT",
        "indexed": True,
        "stored": True,
        "analyzer": ["english"],
    }
    assert fields["sku"]["type"] == "FIELD_TYPE_KEYWORD"
    assert fields["qty"]["type"] == "FIELD_TYPE_INTEGER"
    assert fields["price"]["type"] == "FIELD_TYPE_FLOAT"
    assert fields["created"]["type"] == "FIELD_TYPE_DATE"
    assert fields["active"]["type"] == "FIELD_TYPE_BOOLEAN"
    assert fields["embedding"]["type"] == "FIELD_TYPE_VECTOR"
    assert fields["internal"]["indexed"] is False
    # No Kosha equivalent: left out, never guessed.
    assert "location" not in fields
    assert "author" not in fields


def test_create_without_translatable_mappings_sends_no_schema(server, client):
    client.indices.create(index="p", body={"mappings": {"properties": {"g": {"type": "geo_shape"}}}})
    assert server.calls == [("POST", "v1/namespaces", {"namespace": "p"})]


def test_create_existing_raises_409_unless_ignored(server, client):
    client.indices.create(index="products")
    with pytest.raises(KoshaRequestError) as err:
        client.indices.create(index="products")
    assert err.value.status_code == 409
    # ES callers ignore 400 (resource_already_exists_exception).
    for ignore in (400, 409, [400, 404]):
        resp = client.indices.create(index="products", ignore=ignore)
        assert "already exists" in resp["error"]


def test_delete_calls_the_server_and_reports_missing(server, client):
    client.indices.create(index="products")
    assert client.indices.delete(index="products") == {"acknowledged": True}
    assert server.calls[-1] == ("DELETE", "v1/namespaces/products", None)
    assert "products" not in server.namespaces

    # Deleting again is ES NotFoundError: a 404, not a fake acknowledgement.
    with pytest.raises(KoshaRequestError) as err:
        client.indices.delete(index="products")
    assert err.value.status_code == 404
    assert client.indices.delete(index="products", ignore_unavailable=True) == {
        "acknowledged": True
    }
    assert client.indices.delete(index="products", ignore=[404]) == {"acknowledged": True}


def test_delete_handles_lists_quotes_names_and_requires_an_index(server, client):
    server.namespaces.update({"a", "b c"})
    client.indices.delete(index="a,b c")
    assert [c[:2] for c in server.calls] == [
        ("DELETE", "v1/namespaces/a"),
        ("DELETE", "v1/namespaces/b%20c"),
    ]
    with pytest.raises(ValueError):
        client.indices.delete(index=None)


def test_exists_follows_the_namespace(server, client):
    assert client.indices.exists(index="products") is False
    client.indices.create(index="products")
    assert client.indices.exists(index="products") is True
    client.indices.delete(index="products")
    assert client.indices.exists(index="products") is False


def test_exists_raises_on_errors_other_than_404(monkeypatch, client):
    def boom(self, method, path, body=None):
        raise KoshaRequestError(503, "unavailable")

    monkeypatch.setattr(Transport, "request", boom)
    with pytest.raises(KoshaRequestError):
        client.indices.exists(index="products")
