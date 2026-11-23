"""The client calls only /v1 routes (#350): the unversioned Phase 1 routes
are deprecated and can be turned off server-side."""

from kosha_client import KoshaClient
from kosha_client.transport import KoshaRequestError


class RecordingTransport:
    def __init__(self):
        self.calls = []

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        self.calls.append((method, path, body))
        if path.endswith("/search"):
            return {"results": [], "total_hits": 0, "total_hits_relation": "eq"}
        if path.endswith("/delete"):
            return {"deleted": 0}
        if path.endswith("/documents") or path.endswith("/replace"):
            return {"indexed_count": 1}
        return {}


def client():
    c = KoshaClient("http://kosha.test", api_key="k", namespace="docs")
    c._transport = RecordingTransport()
    return c


def test_every_operation_uses_a_v1_path():
    c = client()
    c.index(index="docs", id="1", body={"title": "hello"}, refresh=True)
    c.update(index="docs", id="1", body={"doc": {"title": "bye"}})
    c.search(index="docs", body={"query": {"match": {"title": "hello"}}})
    c.delete_by_query(index="docs", body={"query": {"term": {"title": "bye"}}})
    c.bulk(body=[{"index": {"_index": "docs", "_id": "2"}}, {"title": "x"}])
    c.ping()
    paths = [path for _, path, _ in c._transport.calls]
    assert paths, "no requests made"
    for path in paths:
        assert path.startswith("v1/"), path
    assert "v1/namespaces/docs/documents" in paths
    assert "v1/namespaces/docs/replace" in paths
    assert "v1/namespaces/docs/search" in paths
    assert "v1/namespaces/docs/delete" in paths
    assert "v1/namespaces/docs/flush" in paths
    assert "v1/healthz" in paths
    # The namespace is in the path, not the body.
    for _, path, body in c._transport.calls:
        if path.startswith("v1/namespaces/") and isinstance(body, dict):
            assert "namespace" not in body, (path, body)


def test_namespace_is_escaped_in_the_path():
    c = client()
    c._request("POST", "flush", {"namespace": "a b"})
    assert c._transport.calls[-1][1] == "v1/namespaces/a%20b/flush"


class OldServerTransport(RecordingTransport):
    """A server older than API 0.9.0: no /v1/.../replace (a bare 404 with
    no error code), legacy /replace still served."""

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        if path.startswith("v1/") and path.endswith("/replace"):
            self.calls.append((method, path, body))
            raise KoshaRequestError(404, "not found", {"error": "not found"})
        return super().request(method, path, body, headers)


def test_partial_updates_fall_back_to_legacy_replace_on_an_older_server():
    c = KoshaClient("http://kosha.test", api_key="k", namespace="docs")
    c._transport = OldServerTransport()
    c.update(index="docs", id="1", body={"doc": {"title": "bye"}})
    c.update(index="docs", id="2", body={"doc": {"title": "again"}})
    paths = [path for _, path, _ in c._transport.calls]
    # One probe of the /v1 route, then legacy /replace from then on.
    assert paths == ["v1/namespaces/docs/replace", "replace", "replace"], paths
    assert c._transport.calls[1][2]["namespace"] == "docs"


class MissingNamespaceTransport(RecordingTransport):
    """A current server: its 404 carries `namespace_not_found`."""

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        self.calls.append((method, path, body))
        raise KoshaRequestError(
            404,
            "namespace 'docs' not found",
            {"error": "namespace 'docs' not found", "code": "namespace_not_found"},
        )


def test_a_current_servers_404_is_raised_not_sent_to_legacy():
    c = KoshaClient("http://kosha.test", api_key="k", namespace="docs")
    c._transport = MissingNamespaceTransport()
    try:
        c._request("POST", "replace", {"namespace": "docs", "documents": []})
    except KoshaRequestError as e:
        assert e.code == "namespace_not_found"
    else:
        raise AssertionError("expected KoshaRequestError")
    assert [path for _, path, _ in c._transport.calls] == ["v1/namespaces/docs/replace"]


class OldServerRestrictedKeyTransport(RecordingTransport):
    """A server older than API 0.9.0 with a scope- or namespace-restricted
    key: its authorization check refuses the unknown /v1/.../replace with a
    bare 403 before routing, while legacy /replace serves the same key."""

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        if path.startswith("v1/") and path.endswith("/replace"):
            self.calls.append((method, path, body))
            msg = "this API key is restricted (scopes or namespaces) and cannot use this route"
            raise KoshaRequestError(403, msg, {"error": msg})
        return super().request(method, path, body, headers)


def test_partial_updates_fall_back_on_an_older_servers_bare_403():
    c = KoshaClient("http://kosha.test", api_key="k", namespace="docs")
    c._transport = OldServerRestrictedKeyTransport()
    c.update(index="docs", id="1", body={"doc": {"title": "bye"}})
    c.update(index="docs", id="2", body={"doc": {"title": "again"}})
    paths = [path for _, path, _ in c._transport.calls]
    assert paths == ["v1/namespaces/docs/replace", "replace", "replace"], paths


class DeniedTransport(RecordingTransport):
    """A current server refusing the key: its 403 carries `permission_denied`."""

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        self.calls.append((method, path, body))
        raise KoshaRequestError(403, "denied", {"error": "denied", "code": "permission_denied"})


def test_a_current_servers_403_is_raised_not_sent_to_legacy():
    c = KoshaClient("http://kosha.test", api_key="k", namespace="docs")
    c._transport = DeniedTransport()
    try:
        c._request("POST", "replace", {"namespace": "docs", "documents": []})
    except KoshaRequestError as e:
        assert e.code == "permission_denied"
    else:
        raise AssertionError("expected KoshaRequestError")
    assert [path for _, path, _ in c._transport.calls] == ["v1/namespaces/docs/replace"]
    assert not getattr(c, "_legacy_replace", False)
