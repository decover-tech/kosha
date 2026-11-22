"""The client calls only /v1 routes (#350): the unversioned Phase 1 routes
are deprecated and can be turned off server-side."""

from kosha_client import KoshaClient


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
