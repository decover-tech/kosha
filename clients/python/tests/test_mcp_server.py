"""kosha-mcp (#584): the MCP tool list and a tool call end to end."""

import asyncio
import json

import pytest

pytest.importorskip("mcp")

from kosha_client import AgentTools, KoshaClient  # noqa: E402
from kosha_client.mcp_server import build_server  # noqa: E402


class FakeTransport:
    def __init__(self):
        self.calls = []

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        self.calls.append((method, path, body))
        return {"results": [], "total_hits": 3, "total_hits_relation": "eq"}


def server(allow_writes=False):
    c = KoshaClient("http://kosha.test", api_key="k")
    c._transport = FakeTransport()
    return build_server(AgentTools(c, namespace="docs"), allow_writes), c._transport


def tool_names(s):
    return sorted(t.name for t in asyncio.run(s.list_tools()))


def test_read_only_tools_by_default():
    s, _ = server()
    assert tool_names(s) == ["aggregate", "count", "filter", "list_branches", "search", "semantic_search"]
    for t in asyncio.run(s.list_tools()):
        assert t.description, t.name
        assert t.annotations.readOnlyHint is True, t.name


def test_writes_are_opt_in():
    s, _ = server(allow_writes=True)
    assert {"branch", "index_documents"} <= set(tool_names(s))


def test_call_tool_hits_the_api():
    s, tr = server()
    result = asyncio.run(s.call_tool("count", {"filter": {"term": {"type": "nda"}}}))
    content = result[0] if isinstance(result, tuple) else result
    assert json.loads(content[0].text) == {"count": 3, "relation": "eq"}
    assert tr.calls[-1][1] == "v1/namespaces/docs/search"
    assert tr.calls[-1][2]["filter"] == {"term": {"type": "nda"}}
