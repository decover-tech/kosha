"""``kosha-mcp``: Kosha as a Model Context Protocol server (#584).

Exposes ``AgentTools`` as MCP tools over stdio (the default, for Claude
Desktop, Claude Code, Cursor) or streamable HTTP. Needs the ``mcp`` extra:
``pip install 'kosha-client[mcp]'``. Configure with ``KOSHA_HOST``,
``KOSHA_API_KEY`` and optionally ``KOSHA_NAMESPACE``; a ``read``-scoped key
is enough unless ``--allow-writes`` is given.
"""

from __future__ import annotations

import argparse
import os
from typing import Any

from .agent import DEFAULT_MAX_RESULTS, AgentTools

INSTRUCTIONS = """\
Kosha is a search engine over the user's documents. Start with `search`
(keywords, BM25) or `semantic_search` (meaning, vectors); narrow with
`filter` (exact field conditions, e.g. {"prefix": {"path": "/contracts/2024/"}}),
and size result sets with `count` / `aggregate` before paging through them.
Every hit has an `id` and, when indexed with one, a `path`. Long field values
are cut to a snippet; `truncated_fields` names them."""

READ_ONLY = {"readOnlyHint": True, "openWorldHint": False}


def build_server(tools: AgentTools, allow_writes: bool = False, **settings: Any):
    """An ``mcp`` ``FastMCP`` server whose tools call ``tools``."""
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("kosha", instructions=INSTRUCTIONS, **settings)

    @server.tool(annotations=READ_ONLY)
    def search(
        query: str,
        namespace: str | None = None,
        filter: dict | None = None,
        fields: list[str] | None = None,
        max_results: int = DEFAULT_MAX_RESULTS,
        include_fields: list[str] | None = None,
    ) -> dict:
        """Keyword (BM25) search. `filter` is a Kosha filter clause such as
        {"term": {"status": "final"}} or {"bool": {"must": [...]}}; `fields`
        restricts matching to those text fields; `include_fields` limits the
        stored fields returned."""
        return tools.search(query, namespace, filter, fields, max_results, include_fields)

    @server.tool(annotations=READ_ONLY)
    def semantic_search(
        field: str,
        query: str | None = None,
        vector: list[float] | None = None,
        namespace: str | None = None,
        filter: dict | None = None,
        k: int = DEFAULT_MAX_RESULTS,
        hybrid: bool = False,
        include_fields: list[str] | None = None,
    ) -> dict:
        """Vector (kNN) search on the vector `field`, for `query` (embedded
        by the server) or a raw `vector`. `hybrid` also runs `query` as
        keyword search and fuses the two rankings (RRF)."""
        return tools.semantic_search(
            field, query, vector, namespace, filter, k, hybrid, include_fields
        )

    @server.tool(annotations=READ_ONLY)
    def filter(
        filter: dict,
        namespace: str | None = None,
        sort: list | None = None,
        max_results: int = DEFAULT_MAX_RESULTS,
        search_after: list | None = None,
        include_fields: list[str] | None = None,
    ) -> dict:
        """Documents matching a filter clause, unscored: term, terms, range,
        prefix, exists, bool, ... `sort` (e.g. [{"date": {"order": "desc"}}])
        with `search_after` (the last hit's sort values) pages."""
        return tools.filter(filter, namespace, sort, max_results, search_after, include_fields)

    @server.tool(annotations=READ_ONLY)
    def count(filter: dict | None = None, query: str = "", namespace: str | None = None) -> dict:
        """The exact number of documents matching `query` and `filter`."""
        return tools.count(filter, query, namespace)

    @server.tool(annotations=READ_ONLY)
    def aggregate(
        aggs: dict, filter: dict | None = None, query: str = "", namespace: str | None = None
    ) -> dict:
        """Aggregations over the matching documents, Elasticsearch-style:
        {"by_type": {"terms": {"field": "type"}}}, range, date_histogram,
        stats, cardinality, ..."""
        return tools.aggregate(aggs, filter, query, namespace)

    @server.tool(annotations=READ_ONLY)
    def list_branches(namespace: str | None = None) -> dict:
        """The namespace's copy-on-write branches."""
        return tools.list_branches(namespace)

    if allow_writes:

        @server.tool(annotations={"readOnlyHint": False, "destructiveHint": False})
        def branch(name: str, namespace: str | None = None) -> dict:
            """Create a copy-on-write branch `name` of the namespace: instant,
            copies nothing, and later writes to it leave the parent
            unchanged. Needs an admin key."""
            return tools.branch(name, namespace)

        @server.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
        def index_documents(
            documents: list[dict],
            namespace: str | None = None,
            keyword_fields: list[str] | None = None,
            flush: bool = True,
        ) -> dict:
            """Upsert documents, each {"id": "...", "fields": {"title": "...",
            "path": "/a/b.txt", "year": 2024}}. Strings are full text unless
            listed in `keyword_fields` (exact-match; `path` always is).
            Replaces any document with the same id."""
            return tools.index_documents(documents, namespace, keyword_fields, flush)

    return server


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="kosha-mcp", description=__doc__.splitlines()[0])
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind address")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port")
    parser.add_argument("--namespace", default=os.environ.get("KOSHA_NAMESPACE"))
    parser.add_argument(
        "--allow-writes",
        action="store_true",
        help="also expose branch and index_documents (needs a write/admin key)",
    )
    args = parser.parse_args(argv)
    tools = AgentTools(namespace=args.namespace)
    server = build_server(tools, args.allow_writes, host=args.host, port=args.port)
    server.run("streamable-http" if args.transport == "http" else "stdio")


if __name__ == "__main__":
    main()
