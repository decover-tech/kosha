"""AgentTools without MCP: index a few files, then search, filter and count.

    KOSHA_HOST=http://localhost:8080 KOSHA_API_KEY=sk-... python examples/agent_tools.py
"""

import json

from kosha_client import AgentTools

tools = AgentTools(namespace="agent-demo")
tools.index_documents(
    [
        {"id": "a", "fields": {"path": "/contracts/2024/acme.txt", "body": "Acme indemnifies Beta.", "year": 2024}},
        {"id": "b", "fields": {"path": "/contracts/2023/beta.txt", "body": "Late fees accrue monthly.", "year": 2023}},
        {"id": "c", "fields": {"path": "/notes/todo.txt", "body": "Review the indemnity clause.", "year": 2024}},
    ]
)
print(json.dumps(tools.search("indemnity", max_results=5), indent=2))
print(json.dumps(tools.filter({"prefix": {"path": "/contracts/"}}, sort=[{"year": {"order": "desc"}}]), indent=2))
print(tools.count({"range": {"year": {"gte": 2024}}}))
