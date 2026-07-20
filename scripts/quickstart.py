#!/usr/bin/env python3
"""Kosha quickstart — test exactly as a third-party customer would.

Usage:
    # Start Kosha locally
    docker compose up --build

    # In another terminal, run this script
    KOSHA_HOST=http://localhost:8080 KOSHA_API_KEY=sk-kosha-dev python scripts/quickstart.py

Or for a hosted instance:
    KOSHA_HOST=https://app.kosha.io KOSHA_API_KEY=sk-acme-corp-xxx python scripts/quickstart.py

What it demonstrates:
    1. Health check
    2. Index documents
    3. Search
    4. Flush and re-search
    5. Stats
"""

import json
import os
import sys

# The client respects KOSHA_HOST and KOSHA_API_KEY env vars automatically.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "clients", "python"))
from kosha_client import KoshaClient  # noqa: E402

client = KoshaClient()
NAMESPACE = "quickstart-demo"


def section(title: str) -> None:
    print(f"\n═══ {title} ═══")


# ── 1. Health ────────────────────────────────────────────────────────────────
section("1. Health check")
try:
    ok = client.ping()
    print(f"   Kosha reachable: {ok}")
except Exception as e:
    print(f"   FAILED: {e}")
    print("   Is Kosha running?  Try: docker compose up --build")
    sys.exit(1)


# ── 2. Index ─────────────────────────────────────────────────────────────────
section("2. Index documents")
docs = [
    {"index": {"_index": NAMESPACE, "_id": "doc-1"}},
    {"title": "breach of contract", "content": "The defendant breached the agreement by failing to deliver goods on time."},
    {"index": {"_index": NAMESPACE, "_id": "doc-2"}},
    {"title": "employment dispute", "content": "The plaintiff alleges wrongful termination and seeks damages."},
    {"index": {"_index": NAMESPACE, "_id": "doc-3"}},
    {"title": "merger analysis", "content": "The proposed merger raises antitrust concerns under Section 7 of the Clayton Act."},
]
result = client.bulk(body=docs)
print(f"   Indexed: {sum(1 for i in result.get('items', []) if i.get('index', {}).get('status') == 201)} documents")


# ── 3. Search ────────────────────────────────────────────────────────────────
section("3. Search")
resp = client.search(index=NAMESPACE, body={"query": {"match": {"content": "breach"}}})
total = resp["hits"]["total"]["value"]
hits = resp["hits"]["hits"]
print(f"   Found {total} result(s) for 'breach':")
for h in hits:
    src = h["_source"]
    print(f"     [{h['_id']}] {src.get('title', '')}  (score={h['_score']:.3f})")


# ── 4. Flush + verify ────────────────────────────────────────────────────────
section("4. Flush + re-search")
# The client already flushes after bulk in Phase 1, but demonstrate the explicit call.
resp = client.transport._request("POST", "flush", body={"namespace": NAMESPACE})
print(f"   Flush: {resp}")

resp = client.search(index=NAMESPACE, body={"query": {"match": {"content": "antitrust"}}})
total = resp["hits"]["total"]["value"]
hits = resp["hits"]["hits"]
print(f"   Found {total} result(s) for 'antitrust':")
for h in hits:
    src = h["_source"]
    print(f"     [{h['_id']}] {src.get('title', '')}  (score={h['_score']:.3f})")


# ── 5. Stats ─────────────────────────────────────────────────────────────────
section("5. Stats")
resp = client.transport._request("GET", "stats")
total_docs = resp.get("total_documents", 0)
namespaces = resp.get("namespaces", [])
print(f"   Total documents: {total_docs}")
for ns in namespaces:
    print(f"     {ns['namespace']}: {ns['documents']} docs, {ns['segments']} segment(s)")


print(f"\n✅  Quickstart complete.  Namespace '{NAMESPACE}' has 3 documents ready to search.")
