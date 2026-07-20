#!/usr/bin/env python3
"""Create a Kosha API key for a customer tenant.

This script talks to the Kosha admin API (requires an existing admin key).

Usage:
    python scripts/create_api_key.py acme-corp "staging key for Acme Corp"
"""

import os
import sys
import urllib.request
import urllib.parse
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "clients", "python"))


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/create_api_key.py <tenant_id> [description]")
        sys.exit(1)

    tenant_id = sys.argv[1]
    description = sys.argv[2] if len(sys.argv) > 2 else ""

    host = os.environ.get("KOSHA_HOST", "http://localhost:8080")
    api_key = os.environ.get("KOSHA_API_KEY")
    if not api_key:
        print("ERROR: KOSHA_API_KEY env var required")
        sys.exit(1)

    url = f"{host.rstrip('/')}/v1/admin/api-keys"
    body = json.dumps({"tenant_id": tenant_id, "description": description}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {api_key}")

    try:
        resp = urllib.request.urlopen(req)
        result = json.loads(resp.read().decode())
        print(f"Created API key for tenant '{result['tenant_id']}':")
        print(f"  API Key: {result['api_key']}")
        print(f"\nSet this as KOSHA_API_KEY for the customer, or add it to the database.")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"ERROR {e.code}: {body}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
