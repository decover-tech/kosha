#!/usr/bin/env python3
"""Python client smoke test against a live server with legacy routes off (#350).

Runs every kosha_client operation that touches the server (index, bulk with
index and update actions, update, search, count, get, delete_by_query,
refresh, ping, indices create/exists/delete) against a server started with
KOSHA_LEGACY_ROUTES=off, so any call to an unversioned route fails with
410 legacy_route_disabled.

  python3 scripts/api/client_smoke.py --host http://localhost:8080 --api-key sk-kosha-dev
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "clients", "python"))

from kosha_client import KoshaClient  # noqa: E402
from kosha_client.transport import KoshaRequestError  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", required=True)
    p.add_argument("--api-key", default=os.environ.get("KOSHA_API_KEY", "sk-kosha-dev"))
    args = p.parse_args()

    ns = "smoke-%s" % uuid.uuid4().hex[:8]
    c = KoshaClient(args.host, api_key=args.api_key, namespace=ns)
    try:
        assert c.ping(), "ping failed"
        c.indices.create(index=ns, body={"mappings": {"properties": {"title": {"type": "text"}, "tag": {"type": "keyword"}}}})
        assert c.indices.exists(index=ns)
        c.index(index=ns, id="1", body={"title": "hello world", "tag": "a"}, refresh=True)
        c.bulk(
            body=[
                {"index": {"_index": ns, "_id": "2"}},
                {"title": "hello there", "tag": "b"},
                {"update": {"_index": ns, "_id": "1"}},
                {"doc": {"tag": "c"}},
            ],
            refresh=True,
        )
        c.update(index=ns, id="2", body={"doc": {"tag": "d"}}, refresh=True)
        hits = c.search(index=ns, body={"query": {"match": {"title": "hello"}}})["hits"]
        assert hits["total"]["value"] == 2, hits
        got = c.get(index=ns, id="1")
        assert got["found"] and got["_source"]["tag"] == "c", got
        deleted = c.delete_by_query(index=ns, body={"query": {"term": {"tag": "d"}}})
        assert deleted["deleted"] == 1, deleted
        c.indices.refresh(index=ns)
        counted = c.count(index=ns, body={"query": {"match": {"title": "hello"}}})
        assert counted["count"] == 1, counted
    except KoshaRequestError as e:
        print("FAIL: %s (code=%s request_id=%s)" % (e, e.code, e.request_id))
        return 1
    finally:
        try:
            c.indices.delete(index=ns)
        except Exception:  # noqa: BLE001 - cleanup only
            pass
    print("client smoke: ok (namespace %s)" % ns)
    return 0


if __name__ == "__main__":
    sys.exit(main())
