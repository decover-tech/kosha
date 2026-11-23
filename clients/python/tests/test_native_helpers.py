"""search_all and bulk_index (#351), against an in-memory fake transport."""

from __future__ import annotations

import json
import threading
import time

import pytest

from kosha_client import BulkError, KoshaClient, KoshaConfigError, KoshaRequestError


def doc(i, tag=None):
    fields = [{"name": "title", "field_type": "Text", "value": f"title {i}"}]
    if tag is not None:
        fields.append({"name": "tag", "field_type": "Keyword", "value": tag})
    return {"id": f"d{i:04d}", "fields": fields}


class SearchTransport:
    """Sorts, applies search_after and pages like the server."""

    def __init__(self, docs):
        self.docs = docs
        self.calls = []

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        self.calls.append((method, path, json.loads(json.dumps(body))))
        keys = [(next(iter(e)), -1 if next(iter(e.values())).get("order") == "desc" else 1) for e in body["sort"]]

        def value(d, k):
            if k == "_id":
                return d["id"]
            return next((f["value"] for f in d["fields"] if f["name"] == k), "")

        def cmp(a, b):
            for (_, direction), x, y in zip(keys, a, b):
                if x != y:
                    return direction if x > y else -direction
            return 0

        import functools

        rows = sorted(
            ((d, [value(d, k) for k, _ in keys]) for d in self.docs),
            key=functools.cmp_to_key(lambda x, y: cmp(x[1], y[1])),
        )
        if body.get("search_after"):
            rows = [r for r in rows if cmp(r[1], body["search_after"]) > 0]
        include = body.get("source_includes")
        results = [
            {
                "doc_id": d["id"],
                "score": 1.0,
                "fields": [f for f in d["fields"] if include is None or f["name"] in include],
            }
            for d, _ in rows[: body["max_results"]]
        ]
        return {"results": results, "total_hits": len(self.docs)}


def client_with(transport):
    c = KoshaClient("http://kosha.test", api_key="k")
    c._transport = transport
    return c


def test_search_all_returns_every_id_once_in_order():
    docs = [doc(i, f"t{i % 7}") for i in range(250)]
    t = SearchTransport(docs)
    c = client_with(t)
    ids = [h["doc_id"] for h in c.search_all("n", {"query_text": ""}, page_size=100)]
    assert ids == [d["id"] for d in docs]
    assert len(t.calls) == 3
    assert t.calls[0][1] == "v1/namespaces/n/search"
    assert t.calls[0][2]["sort"] == [{"_id": {"order": "asc"}}]
    assert t.calls[1][2]["search_after"] == ["d0099"]

    body = {"query_text": "", "sort": [{"tag": {"order": "asc"}}], "source_includes": ["title"]}
    ids = [h["doc_id"] for h in c.search_all("n", body, page_size=100)]
    expected = [d["id"] for d in sorted(docs, key=lambda d: (d["fields"][1]["value"], d["id"]))]
    assert ids == expected
    last = t.calls[-1][2]
    assert last["source_includes"] == ["title", "tag"]
    assert last["sort"] == [{"tag": {"order": "asc"}}, {"_id": {"order": "asc"}}]
    assert body == {"query_text": "", "sort": [{"tag": {"order": "asc"}}], "source_includes": ["title"]}


@pytest.mark.parametrize(
    "bad",
    [
        {"query_text": "", "from": 10},
        {"query_text": "", "search_after": ["x"]},
        {"query_text": "", "rerank": {"query": "x"}},
        {"query_text": "", "knn": {"field": "v", "vector": [1]}},
        {"query_text": "", "mode": "hybrid"},
    ],
)
def test_search_all_rejects_bodies_it_cannot_page(bad):
    with pytest.raises(KoshaConfigError):
        list(client_with(SearchTransport([])).search_all("n", bad))


def test_search_all_fails_instead_of_looping():
    class Stuck:
        def request(self, *a, **k):  # noqa: ANN002, ANN003
            return {"results": [{"doc_id": "same"}]}

    with pytest.raises(KoshaConfigError):
        list(client_with(Stuck()).search_all("n", {"query_text": ""}, page_size=1))


class IndexTransport:
    def __init__(self, fail_id=None, slow_first=None):
        self.fail_id = fail_id
        self.slow_first = slow_first
        self.batches = []
        self.lock = threading.Lock()

    def request(self, method, path, body=None, headers=None):  # noqa: ANN001
        docs = body["documents"]
        if self.slow_first and docs[0]["id"] == self.slow_first:
            time.sleep(0.1)
        with self.lock:
            self.batches.append([(d["id"], d["fields"][0]["value"]) for d in docs])
        if self.fail_id and any(d["id"] == self.fail_id for d in docs):
            raise KoshaRequestError(400, "bad", {"code": "invalid_argument", "retryable": False})
        return {"indexed_count": len(docs)}


def test_bulk_index_batches_by_count():
    t = IndexTransport()
    out = client_with(t).bulk_index("n", (doc(i) for i in range(250)), max_docs_per_batch=40)
    assert out == {"indexed": 250, "batches": 7, "failures": []}
    assert sorted(len(b) for b in t.batches) == [10, 40, 40, 40, 40, 40, 40]


def test_bulk_index_splits_on_the_byte_cap():
    t = IndexTransport()
    big = {"id": "big", "fields": [{"name": "title", "field_type": "Text", "value": "x" * 5000}]}
    one = len(json.dumps(doc(1), separators=(",", ":")).encode()) + 1
    out = client_with(t).bulk_index("n", [doc(1), doc(2), big, doc(3)], max_bytes_per_batch=2 * one + 1, concurrency=1)
    assert out["batches"] == 3
    assert [[i for i, _ in b] for b in t.batches] == [["d0001", "d0002"], ["big"], ["d0003"]]


def test_bulk_index_reports_the_failed_batch():
    t = IndexTransport(fail_id="d0005")
    docs = [doc(i) for i in range(20)]
    with pytest.raises(BulkError) as e:
        client_with(t).bulk_index("n", docs, max_docs_per_batch=4, concurrency=1)
    result = e.value.result
    assert [f["batch_index"] for f in result["failures"]] == [1]
    assert result["failures"][0]["ids"] == ["d0004", "d0005", "d0006", "d0007"]
    assert result["failures"][0]["error"].code == "invalid_argument"
    assert result["indexed"] == 4
    assert len(t.batches) == 2
    assert e.value.code == "invalid_argument"

    out = client_with(IndexTransport(fail_id="d0005")).bulk_index("n", docs, max_docs_per_batch=4, on_error="continue")
    assert out["indexed"] == 16 and out["batches"] == 5
    assert [f["batch_index"] for f in out["failures"]] == [1]


def test_bulk_index_last_occurrence_wins():
    t = IndexTransport(slow_first="d0000")

    def v(i, value):
        return {"id": f"d{i:04d}", "fields": [{"name": "title", "field_type": "Text", "value": value}]}

    docs = [v(0, "a"), v(1, "a"), v(2, "a"), v(0, "b"), v(3, "a"), v(3, "c")]
    out = client_with(t).bulk_index("n", docs, max_docs_per_batch=2, concurrency=4)
    assert out["failures"] == []
    order = [x for b in t.batches for x in b]
    assert order.index(("d0000", "b")) > order.index(("d0000", "a"))
    assert ("d0003", "c") in order and ("d0003", "a") not in order
