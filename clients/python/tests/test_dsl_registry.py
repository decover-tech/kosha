"""The DSL registry in client.py and the compatibility matrix agree.

docs/compat/opensearch.md is generated from scripts/conformance/dsl/cases.yaml
(scripts/conformance/dsl/run.py). These tests fail when the translator
handles a query clause, filter, aggregation, search body key or client method
that has no case there, so the matrix cannot silently fall behind the code.
They also check that the registry sets describe what the translator really
does.
"""

from __future__ import annotations

import inspect
import os

import pytest
import yaml

from kosha_client import KoshaClient, KoshaRequestError
from kosha_client.client import (
    AGG_KINDS,
    FILTER_KINDS,
    QUERY_KINDS,
    SEARCH_BODY_KEYS,
    TEXT_QUERY_KINDS,
    IndexOps,
    TasksOps,
)

CASES = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "scripts", "conformance", "dsl", "cases.yaml"
)

# KoshaClient methods that are not Elasticsearch APIs: Kosha helpers (the
# native /v1 search_all and bulk_index are pinned by clients/conformance,
# not by the DSL matrix) and the connection lifecycle.
NOT_APIS = {"rerank_spec", "close", "search_all", "bulk_index"}


@pytest.fixture(scope="module")
def cases() -> list[dict]:
    with open(CASES, encoding="utf-8") as f:
        return yaml.safe_load(f)


def covered(cases: list[dict], *categories: str) -> set[str]:
    return {
        kind
        for case in cases
        if not categories or case["category"] in categories
        for kind in case.get("kinds") or []
    }


def public_apis() -> set[str]:
    def methods(cls) -> set[str]:
        return {
            name
            for name, member in vars(cls).items()
            if not name.startswith("_") and inspect.isfunction(member)
        }

    apis = methods(KoshaClient) - NOT_APIS
    apis |= {"indices." + name for name in methods(IndexOps)}
    apis |= {"tasks." + name for name in methods(TasksOps)}
    return apis


def test_every_filter_kind_has_a_filter_case(cases):
    assert FILTER_KINDS - covered(cases, "filter") == set()


def test_every_query_kind_has_a_query_case(cases):
    # Filter kinds run in query context too; the filter rows cover them,
    # and one query row covers the scoring difference.
    needed = (QUERY_KINDS - FILTER_KINDS) | {"match_all", "bool"}
    assert needed - covered(cases, "query") == set()


def test_every_aggregation_kind_has_a_case(cases):
    assert AGG_KINDS - covered(cases, "aggregation") == set()


def test_every_search_body_key_has_a_case(cases):
    assert SEARCH_BODY_KEYS - covered(cases) == set()


def test_every_client_api_has_a_case(cases):
    assert public_apis() - covered(cases, "index_api") == set()


def test_cases_name_only_registered_kinds(cases):
    known = QUERY_KINDS | FILTER_KINDS | AGG_KINDS | SEARCH_BODY_KEYS | public_apis()
    assert covered(cases) - known == set()


class _Recording(dict):
    """A search body that records which keys the translator reads."""

    def __init__(self) -> None:
        # Non-empty, so helpers that skip an empty body still look.
        super().__init__({"__probe__": None})
        self.read: set[str] = set()

    def get(self, key, default=None):
        self.read.add(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.read.add(key)
        return super().__getitem__(key)

    def __contains__(self, key):
        self.read.add(key)
        return super().__contains__(key)


def test_search_body_keys_are_the_keys_the_translator_reads():
    body = _Recording()
    KoshaClient._translate_search(body)
    assert body.read == SEARCH_BODY_KEYS


@pytest.mark.parametrize("kind", sorted(FILTER_KINDS))
def test_each_filter_kind_is_translated(kind):
    sample = {
        "match_all": {},
        "term": {"status": "open"},
        "terms": {"status": ["open"]},
        "range": {"priority": {"gte": 1}},
        "exists": {"field": "status"},
        "prefix": {"status": "op"},
        "bool": {"filter": [{"term": {"status": "open"}}]},
    }[kind]
    assert KoshaClient._to_filter({kind: sample})


def test_a_kind_outside_the_registry_is_refused():
    with pytest.raises(KoshaRequestError, match="'nested' as a filter"):
        KoshaClient._to_filter({"nested": {"path": "a", "query": {"match_all": {}}}})
    assert KoshaClient._text_leaf({"query_string": {"query": "a"}}) is None


@pytest.mark.parametrize("kind", sorted(TEXT_QUERY_KINDS))
def test_each_text_kind_is_a_text_leaf(kind):
    sample = {
        "match": {"title": "breach"},
        "multi_match": {"query": "breach", "fields": ["title"]},
        "match_phrase": {"title": "breach of contract"},
        "wildcard": {"title": "bre*"},
    }[kind]
    assert KoshaClient._text_leaf({kind: sample}) is not None


@pytest.mark.parametrize("kind", sorted(AGG_KINDS))
def test_each_aggregation_kind_is_passed_through(kind):
    spec = {"field": "priority"}
    assert KoshaClient._translate_aggs({"a": {kind: spec}}, parent=None) == {"a": {kind: spec}}
