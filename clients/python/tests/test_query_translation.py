"""OpenSearch search body -> Kosha request.

The rule under test throughout: a clause is either translated faithfully or
the request is refused. It is never silently dropped, because dropping a
filter returns documents the caller excluded (and, on a delete, removes
documents the caller meant to keep).
"""

import pytest

from kosha_client import KoshaClient, KoshaRequestError

PROJECT = "7f808c2f2d914c0cbf70912832d51036"


def scope_filter(project=PROJECT):
    """A caller scope filter: a bool with only `should`."""
    return {
        "bool": {
            "should": [{"term": {"projectId": project}}, {"term": {"workspaceId": project}}]
        }
    }


IN_SCOPE = {"bool": {"must_not": [{"term": {"isArchived": False}}]}}
SCOPE_KOSHA = {
    "bool": {
        "should": [{"term": {"projectId": PROJECT}}, {"term": {"workspaceId": PROJECT}}],
        "minimum_should_match": 1,
    }
}


def translate(body):
    kosha, _ = KoshaClient._translate_search(body)
    return kosha


def refused(body):
    with pytest.raises(KoshaRequestError) as raised:
        KoshaClient._translate_search(body)
    assert raised.value.status_code == 400
    return raised.value.error


# ── Filters ────────────────────────────────────────────────────────────────


def test_scope_filter_nested_in_bool_filter_is_kept():
    # The exact nesting the benchmark sends: bool.filter -> bool.filter ->
    # bool.should. Kosha's parser used to reject the inner `filter` (400).
    kosha = translate(
        {
            "query": {
                "bool": {
                    "filter": [{"bool": {"filter": [scope_filter()]}}],
                    "must": [{"match": {"content": "contract"}}],
                }
            }
        }
    )
    assert kosha["query_text"] == "contract"
    assert kosha["filter"] == {"bool": {"must": [SCOPE_KOSHA]}}


def test_booleans_and_numbers_become_the_strings_kosha_stores():
    kosha = translate({"query": {"bool": {"filter": [IN_SCOPE, {"terms": {"pageNumber": [1, 2]}}]}}})
    assert kosha["filter"] == {
        "bool": {
            "must": [
                {"bool": {"must_not": [{"term": {"isArchived": "false"}}]}},
                {"terms": {"pageNumber": ["1", "2"]}},
            ]
        }
    }


def test_should_is_optional_next_to_a_must_and_required_alone():
    # OpenSearch: with a must/filter present and no minimum_should_match,
    # should clauses are optional, so they cannot change a filter's result.
    optional = KoshaClient._to_filter(
        {"bool": {"must": [{"term": {"a": "1"}}], "should": [{"term": {"b": "2"}}]}}
    )
    assert optional == {"bool": {"must": [{"term": {"a": "1"}}]}}

    explicit = KoshaClient._to_filter(
        {
            "bool": {
                "must": [{"term": {"a": "1"}}],
                "should": [{"term": {"b": "2"}}],
                "minimum_should_match": 1,
            }
        }
    )
    assert explicit["bool"]["should"] == [{"term": {"b": "2"}}]
    assert explicit["bool"]["minimum_should_match"] == 1

    alone = KoshaClient._to_filter({"bool": {"should": [{"term": {"b": "2"}}]}})
    assert alone["bool"]["minimum_should_match"] == 1


def test_exists_and_range_options():
    assert KoshaClient._to_filter({"exists": {"field": "startMs"}}) == {
        "exists": {"field": "startMs"}
    }
    ranged = KoshaClient._to_filter(
        {
            "range": {
                "publishedAt": {
                    "gte": "2024-01-01",
                    "lte": "2024-12-31",
                    "format": "strict_date_optional_time",
                    "time_zone": "UTC",
                }
            }
        }
    )
    assert ranged == {"range": {"publishedAt": {"gte": "2024-01-01", "lte": "2024-12-31"}}}


def test_prefix_short_and_long_forms():
    assert KoshaClient._to_filter({"prefix": {"path": "/matters/2026/"}}) == {
        "prefix": {"path": "/matters/2026/"}
    }
    # Long form, default case: sent in the short form.
    assert KoshaClient._to_filter(
        {"prefix": {"path": {"value": "/a/", "boost": 2.0, "rewrite": "constant_score"}}}
    ) == {"prefix": {"path": "/a/"}}
    assert KoshaClient._to_filter(
        {"prefix": {"sku": {"value": "ab", "case_insensitive": True}}}
    ) == {"prefix": {"sku": {"value": "ab", "case_insensitive": True}}}
    assert KoshaClient._to_filter(
        {"prefix": {"sku": {"value": "ab", "case_insensitive": False}}}
    ) == {"prefix": {"sku": "ab"}}


def test_prefix_composes_in_bool_and_query_context():
    kosha = translate(
        {
            "query": {
                "bool": {
                    "must": [{"match": {"content": "contract"}}],
                    "filter": [
                        {"prefix": {"status": "arch"}},
                        {"range": {"createdAt": {"gte": "2026-01-01"}}},
                    ],
                    "must_not": [{"prefix": {"path": "/tmp/"}}],
                }
            }
        }
    )
    assert kosha["query_text"] == "contract"
    assert kosha["filter"] == {
        "bool": {
            "must": [
                {"prefix": {"status": "arch"}},
                {"range": {"createdAt": {"gte": "2026-01-01"}}},
            ],
            "must_not": [{"prefix": {"path": "/tmp/"}}],
        }
    }


@pytest.mark.parametrize(
    "clause",
    [
        {"nested": {"path": "comments", "query": {"match_all": {}}}},
        {"term": {"_id": "abc"}},
        {"ids": {"values": ["a"]}},
        {"prefix": {"title": {"value": "ab", "fuzziness": 1}}},
        {"prefix": {"title": 12}},
        {"prefix": {"title": "ab", "path": "/a"}},
        {"prefix": {"title": {"case_insensitive": True}}},
        {"prefix": {"title": {"value": "ab", "case_insensitive": "yes"}}},
        {"bool": {"should": [{"term": {"a": "1"}}], "minimum_should_match": "75%"}},
    ],
)
def test_filters_kosha_cannot_express_are_refused(clause):
    message = refused({"query": {"bool": {"filter": [clause]}}})
    assert "unsupported by Kosha" in message


# ── Text ───────────────────────────────────────────────────────────────────


def test_match_defaults_to_or_like_opensearch():
    kosha = translate({"query": {"match": {"content": "breach of contract"}}})
    assert kosha["query_text"] == "breach of contract"
    assert kosha["operator"] == "or"
    kosha = translate(
        {"query": {"match": {"content": {"query": "breach contract", "operator": "and"}}}}
    )
    assert kosha["operator"] == "and"


def test_one_term_across_fields_is_one_clause():
    # A typical keyword query builder searches each term in four fields:
    # one clause over those four fields (Kosha's `fields`, #249).
    per_field = [
        {"match": {field: {"query": "indemnity"}}}
        for field in ("content", "title", "author", "labels")
    ]
    kosha = translate(
        {
            "query": {
                "bool": {
                    "must": [{"bool": {"should": per_field, "minimum_should_match": 1}}],
                    "filter": [scope_filter()],
                }
            }
        }
    )
    assert kosha["query_text"] == "indemnity"
    assert kosha["fields"] == ["content", "title", "author", "labels"]
    assert kosha["filter"] == SCOPE_KOSHA


def test_match_on_a_field_restricts_and_boosts_it():
    kosha = translate({"query": {"match": {"title": {"query": "indemnity", "boost": 2}}}})
    assert kosha["query_text"] == "indemnity"
    assert kosha["fields"] == ["title^2"]
    kosha = translate({"query": {"match": {"content": "indemnity"}}})
    assert kosha["fields"] == ["content"]


def test_multi_match_maps_fields_and_boosts():
    kosha = translate(
        {
            "query": {
                "multi_match": {
                    "query": "breach of contract",
                    "fields": ["title^3", "content"],
                    "type": "most_fields",
                    "operator": "and",
                }
            }
        }
    )
    assert kosha["query_text"] == "breach of contract"
    assert kosha["operator"] == "and"
    assert kosha["fields"] == ["title^3", "content"]
    # No fields (or `*`) searches every text field: no restriction.
    for spec in ({"query": "x"}, {"query": "x", "fields": ["*"]}):
        assert "fields" not in translate({"query": {"multi_match": spec}})


def test_same_term_in_several_fields_keeps_the_highest_boost():
    should = [
        {"match": {"title": {"query": "indemnity", "boost": 2}}},
        {"match": {"title": {"query": "indemnity", "boost": 5}}},
        {"match": {"content": {"query": "indemnity"}}},
    ]
    kosha = translate({"query": {"bool": {"should": should, "minimum_should_match": 1}}})
    assert kosha["fields"] == ["title^5", "content"]


def test_required_clauses_on_different_fields_search_their_union():
    kosha = translate(
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"title": "indemnity"}},
                        {"match": {"content": "liability"}},
                    ]
                }
            }
        }
    )
    assert kosha["query_text"] == "indemnity liability"
    assert kosha["fields"] == ["title", "content"]
    # One clause over every field lifts the restriction.
    kosha = translate(
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"title": "indemnity"}},
                        {"multi_match": {"query": "liability"}},
                    ]
                }
            }
        }
    )
    assert "fields" not in kosha


def test_one_phrase_across_fields_keeps_every_field():
    should = [
        {"match_phrase": {field: "breach of contract"}} for field in ("content", "title")
    ]
    kosha = translate({"query": {"bool": {"should": should, "minimum_should_match": 1}}})
    assert kosha["match_phrase"] == {
        "field": "content",
        "phrase": "breach of contract",
        "slop": 0,
        "fields": ["content", "title"],
    }


def test_several_required_terms_are_joined_with_and():
    kosha = translate(
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"content": "indemnity"}},
                        {"match": {"content": "liability"}},
                    ]
                }
            }
        }
    )
    assert kosha["query_text"] == "indemnity liability"
    assert kosha["operator"] == "and"


def test_or_of_terms_is_or():
    kosha = translate(
        {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"content": "indemnity"}},
                        {"match": {"content": "liability"}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        }
    )
    assert kosha["query_text"] == "indemnity liability"
    assert kosha["operator"] == "or"


def test_optional_score_boosts_do_not_change_the_query():
    kosha = translate(
        {
            "query": {
                "bool": {
                    "must": [{"match": {"content": "indemnity"}}],
                    "should": [{"match_phrase": {"content": {"query": "indemnity", "boost": 2}}}],
                    "minimum_should_match": 0,
                }
            }
        }
    )
    assert kosha["query_text"] == "indemnity"
    assert "match_phrase" not in kosha


def test_phrase_inside_a_bool_is_kept():
    # This is the benchmark's phrase request. The phrase used to be dropped,
    # leaving a project-wide filter query: every phrase "matched" 10,000.
    kosha = translate(
        {
            "size": 20,
            "track_total_hits": True,
            "query": {
                "bool": {
                    "filter": [scope_filter()],
                    "must": [{"match_phrase": {"content": "breach of contract"}}],
                }
            },
        }
    )
    assert kosha["match_phrase"] == {
        "field": "content",
        "phrase": "breach of contract",
        "slop": 0,
    }
    assert kosha["query_text"] == ""
    assert kosha["filter"] == SCOPE_KOSHA
    assert kosha["exact_total_hits"] is True


@pytest.mark.parametrize(
    "query",
    [
        # NOT over text.
        {"bool": {"must_not": [{"match": {"content": "draft"}}]}},
        # Text OR a filter.
        {
            "bool": {
                "should": [{"match": {"content": "a"}}, {"term": {"tag": "x"}}],
                "minimum_should_match": 1,
            }
        },
        # Two different phrases.
        {
            "bool": {
                "must": [
                    {"match_phrase": {"content": "first phrase"}},
                    {"match_phrase": {"content": "second phrase"}},
                ]
            }
        },
        # Proximity.
        {"span_near": {"clauses": [], "slop": 3}},
        {"intervals": {"content": {"match": {"query": "a b"}}}},
        {"script_score": {"query": {"match_all": {}}, "script": {"source": "1"}}},
    ],
)
def test_queries_kosha_cannot_express_are_refused(query):
    assert "unsupported by Kosha" in refused({"query": query})


# ── Vector search ──────────────────────────────────────────────────────────


def test_knn_filter_is_carried_into_the_vector_search():
    vector = [0.1] * 16
    kosha = translate(
        {
            "size": 10,
            "query": {
                "knn": {
                    "contentEmbedding": {
                        "vector": vector,
                        "k": 30,
                        "filter": {"bool": {"must": [scope_filter(), IN_SCOPE]}},
                    }
                }
            },
        }
    )
    knn = kosha["knn"]
    assert knn["field"] == "contentEmbedding"
    assert knn["k"] == 30 and knn["vector"] == vector
    assert knn["filter"] == {
        "bool": {
            "must": [
                SCOPE_KOSHA,
                {"bool": {"must_not": [{"term": {"isArchived": "false"}}]}},
            ]
        }
    }


def test_knn_filter_that_cannot_be_expressed_refuses_the_search():
    message = refused(
        {
            "query": {
                "knn": {
                    "contentEmbedding": {
                        "vector": [0.1] * 16,
                        "k": 5,
                        "filter": {"nested": {"path": "comments", "query": {}}},
                    }
                }
            }
        }
    )
    assert "nested" in message


# ── Everything around the query ────────────────────────────────────────────


def test_paging_sort_collapse_source_and_highlight():
    kosha, context = KoshaClient._translate_search(
        {
            "from": 40,
            "size": 20,
            "query": {"match": {"content": "contract"}},
            "sort": [{"_score": {"order": "desc"}}, {"pageNumber": {"order": "asc"}}, "_doc"],
            "collapse": {"field": "documentId"},
            "_source": {"excludes": ["contentEmbedding"]},
            "highlight": {
                "fields": {
                    "content": {"number_of_fragments": 0, "pre_tags": ["<b>"], "post_tags": ["</b>"]}
                }
            },
        }
    )
    assert (kosha["from"], kosha["max_results"]) == (40, 20)
    assert kosha["sort"] == [
        {"_score": {"order": "desc"}},
        {"pageNumber": {"order": "asc"}},
        {"_id": {"order": "asc"}},
    ]
    assert kosha["collapse"] == {"field": "documentId"}
    assert kosha["source_excludes"] == ["contentEmbedding"]
    assert kosha["highlight"] == {"fields": {"content": {"pre_tags": ["<b>"], "post_tags": ["</b>"]}}}
    assert context["collapse_field"] == "documentId"

    assert translate({"_source": ["documentId"]})["source_includes"] == ["documentId"]
    assert translate({"_source": False})["source_includes"] == []
    cursor = translate({"from": 40, "size": 5, "sort": [{"_id": "asc"}], "search_after": [""]})
    assert (cursor["from"], cursor["max_results"], cursor["search_after"]) == (0, 5, [""])


def test_aggregations_pass_through_and_sub_aggregations_are_refused():
    aggs = {
        "per_document": {"terms": {"field": "documentId", "size": 1000}},
        "total_documents": {"cardinality": {"field": "documentId", "precision_threshold": 40000}},
    }
    assert translate({"size": 0, "aggs": aggs})["aggs"] == aggs
    message = refused(
        {
            "aggs": {
                "a": {
                    "terms": {"field": "authorEmail"},
                    "aggs": {"docs": {"cardinality": {"field": "documentId"}}},
                }
            }
        }
    )
    assert "sub-aggregations" in message
    assert "top_hits" in refused({"aggs": {"one": {"top_hits": {"size": 1}}}})
    assert "stats" in refused({"aggs": {"s": {"stats": {"field": "pageNumber"}}}})
    assert "sub-aggregations under 'max'" in refused(
        {"aggs": {"m": {"max": {"field": "a"}, "aggs": {"x": {"min": {"field": "b"}}}}}}
    )


def test_metric_aggregations_and_metric_sub_aggregations_pass_through():
    metrics = {
        kind: {kind: {"field": "pageNumber"}}
        for kind in ("min", "max", "sum", "avg", "value_count")
    }
    assert translate({"size": 0, "aggs": metrics})["aggs"] == metrics
    nested = {
        "by_sender": {
            "terms": {"field": "senderEmail"},
            "meta": {"ignored": True},
            "aggregations": {"pages": {"sum": {"field": "pageNumber"}}},
        }
    }
    assert translate({"size": 0, "aggs": nested})["aggs"] == {
        "by_sender": {
            "terms": {"field": "senderEmail"},
            "aggs": {"pages": {"sum": {"field": "pageNumber"}}},
        }
    }


def test_histogram_aggregations_pass_through_with_metric_sub_aggregations():
    aggs = {
        "by_size": {"histogram": {"field": "bytes", "interval": 1024, "min_doc_count": 1}},
        "over_time": {
            "date_histogram": {"field": "sentAt", "fixed_interval": "1h", "offset": "+30m"},
            "aggs": {"pages": {"sum": {"field": "pageNumber"}}},
        },
    }
    assert translate({"size": 0, "aggs": aggs})["aggs"] == aggs
    assert "sub-aggregations other than" in refused(
        {
            "aggs": {
                "h": {
                    "histogram": {"field": "bytes", "interval": 1},
                    "aggs": {"t": {"terms": {"field": "senderEmail"}}},
                }
            }
        }
    )


def test_response_reports_relation_sort_values_collapse_fields_and_highlight():
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k")
    result = {
        "total_hits": 10000,
        "total_hits_relation": "gte",
        "aggregations": {"per_document": {"buckets": []}},
        "results": [
            {
                "doc_id": "c1",
                "score": 3.5,
                "highlight": {"content": ["a <b>contract</b>"]},
                "fields": [
                    {"name": "documentId", "field_type": "Text", "value": "docA"},
                    {"name": "pageNumber", "field_type": "Float", "value": "7"},
                ],
            }
        ],
    }
    context = {
        "sort": [{"_score": {"order": "desc"}}, {"pageNumber": {"order": "asc"}}, {"_id": {"order": "asc"}}],
        "collapse_field": "documentId.keyword",
    }
    response = client._build_search_response(result, took_ms=3, context=context)
    assert response["hits"]["total"] == {"value": 10000, "relation": "gte"}
    hit = response["hits"]["hits"][0]
    assert hit["sort"] == [3.5, 7.0, "c1"]
    assert hit["fields"] == {"documentId.keyword": ["docA"]}
    assert hit["highlight"] == {"content": ["a <b>contract</b>"]}
    assert response["aggregations"] == {"per_document": {"buckets": []}}


def test_highlight_translates_every_field_with_its_own_tags():
    """#195: several fields, per-field tags, top-level tags as the fallback."""
    kosha = translate(
        {
            "query": {"match": {"content": "contract"}},
            "highlight": {
                "pre_tags": ["<em>"],
                "post_tags": ["</em>"],
                "fields": {
                    "title": {},
                    "content": {"pre_tags": "<mark>", "post_tags": ["</mark>"], "fragment_size": 50},
                },
            },
        }
    )
    assert kosha["highlight"] == {
        "pre_tags": ["<em>"],
        "post_tags": ["</em>"],
        "fields": {"title": {}, "content": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]}},
    }
    # ES's ordered list form, order kept.
    listed = translate({"highlight": {"fields": [{"b": {}}, {"a": None}]}})["highlight"]
    assert list(listed["fields"]) == ["b", "a"]
    assert "fields" in refused({"highlight": {"fields": "content"}})


def test_highlight_round_trip_through_search(monkeypatch):
    """#195: ES request -> Kosha request -> Kosha response -> ES hit."""
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k", namespace="idx")
    sent = {}

    def fake_request(method, path, body=None):
        sent.update(body=body)
        # What the server answers for a `highlight.fields` request: the
        # per-field map, with fields the hit lacks left out.
        return {
            "total_hits": 1,
            "results": [
                {
                    "doc_id": "c1",
                    "score": 1.0,
                    "fields": [
                        {"name": "title", "field_type": "Text", "value": "Contract law"},
                        {"name": "content", "field_type": "Text", "value": "a contract"},
                    ],
                    "highlight": {
                        "title": ["<mark>Contract</mark> law"],
                        "content": ["a <em>contract</em>"],
                    },
                }
            ],
        }

    monkeypatch.setattr(client, "_request", fake_request)
    response = client.search(
        body={
            "query": {"match": {"content": "contract"}},
            "highlight": {
                "pre_tags": ["<em>"],
                "post_tags": ["</em>"],
                "fields": {
                    "title": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                    "content": {},
                    "missing": {},
                },
            },
        }
    )
    assert sent["body"]["highlight"]["fields"] == {
        "title": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
        "content": {},
        "missing": {},
    }
    hit = response["hits"]["hits"][0]
    assert hit["highlight"] == {
        "title": ["<mark>Contract</mark> law"],
        "content": ["a <em>contract</em>"],
    }
    # No highlight requested (or nothing matched): no `highlight` key.
    monkeypatch.setattr(
        client,
        "_request",
        lambda *a, **k: {"total_hits": 1, "results": [{"doc_id": "c2", "score": 1.0, "fields": []}]},
    )
    assert "highlight" not in client.search(body={"query": {"match_all": {}}})["hits"]["hits"][0]


def test_count_counts_the_query_it_was_given(monkeypatch):
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k", namespace="idx")
    sent = {}

    def fake_request(method, path, body=None):
        sent.update(method=method, path=path, body=body)
        return {"results": [], "total_hits": 42, "total_hits_relation": "eq"}

    monkeypatch.setattr(client, "_request", fake_request)
    out = client.count(index="idx", body={"query": {"bool": {"filter": [scope_filter()]}}})
    assert out["count"] == 42
    assert sent["body"]["filter"] == {"bool": {"must": [SCOPE_KOSHA]}}
    assert sent["body"]["max_results"] == 0
    assert sent["body"]["exact_total_hits"] is True


def test_delete_by_query_refuses_a_query_it_cannot_fully_express(monkeypatch):
    client = KoshaClient(hosts=["http://localhost:9"], api_key="k", namespace="idx")
    calls = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        return {"deleted": 3}

    monkeypatch.setattr(client, "_request", fake_request)
    # Fully expressible: sent as a filter.
    out = client.delete_by_query(
        index="idx", body={"query": {"bool": {"filter": [scope_filter()]}}}
    )
    assert out["deleted"] == 3
    assert calls[-1][2]["filter"] == {"bool": {"must": [SCOPE_KOSHA]}}

    # One clause Kosha cannot apply: nothing is sent. Sending the rest of
    # the filter would delete every document of the project.
    calls.clear()
    with pytest.raises(KoshaRequestError):
        client.delete_by_query(
            index="idx",
            body={
                "query": {
                    "bool": {
                        "filter": [
                            scope_filter(),
                            {"nested": {"path": "comments", "query": {}}},
                        ]
                    }
                }
            },
        )
    assert calls == []
    with pytest.raises(KoshaRequestError):
        client.delete_by_query(index="idx", body={})
