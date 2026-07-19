"""Kosha client — a drop-in replacement for opensearchpy.OpenSearch.

Usage (swap two lines in Sage's connection_factory.py)::

    # Before:
    # from opensearchpy import OpenSearch
    # client = OpenSearch(hosts=[...], http_auth=(...))

    # After:
    from kosha_client import KoshaClient as OpenSearch
    client = OpenSearch(hosts=[...], http_auth=(...))

All existing opensearch_dsl code (Search, Q, Document.save, helpers.bulk)
works because KoshaClient.search(), .index(), .bulk() return dicts in the
same shape as the OpenSearch / Elasticsearch JSON API.
"""

from .client import KoshaClient

__all__ = ["KoshaClient"]
