"""Kosha client — a drop-in replacement for opensearchpy.OpenSearch.

Usage (swap two lines where you build your OpenSearch client)::

    # Before:
    # from opensearchpy import OpenSearch
    # client = OpenSearch(hosts=[...], http_auth=(...))

    # After:
    from kosha_client import KoshaClient as OpenSearch
    client = OpenSearch(hosts=[...], http_auth=(...))

All existing opensearch_dsl code (Search, Q, Document.save, helpers.bulk)
works because KoshaClient.search(), .index(), .bulk() return dicts in the
same shape as the OpenSearch / Elasticsearch JSON API.

``KoshaClient.search_all()`` and ``.bulk_index()`` take native /v1 bodies
and behave like the TypeScript and Go SDKs' ``searchAll`` / ``bulkIndex``.
"""

from .client import KoshaClient
from .native import BulkError, KoshaConfigError
from .transport import KoshaRequestError

__all__ = ["BulkError", "KoshaClient", "KoshaConfigError", "KoshaRequestError"]
