"""OpenSearch compatibility shim.

``opensearchpy.helpers.bulk()`` (used by ParagraphRepository._bulk_insert_sources)
reaches into ``client.transport.serializer`` to JSON-encode and chunk
actions by size, then calls ``client.bulk(body=ndjson)`` once per chunk.

This module provides the exact duck-typed objects that
``opensearchpy.helpers.bulk()`` and ``opensearch_dsl`` expect so that the
KoshaClient can be used as a drop-in replacement for ``opensearchpy.OpenSearch``.
"""

from __future__ import annotations

import json
from typing import Any

from .transport import json_default


class Serializer:
    """Mimics ``opensearchpy.serializer.JSONSerializer``.

    ``opensearchpy.helpers.bulk()`` grabs ``client.transport.serializer.dumps``
    to encode action lines.  This shim makes that work without importing
    opensearchpy itself.
    """

    def dumps(self, data: Any) -> str:
        if isinstance(data, str):
            return data
        return json.dumps(data, default=json_default)


class Transport:
    """Duck-typed transport for ``opensearchpy.helpers.bulk()``.

    ``helpers.bulk()`` reads ``client.transport.serializer``.  That's the
    *only* attribute it accesses — no actual HTTP transport logic.
    """

    def __init__(self) -> None:
        self.serializer = Serializer()
