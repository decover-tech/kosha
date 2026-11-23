#!/usr/bin/env python3
"""Copy Elasticsearch / OpenSearch indices into Kosha through the /v1 API.

Scrolls one or more ES indices and writes every document to a Kosha
namespace with ``POST /v1/namespaces/{ns}/documents``, then flushes the
namespace and checks that Kosha holds as many documents as ES counted. The
step-by-step guide is docs/migration/from-elasticsearch.md.

Each index is copied into a namespace of the same name (``--namespace``
renames a single index). With ``--create-schema-from-mapping`` the ES mapping
of each index becomes the namespace schema, and its field types decide how
values are sent: ``keyword`` stays Keyword, ``long`` stays Integer, ``date``
stays Date. Without it, values are typed from their JSON shape exactly as
kosha-client's ``index()`` does (string -> Text, number -> Float, ...), so a
copied document is indistinguishable from one written through the client.

Stdlib-only, so it runs from a bare ``python:3.x-slim`` container. ES auth is
HTTP basic (the default; credentials in the URL or the environment) or AWS
SigV4 (``ELASTICSEARCH_AUTH_MODE=sigv4``, service name "es"), which needs
``boto3``/``botocore`` and credentials resolvable from the environment.

Usage:
    # Everything matching a pattern
    python scripts/copy_es_to_kosha.py --pattern 'logs-*'

    # One index, keeping keyword / integer / date semantics from its mapping
    python scripts/copy_es_to_kosha.py --index products --create-schema-from-mapping

    # One index, capped, into a scratch namespace (smoke test)
    python scripts/copy_es_to_kosha.py --index products --namespace products-smoke --limit 100

    # Parallel sliced scroll (4 ES partitions writing concurrently)
    python scripts/copy_es_to_kosha.py --index products --workers 4

    # Count what would move, write nothing
    python scripts/copy_es_to_kosha.py --pattern 'logs-*' --dry-run

Env (each also has a flag):
    ELASTICSEARCH_HOST       http(s)://host[:port], may embed user:pass@
                             (default http://localhost:9200; --es-host)
    ELASTICSEARCH_USERNAME   basic-auth user (overrides URL userinfo)
    ELASTICSEARCH_PASSWORD   basic-auth password
    ELASTICSEARCH_AUTH_MODE  "basic" (default) or "sigv4"
    AWS_REGION / AWS_DEFAULT_REGION  SigV4 region (default us-east-1)
    KOSHA_HOST               default http://localhost:8080 (--kosha-host)
    KOSHA_API_KEY            sent as ``Authorization: Bearer ...`` when set

Exit status: 0 when every index was copied and its count verified, 1 when a
count does not match, 2 on a usage error.
"""

from __future__ import annotations

import argparse
import base64
import datetime
import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("copy_es_to_kosha")

DEFAULT_ES_HOST = "http://localhost:9200"
DEFAULT_KOSHA_HOST = "http://localhost:8080"

# Mirror of KoshaClient._MIN_VECTOR_DIM: below this, a numeric list is an
# incidental tuple (coordinates, bounding boxes, ...), not an embedding, and
# must not go into the shared Vector store. Keep in sync with
# clients/python/kosha_client/client.py.
_MIN_VECTOR_DIM = 8

# Elasticsearch mapping type -> proto FieldType name (the namespace schema).
# Mirror of kosha_client IndexOps._FIELD_TYPES_FROM_ES; the smoke test
# (scripts/tests/test_copy_es_to_kosha.py) fails if they drift. Types not
# listed (object, nested, geo_point, ...) are left out of the schema.
FIELD_TYPES_FROM_ES = {
    "text": "FIELD_TYPE_TEXT",
    "match_only_text": "FIELD_TYPE_TEXT",
    "keyword": "FIELD_TYPE_KEYWORD",
    "constant_keyword": "FIELD_TYPE_KEYWORD",
    "long": "FIELD_TYPE_INTEGER",
    "integer": "FIELD_TYPE_INTEGER",
    "short": "FIELD_TYPE_INTEGER",
    "byte": "FIELD_TYPE_INTEGER",
    "double": "FIELD_TYPE_FLOAT",
    "float": "FIELD_TYPE_FLOAT",
    "half_float": "FIELD_TYPE_FLOAT",
    "scaled_float": "FIELD_TYPE_FLOAT",
    "date": "FIELD_TYPE_DATE",
    "date_nanos": "FIELD_TYPE_DATE",
    "boolean": "FIELD_TYPE_BOOLEAN",
    "knn_vector": "FIELD_TYPE_VECTOR",
    "dense_vector": "FIELD_TYPE_VECTOR",
}

# Analyzers Elasticsearch and Kosha both have under the same name; any other
# analyzer in a mapping is dropped (the server refuses unknown names), and
# the field gets Kosha's default analysis.
KOSHA_ANALYZERS = frozenset({"standard", "whitespace", "keyword"})

# Proto FieldType name -> the document field_type the server takes.
_DOC_FIELD_TYPES = {
    "FIELD_TYPE_TEXT": "Text",
    "FIELD_TYPE_KEYWORD": "Keyword",
    "FIELD_TYPE_INTEGER": "Integer",
    "FIELD_TYPE_FLOAT": "Float",
    "FIELD_TYPE_DATE": "Date",
    "FIELD_TYPE_BOOLEAN": "Boolean",
    "FIELD_TYPE_VECTOR": "Vector",
}

# Kosha statuses worth retrying: load shedding, and an unavailable or
# restarting pod.
_RETRY_STATUSES = (429, 502, 503, 504)


# ─── Elasticsearch: just enough of the API to bulk-read indices ─────────────


class EsScrollClient:
    """_cat/indices, _count, _mapping and sliced scroll.

    Auth: HTTP basic (default) or AWS SigV4 (``auth_mode="sigv4"``,
    service name "es").
    """

    def __init__(
        self,
        host: str,
        username: str | None,
        password: str | None,
        auth_mode: str = "basic",
        region: str = "us-east-1",
        timeout: int = 60,
    ) -> None:
        # Credentials may be embedded in the URL (user:pass@host); explicit
        # username/password args win. Strip userinfo from the base URL either
        # way so nothing sensitive leaks into logs/CanonicalRequests.
        parsed = urllib.parse.urlsplit(host)
        if parsed.username and username is None:
            username = urllib.parse.unquote(parsed.username)
            password = urllib.parse.unquote(parsed.password or "")
        netloc = parsed.hostname or "localhost"
        if parsed.port:
            netloc += f":{parsed.port}"
        self._base = urllib.parse.urlunsplit((parsed.scheme or "http", netloc, parsed.path.rstrip("/"), "", ""))

        self._auth_mode = (auth_mode or "basic").lower()
        self._timeout = timeout

        if self._auth_mode == "sigv4":
            try:
                import boto3
                from botocore.auth import SigV4Auth
                from botocore.awsrequest import AWSRequest
            except ImportError as exc:
                raise RuntimeError(
                    "ELASTICSEARCH_AUTH_MODE=sigv4 requires boto3/botocore — "
                    "run from an environment that has them installed."
                ) from exc
            creds = boto3.Session().get_credentials()
            if creds is None:
                raise RuntimeError(
                    "ELASTICSEARCH_AUTH_MODE=sigv4 but no AWS credentials available."
                )
            self._frozen_creds = creds.get_frozen_credentials()
            self._sigv4 = SigV4Auth
            self._aws_request = AWSRequest
            self._region = region
        else:
            self._basic_header = None
            if username is not None:
                token = base64.b64encode(f"{username}:{password or ''}".encode()).decode()
                self._basic_header = "Basic " + token

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        url = self._base + path
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if self._auth_mode == "sigv4":
            req = self._aws_request(method=method, url=url, data=data or b"", headers=headers)
            self._sigv4(self._frozen_creds, "es", self._region).add_auth(req)
            headers = dict(req.headers)
        elif self._basic_header:
            headers["Authorization"] = self._basic_header
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            detail = e.read().decode()[:500]
            raise RuntimeError(f"ES {method} {path} → {e.code}: {detail}") from e

    def list_indices(self, pattern: str) -> list[str]:
        """Resolve an index pattern to concrete, non-system index names."""
        rows = self._request("GET", f"/_cat/indices/{urllib.parse.quote(pattern, safe='*')}?format=json")
        names = [row["index"] for row in rows if not row.get("index", ".").startswith(".")]
        return sorted(set(names))

    def count(self, index: str, query: dict | None = None) -> int:
        body = {"query": query} if query else None
        result = self._request("POST", f"/{urllib.parse.quote(index)}/_count", body)
        return int(result.get("count", 0))

    def mapping_properties(self, index: str) -> dict:
        """The top-level ``properties`` of an index's mapping.

        ``GET /{index}/_mapping`` is keyed by the concrete index name (an
        alias answers with its backing index); the properties of every index
        it returns are merged.
        """
        result = self._request("GET", f"/{urllib.parse.quote(index)}/_mapping")
        merged: dict = {}
        for entry in result.values():
            mappings = (entry or {}).get("mappings") or {}
            # ES 6 nests the properties under a mapping type name.
            if "properties" not in mappings and len(mappings) == 1:
                mappings = next(iter(mappings.values())) or {}
            merged.update(mappings.get("properties") or {})
        return merged

    def open_scroll(
        self,
        index: str,
        size: int,
        keepalive: str,
        slice_id: int | None = None,
        slice_max: int | None = None,
    ) -> tuple[str, list, int]:
        body: dict = {
            "size": size,
            "sort": ["_doc"],
            "track_total_hits": True,
            "query": {"match_all": {}},
        }
        # Sliced scroll: each worker gets a disjoint partition of the index.
        # Omit the key for the single-worker path.
        if slice_id is not None and slice_max is not None and slice_max > 1:
            body["slice"] = {"id": slice_id, "max": slice_max}
        result = self._request(
            "POST", f"/{urllib.parse.quote(index)}/_search?scroll={keepalive}", body
        )
        hits = result.get("hits", {})
        total = hits.get("total", {})
        total_value = total.get("value", 0) if isinstance(total, dict) else int(total)
        return result["_scroll_id"], hits.get("hits", []), total_value

    def next_scroll(self, scroll_id: str, keepalive: str) -> tuple[str, list]:
        result = self._request(
            "POST", "/_search/scroll", {"scroll_id": scroll_id, "scroll": keepalive}
        )
        return result.get("_scroll_id", scroll_id), result.get("hits", {}).get("hits", [])

    def clear_scroll(self, scroll_id: str) -> None:
        try:
            self._request("DELETE", "/_search/scroll", {"scroll_id": [scroll_id]})
        except Exception as exc:  # best-effort — a leaked scroll expires anyway
            logger.warning("failed to clear scroll: %s", exc)


# ─── Kosha: the /v1 API ─────────────────────────────────────────────────────


class KoshaError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class KoshaWriter:
    def __init__(self, host: str, api_key: str | None, timeout: int = 300, max_retries: int = 5) -> None:
        self._base = host.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        for attempt in range(self._max_retries + 1):
            req = urllib.request.Request(self._base + path, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    return json.loads(resp.read().decode() or "{}")
            except urllib.error.HTTPError as e:
                detail = e.read().decode()[:500]
                if e.code in _RETRY_STATUSES and attempt < self._max_retries:
                    delay = _retry_after(e.headers.get("Retry-After"), attempt)
                    logger.warning("Kosha %s %s → %s; retrying in %.1fs", method, path, e.code, delay)
                    time.sleep(delay)
                    continue
                raise KoshaError(e.code, f"Kosha {method} {path} → {e.code}: {detail}") from e
        raise AssertionError("unreachable")

    @staticmethod
    def _ns(namespace: str, action: str) -> str:
        """``/v1/namespaces/{namespace}/{action}``."""
        return "/v1/namespaces/%s/%s" % (urllib.parse.quote(namespace, safe=""), action)

    def create_namespace(self, namespace: str, schema: dict | None) -> bool:
        """Create the namespace; False if it already exists (left unchanged)."""
        body: dict = {"namespace": namespace}
        if schema:
            body["schema"] = schema
        try:
            self._request("POST", "/v1/namespaces", body)
            return True
        except KoshaError as e:
            if e.status == 409:
                return False
            raise

    def index_batch(self, namespace: str, documents: list[dict]) -> int:
        result = self._request("POST", self._ns(namespace, "documents"), {"documents": documents})
        return int(result.get("indexed_count", len(documents)))

    def flush(self, namespace: str) -> None:
        self._request("POST", self._ns(namespace, "flush"), {})

    def count(self, namespace: str) -> int:
        """Exact number of live documents (a filter matching everything)."""
        result = self._request(
            "POST",
            self._ns(namespace, "search"),
            {
                "query_text": "",
                "filter": {"match_all": {}},
                "max_results": 0,
                "exact_total_hits": True,
                "no_cache": True,
                "consistency": "strong",
            },
        )
        return int(result.get("total_hits", 0))


def _retry_after(header: str | None, attempt: int) -> float:
    try:
        return min(30.0, max(0.0, float(header)))
    except (TypeError, ValueError):
        return min(30.0, 0.5 * (2**attempt))


# ─── Mapping -> schema and field types ──────────────────────────────────────


def schema_from_mapping(properties: dict) -> tuple[dict | None, dict[str, str]]:
    """``(NamespaceSchema or None, {field: document field_type})``.

    Each top-level property with a type Kosha has becomes a schema field
    (``index: false`` -> ``indexed: false``; a text field keeps its
    analyzer when Kosha has one of that name). Object, nested and unknown
    types are skipped and logged.
    """
    fields: dict[str, dict] = {}
    types: dict[str, str] = {}
    for name, spec in (properties or {}).items():
        if not isinstance(spec, dict):
            continue
        proto = FIELD_TYPES_FROM_ES.get(str(spec.get("type", "")))
        if proto is None:
            logger.info("mapping for %r (type %r) has no Kosha equivalent; left out of the schema", name, spec.get("type", "object"))
            continue
        field = {"type": proto, "indexed": spec.get("index", True) is not False, "stored": True}
        analyzer = spec.get("analyzer")
        if proto == "FIELD_TYPE_TEXT" and isinstance(analyzer, str):
            if analyzer in KOSHA_ANALYZERS:
                field["analyzer"] = [analyzer]
            else:
                logger.info("field %r: analyzer %r has no Kosha equivalent; the default is used", name, analyzer)
        fields[name] = field
        types[name] = _DOC_FIELD_TYPES[proto]
    return ({"fields": fields} if fields else None), types


# ─── Field conversion ───────────────────────────────────────────────────────


def _field(name: str, value: str, field_type: str) -> dict:
    return {"name": name, "field_type": field_type, "value": value}


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _shape_field(k: str, v) -> dict | None:
    """One field typed from its JSON shape, as KoshaClient._source_to_fields
    does: str -> Text, bool -> Boolean, number -> Float, numeric list (len >=
    _MIN_VECTOR_DIM) -> Vector, other list or object -> Keyword (JSON)."""
    if isinstance(v, str):
        return _field(k, v, "Text")
    if isinstance(v, bool):
        return _field(k, str(v).lower(), "Boolean")
    if _is_number(v):
        return _field(k, str(v), "Float")
    if isinstance(v, (list, tuple)) and len(v) >= _MIN_VECTOR_DIM and all(_is_number(x) for x in v):
        return _field(k, json.dumps(list(v)), "Vector")
    if isinstance(v, (list, tuple, dict)):
        # A nested object (#206) or a list: whole, as JSON text in a Keyword
        # field; the server indexes an object's leaves under dotted paths.
        return _field(k, json.dumps(v if isinstance(v, dict) else list(v), default=str), "Keyword")
    return None  # None and anything else: skipped, as the client does


def _typed_value(v, field_type: str) -> str | None:
    """``v`` as the string Kosha stores for ``field_type``, or None if it
    does not fit (the caller then falls back to the JSON shape)."""
    if field_type in ("Text", "Keyword"):
        if isinstance(v, bool):
            return str(v).lower()
        if isinstance(v, (str, int, float)):
            return str(v)
        return None
    if field_type == "Integer":
        if _is_number(v) and float(v).is_integer():
            return str(int(v))
        if isinstance(v, str) and v.strip().lstrip("-").isdigit():
            return v.strip()
        return None
    if field_type == "Float":
        if _is_number(v):
            return str(v)
        if isinstance(v, str):
            try:
                float(v)
                return v.strip()
            except ValueError:
                return None
        return None
    if field_type == "Date":
        if isinstance(v, str):
            return v
        if _is_number(v):
            # epoch_millis, ES's other default date format: as ISO-8601 UTC,
            # so it orders with string dates.
            ts = datetime.datetime.fromtimestamp(v / 1000.0, tz=datetime.timezone.utc)
            return ts.isoformat().replace("+00:00", "Z")
        return None
    if field_type == "Boolean":
        if isinstance(v, bool):
            return str(v).lower()
        if isinstance(v, str) and v.lower() in ("true", "false"):
            return v.lower()
        return None
    if field_type == "Vector":
        if isinstance(v, (list, tuple)) and v and all(_is_number(x) for x in v):
            return json.dumps(list(v))
        return None
    return None


def source_to_fields(source: dict, types: dict[str, str] | None = None) -> list[dict]:
    """Convert an ES ``_source`` doc into Kosha fields.

    A field named in ``types`` (from the mapping) is sent with that type; a
    list of values for a non-vector field becomes one field per value, which
    Kosha treats as multi-valued (a term filter matches any of them). Every
    other field is typed from its JSON shape, identically to kosha-client.
    """
    types = types or {}
    fields: list[dict] = []
    for k, v in source.items():
        if k.startswith("__type__"):
            continue
        field_type = types.get(k)
        if field_type is not None and v is not None:
            values = v if isinstance(v, list) and field_type != "Vector" else [v]
            typed = [_typed_value(x, field_type) for x in values]
            if all(t is not None for t in typed):
                fields.extend(_field(k, t, field_type) for t in typed)
                continue
        shaped = _shape_field(k, v)
        if shaped is not None:
            fields.append(shaped)
    return fields


# ─── Copy driver ─────────────────────────────────────────────────────────────


def resolve_indices(es: EsScrollClient, args: argparse.Namespace) -> list[str]:
    """ES indices/aliases to copy, in stable order, de-duplicated.

    ``--index`` values are kept verbatim, not resolved through
    ``_cat/indices``: an alias's concrete backing index usually has a
    different name (``products`` -> ``products_v2``), and the namespace takes
    the name given. ``--pattern`` expands through ``_cat/indices``.
    """
    indices: list[str] = []
    for name in args.index or []:
        try:
            es.count(name)
        except RuntimeError as exc:
            logger.warning("index %r not reachable (%s) — skipping", name, exc)
            continue
        indices.append(name)
    for pattern in args.pattern or []:
        matched = es.list_indices(pattern)
        if not matched:
            logger.warning("pattern %r matched no indices — skipping", pattern)
        indices.extend(matched)
    if not indices:
        raise RuntimeError("no indices matched — nothing to copy")
    return list(dict.fromkeys(indices))


def copy_index(
    es: EsScrollClient,
    kosha: KoshaWriter,
    index: str,
    namespace: str,
    args: argparse.Namespace,
    types: dict[str, str] | None = None,
) -> tuple[int, int]:
    """Scroll one ES index and write every doc into a Kosha namespace.

    Returns (docs_copied, docs_with_embedding_field).
    """
    total = es.count(index)
    planned = min(total, args.limit) if args.limit else total
    workers = max(1, int(args.workers))
    scope = f"capped by --limit from {total}" if args.limit and args.limit < total else "full index"
    logger.info(
        "index %s → namespace %s: %s docs (%s, workers=%s)",
        index, namespace, planned, scope, workers,
    )

    progress = {"copied": 0, "with_embedding": 0}
    progress_lock = threading.Lock()
    claimed_total = [0]  # documents reserved under --limit
    log_every = max(args.batch_size, 200)

    def _run_slice(slice_id: int) -> None:
        local_emb = 0
        batch: list[dict] = []

        def send_batch() -> None:
            nonlocal batch
            if not batch:
                return
            n = len(batch)
            kosha.index_batch(namespace, batch)
            batch = []
            if args.sleep > 0:
                time.sleep(args.sleep)
            with progress_lock:
                progress["copied"] += n
                total_copied = progress["copied"]
            if total_copied % log_every < n or total_copied >= planned:
                logger.info("index %s: %s / %s docs copied", index, min(total_copied, planned), planned)

        def claim() -> bool:
            """Reserve one document under --limit (shared by the workers)."""
            if not args.limit:
                return True
            with progress_lock:
                if claimed_total[0] >= args.limit:
                    return False
                claimed_total[0] += 1
                return True

        scroll_id, hits, _ = es.open_scroll(
            index, args.scroll_size, args.scroll_keepalive, slice_id=slice_id, slice_max=workers,
        )
        try:
            done = False
            while hits and not done:
                for hit in hits:
                    doc_id = hit.get("_id")
                    if not doc_id:
                        logger.warning("hit without _id in %s — skipping", index)
                        continue
                    if not claim():
                        done = True
                        break
                    source = hit.get("_source") or {}
                    if args.embedding_field and args.embedding_field in source:
                        local_emb += 1
                    batch.append({"id": doc_id, "fields": source_to_fields(source, types)})
                    if len(batch) >= args.batch_size:
                        send_batch()
                if done:
                    break
                scroll_id, hits = es.next_scroll(scroll_id, args.scroll_keepalive)
        finally:
            es.clear_scroll(scroll_id)

        send_batch()
        with progress_lock:
            progress["with_embedding"] += local_emb

    if workers == 1:
        _run_slice(0)
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"scroll-{index}") as pool:
            futures = [pool.submit(_run_slice, i) for i in range(workers)]
            for fut in as_completed(futures):
                fut.result()  # surface worker errors

    copied, with_emb = progress["copied"], progress["with_embedding"]
    if copied > 0:
        kosha.flush(namespace)
    logger.info(
        "index %s: %s namespace %s — %s docs copied%s",
        index, "flushed" if copied else "skipped (empty)", namespace, copied,
        f", {with_emb} with {args.embedding_field}" if args.embedding_field else "",
    )
    return copied, with_emb


def dry_run(es: EsScrollClient, indices: list[str], args: argparse.Namespace) -> None:
    print("\n Dry-run — no writes to Kosha:\n")
    grand_total = 0
    for index in indices:
        total = es.count(index)
        planned = min(total, args.limit) if args.limit else total
        grand_total += planned
        line = f"  {index:<48}{planned:>12,} docs"
        if args.embedding_field:
            with_emb = es.count(index, {"exists": {"field": args.embedding_field}})
            line += f"   ({with_emb:>12,} with {args.embedding_field})"
        print(line)
    print(f"  {'TOTAL':<48}{grand_total:>12,} docs\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Copy Elasticsearch / OpenSearch indices into Kosha through the /v1 API."
    )
    parser.add_argument("--index", action="append",
                        help="Exact ES index or alias to copy (repeatable).")
    parser.add_argument("--pattern", action="append",
                        help="ES index pattern resolved via _cat/indices (repeatable).")
    parser.add_argument("--namespace",
                        help="Kosha namespace for the copy (only with a single index; default: the index name).")
    parser.add_argument("--create-schema-from-mapping", action="store_true",
                        help="Create each namespace with a schema from the ES mapping and send values "
                             "with the mapped types (keyword, integer, date, ...). An existing "
                             "namespace keeps its schema.")
    parser.add_argument("--limit", type=int,
                        help="Max docs per index (smoke tests).")
    parser.add_argument("--batch-size", type=int, default=200,
                        help="Docs per Kosha write (default 200).")
    parser.add_argument("--scroll-size", type=int, default=1000,
                        help="ES scroll page size (default 1000).")
    parser.add_argument("--scroll-keepalive", default="5m",
                        help="ES scroll context keepalive (default 5m).")
    parser.add_argument("--workers", type=int, default=1,
                        help="Parallel ES sliced-scroll workers per index (default 1).")
    parser.add_argument("--sleep", type=float, default=0.0,
                        help="Seconds to sleep between batches (throttle, default 0).")
    parser.add_argument("--embedding-field",
                        help="Also report how many docs carry this field (an embedding, say).")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip the count check after each index.")
    parser.add_argument("--allow-extra", action="store_true",
                        help="On a re-run, accept a namespace that holds more documents than "
                             "Elasticsearch (documents deleted there since an earlier copy are "
                             "not removed); the excess is logged as a warning, not a failure.")
    parser.add_argument("--timeout", type=int, default=120,
                        help="Per-request timeout in seconds (default 120).")
    parser.add_argument("--es-host", default=os.environ.get("ELASTICSEARCH_HOST", DEFAULT_ES_HOST),
                        help="ES / OpenSearch URL (default $ELASTICSEARCH_HOST or %s)." % DEFAULT_ES_HOST)
    parser.add_argument("--kosha-host", default=os.environ.get("KOSHA_HOST", DEFAULT_KOSHA_HOST),
                        help="Kosha URL (default $KOSHA_HOST or %s)." % DEFAULT_KOSHA_HOST)
    parser.add_argument("--dry-run", action="store_true",
                        help="Count docs per index, write nothing.")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.workers < 1:
        parser.error("--workers must be >= 1")
    if not args.index and not args.pattern:
        parser.error("name what to copy with --index and/or --pattern")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    es = EsScrollClient(
        host=args.es_host,
        username=os.environ.get("ELASTICSEARCH_USERNAME"),
        password=os.environ.get("ELASTICSEARCH_PASSWORD"),
        auth_mode=os.environ.get("ELASTICSEARCH_AUTH_MODE", "basic"),
        region=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1",
        timeout=args.timeout,
    )

    indices = resolve_indices(es, args)
    if args.namespace and len(indices) != 1:
        parser.error("--namespace is only valid when exactly one index is being copied")

    if args.dry_run:
        dry_run(es, indices, args)
        return 0

    kosha = KoshaWriter(
        host=args.kosha_host,
        api_key=os.environ.get("KOSHA_API_KEY"),
        timeout=max(args.timeout, 300),
    )

    grand_copied = 0
    mismatches: list[str] = []
    for index in indices:
        namespace = args.namespace or index
        types = None
        if args.create_schema_from_mapping:
            schema, types = schema_from_mapping(es.mapping_properties(index))
            if kosha.create_namespace(namespace, schema):
                logger.info("namespace %s created with %d schema fields", namespace, len((schema or {}).get("fields", {})))
            else:
                logger.warning("namespace %s already exists; its schema is left as it is", namespace)
        copied, _ = copy_index(es, kosha, index, namespace, args, types)
        grand_copied += copied
        if not args.no_verify:
            want = es.count(index)
            if args.limit:
                want = min(want, args.limit)
            got = kosha.count(namespace)
            if got == want:
                logger.info("verified %s: %s documents in ES and in Kosha", namespace, got)
            elif args.limit and got > want:
                # A capped copy into a namespace that already held documents.
                logger.warning("namespace %s holds %s documents; %s were copied (--limit)", namespace, got, want)
            elif args.allow_extra and got > want:
                # A re-run: the copy upserts but never deletes, so documents
                # deleted in Elasticsearch since an earlier copy remain.
                logger.warning(
                    "namespace %s holds %s documents, %s more than Elasticsearch's %s "
                    "(documents deleted in Elasticsearch since an earlier copy are not removed; "
                    "--allow-extra)",
                    namespace, got, got - want, want,
                )
            else:
                mismatches.append(f"{index} → {namespace}: ES {want}, Kosha {got}")

    logger.info("done: %s docs copied across %s indices", grand_copied, len(indices))
    if mismatches:
        for line in mismatches:
            logger.error("count mismatch: %s", line)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
