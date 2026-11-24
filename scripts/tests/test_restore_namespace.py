"""Point-in-time recovery tests using a paginated, versioned S3 double."""

from datetime import datetime, timedelta, timezone
import hashlib
from io import BytesIO
import json
from pathlib import Path
import sys
import zlib

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import restore_namespace  # noqa: E402
from restore_namespace import (  # noqa: E402
    ManifestRow,
    RestoreError,
    VersionedObjects,
    check_deletion_ledger,
    publish,
    read_manifests,
)


TARGET = datetime(2025, 1, 2, tzinfo=timezone.utc)
BEFORE = TARGET - timedelta(seconds=2)
AFTER = TARGET + timedelta(seconds=2)


class Bucket:
    def __init__(self):
        self.versions = []
        self.payloads = {}
        self.uploaded = {}

    def add(self, key, version, at, content=None):
        item = {"Key": key, "VersionId": version, "LastModified": at}
        collection = "DeleteMarkers"
        if content is not None:
            self.payloads[key, version] = content
            item["Size"] = len(content)
            collection = "Versions"
        self.versions.append((collection, item))

    def get_paginator(self, operation):
        assert operation == "list_object_versions"
        return self

    def paginate(self, *, Bucket, Prefix):
        for collection, item in self.versions:
            if item["Key"].startswith(Prefix):
                yield {collection: [item]}

    def get_object(self, *, Bucket, Key, VersionId):
        return {"Body": BytesIO(self.payloads[Key, VersionId])}

    def upload_file(self, filename, bucket, key):
        self.uploaded[key] = Path(filename).read_bytes()

    def head_object(self, *, Bucket, Key):
        return {"ContentLength": len(self.uploaded[Key]), "VersionId": "new"}


def segment(bucket, directory):
    files = {
        "footer.json": b"old footer",
        "doc_store.bin": b"old docs",
        "inverted.idx": b"old postings",
        "filters.bin": b"old filters",
    }
    files["_COMMIT"] = json.dumps({
        "format": 1,
        "files": [
            {"name": name, "size": len(body), "crc32": zlib.crc32(body)}
            for name, body in files.items()
        ],
    }).encode()
    for name, data in files.items():
        bucket.add(f"{directory}/{name}", "old", BEFORE, data)
    return files


def row(name, version, body):
    return ManifestRow(name, BEFORE, version, json.dumps(body), BEFORE)


class Database:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def execute(self, sql, params):
        self.queries.append((sql, params))
        return iter(self.rows)


def test_restores_historical_branch_manifest_and_pinned_objects(tmp_path):
    bucket = Bucket()
    directory = "live/tenant/parent/seg-1"
    files = segment(bucket, directory)
    bucket.add(f"{directory}/footer.json", "new", AFTER, b"new footer")
    bucket.add(f"{directory}/doc_store.bin", "deleted", AFTER)
    bucket.add("live/tenant/parent/not-referenced/footer.json", "orphan", BEFORE, b"orphan")
    bitmap = b"deletion bitmap"
    generation = hashlib.sha256(bitmap).hexdigest()
    bitmap_path = f"tenant/parent/seg-1/deletes-{generation}.roaring"
    bucket.add(f"live/{bitmap_path}", "bitmap", BEFORE, bitmap)
    bucket.add(f"{directory}/deletes-{'0' * 64}.roaring", "extra", BEFORE, b"obsolete")
    historical = row("tenant/child", 12, {
        "version": 12,
        "segments": [{"segment_id": "seg-1", "doc_count": 10}],
        "branch": {"parent": "tenant/parent", "parent_version": 7,
                   "segment_homes": {"seg-1": "tenant/parent"}},
        "footer_crc32": {"seg-1": zlib.crc32(files["footer.json"])},
        "deletion_vectors": {
            "seg-1": {"namespace": "tenant/parent", "generation": generation, "count": 1},
        },
    })
    source = VersionedObjects(bucket, "live-bucket", "live", TARGET)
    destination_bucket = Bucket()
    destination = VersionedObjects(destination_bucket, "restore-bucket", "restore", TARGET)
    recovered = Database([
        (historical.namespace, historical.created_at, historical.version,
         historical.raw_manifest, historical.updated_at)
    ])

    rows = read_manifests(recovered, historical.namespace)
    plan = source.plan(rows)
    source.copy_verified(plan, destination, tmp_path)
    destination_db = Database([])
    publish(destination_db, rows)

    assert destination_bucket.uploaded == {
        **{f"restore/tenant/parent/seg-1/{name}": body for name, body in files.items()},
        f"restore/{bitmap_path}": bitmap,
    }
    assert list(destination_bucket.uploaded)[-1].endswith("/_COMMIT")
    assert destination_db.queries[-1][1] == (
        historical.namespace, 12, historical.raw_manifest, historical.updated_at
    )


def test_restores_shards_only_when_all_manifest_rows_exist():
    rows = [
        row("tenant/index", 3, {"shards": 2, "segments": []}),
        row("tenant/index/_shard-0000", 8, []),
        row("tenant/index/_shard-0001", 9, []),
    ]
    records = [(r.namespace, r.created_at, r.version, r.raw_manifest, r.updated_at) for r in rows]
    assert read_manifests(Database(records), "tenant/index") == rows
    with pytest.raises(RestoreError, match="shard manifests"):
        read_manifests(Database(records[:-1]), "tenant/index")


@pytest.mark.parametrize("at, message", [
    (BEFORE + timedelta(seconds=1), "missing or deleted"),
    (BEFORE, "do not uniquely order"),
    (TARGET, "do not uniquely order"),
])
def test_does_not_fall_back_to_a_version_hidden_at_the_cutoff(at, message):
    bucket = Bucket()
    directory = "tenant/index/seg-1"
    segment(bucket, directory)
    bucket.add(f"{directory}/doc_store.bin", "delete", at)
    with pytest.raises(RestoreError, match=message):
        VersionedObjects(bucket, "source", "", TARGET).plan_segment(directory)


def test_corrupt_object_is_not_uploaded(tmp_path):
    bucket = Bucket()
    directory = "tenant/index/seg-1"
    segment(bucket, directory)
    bucket.payloads[f"{directory}/doc_store.bin", "old"] = b"bad docs"
    source = VersionedObjects(bucket, "source", "", TARGET)
    destination = VersionedObjects(Bucket(), "destination", "", TARGET)
    with pytest.raises(RestoreError, match="checksum"):
        source.copy_verified(source.plan_segment(directory), destination, tmp_path)
    assert f"{directory}/doc_store.bin" not in destination.client.uploaded
    assert f"{directory}/_COMMIT" not in destination.client.uploaded


@pytest.mark.parametrize("delete_marker", [False, True])
def test_tied_bitmap_uploads_use_manifest_hash_unless_a_delete_marker_is_tied(delete_marker):
    bucket = Bucket()
    directory = "tenant/index/seg-1"
    segment(bucket, directory)
    bitmap = b"deletion bitmap"
    generation = hashlib.sha256(bitmap).hexdigest()
    path = f"{directory}/deletes-{generation}.roaring"
    bucket.add(path, "corrupt", BEFORE, b"wrong bitmap")
    bucket.add(path, "bitmap", BEFORE, bitmap)
    bucket.add(path, "duplicate", BEFORE, bitmap)
    bucket.add(path, "after", AFTER, bitmap)
    manifests = [row("tenant/index", 2, {
        "segments": [{"segment_id": "seg-1", "doc_count": 10}],
        "deletion_vectors": {"seg-1": {"generation": generation, "count": 1}},
    })]
    source = VersionedObjects(bucket, "source", "", TARGET)

    if delete_marker:
        bucket.add(path, "deleted", BEFORE)
        with pytest.raises(RestoreError, match="do not uniquely order"):
            source.plan(manifests)
    else:
        selected = source.plan(manifests)[path]
        assert selected.version in {"bitmap", "duplicate"}
        assert selected.sha256 == generation


def test_current_ledger_blocks_post_target_or_pending_erasure():
    old = Database([("earlier", "completed")])
    check_deletion_ledger(old, Database([("earlier", "completed")]), ["tenant/index"])
    for record in (("later", "completed"), ("earlier", "pending")):
        with pytest.raises(RestoreError, match="erasure blocks"):
            check_deletion_ledger(old, Database([record]), ["tenant/index"])


@pytest.mark.parametrize("prefix, expected", [
    ("", "tenant/index"),
    ("/", "/tenant/index"),
    ("///", "/tenant/index"),
    ("/live///", "/live/tenant/index"),
    ("live/", "live/tenant/index"),
])
def test_cli_prefixes_follow_server_key_semantics(monkeypatch, prefix, expected):
    def recover(args):
        for bucket, value in (
            (args.source_bucket, args.source_prefix),
            (args.destination_bucket, args.destination_prefix),
        ):
            store = VersionedObjects(Bucket(), bucket, value, TARGET)
            assert store.key("/tenant/index") == expected
        return {}

    monkeypatch.setattr(restore_namespace, "recover", recover)
    assert restore_namespace.main([
        "--namespace", "tenant/index", "--at", TARGET.isoformat(),
        "--recovered-db", "pitr", "--ledger-db", "ledger",
        "--source-bucket", "source", "--source-prefix", prefix,
        "--destination-bucket", "destination", "--destination-prefix", prefix,
    ]) == 0
