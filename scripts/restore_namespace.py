#!/usr/bin/env python3
"""Recover one published namespace into an isolated, empty Kosha deployment."""

from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
import zlib


COMMIT = "_COMMIT"
REQUIRED_FILES = {"footer.json", "doc_store.bin", "inverted.idx"}
DELETION_FILE = re.compile(r"deletes-[a-fA-F0-9]{64}\.roaring")
SHARD_NAME = re.compile(r"_shard-[0-9]{4}")


class RestoreError(Exception):
    pass


def validate_path(path, components):
    if not isinstance(path, str):
        raise RestoreError("expected a storage path")
    parts = path.split("/")
    if len(parts) not in components or any(
        part in ("", ".", "..") or "\\" in part
        or any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in part)
        for part in parts
    ):
        raise RestoreError(f"invalid storage path: {path!r}")
    return path


@dataclass(frozen=True)
class ManifestRow:
    namespace: str
    created_at: datetime
    version: int
    raw_manifest: str
    updated_at: datetime

    @property
    def manifest(self):
        value = json.loads(self.raw_manifest)
        return {"segments": value} if isinstance(value, list) else value


def read_manifests(connection, root):
    rows = {}
    for row in connection.execute(
        """SELECT n.id, n.created_at, m.version, m.segments_json, m.updated_at
             FROM kosha.namespaces n JOIN kosha.manifests m ON m.namespace_id = n.id
            WHERE n.id = %s OR starts_with(n.id, %s)""",
        (root, f"{root}/_shard-"),
    ):
        rows[row[0]] = ManifestRow(*row)
    if root not in rows:
        raise RestoreError(f"{root}: no manifest in the recovered database")
    shards = rows[root].manifest.get("shards")
    if shards is not None and (type(shards) is not int or not 1 <= shards <= 256):
        raise RestoreError(f"{root}: invalid shard count")
    names = [root]
    if shards and shards > 1:
        names.extend(f"{root}/_shard-{i:04d}" for i in range(shards))
    if set(names) != rows.keys():
        raise RestoreError(f"{root}: recovered shard manifests do not match the root")
    return [rows[name] for name in names]


def deletion_subjects(rows):
    subjects = {row.namespace for row in rows}
    for row in rows:
        manifest = row.manifest
        branch = manifest.get("branch")
        if branch:
            subjects.add(branch["parent"])
            subjects.update(branch.get("segment_homes", {}).values())
        for vector in manifest.get("deletion_vectors", {}).values():
            if vector.get("namespace"):
                subjects.add(vector["namespace"])
    for subject in subjects:
        validate_path(subject, (2, 3))
    return sorted(subjects | {subject.split("/")[0] for subject in subjects})


def check_deletion_ledger(recovered, current, subjects):
    sql = "SELECT id, state FROM kosha.deletion_records WHERE subject = ANY(%s)"
    old = {id_ for id_, state in recovered.execute(sql, (subjects,)) if state == "completed"}
    for id_, state in current.execute(sql, (subjects,)):
        if state != "completed" or id_ not in old:
            raise RestoreError("a pending or post-recovery erasure blocks this restore")


@dataclass(frozen=True)
class ObjectVersion:
    key: str
    version: str
    size: int
    crc32: int | None = None
    sha256: str | None = None


class VersionedObjects:
    def __init__(self, client, bucket, prefix, at):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.at = at

    def key(self, path):
        return f"{self.prefix}/{path}" if self.prefix else path

    def versions(self, path):
        prefix = self.key(path)
        newest = {}
        for page in self.client.get_paginator("list_object_versions").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            for collection in ("Versions", "DeleteMarkers"):
                for item in page.get(collection, ()):
                    modified = item["LastModified"]
                    if modified > self.at:
                        continue
                    key = item["Key"]
                    prev = newest.get(key)
                    if prev is None or modified > prev[0]:
                        version = None if collection == "DeleteMarkers" else ObjectVersion(
                            key, item["VersionId"], item["Size"]
                        )
                        newest[key] = (modified, version, modified == self.at)
                    elif modified == prev[0]:
                        newest[key] = (modified, prev[1], True)
        return newest

    def select(self, path, versions):
        key = self.key(path)
        _, version, ambiguous = versions.get(key, (None, None, False))
        if ambiguous:
            raise RestoreError(f"{key}: S3 timestamps do not uniquely order versions at the cutoff")
        if version is None:
            raise RestoreError(f"{key}: missing or deleted at the cutoff")
        return version

    def open(self, version):
        return closing(self.client.get_object(
            Bucket=self.bucket, Key=version.key, VersionId=version.version
        )["Body"])

    def plan(self, rows):
        objects = {}
        segments = {}
        for row in rows:
            manifest = row.manifest
            vectors = manifest.get("deletion_vectors", {})
            footers = manifest.get("footer_crc32", {})
            homes = (manifest.get("branch") or {}).get("segment_homes", {})
            listed = set()
            for entry in manifest["segments"]:
                segment = validate_path(entry["segment_id"], (1,))
                listed.add(segment)
                home = validate_path(homes.get(segment, row.namespace), (2, 3))
                directory = f"{home}/{segment}"
                if directory not in segments:
                    segments[directory] = self.plan_segment(directory)
                files = segments[directory]
                footer = files[f"{directory}/footer.json"]
                if segment in footers and footer.crc32 != footers[segment]:
                    raise RestoreError(f"{directory}: footer checksum differs from the manifest")
                objects.update(files)
                if segment in vectors:
                    vector = vectors[segment]
                    home = validate_path(vector.get("namespace") or row.namespace, (2, 3))
                    name = f"deletes-{vector['generation']}.roaring"
                    if not DELETION_FILE.fullmatch(name):
                        raise RestoreError(f"{directory}: invalid deletion generation")
                    path = f"{home}/{segment}/{name}"
                    if path not in objects:
                        version = self.select(path, self.versions(path))
                        objects[path] = replace(version, sha256=vector["generation"].lower())
            if vectors.keys() - listed:
                raise RestoreError(f"{row.namespace}: deletion vector references an absent segment")
        return objects

    def plan_segment(self, directory):
        versions = self.versions(f"{directory}/")
        commit_path = f"{directory}/{COMMIT}"
        marker = self.select(commit_path, versions)
        with self.open(marker) as body:
            data = body.read()
        commit = json.loads(data)
        if commit["format"] != 1:
            raise RestoreError(f"{commit_path}: unsupported commit format")
        files = {}
        names = set()
        for entry in commit["files"]:
            name = validate_path(entry["name"], (1,))
            if name in names or name == COMMIT:
                raise RestoreError(f"{commit_path}: invalid file inventory")
            names.add(name)
            if DELETION_FILE.fullmatch(name):
                continue  # Only the manifest selects live deletion generations.
            path = f"{directory}/{name}"
            version = self.select(path, versions)
            if version.size != entry["size"]:
                raise RestoreError(f"{path}: object size differs from the commit marker")
            files[path] = replace(version, crc32=entry["crc32"])
        if not REQUIRED_FILES <= names or not names & {"filters.bin", "filters.toc"}:
            raise RestoreError(f"{commit_path}: missing required segment files")
        files[commit_path] = replace(marker, crc32=zlib.crc32(data))
        return files

    def copy_verified(self, objects, destination, scratch):
        with TemporaryDirectory(dir=scratch, prefix="kosha-restore-") as temp:
            filename = Path(temp) / "object"
            for path in sorted(objects, key=lambda name: name.endswith(f"/{COMMIT}")):
                version = objects[path]
                crc, size, digest = 0, 0, hashlib.sha256()
                with self.open(version) as body, filename.open("wb") as output:
                    while chunk := body.read(1024 * 1024):
                        output.write(chunk)
                        size += len(chunk)
                        crc = zlib.crc32(chunk, crc)
                        if version.sha256 is not None:
                            digest.update(chunk)
                if (
                    size != version.size
                    or (version.crc32 is not None and crc != version.crc32)
                    or (version.sha256 is not None and digest.hexdigest() != version.sha256)
                ):
                    raise RestoreError(
                        f"{version.key}: version failed checksum or size verification"
                    )
                key = destination.key(path)
                destination.client.upload_file(str(filename), destination.bucket, key)
                head = destination.client.head_object(Bucket=destination.bucket, Key=key)
                if head["ContentLength"] != size or head.get("VersionId") in (None, "null"):
                    raise RestoreError(f"{key}: destination upload was not verified as versioned")

    def require_versioning(self):
        if self.client.get_bucket_versioning(Bucket=self.bucket).get("Status") != "Enabled":
            raise RestoreError(f"{self.bucket}: bucket versioning must be Enabled")

    def require_empty(self):
        page = self.client.list_object_versions(Bucket=self.bucket, MaxKeys=1)
        if page.get("Versions") or page.get("DeleteMarkers"):
            raise RestoreError(f"{self.bucket}: destination bucket is not empty")


def publish(connection, rows):
    for row in rows:
        connection.execute(
            "INSERT INTO kosha.namespaces (id, created_at) VALUES (%s, %s)",
            (row.namespace, row.created_at),
        )
        connection.execute(
            """INSERT INTO kosha.manifests (namespace_id, version, segments_json, updated_at)
               VALUES (%s, %s, %s, %s)""",
            (row.namespace, row.version, row.raw_manifest, row.updated_at),
        )


def recover(args):
    import boto3
    from botocore.config import Config
    import psycopg

    def s3(profile, endpoint, bucket, prefix):
        client = boto3.Session(profile_name=profile).client(
            "s3", endpoint_url=endpoint,
            config=Config(s3={"addressing_style": "path"}) if endpoint else None,
        )
        return VersionedObjects(client, bucket, prefix, args.at)

    source = s3(args.source_profile, args.source_endpoint, args.source_bucket, args.source_prefix)
    source.require_versioning()
    with psycopg.connect(service=args.recovered_db) as recovered, psycopg.connect(
        service=args.ledger_db, autocommit=True
    ) as ledger:
        recovered.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        ledger.execute("SET default_transaction_read_only = on")
        rows = read_manifests(recovered, args.namespace)
        if any(max(row.created_at, row.updated_at) > args.at for row in rows):
            raise RestoreError("the recovered database contains a manifest newer than the target")
        subjects = deletion_subjects(rows)
        check_deletion_ledger(recovered, ledger, subjects)
        objects = source.plan(rows)
        report = {
            "target_utc": args.at.isoformat(),
            "manifests": {row.namespace: row.version for row in rows},
            "objects": len(objects),
            "bytes": sum(obj.size for obj in objects.values()),
            "applied": args.apply,
        }
        if args.apply:
            destination = s3(args.destination_profile, args.destination_endpoint,
                             args.destination_bucket, args.destination_prefix)
            destination.require_versioning()
            with psycopg.connect(service=args.destination_db) as db:
                db.execute("LOCK TABLE kosha.namespaces, kosha.manifests IN ACCESS EXCLUSIVE MODE")
                if db.execute("SELECT EXISTS (SELECT 1 FROM kosha.namespaces)").fetchone()[0]:
                    raise RestoreError("destination database already contains namespaces")
                destination.require_empty()
                source.copy_verified(objects, destination, args.scratch)
                check_deletion_ledger(recovered, ledger, subjects)
                publish(db, rows)
        return report


def parse_target(raw):
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if value.utcoffset() is None or value.microsecond or value >= datetime.now(timezone.utc):
            raise ValueError
        return value.astimezone(timezone.utc)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            "expected a past ISO-8601 timestamp with a UTC offset and whole seconds"
        ) from e


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True, help="stored tenant/name")
    parser.add_argument(
        "--at", required=True, type=parse_target, help="exact PostgreSQL PITR target"
    )
    parser.add_argument("--recovered-db", required=True, help="libpq service for the PITR clone")
    parser.add_argument("--ledger-db", required=True, help="libpq service for the erasure ledger")
    parser.add_argument(
        "--destination-db", help="libpq service for the empty recovery database"
    )
    for side in ("source", "destination"):
        parser.add_argument(f"--{side}-bucket", required=side == "source")
        parser.add_argument(f"--{side}-prefix", default="")
        parser.add_argument(f"--{side}-profile", help="AWS credentials profile")
        parser.add_argument(f"--{side}-endpoint", help="S3-compatible endpoint URL")
    parser.add_argument("--scratch", help="private scratch directory for the largest object")
    parser.add_argument("--apply", action="store_true", help="populate the isolated destination")
    args = parser.parse_args(argv)
    try:
        validate_path(args.namespace, (2,))
        if SHARD_NAME.fullmatch(args.namespace.split("/")[-1]):
            raise RestoreError("specify the namespace root, not a shard")
        for field in ("source_prefix", "destination_prefix"):
            setattr(args, field, getattr(args, field).rstrip("/"))
        if args.recovered_db == args.ledger_db:
            raise RestoreError("recovered database and current ledger must use distinct services")
        if args.apply and (
            not args.destination_db or not args.destination_bucket
            or args.destination_db in (args.recovered_db, args.ledger_db)
            or args.destination_bucket == args.source_bucket
        ):
            raise RestoreError("--apply requires a separate destination database and bucket")
        print(json.dumps(recover(args), sort_keys=True))
    except Exception as e:
        print(f"restore failed: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
