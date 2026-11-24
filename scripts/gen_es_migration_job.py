#!/usr/bin/env python3
"""Render the direct ES -> Kosha migration Job manifest (``kosha-server migrate``).

    python scripts/gen_es_migration_job.py --index my_index --index other_index \
        > k8s/stage/es-to-kosha-migration-job.yaml

or ``make migration-job-manifest MIGRATION_INDICES="my_index other_index"``.

The Job runs ``kosha-server migrate``, which sliced-scrolls OpenSearch, builds
segments in-process, uploads them to S3 and publishes their manifest entries
directly (no HTTP API). For copying through the public ``/v1`` API instead,
see ``scripts/copy_es_to_kosha.py`` and docs/migration/from-elasticsearch.md.

The manifest is applied by hand and is intentionally not part of the
kustomize overlay, so normal deployments cannot restart a one-shot backfill.

Index names are arguments, never committed defaults. Pass the exact names or
aliases to copy: each becomes a namespace of the same name, and resolving an
alias through ``_cat/indices`` would return its concrete backing index name
instead.

The cluster-specific endpoints are read from the environment (or flags) so no
account id, registry, or search-domain hostname is committed. The defaults
render placeholders; set these before applying the manifest for real:

    KOSHA_MIGRATION_ES_HOST     OpenSearch endpoint to read from (--es-host)
    KOSHA_MIGRATION_IMAGE       fully-qualified kosha-server image (--image)
    AWS_REGION                  region for both, default us-east-1 (--region)
"""

from __future__ import annotations

import argparse
import os
import sys

JOB = """\
# GENERATED FILE — do not edit by hand.
# Source of truth: scripts/gen_es_migration_job.py
# Regenerate with: make migration-job-manifest
#
# Direct backfill: sliced-scrolls OpenSearch, builds Kosha segments in-process,
# uploads each segment to S3, then publishes its Postgres manifest entry.
apiVersion: batch/v1
kind: Job
metadata:
  name: es-to-kosha-migration
  namespace: kosha
  labels:
    app: es-to-kosha-migration
    app.kubernetes.io/part-of: kosha
spec:
  backoffLimit: 3
  ttlSecondsAfterFinished: 86400
  activeDeadlineSeconds: 21600
  template:
    metadata:
      labels:
        app: es-to-kosha-migration
    spec:
      restartPolicy: Never
      # Uses the same S3 IRSA as kosha-server. This role must ALSO have
      # es:ESHttp* access to the staging OpenSearch domain.
      serviceAccountName: kosha
      containers:
        - name: migrate
          image: "__KOSHA_IMAGE__"
          imagePullPolicy: Always
          envFrom:
            - configMapRef:
                name: kosha-config
          env:
            - name: ELASTICSEARCH_HOST
              value: "__ES_HOST__"
            - name: AWS_REGION
              value: "__REGION__"
            - name: AWS_DEFAULT_REGION
              value: "__REGION__"
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kosha-secret
                  key: database-url
          command:
            - /usr/local/bin/kosha-server
            - migrate
          args:
__ARGS__
          resources:
            requests:
              cpu: "2"
              memory: "4Gi"
              ephemeral-storage: "10Gi"
            limits:
              cpu: "4"
              memory: "8Gi"
              ephemeral-storage: "30Gi"
          volumeMounts:
            - name: cache
              mountPath: /var/cache/kosha
      volumes:
        - name: cache
          emptyDir:
            sizeLimit: "30Gi"
"""


def render(
    indices: list[str],
    es_host: str = "https://<opensearch-endpoint>",
    region: str = "us-east-1",
    image: str = "<registry>/decover/kosha:main",
) -> str:
    if not indices:
        raise ValueError("at least one index is required")
    parts: list[str] = []
    for name in indices:
        parts.extend(["--index", name])
    parts.extend(
        [
            "--workers",
            "4",
            "--batch-size",
            "1000",
            "--scroll-size",
            "2000",
            "--flush-docs",
            "20000",
        ]
    )
    args = "\n".join(f'            - "{part}"' for part in parts)
    return (
        JOB.replace("__ES_HOST__", es_host)
        .replace("__REGION__", region)
        .replace("__KOSHA_IMAGE__", image)
        .replace("__ARGS__", args)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Render the ES -> Kosha migration Job manifest to stdout."
    )
    parser.add_argument(
        "--index",
        action="append",
        required=True,
        help="Exact ES index or alias to copy (repeatable); also the namespace name.",
    )
    parser.add_argument(
        "--es-host",
        default=os.environ.get("KOSHA_MIGRATION_ES_HOST", "https://<opensearch-endpoint>"),
    )
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument(
        "--image",
        default=os.environ.get("KOSHA_MIGRATION_IMAGE", "<registry>/decover/kosha:main"),
    )
    args = parser.parse_args(argv)
    # end="" — render() already ends in a newline, and a second one would
    # fail the end-of-file-fixer pre-commit hook on the generated manifest.
    print(render(args.index, args.es_host, args.region, args.image), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
