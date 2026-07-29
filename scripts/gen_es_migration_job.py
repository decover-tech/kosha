#!/usr/bin/env python3
"""Render the direct ES → Kosha migration Job manifest for staging.

    python scripts/gen_es_migration_job.py > k8s/stage/es-to-kosha-migration-job.yaml

The manifest is applied by hand and is intentionally not part of the
kustomize overlay, so normal deployments cannot restart a one-shot backfill.
"""

from __future__ import annotations

ES_HOST = "https://vpc-decoverai-nonprod-search-rdaf2ygjik3teui5prmuotjlx4.us-east-1.es.amazonaws.com"
REGION = "us-east-1"
KOSHA_IMAGE = "010928200670.dkr.ecr.us-east-1.amazonaws.com/decover/kosha:main"

# Exact alias names Sage reads. Never resolve these through `_cat/indices`,
# which returns concrete *_v2 backing names and would mis-name namespaces.
INDEX_NAMES = [
    "paragraph_index_hnsw",
    "page_index",
    "findings_index",
    "document_index",
    "completions_index",
    "cases_index",
]

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


def render() -> str:
    parts: list[str] = []
    for name in INDEX_NAMES:
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
        JOB.replace("__ES_HOST__", ES_HOST)
        .replace("__REGION__", REGION)
        .replace("__KOSHA_IMAGE__", KOSHA_IMAGE)
        .replace("__ARGS__", args)
    )


if __name__ == "__main__":
    print(render())
