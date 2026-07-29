#!/usr/bin/env python3
"""Render the ES → Kosha migration Job manifest for staging.

Embeds ``scripts/copy_es_to_kosha.py`` into a ConfigMap so the whole
migration ships as ONE self-contained manifest:

    python scripts/gen_es_migration_job.py > k8s/stage/es-to-kosha-migration-job.yaml

(``make migration-job-manifest`` does exactly this.)

The manifest is applied by hand (``kubectl apply -f ...``) — it is NOT part
of the kustomize overlay, because CI re-applies ``k8s/stage`` on every merge
to main and a one-shot backfill Job must not be re-run by deployments.
"""

from __future__ import annotations

import pathlib
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[1]
COPY_SCRIPT = ROOT / "scripts" / "copy_es_to_kosha.py"

# Staging values — mirror backend/deployments/k8s/stage/without-localdb-services/configmap.yaml.
ES_HOST = "https://vpc-decoverai-nonprod-search-rdaf2ygjik3teui5prmuotjlx4.us-east-1.es.amazonaws.com"
ES_AUTH_MODE = "sigv4"
REGION = "us-east-1"

# All index families the backend keeps on the default staging ES domain
# (backend/common/settings.py). Patterns that match nothing are skipped with
# a warning, so listing a family that doesn't exist on staging is harmless.
# Case-law search uses a separate managed OpenSearch cluster (its own basic
# auth) and is intentionally NOT migrated by this Job.
INDEX_PATTERNS = [
    "page_index*",
    "findings_index*",
    "document_index*",
    "completions_index*",
    "cases_index*",
]

# Copied by exact name (--index), NOT resolved through _cat/indices.
# paragraph_index_hnsw is an ALIAS for the concrete index
# paragraph_index_hnsw_v2, and _cat/indices resolves aliases to their concrete
# backing index — which would name the Kosha namespace after _v2, while Sage
# looks its namespace up under the alias string. Scroll/count work fine
# against an alias, so copy it under the name Sage actually reads.
INDEX_NAMES = [
    "paragraph_index_hnsw",
]

CONFIGMAP_HEADER = """\
# GENERATED FILE — do not edit by hand.
# Source of truth: scripts/copy_es_to_kosha.py
# Regenerate with: make migration-job-manifest
#
# One-shot backfill: copies every backend search index from the staging
# OpenSearch domain into the staging Kosha deployment (DESIGN.md §14).
# Apply manually, NOT via the kustomize overlay:
#   kubectl apply -f k8s/stage/es-to-kosha-migration-job.yaml
#   kubectl logs -n kosha -f job/es-to-kosha-migration
# Re-running is safe: doc ids are preserved, so re-copied docs upsert in place.
apiVersion: v1
kind: ConfigMap
metadata:
  name: es-to-kosha-migration-script
  namespace: kosha
  labels:
    app: es-to-kosha-migration
    app.kubernetes.io/part-of: kosha
data:
  copy_es_to_kosha.py: |
"""

JOB = """\
---
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
  ttlSecondsAfterFinished: 86400 # keep the finished pod around 24h for logs
  # Generous ceiling so a wedged scroll can't run forever. Raise (or delete)
  # for a very large corpus.
  activeDeadlineSeconds: 21600 # 6h
  template:
    metadata:
      labels:
        app: es-to-kosha-migration
    spec:
      restartPolicy: Never
      # Deliberately NO serviceAccountName: the default SA has no IRSA, so
      # boto3 falls back to the EC2 node instance profile — the same AWS
      # identity the backend pods (sage/celery) use today when they SigV4 to
      # the staging OpenSearch domain. The kosha SA's IRSA role is NOT known
      # to have es:ESHttp* — do not use it here.
      containers:
        - name: migrate
          image: python:3.12-slim
          env:
            - name: ELASTICSEARCH_HOST
              value: "__ES_HOST__"
            - name: ELASTICSEARCH_AUTH_MODE
              value: "__ES_AUTH_MODE__"
            - name: AWS_REGION
              value: "__REGION__"
            - name: AWS_DEFAULT_REGION
              value: "__REGION__"
            - name: KOSHA_HOST
              value: "http://kosha-service.kosha.svc.cluster.local:8080"
            - name: KOSHA_API_KEY
              valueFrom:
                secretKeyRef:
                  name: kosha-secret
                  key: bootstrap-api-key
          command:
            - /bin/sh
            - -c
            - |
              set -e
              # boto3/botocore for SigV4 signing against the managed ES domain
              # (requires NAT egress to PyPI, which the staging cluster has).
              pip install --quiet --disable-pip-version-check boto3
              exec python /scripts/copy_es_to_kosha.py __ARGS__
          resources:
            requests:
              cpu: "250m"
              memory: "512Mi"
            limits:
              cpu: "1"
              memory: "2Gi"
          volumeMounts:
            - name: script
              mountPath: /scripts
              readOnly: true
      volumes:
        - name: script
          configMap:
            name: es-to-kosha-migration-script
"""


def render() -> str:
    script = COPY_SCRIPT.read_text()
    # YAML block scalar: indent every (non-empty) line by 4 spaces.
    embedded = "\n".join(
        "    " + line if line.strip() else "" for line in script.splitlines()
    )
    args = " ".join(f"--index '{n}'" for n in INDEX_NAMES)
    args += " " + " ".join(f"--pattern '{p}'" for p in INDEX_PATTERNS)
    args += " --batch-size 200 --scroll-size 1000"
    job = JOB.replace("__ES_HOST__", ES_HOST).replace("__ES_AUTH_MODE__", ES_AUTH_MODE)
    job = job.replace("__REGION__", REGION).replace("__ARGS__", args)
    return CONFIGMAP_HEADER + embedded + "\n" + job


if __name__ == "__main__":
    print(render())
