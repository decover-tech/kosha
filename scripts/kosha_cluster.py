#!/usr/bin/env python3
"""Install a fresh Kosha stack or inspect it using an existing kubeconfig."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


BASE = Path(__file__).resolve().parents[1] / "k8s" / "base"
DEPLOYMENTS = ("deployment/kosha", "deployment/kosha-query", "deployment/kosha-indexer")


def image_override(image: str) -> dict[str, str]:
    if "@" in image:
        name, version = image.split("@", 1)
        if name and version:
            return {"name": "kosha", "newName": name, "digest": version}
    elif ":" in image.rsplit("/", 1)[-1]:
        name, version = image.rsplit(":", 1)
        if name and version and version not in {"main", "latest"}:
            return {"name": "kosha", "newName": name, "newTag": version}
    raise argparse.ArgumentTypeError("use a release tag or digest, not main or latest")


def render_overlay(root: Path, args: argparse.Namespace) -> Path:
    shutil.copytree(BASE, root / "base")
    overlay = root / "install"
    overlay.mkdir()
    config = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "kosha-config", "namespace": "kosha"},
        "data": {
            "KOSHA_S3_BUCKET": args.s3_bucket,
            "KOSHA_S3_PREFIX": args.s3_prefix,
            "AWS_DEFAULT_REGION": args.aws_region,
        },
    }
    account = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "kosha", "namespace": "kosha"},
    }
    if args.irsa_role_arn:
        account["metadata"]["annotations"] = {
            "eks.amazonaws.com/role-arn": args.irsa_role_arn,
        }
    namespace = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "kosha"},
        "$patch": "delete",
    }
    kustomization = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "namespace": "kosha",
        "resources": ["../base"],
        "images": [args.image],
        "patches": [
            {"patch": json.dumps(patch)} for patch in (config, account, namespace)
        ],
    }
    (overlay / "kustomization.yaml").write_text(
        json.dumps(kustomization), encoding="utf-8",
    )
    return overlay


def install(kubectl: list[str], args: argparse.Namespace) -> None:
    existing = subprocess.run(
        [*kubectl, "get", *DEPLOYMENTS, "--ignore-not-found", "-o", "name"],
        check=True, stdout=subprocess.PIPE, text=True,
    ).stdout
    if existing.strip():
        raise ValueError(
            "refusing to install over existing Kosha deployments. "
            "This command does not upgrade or roll back a cluster. "
            "Old readers cannot read new manifests. "
            "See README.md for the manifest/WAL upgrade note."
        )
    account = subprocess.run(
        [*kubectl, "get", "serviceaccount/kosha", "--ignore-not-found", "-o", "name"],
        check=True, stdout=subprocess.PIPE, text=True,
    ).stdout
    if account.strip():
        raise ValueError(
            "serviceaccount/kosha already exists. The installer creates this account; "
            "no resources were created."
        )
    subprocess.run([*kubectl, "get", "secret/kosha-secret", "-o", "name"], check=True)
    with tempfile.TemporaryDirectory(prefix="kosha-install-") as scratch:
        overlay = render_overlay(Path(scratch), args)
        subprocess.run([*kubectl, "create", "-k", str(overlay)], check=True)
    for deployment in DEPLOYMENTS:
        subprocess.run(
            [*kubectl, "rollout", "status", deployment, "--timeout=20m"], check=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True, help="target kubeconfig context")
    commands = parser.add_subparsers(dest="command", required=True)
    fresh = commands.add_parser(
        "install", help="install only; refuses existing deployments",
    )
    fresh.add_argument("--image", required=True, type=image_override)
    fresh.add_argument("--s3-bucket", required=True)
    fresh.add_argument("--s3-prefix", required=True)
    fresh.add_argument("--aws-region", default="us-east-1")
    fresh.add_argument("--irsa-role-arn", help="EKS IAM role for the service account")
    fresh.add_argument(
        "--confirm-empty-storage", action="store_true", required=True,
        help="confirm the Postgres database and S3 bucket/prefix have no Kosha data",
    )
    commands.add_parser("status", help="show images and readiness counts for all roles")
    args = parser.parse_args()
    kubectl = ["kubectl", "--context", args.context, "--namespace", "kosha"]
    try:
        if args.command == "install":
            install(kubectl, args)
        else:
            subprocess.run([*kubectl, "get", *DEPLOYMENTS, "-o", "wide"], check=True)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"kosha cluster: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
