#!/usr/bin/env python3
"""Install, upgrade, or health-check Kosha stacks using an existing kubeconfig.

A multi-tenant SaaS cell is installed with ``install --overlay saas``
(k8s/saas). Tenants are onboarded through the control-plane APIs (console
sign-up or ``/v1/admin/orgs``), never by installing another stack; see
docs/deployment-model.md.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


K8S = Path(__file__).resolve().parents[1] / "k8s"
BASE = K8S / "base"
OVERLAYS = ("base", "saas")
DEPLOYMENTS = ("deployment/kosha", "deployment/kosha-query", "deployment/kosha-indexer")
# Upgrade order: readers before the writer. New binaries read the old manifest
# and WAL formats, old binaries cannot read the new ones (README, "S3 WAL").
# The indexer also writes manifests, so it is stopped for the whole upgrade.
ROLLOUT_ORDER = ("kosha-query", "kosha", "kosha-indexer")
ROLE = {"kosha": "ingest", "kosha-query": "query", "kosha-indexer": "indexer"}
SAAS_SECRET_KEYS = (
    "database-url", "bootstrap-api-key", "admin-bootstrap-key", "internal-api-key",
    "stripe-api-key",
)
# Admin bootstrap credential (KOSHA_ADMIN_API_KEY) for /v1/admin/*. Required
# by k8s/saas; a base install wires it when kosha-secret has the key. It is a
# separate key from `admin-api-key`, which the segment-GC CronJob reads and
# may hold a revocable per-operator credential.
ADMIN_KEY_PATCH = [{
    "op": "add",
    "path": "/spec/template/spec/containers/0/env/-",
    "value": {
        "name": "KOSHA_ADMIN_API_KEY",
        "valueFrom": {"secretKeyRef": {
            "name": "kosha-secret", "key": "admin-bootstrap-key", "optional": True,
        }},
    },
}]
TLS_SECRET_KEYS = ("tls.crt", "tls.key", "ca.crt")
KEY_NAMES = '{{range $k, $v := .data}}{{$k}}{{"\\n"}}{{end}}'
UPGRADE_NOTE = "See README.md, \"S3 WAL\", for the manifest/WAL upgrade note."
WAL_SWITCH_NOTE = (
    "upgrade reads KOSHA_WAL_MODE from configmap/kosha-config, not from the running "
    "ingest pod. Switch the WAL mode as its own drained step before any upgrade: stop "
    "writes, flush every namespace (POST /flush), restart ingest on the current release "
    "with the new mode, and confirm it is healthy; only then run upgrade. See "
    "k8s/README.md, \"Switching the WAL mode\"."
)


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


def image_ref(override: dict[str, str]) -> str:
    if "digest" in override:
        return f"{override['newName']}@{override['digest']}"
    return f"{override['newName']}:{override['newTag']}"


def release_version(image: str) -> tuple[int, int, int] | None:
    """The semver of a tagged image, or None for a digest or another tag."""
    if "@" in image:
        return None
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", image.rsplit(":", 1)[-1])
    return tuple(int(part) for part in match.groups()) if match else None


def https_origin(value: str) -> str:
    if not re.fullmatch(r"https://[^/\s]+", value.rstrip("/")):
        raise argparse.ArgumentTypeError("use an https:// origin, for example https://console.example.com")
    return value.rstrip("/")


def render_overlay(root: Path, args: argparse.Namespace) -> Path:
    shutil.copytree(BASE, root / "base")
    saas = getattr(args, "overlay", "base") == "saas"
    if saas:
        shutil.copytree(K8S / "components", root / "components")
        shutil.copytree(K8S / "saas", root / "saas")
    overlay = root / "install"
    overlay.mkdir()
    data = {
        "KOSHA_S3_BUCKET": args.s3_bucket,
        "KOSHA_S3_PREFIX": args.s3_prefix,
        "AWS_DEFAULT_REGION": args.aws_region,
    }
    if saas:
        data["KOSHA_CONSOLE_PUBLIC_URL"] = args.console_public_url
        if args.kms_key_id:
            data["KOSHA_S3_SSE_KMS_KEY_ID"] = args.kms_key_id
    config = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "kosha-config", "namespace": "kosha"},
        "data": data,
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
    scheme = "https" if saas else "http"
    query = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "kosha-query", "namespace": "kosha"},
        "spec": {"template": {"spec": {"containers": [{
            "name": "kosha",
            "env": [{
                "name": "KOSHA_INGEST_HOST",
                "value": f"{scheme}://kosha-ingest-service.kosha.svc:8080",
            }],
        }]}}},
    }
    kustomization = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "namespace": "kosha",
        "resources": ["../saas" if saas else "../base"],
        "images": [args.image],
        "patches": [
            {"patch": json.dumps(patch)}
            for patch in (config, account, namespace, query)
        ],
    }
    if not saas:
        kustomization["patches"].append({
            "patch": json.dumps(ADMIN_KEY_PATCH),
            "target": {"kind": "Deployment", "name": "kosha(-query|-indexer)?"},
        })
    (overlay / "kustomization.yaml").write_text(
        json.dumps(kustomization), encoding="utf-8",
    )
    return overlay


def read(kubectl: list[str], *command: str) -> str:
    return subprocess.run(
        [*kubectl, *command], check=True, stdout=subprocess.PIPE, text=True,
    ).stdout


def require_secret_keys(kubectl: list[str], secret: str, keys: tuple[str, ...]) -> None:
    """Fail unless the Secret has every key. Only key names are read."""
    present = set(read(kubectl, "get", f"secret/{secret}", "-o", f"go-template={KEY_NAMES}").split())
    missing = [key for key in keys if key not in present]
    if missing:
        raise ValueError(
            f"secret/{secret} is missing keys {', '.join(missing)}; "
            "no resources were created."
        )


def install(kubectl: list[str], args: argparse.Namespace) -> None:
    existing = read(kubectl, "get", *DEPLOYMENTS, "--ignore-not-found", "-o", "name")
    if existing.strip():
        raise ValueError(
            "refusing to install over existing Kosha deployments. "
            "Use the upgrade command to move a running stack to a new release. "
            + UPGRADE_NOTE
        )
    account = read(kubectl, "get", "serviceaccount/kosha", "--ignore-not-found", "-o", "name")
    if account.strip():
        raise ValueError(
            "serviceaccount/kosha already exists. The installer creates this account; "
            "no resources were created."
        )
    if args.overlay == "saas":
        require_secret_keys(kubectl, "kosha-secret", SAAS_SECRET_KEYS)
        require_secret_keys(kubectl, "kosha-internal-tls", TLS_SECRET_KEYS)
    else:
        subprocess.run([*kubectl, "get", "secret/kosha-secret", "-o", "name"], check=True)
    with tempfile.TemporaryDirectory(prefix="kosha-install-") as scratch:
        overlay = render_overlay(Path(scratch), args)
        subprocess.run([*kubectl, "create", "-k", str(overlay)], check=True)
    for deployment in DEPLOYMENTS:
        subprocess.run(
            [*kubectl, "rollout", "status", deployment, "--timeout=20m"], check=True,
        )


def container(deployment: dict) -> dict:
    return deployment["spec"]["template"]["spec"]["containers"][0]


def is_ready(deployment: dict) -> bool:
    spec, status = deployment["spec"], deployment.get("status", {})
    want = spec.get("replicas", 1)
    return (
        status.get("observedGeneration", 0) >= deployment["metadata"].get("generation", 0)
        and status.get("readyReplicas", 0) == want
        and status.get("updatedReplicas", 0) == want
        and status.get("replicas", 0) == want
    )


def image_repo(image: str) -> str:
    """The image without its tag or digest."""
    name = image.split("@", 1)[0]
    head, _, tail = name.rpartition("/")
    return f"{head}/{tail.split(':', 1)[0]}" if head else tail.split(":", 1)[0]


def plan_upgrade(deployments: dict[str, dict], target: str, wal_mode: str,
                 args: argparse.Namespace) -> list[str]:
    """Validate the stack and return the roles whose image is not yet ``target``.

    Raises ValueError for any state the README compatibility rules forbid.
    Resumable states: along ROLLOUT_ORDER, images form contiguous groups that
    never get newer (readers at least as new as the writer), for example a
    finished query rollout, or a failed ingest release being fixed forward.
    """
    missing = [name for name in ROLLOUT_ORDER if name not in deployments]
    if missing:
        raise ValueError(f"deployments not found: {', '.join(missing)}. Install first.")
    images = [container(deployments[name])["image"] for name in ROLLOUT_ORDER]
    done = 0
    while done < len(images) and images[done] == target:
        done += 1
    rest = images[done:]
    found = ", ".join(f"{ROLE[n]}={i}" for n, i in zip(ROLLOUT_ORDER, images))
    mixed = ValueError(
        f"roles run mixed images ({found}) that are not a resumable upgrade "
        f"to {target}. Bring every role to one release first. " + UPGRADE_NOTE
    )
    if target in rest or len(set(images)) != sum(
        1 for i, image in enumerate(images) if i == 0 or images[i - 1] != image
    ):
        raise mixed
    versions = [release_version(image) for image in rest]
    known = [v for v in versions if v is not None]
    if len(set(rest)) > 1:
        if None in versions and not args.confirm_forward_only:
            raise ValueError(
                f"cannot order {found} by release tag. Pass --confirm-forward-only "
                "after checking each role's image is at least as new as the next "
                "role's and the target is not older. " + UPGRADE_NOTE
            )
        if any(a < b for a, b in zip(known, known[1:])):
            raise mixed
    new = release_version(target)
    if rest and new and any(new < old for old in known):
        raise ValueError(
            f"refusing to roll back to {target} (running {found}): older readers "
            "cannot read the new manifest object or v2/v3 WAL frames, and "
            "rolling back requires converting both formats. " + UPGRADE_NOTE
        )
    if rest and (new is None or None in versions) and not args.confirm_forward_only:
        raise ValueError(
            f"cannot order {found} and {target} by release tag. Pass "
            "--confirm-forward-only after checking the target is not older."
        )
    if rest and wal_mode.strip().lower() != "s3" and not args.allow_local_wal:
        raise ValueError(
            "KOSHA_WAL_MODE is not s3: the ingest WAL lives on the node's disk and "
            "must be preserved across the restart. Pass --allow-local-wal only if "
            "the ingest pod will come back on the same node. If you are moving to "
            "S3 WAL, do not combine it with this upgrade. " + WAL_SWITCH_NOTE
        )
    ingest = deployments["kosha"]["spec"]
    if ingest.get("replicas", 1) > 1 or ingest.get("strategy", {}).get("type") != "Recreate":
        raise ValueError(
            "deployment/kosha must run one replica with strategy Recreate: two "
            "ingest pods must never write the same WAL. " + UPGRADE_NOTE
        )
    # Query must be fully rolled out (no old reader pods left) before ingest
    # changes, including when its new image was set by an interrupted run.
    # Ingest may be down only on a failed newer release being fixed forward.
    oldest = images[-1]
    for name, image in zip(ROLLOUT_ORDER, images):
        if name == "kosha-indexer" and deployments[name]["spec"].get("replicas", 1) == 0:
            continue
        if name == "kosha" and image not in (oldest, target) and name in ROLLOUT_ORDER[done:]:
            continue
        if not is_ready(deployments[name]):
            raise ValueError(
                f"deployment/{name} is not fully rolled out and ready; "
                "wait for or fix it before upgrading."
            )
    return list(ROLLOUT_ORDER[done:])


def stale_cronjobs(
    jobs: dict, target: str, current: set[str] = frozenset(),
) -> list[tuple[str, str]]:
    """(cronjob, container) pairs running another tag of the target image, or
    an image the Deployments run (so a registry switch still finds them)."""
    repo = image_repo(target)
    return [
        (job["metadata"]["name"], box["name"])
        for job in jobs.get("items", [])
        for box in job["spec"]["jobTemplate"]["spec"]["template"]["spec"].get("containers", [])
        if box.get("image") != target
        and (image_repo(box.get("image", "")) == repo or box.get("image") in current)
    ]


def upgrade(kubectl: list[str], args: argparse.Namespace) -> None:
    target = image_ref(args.image)
    listed = json.loads(read(kubectl, "get", *DEPLOYMENTS, "--ignore-not-found", "-o", "json"))
    deployments = {d["metadata"]["name"]: d for d in listed.get("items", [])}
    wal_mode = read(
        kubectl, "get", "configmap/kosha-config", "-o", "jsonpath={.data.KOSHA_WAL_MODE}",
    )
    pending = plan_upgrade(deployments, target, wal_mode, args)
    current = {container(d)["image"] for d in deployments.values()}
    jobs = stale_cronjobs(
        json.loads(read(kubectl, "get", "cronjob", "-o", "json")), target, current,
    )
    indexer = deployments["kosha-indexer"]
    stopped = indexer["spec"].get("replicas", 1) == 0
    replicas = indexer["spec"].get("replicas", 1) or args.indexer_replicas
    if not pending and not stopped and not jobs:
        print(f"all roles already run {target}")
        return
    steps = []
    if pending:
        steps.append(f"scale indexer to 0 (back to {replicas} after the upgrade)")
        steps += [
            f"{ROLE[name]}: {container(deployments[name])['image']} -> {target}"
            for name in pending
        ]
    elif stopped:
        steps.append(f"scale indexer back to {replicas}")
    steps += [f"cronjob/{job} {box} -> {target}" for job, box in jobs]
    print("upgrade plan:\n  " + "\n  ".join(steps))
    if args.dry_run:
        return

    def rollout(name: str) -> None:
        subprocess.run(
            [*kubectl, "rollout", "status", f"deployment/{name}", "--timeout=20m"],
            check=True,
        )

    def set_image(name: str) -> None:
        subprocess.run([
            *kubectl, "set", "image", f"deployment/{name}",
            f"{container(deployments[name])['name']}={target}",
        ], check=True)

    def scale_indexer(count: int) -> None:
        subprocess.run([
            *kubectl, "scale", "deployment/kosha-indexer", f"--replicas={count}",
        ], check=True)

    if pending:
        scale_indexer(0)
        rollout("kosha-indexer")
    if "kosha-query" in pending:
        set_image("kosha-query")
        try:
            rollout("kosha-query")
        except subprocess.CalledProcessError:
            # Nothing has written the new formats yet, so undoing is safe.
            subprocess.run([*kubectl, "rollout", "undo", "deployment/kosha-query"], check=True)
            # Restart the indexer only if it was running on ingest's image. After
            # a failed ingest release it stays stopped: ingest may already have
            # written formats the old indexer cannot read.
            if not stopped and (
                container(indexer)["image"] == container(deployments["kosha"])["image"]
            ):
                scale_indexer(replicas)
            raise ValueError(
                "query rollout failed and was undone; ingest and indexer images were "
                "not changed. Fix the release and retry."
            ) from None
    if "kosha" in pending:
        set_image("kosha")
        try:
            rollout("kosha")
        except subprocess.CalledProcessError:
            raise ValueError(
                "ingest rollout failed. Do not roll back query or ingest: the new "
                "ingest may already have written the new manifest/WAL formats. "
                "Fix forward: rerun upgrade with this or a newer fixed release; "
                "the indexer stays at 0 replicas until then. " + UPGRADE_NOTE
            ) from None
    if "kosha-indexer" in pending:
        set_image("kosha-indexer")
    if pending or stopped:
        scale_indexer(replicas)
        rollout("kosha-indexer")
    for job, box in jobs:
        subprocess.run([*kubectl, "set", "image", f"cronjob/{job}", f"{box}={target}"], check=True)
    print(f"upgraded to {target}")


def cluster_health(kubectl: list[str]) -> dict:
    """One cluster's Kosha health from Deployments, CronJobs, HPA and PDB."""
    listed = json.loads(read(kubectl, "get", "deployment,cronjob,hpa,pdb", "-o", "json"))
    by_kind: dict[str, list[dict]] = {}
    for item in listed.get("items", []):
        by_kind.setdefault(item.get("kind", ""), []).append(item)
    found = {d["metadata"]["name"]: d for d in by_kind.get("Deployment", [])}
    roles, problems = [], []
    for name in ROLLOUT_ORDER:
        deployment = found.get(name)
        if deployment is None:
            problems.append(f"{ROLE[name]}: deployment/{name} missing")
            continue
        status = deployment.get("status", {})
        ready = is_ready(deployment)
        roles.append({
            "role": ROLE[name],
            "deployment": name,
            "image": container(deployment)["image"],
            "replicas": deployment["spec"].get("replicas", 1),
            "ready": status.get("readyReplicas", 0),
            "updated": status.get("updatedReplicas", 0),
            "healthy": ready,
        })
        if not ready:
            problems.append(f"{ROLE[name]}: not ready")
        elif name != "kosha-indexer" and deployment["spec"].get("replicas", 1) == 0:
            problems.append(f"{ROLE[name]}: scaled to 0 replicas")
    images = sorted({role["image"] for role in roles})
    if len(images) > 1:
        problems.append("roles run different images (unfinished upgrade?)")
    cronjobs = [{
        "name": job["metadata"]["name"],
        "suspended": bool(job["spec"].get("suspend", False)),
        "last_success": job.get("status", {}).get("lastSuccessfulTime"),
    } for job in by_kind.get("CronJob", [])]
    hpa = [{
        "name": h["metadata"]["name"],
        "current": h.get("status", {}).get("currentReplicas"),
        "desired": h.get("status", {}).get("desiredReplicas"),
        "min": h["spec"].get("minReplicas"),
        "max": h["spec"].get("maxReplicas"),
    } for h in by_kind.get("HorizontalPodAutoscaler", [])]
    pdb = [{
        "name": p["metadata"]["name"],
        "disruptions_allowed": p.get("status", {}).get("disruptionsAllowed"),
    } for p in by_kind.get("PodDisruptionBudget", [])]
    return {
        "healthy": not problems,
        "images": images,
        "roles": roles,
        "cronjobs": cronjobs,
        "hpa": hpa,
        "pdb": pdb,
        "problems": problems,
    }


def status(contexts: list[str], as_json: bool) -> bool:
    reports = []
    for context in contexts:
        kubectl = ["kubectl", "--context", context, "--namespace", "kosha"]
        try:
            report = cluster_health(kubectl)
        except (OSError, ValueError, subprocess.CalledProcessError) as exc:
            report = {"healthy": False, "problems": [f"cannot read cluster: {exc}"]}
        reports.append({"context": context, **report})
    if as_json:
        print(json.dumps(reports, indent=2))
    else:
        for report in reports:
            state = "HEALTHY" if report["healthy"] else "UNHEALTHY"
            print(f"{report['context']}: {state} {' '.join(report.get('images', []))}".rstrip())
            for role in report.get("roles", []):
                print(f"  {role['role']:8} {role['ready']}/{role['replicas']} ready  {role['image']}")
            for job in report.get("cronjobs", []):
                print(f"  cronjob {job['name']} suspended={job['suspended']} last_success={job['last_success']}")
            for problem in report["problems"]:
                print(f"  problem: {problem}")
    return all(report["healthy"] for report in reports)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--context", required=True, action="append",
        help="target kubeconfig context; status accepts it more than once (one per cell)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    fresh = commands.add_parser(
        "install", help="install only; refuses existing deployments",
    )
    fresh.add_argument("--image", required=True, type=image_override)
    fresh.add_argument(
        "--overlay", choices=OVERLAYS, default="base",
        help="base: k8s/base; saas: a multi-tenant SaaS cell (k8s/saas)",
    )
    fresh.add_argument("--s3-bucket", required=True)
    fresh.add_argument("--s3-prefix", required=True)
    fresh.add_argument("--aws-region", default="us-east-1")
    fresh.add_argument("--irsa-role-arn", help="EKS IAM role for the service account")
    fresh.add_argument(
        "--console-public-url", type=https_origin,
        help="saas: the console's public https origin (KOSHA_CONSOLE_PUBLIC_URL)",
    )
    fresh.add_argument(
        "--kms-key-id",
        help="saas: default SSE-KMS key for tenants without their own key",
    )
    fresh.add_argument(
        "--confirm-empty-storage", action="store_true", required=True,
        help="confirm the Postgres database and S3 bucket/prefix have no Kosha data",
    )
    move = commands.add_parser(
        "upgrade",
        help="roll every role forward to one release, readers before the writer",
        description="Roll every role forward to one release, readers before the "
        "writer. " + WAL_SWITCH_NOTE,
    )
    move.add_argument("--image", required=True, type=image_override)
    move.add_argument("--dry-run", action="store_true", help="print the plan only")
    move.add_argument(
        "--confirm-forward-only", action="store_true",
        help="the images have no comparable release tags; confirm this is not a rollback",
    )
    move.add_argument(
        "--allow-local-wal", action="store_true",
        help="KOSHA_WAL_MODE is not s3 and the ingest pod keeps its node and WAL directory",
    )
    move.add_argument(
        "--indexer-replicas", type=int, default=1,
        help="indexer replicas to restore when resuming with the indexer at 0",
    )
    health = commands.add_parser(
        "status", help="read-only health report for each --context; exits 1 if any is unhealthy",
    )
    health.add_argument("--json", action="store_true", help="machine-readable report")
    args = parser.parse_args()
    if args.command == "install" and args.overlay == "saas" and not args.console_public_url:
        parser.error("install --overlay saas requires --console-public-url")
    if args.command != "status" and len(args.context) != 1:
        parser.error(f"{args.command} takes exactly one --context")
    kubectl = ["kubectl", "--context", args.context[0], "--namespace", "kosha"]
    try:
        if args.command == "install":
            install(kubectl, args)
        elif args.command == "upgrade":
            upgrade(kubectl, args)
        else:
            return 0 if status(args.context, args.json) else 1
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"kosha cluster: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
