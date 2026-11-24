"""Smoke tests for install, upgrade and status in scripts/kosha_cluster.py."""

from pathlib import Path
import json
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kosha_cluster  # noqa: E402


KUBECTL = ["kubectl", "--context", "customer", "--namespace", "kosha"]
DEPLOYMENTS = ["deployment/kosha", "deployment/kosha-query", "deployment/kosha-indexer"]
LOOKUP = ["get", *DEPLOYMENTS, "--ignore-not-found", "-o", "name"]
ACCOUNT = ["get", "serviceaccount/kosha", "--ignore-not-found", "-o", "name"]
SECRET = ["get", "secret/kosha-secret", "-o", "name"]
INSTALL = [
    "install", "--image", "registry.example:5000/kosha:1.2.3",
    "--s3-bucket", "test-bucket", "--s3-prefix", "test-prefix/",
    "--irsa-role-arn", "arn:aws:iam::123456789012:role/kosha",
    "--confirm-empty-storage",
]


@pytest.fixture
def kubectl(monkeypatch):
    calls = []
    responses = {}
    rendered = {}

    def run(command, **kwargs):
        assert command[:len(KUBECTL)] == KUBECTL
        assert kwargs["check"]
        args = command[len(KUBECTL):]
        calls.append(args)
        if args[:2] == ["create", "-k"]:
            overlay = Path(args[2])
            rendered.update(json.loads((overlay / "kustomization.yaml").read_text()))
            assert (overlay / rendered["resources"][0] / "kustomization.yaml").is_file()
            assert (overlay / ".." / "base" / "deployment.yaml").is_file()
        response = responses.get(tuple(args), "")
        if isinstance(response, int):
            raise subprocess.CalledProcessError(response, command)
        return subprocess.CompletedProcess(command, 0, stdout=response)

    def invoke(*args):
        monkeypatch.setattr(
            sys, "argv", ["kosha_cluster.py", "--context", "customer", *args],
        )
        return kosha_cluster.main()

    monkeypatch.setattr(subprocess, "run", run)
    return invoke, calls, responses, rendered


def test_install_configures_existing_base_and_waits_for_all_roles(kubectl):
    invoke, calls, _, rendered = kubectl
    assert invoke(*INSTALL) == 0
    assert calls[:3] == [LOOKUP, ACCOUNT, SECRET]
    assert calls[3][:2] == ["create", "-k"]
    assert not Path(calls[3][2]).exists()
    assert calls[4:] == [
        ["rollout", "status", deployment, "--timeout=20m"] for deployment in DEPLOYMENTS
    ]
    assert rendered["images"] == [{
        "name": "kosha", "newName": "registry.example:5000/kosha", "newTag": "1.2.3",
    }]
    config, account, namespace, _, admin = [
        json.loads(patch["patch"]) for patch in rendered["patches"]
    ]
    assert rendered["patches"][4]["target"]["kind"] == "Deployment"
    assert admin[0]["value"]["name"] == "KOSHA_ADMIN_API_KEY"
    assert admin[0]["value"]["valueFrom"]["secretKeyRef"] == {
        "name": "kosha-secret", "key": "admin-bootstrap-key", "optional": True,
    }
    assert config["data"] == {
        "KOSHA_S3_BUCKET": "test-bucket",
        "KOSHA_S3_PREFIX": "test-prefix/",
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    assert account["metadata"]["annotations"] == {
        "eks.amazonaws.com/role-arn": "arn:aws:iam::123456789012:role/kosha",
    }
    assert namespace["$patch"] == "delete"


def test_query_forwarding_uses_cluster_dns_search_path(kubectl):
    invoke, _, _, rendered = kubectl
    assert invoke(*INSTALL) == 0
    patches = [json.loads(patch["patch"]) for patch in rendered["patches"]]
    query = next(patch for patch in patches if patch["kind"] == "Deployment")
    assert query["metadata"] == {"name": "kosha-query", "namespace": "kosha"}
    containers = query["spec"]["template"]["spec"]["containers"]
    assert containers == [{
        "name": "kosha",
        "env": [{
            "name": "KOSHA_INGEST_HOST",
            "value": "http://kosha-ingest-service.kosha.svc:8080",
        }],
    }]


@pytest.mark.parametrize("deployment", DEPLOYMENTS)
def test_refuses_to_upgrade_any_existing_role(kubectl, capsys, deployment):
    invoke, calls, responses, _ = kubectl
    responses[tuple(LOOKUP)] = deployment + "\n"
    assert invoke(*INSTALL) == 1
    assert calls == [LOOKUP]
    err = capsys.readouterr().err
    assert "Use the upgrade command" in err
    assert "manifest/WAL upgrade note" in err


def test_rejects_preexisting_service_account_before_creating_anything(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    responses[tuple(ACCOUNT)] = "serviceaccount/kosha\n"
    assert invoke(*INSTALL) == 1
    assert calls == [LOOKUP, ACCOUNT]
    assert "serviceaccount/kosha already exists" in capsys.readouterr().err


@pytest.mark.parametrize("check", [LOOKUP, ACCOUNT, SECRET])
def test_preflight_failure_does_not_create_resources(kubectl, check):
    invoke, calls, responses, _ = kubectl
    responses[tuple(check)] = 1
    assert invoke(*INSTALL) == 1
    checks = [LOOKUP, ACCOUNT, SECRET]
    assert calls == checks[:checks.index(check) + 1]


def test_requires_empty_storage_confirmation(kubectl):
    invoke, calls, _, _ = kubectl
    with pytest.raises(SystemExit) as exc:
        invoke(*INSTALL[:-1])
    assert exc.value.code == 2
    assert not calls


SAAS_INSTALL = [
    *INSTALL[:-1], "--overlay", "saas",
    "--console-public-url", "https://console.example.com/",
    "--kms-key-id", "alias/kosha-default", "--confirm-empty-storage",
]
KEY_NAMES = '{{range $k, $v := .data}}{{$k}}{{"\\n"}}{{end}}'
SAAS_SECRET = ["get", "secret/kosha-secret", "-o", f"go-template={KEY_NAMES}"]
TLS_SECRET = ["get", "secret/kosha-internal-tls", "-o", f"go-template={KEY_NAMES}"]


def saas_secrets(responses):
    responses[tuple(SAAS_SECRET)] = (
        "admin-bootstrap-key\nbootstrap-api-key\ndatabase-url\ninternal-api-key\nstripe-api-key\n"
    )
    responses[tuple(TLS_SECRET)] = "ca.crt\ntls.crt\ntls.key\n"


def test_saas_install_uses_saas_overlay_and_checks_secret_keys(kubectl):
    invoke, calls, responses, rendered = kubectl
    saas_secrets(responses)
    assert invoke(*SAAS_INSTALL) == 0
    assert calls[:4] == [LOOKUP, ACCOUNT, SAAS_SECRET, TLS_SECRET]
    assert calls[4][:2] == ["create", "-k"]
    assert rendered["resources"] == ["../saas"]
    assert len(rendered["patches"]) == 4  # k8s/saas wires the admin key itself
    patches = [json.loads(patch["patch"]) for patch in rendered["patches"]]
    config = patches[0]["data"]
    assert config["KOSHA_CONSOLE_PUBLIC_URL"] == "https://console.example.com"
    assert config["KOSHA_S3_SSE_KMS_KEY_ID"] == "alias/kosha-default"
    query = patches[3]["spec"]["template"]["spec"]["containers"][0]["env"][0]
    assert query["value"] == "https://kosha-ingest-service.kosha.svc:8080"


def test_saas_install_refuses_missing_secret_keys(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    saas_secrets(responses)
    responses[tuple(SAAS_SECRET)] = "database-url\nbootstrap-api-key\n"
    assert invoke(*SAAS_INSTALL) == 1
    assert calls == [LOOKUP, ACCOUNT, SAAS_SECRET]
    err = capsys.readouterr().err
    assert "admin-bootstrap-key, internal-api-key, stripe-api-key" in err


def test_saas_install_requires_console_url(kubectl):
    invoke, calls, _, _ = kubectl
    with pytest.raises(SystemExit) as exc:
        invoke(*INSTALL[:-1], "--overlay", "saas", "--confirm-empty-storage")
    assert exc.value.code == 2
    assert not calls


def test_saas_overlay_renders_with_kustomize(tmp_path):
    """The rendered SaaS install builds; skipped where kustomize is absent."""
    if shutil.which("kustomize"):
        build = ["kustomize", "build"]
    elif shutil.which("kubectl"):
        build = ["kubectl", "kustomize"]
    else:
        pytest.skip("kustomize not installed")
    args = kosha_cluster.argparse.Namespace(
        overlay="saas", s3_bucket="b", s3_prefix="p/", aws_region="us-east-1",
        irsa_role_arn=None, console_public_url="https://console.example.com",
        kms_key_id=None, image=kosha_cluster.image_override("kosha:1.2.3"),
    )
    overlay = kosha_cluster.render_overlay(tmp_path, args)
    out = subprocess.run(
        [*build, str(overlay)], check=True, stdout=subprocess.PIPE, text=True,
    ).stdout
    assert "KOSHA_INTERNAL_TLS: required" in out
    assert 'KOSHA_LEGACY_ROUTES: "off"' in out
    assert "KOSHA_CONSOLE_PUBLIC_URL: https://console.example.com" in out
    assert "https://kosha-ingest-service.kosha.svc:8080" in out
    assert "image: kosha:1.2.3" in out
    assert out.count("key: admin-bootstrap-key") == 3  # Deployments
    assert out.count("key: admin-api-key") == 1  # segment GC
    assert out.count("name: KOSHA_ADMIN_API_KEY") == 4  # 3 Deployments + segment GC


# --- upgrade -------------------------------------------------------------

OLD, NEW = "registry.example:5000/kosha:1.2.3", "registry.example:5000/kosha:1.3.0"
UP_LOOKUP = ["get", *DEPLOYMENTS, "--ignore-not-found", "-o", "json"]
WAL = ["get", "configmap/kosha-config", "-o", "jsonpath={.data.KOSHA_WAL_MODE}"]
CRONJOBS = ["get", "cronjob", "-o", "json"]


def deployment(name, image, replicas=1, ready=True, strategy="RollingUpdate"):
    have = replicas if ready else 0
    return {
        "kind": "Deployment",
        "metadata": {"name": name, "generation": 2},
        "spec": {
            "replicas": replicas,
            "strategy": {"type": strategy},
            "template": {"spec": {"containers": [{"name": "kosha", "image": image}]}},
        },
        "status": {
            "observedGeneration": 2, "replicas": replicas,
            "readyReplicas": have, "updatedReplicas": have,
        },
    }


def stack(query=OLD, ingest=OLD, indexer=OLD, indexer_replicas=1, query_ready=True,
          **ingest_kw):
    ingest_kw.setdefault("strategy", "Recreate")
    return json.dumps({"items": [
        deployment("kosha", ingest, **ingest_kw),
        deployment("kosha-query", query, replicas=3, ready=query_ready),
        deployment("kosha-indexer", indexer, replicas=indexer_replicas, strategy="Recreate"),
    ]})


def cronjobs(image=OLD):
    return json.dumps({"items": [{
        "metadata": {"name": "kosha-usage-reconcile"},
        "spec": {"jobTemplate": {"spec": {"template": {"spec": {
            "containers": [{"name": "reconcile", "image": image}],
        }}}}},
    }]})


def rollout(name):
    return ["rollout", "status", f"deployment/{name}", "--timeout=20m"]


def set_image(name, image=NEW):
    return ["set", "image", f"deployment/{name}", f"kosha={image}"]


def scale(count):
    return ["scale", "deployment/kosha-indexer", f"--replicas={count}"]


def prime(responses, wal="s3", **kw):
    responses[tuple(UP_LOOKUP)] = stack(**kw)
    responses[tuple(WAL)] = wal
    responses[tuple(CRONJOBS)] = cronjobs()


def test_upgrade_rolls_readers_before_writer_with_indexer_stopped(kubectl):
    invoke, calls, responses, _ = kubectl
    prime(responses)
    assert invoke("upgrade", "--image", NEW) == 0
    assert calls == [
        UP_LOOKUP, WAL, CRONJOBS,
        scale(0), rollout("kosha-indexer"),
        set_image("kosha-query"), rollout("kosha-query"),
        set_image("kosha"), rollout("kosha"),
        set_image("kosha-indexer"), scale(1), rollout("kosha-indexer"),
        ["set", "image", "cronjob/kosha-usage-reconcile", f"reconcile={NEW}"],
    ]


def test_upgrade_dry_run_changes_nothing(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    prime(responses)
    assert invoke("upgrade", "--image", NEW, "--dry-run") == 0
    assert calls == [UP_LOOKUP, WAL, CRONJOBS]
    assert "query: " in capsys.readouterr().out


def test_upgrade_resumes_after_query_was_upgraded(kubectl):
    invoke, calls, responses, _ = kubectl
    prime(responses, query=NEW, indexer_replicas=0)
    assert invoke("upgrade", "--image", NEW) == 0
    assert set_image("kosha-query") not in calls
    assert calls[3:9] == [
        scale(0), rollout("kosha-indexer"), set_image("kosha"), rollout("kosha"),
        set_image("kosha-indexer"), scale(1),
    ]


def test_upgrade_noop_when_current(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    prime(responses, query=NEW, ingest=NEW, indexer=NEW)
    responses[tuple(CRONJOBS)] = cronjobs(NEW)
    assert invoke("upgrade", "--image", NEW) == 0
    assert calls == [UP_LOOKUP, WAL, CRONJOBS]
    assert "already run" in capsys.readouterr().out


@pytest.mark.parametrize("kw,args,message", [
    ({"query": NEW, "ingest": OLD, "indexer": NEW}, [], "mixed images"),
    ({"query": "registry.example:5000/kosha:1.1.0"}, [], "mixed images"),
    ({}, [], "refusing to roll back"),
    ({"wal": "local"}, [], "KOSHA_WAL_MODE is not s3"),
    ({"strategy": "RollingUpdate"}, [], "strategy Recreate"),
    ({"replicas": 2}, [], "one replica"),
    ({"ready": False}, [], "not fully rolled out"),
])
def test_upgrade_refuses_unsafe_states(kubectl, capsys, kw, args, message):
    invoke, calls, responses, _ = kubectl
    prime(responses, **kw)
    target = "registry.example:5000/kosha:1.0.0" if message == "refusing to roll back" else NEW
    assert invoke("upgrade", "--image", target, *args) == 1
    assert calls == [UP_LOOKUP, WAL]
    assert message in capsys.readouterr().err


def test_upgrade_local_wal_needs_explicit_opt_in(kubectl):
    invoke, calls, responses, _ = kubectl
    prime(responses, wal="")
    assert invoke("upgrade", "--image", NEW, "--allow-local-wal", "--dry-run") == 0


def test_upgrade_local_wal_refusal_says_to_switch_modes_separately(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    prime(responses, wal="local")
    assert invoke("upgrade", "--image", NEW) == 1
    err = capsys.readouterr().err
    assert "own drained step before any upgrade" in err
    assert "POST /flush" in err
    assert calls == [UP_LOOKUP, WAL]


def test_upgrade_help_documents_the_wal_switch(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["kosha_cluster.py", "upgrade", "--help"])
    with pytest.raises(SystemExit):
        kosha_cluster.main()
    assert "drained step" in " ".join(capsys.readouterr().out.split())


def test_upgrade_digest_needs_forward_only_confirmation(kubectl, capsys):
    invoke, _, responses, _ = kubectl
    prime(responses)
    digest = "registry.example:5000/kosha@sha256:" + "a" * 64
    assert invoke("upgrade", "--image", digest, "--dry-run") == 1
    assert "--confirm-forward-only" in capsys.readouterr().err
    assert invoke("upgrade", "--image", digest, "--dry-run", "--confirm-forward-only") == 0


def test_failed_query_rollout_is_undone(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    prime(responses)
    responses[tuple(rollout("kosha-query"))] = 1
    assert invoke("upgrade", "--image", NEW) == 1
    assert calls[-2:] == [["rollout", "undo", "deployment/kosha-query"], scale(1)]
    assert set_image("kosha") not in calls
    assert "was undone" in capsys.readouterr().err


def test_failed_ingest_rollout_is_not_rolled_back(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    prime(responses)
    responses[tuple(rollout("kosha"))] = 1
    assert invoke("upgrade", "--image", NEW) == 1
    assert calls[-1] == rollout("kosha")
    assert not any(call[:2] == ["rollout", "undo"] for call in calls)
    assert "Do not roll back" in capsys.readouterr().err


def test_upgrade_resume_waits_for_query_rollout(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    prime(responses, query=NEW, query_ready=False)
    assert invoke("upgrade", "--image", NEW) == 1
    assert calls == [UP_LOOKUP, WAL]
    assert "kosha-query is not fully rolled out" in capsys.readouterr().err


def test_upgrade_retry_restores_indexer_and_cronjobs(kubectl):
    invoke, calls, responses, _ = kubectl
    prime(responses, query=NEW, ingest=NEW, indexer=NEW, indexer_replicas=0)
    assert invoke("upgrade", "--image", NEW) == 0
    assert calls[3:] == [
        scale(1), rollout("kosha-indexer"),
        ["set", "image", "cronjob/kosha-usage-reconcile", f"reconcile={NEW}"],
    ]


def test_upgrade_fixes_forward_past_a_failed_ingest_release(kubectl):
    invoke, calls, responses, _ = kubectl
    fixed = "registry.example:5000/kosha:1.3.1"
    prime(responses, query=NEW, ingest=NEW, ready=False, indexer_replicas=0)
    assert invoke("upgrade", "--image", fixed) == 0
    assert set_image("kosha-query", fixed) in calls
    assert set_image("kosha", fixed) in calls
    assert set_image("kosha-indexer", fixed) in calls


def test_upgrade_refuses_reader_older_than_writer(kubectl, capsys):
    invoke, _, responses, _ = kubectl
    prime(responses, query=OLD, ingest=NEW, indexer="registry.example:5000/kosha:1.0.0")
    assert invoke("upgrade", "--image", "registry.example:5000/kosha:1.4.0") == 1
    assert "mixed images" in capsys.readouterr().err


def test_upgrade_updates_cronjobs_across_a_registry_switch(kubectl):
    invoke, calls, responses, _ = kubectl
    moved = "mirror.example/kosha/kosha:1.3.0"
    prime(responses)
    assert invoke("upgrade", "--image", moved) == 0
    assert ["set", "image", "cronjob/kosha-usage-reconcile", f"reconcile={moved}"] in calls


def test_failed_fix_forward_query_rollout_keeps_indexer_stopped(kubectl):
    invoke, calls, responses, _ = kubectl
    fixed = "registry.example:5000/kosha:1.3.1"
    prime(responses, query=NEW, ingest=NEW, ready=False, indexer_replicas=0)
    responses[tuple(rollout("kosha-query"))] = 1
    assert invoke("upgrade", "--image", fixed) == 1
    assert ["rollout", "undo", "deployment/kosha-query"] in calls
    assert scale(1) not in calls


def test_upgrade_takes_one_context(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", [
        "kosha_cluster.py", "--context", "a", "--context", "b", "upgrade", "--image", NEW,
    ])
    with pytest.raises(SystemExit) as exc:
        kosha_cluster.main()
    assert exc.value.code == 2


# --- status --------------------------------------------------------------

HEALTH = ["get", "deployment,cronjob,hpa,pdb", "-o", "json"]


def health_items(**kw):
    items = json.loads(stack(**kw))["items"]
    items.append({"kind": "HorizontalPodAutoscaler", "metadata": {"name": "kosha-query"},
                  "spec": {"minReplicas": 3, "maxReplicas": 12},
                  "status": {"currentReplicas": 3, "desiredReplicas": 3}})
    items.append({"kind": "PodDisruptionBudget", "metadata": {"name": "kosha-query"},
                  "spec": {}, "status": {"disruptionsAllowed": 1}})
    return json.dumps({"items": items})


def test_status_is_read_only_and_reports_health(kubectl, capsys):
    invoke, calls, responses, _ = kubectl
    responses[tuple(HEALTH)] = health_items()
    assert invoke("status", "--json") == 0
    assert calls == [HEALTH]
    [report] = json.loads(capsys.readouterr().out)
    assert report["context"] == "customer"
    assert report["healthy"] and report["images"] == [OLD]
    assert [r["role"] for r in report["roles"]] == ["query", "ingest", "indexer"]
    assert report["hpa"][0]["max"] == 12
    assert report["pdb"][0]["disruptions_allowed"] == 1


def test_status_flags_unfinished_upgrade(kubectl, capsys):
    invoke, _, responses, _ = kubectl
    responses[tuple(HEALTH)] = health_items(query=NEW)
    assert invoke("status") == 1
    out = capsys.readouterr().out
    assert "UNHEALTHY" in out and "different images" in out


def test_status_reports_each_cluster(monkeypatch, capsys):
    def run(command, **kwargs):
        assert command[-4:] == HEALTH
        if command[2] == "cell-b":
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0, stdout=health_items())

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(sys, "argv", [
        "kosha_cluster.py", "--context", "cell-a", "--context", "cell-b", "status", "--json",
    ])
    assert kosha_cluster.main() == 1
    reports = json.loads(capsys.readouterr().out)
    assert [(r["context"], r["healthy"]) for r in reports] == [
        ("cell-a", True), ("cell-b", False),
    ]
