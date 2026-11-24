"""Smoke tests for the Kubernetes installer in scripts/kosha_cluster.py."""

from pathlib import Path
import json
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kosha_cluster  # noqa: E402


KUBECTL = ["kubectl", "--context", "customer", "--namespace", "kosha"]
DEPLOYMENTS = ["deployment/kosha", "deployment/kosha-query", "deployment/kosha-indexer"]
LOOKUP = ["get", *DEPLOYMENTS, "--ignore-not-found", "-o", "name"]
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
            assert (overlay / rendered["resources"][0] / "deployment.yaml").is_file()
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
    assert calls[:2] == [LOOKUP, SECRET]
    assert calls[2][:2] == ["create", "-k"]
    assert not Path(calls[2][2]).exists()
    assert calls[3:] == [
        ["rollout", "status", deployment, "--timeout=20m"] for deployment in DEPLOYMENTS
    ]
    assert rendered["images"] == [{
        "name": "kosha", "newName": "registry.example:5000/kosha", "newTag": "1.2.3",
    }]
    config, account, namespace = [
        json.loads(patch["patch"]) for patch in rendered["patches"]
    ]
    assert config["data"] == {
        "KOSHA_S3_BUCKET": "test-bucket",
        "KOSHA_S3_PREFIX": "test-prefix/",
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    assert account["metadata"]["annotations"] == {
        "eks.amazonaws.com/role-arn": "arn:aws:iam::123456789012:role/kosha",
    }
    assert namespace["$patch"] == "delete"


@pytest.mark.parametrize("deployment", DEPLOYMENTS)
def test_refuses_to_upgrade_any_existing_role(kubectl, capsys, deployment):
    invoke, calls, responses, _ = kubectl
    responses[tuple(LOOKUP)] = deployment + "\n"
    assert invoke(*INSTALL) == 1
    assert calls == [LOOKUP]
    assert "Old readers cannot read new manifests" in capsys.readouterr().err


@pytest.mark.parametrize("check", [LOOKUP, SECRET])
def test_preflight_failure_does_not_create_resources(kubectl, check):
    invoke, calls, responses, _ = kubectl
    responses[tuple(check)] = 1
    assert invoke(*INSTALL) == 1
    assert calls == ([LOOKUP] if check == LOOKUP else [LOOKUP, SECRET])


def test_requires_empty_storage_confirmation(kubectl):
    invoke, calls, _, _ = kubectl
    with pytest.raises(SystemExit) as exc:
        invoke(*INSTALL[:-1])
    assert exc.value.code == 2
    assert not calls


def test_status_is_read_only(kubectl):
    invoke, calls, _, _ = kubectl
    assert invoke("status") == 0
    assert calls == [["get", *DEPLOYMENTS, "-o", "wide"]]
