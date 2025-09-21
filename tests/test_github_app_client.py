from pathlib import Path

import pytest

from multiai.orchestrator import gateway


class DummyLeaseManager:
    def acquire(self, shards, holder, **_: object):  # pragma: no cover - simple helper
        return {s: object() for s in shards}

    def renew(self, *args: object, **kwargs: object) -> None:  # pragma: no cover
        return None

    def release(self, *args: object, **kwargs: object) -> None:  # pragma: no cover
        return None


def _fake_attestation(tmp_path: Path):
    out = tmp_path / "attestation.json"
    out.write_text("{}", encoding="utf-8")
    return str(out)


def test_open_pr_invokes_push_branch(monkeypatch, tmp_path):
    calls = []

    def fake_push(branch, **kwargs):
        calls.append((branch, kwargs))

    monkeypatch.setattr(gateway, "github_app", type("Stub", (), {"push_branch": staticmethod(fake_push)}))
    monkeypatch.setattr(
        gateway,
        "attest",
        type("AttestStub", (), {"write_attestation": staticmethod(lambda *a, **k: _fake_attestation(tmp_path))}),
    )

    orch = gateway.Orchestrator(lease_manager=DummyLeaseManager())
    cp = gateway.ChangeProposal(
        job_id="job-1",
        shards=["alpha"],
        title="Add new metrics",
        prompt="describe the change",
        description="Automated proposal body",
        diff_paths=["README.md"],
    )

    orch.prepare(cp)
    orch.open_pr()

    assert calls, "push_branch was not invoked"
    branch, kwargs = calls[0]
    assert branch == "ai/job-1"
    assert Path(kwargs["attestation_path"]).name == "attestation.json"
    assert kwargs["title"] == cp.title
    assert kwargs["body"] == cp.description


def test_open_pr_surfaces_push_failure(monkeypatch, tmp_path):
    class Boom(Exception):
        pass

    def fake_push(*args, **kwargs):  # pragma: no cover - behaviour tested via exception propagation
        raise Boom("push failed")

    monkeypatch.setattr(gateway, "github_app", type("Stub", (), {"push_branch": staticmethod(fake_push)}))
    monkeypatch.setattr(
        gateway,
        "attest",
        type("AttestStub", (), {"write_attestation": staticmethod(lambda *a, **k: _fake_attestation(tmp_path))}),
    )

    orch = gateway.Orchestrator(lease_manager=DummyLeaseManager())
    cp = gateway.ChangeProposal(
        job_id="job-2",
        shards=["beta"],
        title="Failure propagation",
        prompt="trigger failure",
    )

    orch.prepare(cp)
    with pytest.raises(Boom):
        orch.open_pr()
