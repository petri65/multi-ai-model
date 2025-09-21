import importlib


def test_autonomy_orchestrator_reaches_git(monkeypatch, tmp_path):
    orchestrator = importlib.import_module("scripts.autonomy_orchestrator")
    orchestrator = importlib.reload(orchestrator)

    repo_root = tmp_path / "repo"
    off_dir = repo_root / "data" / "offchain"
    on_dir = repo_root / "data" / "onchain"
    out_dir = repo_root / "outputs"
    state_path = repo_root / ".autonomy_state.json"

    monkeypatch.setattr(orchestrator, "REPO", repo_root)
    monkeypatch.setattr(orchestrator, "OFF_DIR", off_dir)
    monkeypatch.setattr(orchestrator, "ON_DIR", on_dir)
    monkeypatch.setattr(orchestrator, "OUT_DIR", out_dir)
    monkeypatch.setattr(orchestrator, "STATE", state_path)
    monkeypatch.setattr(orchestrator, "ts", lambda: "20240101-000000")

    off_dir.mkdir(parents=True, exist_ok=True)
    on_dir.mkdir(parents=True, exist_ok=True)
    (off_dir / "off.parquet").touch()
    (on_dir / "on.parquet").touch()

    commands = []

    def fake_run(cmd, env=None, cwd=None):  # pragma: no cover - simple command capture
        commands.append(cmd)
        return ""

    monkeypatch.setattr(orchestrator, "run", fake_run)

    orchestrator.main()

    prefixes = [tuple(cmd[:3]) for cmd in commands if len(cmd) >= 3]
    expected = {
        ("multiai", "run", "daily-merge"),
        ("multiai", "run", "build-features"),
        ("multiai", "run", "build-targets"),
        ("multiai", "run", "train-bayes"),
        ("multiai", "run", "predict-bayes"),
    }

    for item in expected:
        assert item in prefixes, f"missing command prefix {item}"

    git_commands = [cmd for cmd in commands if cmd and cmd[0] == "git"]
    assert git_commands, "git commands were not reached"

    assert state_path.exists(), "state file was not written"

