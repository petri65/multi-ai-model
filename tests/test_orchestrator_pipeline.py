import os
import json
import importlib

import numpy as np
import pandas as pd
import pytest

def test_orchestrator_pipeline_end_to_end(tmp_path, monkeypatch):
    pytest.importorskip("torch", reason="requires PyTorch for Bayesian LSTM pipeline")

    state_dir = tmp_path / "state"
    queue_path = tmp_path / "queue.jsonl"
    monkeypatch.setenv("MULTIAI_STATE_DIR", str(state_dir))
    monkeypatch.setenv("MULTIAI_QUEUE_PATH", str(queue_path))
    monkeypatch.chdir(tmp_path)

    from multiai.orchestrator import state as state_mod
    from multiai.orchestrator import queue as queue_mod
    from multiai.orchestrator import cli as cli_mod

    st = importlib.reload(state_mod)
    q = importlib.reload(queue_mod)
    cli = importlib.reload(cli_mod)

    rng = np.random.default_rng(42)
    periods = 400
    timestamps = pd.date_range("2025-01-01", periods=periods, freq="s", tz="UTC")
    price = 100 + np.cumsum(rng.normal(0, 0.05, size=periods))
    merged_df = pd.DataFrame({
        "timestamp": timestamps,
        "trade_price": price,
        "best_bid": price - 0.01,
        "best_ask": price + 0.01,
        "volume": rng.lognormal(mean=0.0, sigma=0.1, size=periods),
    })

    merged_path = tmp_path / "merged.parquet"
    merged_df.to_parquet(merged_path, index=False)
    st.set_artifact("merged", str(merged_path))

    for task_type, payload in cli.next_steps():
        q.enqueue(task_type, payload, queue_path=str(queue_path))

    processed = []
    while True:
        task = q.dequeue(queue_path=str(queue_path))
        if task is None:
            break
        assert cli.handle(task) is True
        processed.append(task["type"])

    final_state = st.load()
    expected_keys = [
        "with_targets",
        "with_features",
        "train_path",
        "test_path",
        "model_dir",
        "pred_path",
        "paper_trading_log",
        "paper_trading_equity",
        "paper_trading_alerts",
    ]
    for key in expected_keys:
        assert key in final_state, f"missing artifact {key}"
        path = final_state[key]
        if key == "model_dir":
            assert os.path.isdir(path)
            assert os.path.exists(os.path.join(path, "model.pt"))
            meta_path = os.path.join(path, "meta.json")
            assert os.path.exists(meta_path)
            with open(meta_path, "r", encoding="utf-8") as fh:
                meta = json.load(fh)
            assert "seq_len" in meta and meta["seq_len"] > 0
        else:
            assert os.path.exists(path)

    assert q.length(queue_path=str(queue_path)) == 0
    # Ensure predictions parquet has rows
    pred_df = pd.read_parquet(final_state["pred_path"])
    assert len(pred_df) > 0
