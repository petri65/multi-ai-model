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
    off_df = pd.DataFrame({
        "timestamp": timestamps,
        "trade_price": price,
        "best_bid": price - 0.01,
        "best_ask": price + 0.01,
        "volume": rng.lognormal(mean=0.0, sigma=0.1, size=periods),
    })
    off_q1s_path = tmp_path / "off_chain_q1s.parquet"
    off_split_path = tmp_path / "off_chain_q1s_split.parquet"
    off_df.to_parquet(off_q1s_path, index=False)
    off_df.to_parquet(off_split_path, index=False)
    st.set_artifact("off_chain_q1s", str(off_q1s_path))
    st.set_artifact("off_chain_q1s_split", str(off_split_path))

    on_df = pd.DataFrame({
        "timestamp": timestamps,
        "onchain_tx_count": rng.poisson(5, size=periods),
        "onchain_avg_fee": rng.normal(0.05, 0.005, size=periods),
    })
    on_q1s_path = tmp_path / "on_chain_q1s.parquet"
    on_df.to_parquet(on_q1s_path, index=False)
    st.set_artifact("on_chain_q1s", str(on_q1s_path))

    whale_counts = rng.integers(0, 3, size=periods)
    whale_totals = rng.uniform(0.0, 200.0, size=periods)
    whale_avg = np.where(whale_counts > 0, whale_totals / whale_counts, 0.0)
    whales_df = pd.DataFrame({
        "timestamp": timestamps,
        "whale_tx_count_10m": whale_counts,
        "whale_total_value_ltc_10m": whale_totals,
        "whale_avg_value_ltc_10m": whale_avg,
        "whale_max_value_ltc_10m": np.maximum(whale_totals * 0.6, whale_avg),
        "whale_topN_sum_ltc_10m": whale_totals * 0.8,
    })
    whales_path = tmp_path / "whale_metrics_q1s.parquet"
    whales_df.to_parquet(whales_path, index=False)
    st.set_artifact("whale_metrics_q1s", str(whales_path))
    st.set_artifact("whale_metrics_q1s_split", str(whales_path))

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
    assert "data.merge_on_offchain" in processed
    assert "merged" in final_state
    merged_path = final_state["merged"]
    assert os.path.exists(merged_path)
    merged_df = pd.read_parquet(merged_path)
    assert "whale_tx_count_10m" in merged_df.columns

    expected_keys = [
        "merged",
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
