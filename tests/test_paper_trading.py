import os

import numpy as np
import pandas as pd
import pytest

from multiai.paper_trading import PaperTradingSession, SessionConfig, run as run_session
from multiai.tools.combiner import combine_allocations


def make_predictions(timestamps, mu_values, sigma_values):
    data = {"timestamp": timestamps}
    for idx, (mu, sigma) in enumerate(zip(mu_values, sigma_values), start=1):
        horizon = idx * 30
        data[f"pred_mu_h{horizon}"] = mu
        data[f"pred_sigma_h{horizon}"] = sigma
    return pd.DataFrame(data)


def test_paper_trading_duration_and_capital(tmp_path):
    timestamps = pd.date_range("2025-01-01", periods=5, freq="s", tz="UTC")
    mu = [np.zeros(len(timestamps))]
    sigmas = [np.full(len(timestamps), 0.01)]
    preds = make_predictions(timestamps, mu, sigmas)
    market = pd.DataFrame({"timestamp": timestamps, "trade_price": np.linspace(100, 101, len(timestamps))})

    cfg = SessionConfig(duration_seconds=3, initial_capital=5_000.0, exposure_cap=0.0, hysteresis=0.0, stop_loss=0.1, take_profit=0.1)
    session = PaperTradingSession(cfg)
    logs_df, alerts_df = session.run(preds, market, price_col="trade_price")

    assert len(logs_df) == 3, "session should respect duration limit"
    assert pytest.approx(logs_df["equity"].iloc[0], rel=1e-6) == cfg.initial_capital
    assert pytest.approx(logs_df["cash"].iloc[0], rel=1e-6) == cfg.initial_capital
    assert logs_df["equity"].min() > cfg.initial_capital * 0.9, "capital should stay near initial level"
    assert alerts_df.empty


def test_paper_trading_weighted_kelly(tmp_path):
    timestamps = pd.date_range("2025-01-02", periods=2, freq="s", tz="UTC")
    mu = [np.full(len(timestamps), 0.01), np.full(len(timestamps), 0.015)]
    sigmas = [np.full(len(timestamps), 0.02), np.full(len(timestamps), 0.03)]
    preds = make_predictions(timestamps, mu, sigmas)
    market = pd.DataFrame({"timestamp": timestamps, "trade_price": np.full(len(timestamps), 100.0)})

    cfg = SessionConfig(duration_seconds=1, exposure_cap=0.3, hysteresis=0.0, stop_loss=0.5, take_profit=0.5, cost_bps_per_leg=0.0)
    session = PaperTradingSession(cfg)
    logs_df, _ = session.run(preds, market, price_col="trade_price")

    first_row = logs_df.iloc[0]
    weights = np.array([first_row["kelly_weight_h30"], first_row["kelly_weight_h60"]], dtype=float)
    vol = np.array([preds.loc[0, "pred_sigma_h30"], preds.loc[0, "pred_sigma_h60"]], dtype=float)
    expected = combine_allocations(weights, vol, cap=cfg.exposure_cap)
    assert pytest.approx(first_row["kelly_weighted"], rel=1e-6) == expected
    assert pytest.approx(first_row["allocation_fraction"], rel=1e-6) == expected


def test_paper_trading_run_persists_artifacts(tmp_path):
    timestamps = pd.date_range("2025-01-03", periods=4, freq="s", tz="UTC")
    preds = pd.DataFrame({
        "timestamp": timestamps,
        "pred_mu_h30": 0.0,
        "pred_sigma_h30": 0.02,
    })
    market = pd.DataFrame({"timestamp": timestamps, "trade_price": 100 + np.arange(len(timestamps))})

    pred_path = tmp_path / "predictions.parquet"
    market_path = tmp_path / "market.parquet"
    preds.to_parquet(pred_path, index=False)
    market.to_parquet(market_path, index=False)

    out_dir = tmp_path / "paper"
    cfg = SessionConfig(duration_seconds=2, initial_capital=1_000.0)
    result = run_session(str(pred_path), str(market_path), out_dir=str(out_dir), config=cfg)

    assert os.path.exists(result.log_path)
    assert os.path.exists(result.equity_path)
    assert os.path.exists(result.alerts_path)

    log_df = pd.read_parquet(result.log_path)
    assert len(log_df) == 2
    equity_df = pd.read_parquet(result.equity_path)
    assert list(equity_df.columns) == ["timestamp", "equity"]
    alerts_df = pd.read_parquet(result.alerts_path)
    assert "timestamp" in alerts_df.columns and "type" in alerts_df.columns
