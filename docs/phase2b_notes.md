# Phase-2b: Align targets & features with Project Plan

- **Targets** now built from `trade_price` by default (`build_targets.py`), creating `target_ret_{H}s` for H∈{10,30,60,90,120,240}.
- **Features** default primary price is `trade_price`; if missing, fallback to `mid_price`, else synthesize from `best_bid`/`best_ask`.
- CLI gains:
  - `multiai run build-features --merged merged.parquet --out features.parquet`
  - `multiai run build-targets --merged merged.parquet --out targets.parquet`
  - `multiai run train-bayes ...` (Bayesian MC dropout LSTM training)
  - `multiai run predict-bayes ...` (Bayesian MC dropout LSTM inference + Kelly sizing)

This follows the Project Plan sections on post-processing, horizons, and trading logic.
