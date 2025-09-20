import pandas as pd
import numpy as np

def run_build_targets(merged_path: str, out_path: str, price_col: str = "trade_price",
                      horizons=(10,30,60,90,120,240), verbose: bool=False):
    """
    Create forward-looking log-return targets from the given price column.
    Targets: target_ret_{H}s for H in horizons.
    Rows that cannot compute all horizons are dropped (causality-safe).
    """
    df = pd.read_parquet(merged_path)
    if "timestamp" not in df.columns:
        raise ValueError("timestamp column missing in merged file")
    if price_col not in df.columns:
        if "mid_price" in df.columns:
            if verbose: print(f"[targets] '{price_col}' missing; using 'mid_price' as fallback.")
            df[price_col] = df["mid_price"]
        elif "best_bid" in df.columns and "best_ask" in df.columns:
            if verbose: print(f"[targets] '{price_col}' missing; synthesizing from best_bid/best_ask.")
            df[price_col] = (df["best_bid"] + df["best_ask"]) / 2.0
        else:
            raise ValueError(f"Price column '{price_col}' not found and cannot be synthesized from best_bid/best_ask.")

    df = df.sort_values("timestamp").reset_index(drop=True)
    p = df[price_col].astype(float)

    for h in horizons:
        # forward log-return over h seconds
        df[f"target_ret_{h}s"] = np.log(p.shift(-h) / p)

    # drop rows with any NaN target (strictly causal; no peeking)
    target_cols = [f"target_ret_{h}s" for h in horizons]
    out = df[["timestamp"] + target_cols].dropna().reset_index(drop=True)
    out.to_parquet(out_path, index=False)
    if verbose:
        print(f"[targets] → {out_path} rows={len(out)} cols={len(out.columns)}")
