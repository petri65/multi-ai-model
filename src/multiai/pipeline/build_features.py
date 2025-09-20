import pandas as pd
import numpy as np

def run_build_features(merged_path: str, out_path: str, price_col: str = "trade_price", verbose: bool=False):
    """
    Build model-ready features from the merged on/off-chain Parquet.
    - Ensures a primary price column (defaults to 'trade_price' per Project Plan).
    - Computes 1-second simple returns `ret_1s` from the chosen price.
    - Adds convenience `spread_l1` if best bid/ask are present.
    - Drops any residual object-typed columns (all lists must have been split earlier).
    """
    df = pd.read_parquet(merged_path)
    if "timestamp" not in df.columns:
        raise ValueError("timestamp column missing in merged file")
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Ensure primary price column per Project Plan: prefer 'trade_price', fallback to mid_price, else synthesize from best bid/ask.
    if price_col not in df.columns:
        if "mid_price" in df.columns:
            if verbose: print(f"[features] '{price_col}' missing; using 'mid_price' as fallback.")
            df[price_col] = df["mid_price"]
        elif "best_bid" in df.columns and "best_ask" in df.columns:
            if verbose: print(f"[features] '{price_col}' missing; synthesizing from best_bid/best_ask.")
            df[price_col] = (df["best_bid"] + df["best_ask"]) / 2.0
        else:
            raise ValueError(f"Price column '{price_col}' not found and cannot be synthesized from best_bid/best_ask.")

    # 1-second return used by backtest gate
    df["ret_1s"] = df[price_col].pct_change().fillna(0.0)

    # Convenience: L1 spread if available
    if "best_ask" in df.columns and "best_bid" in df.columns:
        df["spread_l1"] = df["best_ask"] - df["best_bid"]

    # Enforce: no object dtypes for model input
    obj_cols = [c for c in df.columns if df[c].dtype == 'object']
    if obj_cols:
        if verbose: print(f"[features] dropping object-typed columns (lists should be split): {obj_cols}")
        df = df.drop(columns=obj_cols)

    df.to_parquet(out_path, index=False)
    if verbose:
        print(f"[features] → {out_path} rows={len(df)} cols={len(df.columns)}")
