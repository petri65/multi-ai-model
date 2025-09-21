import os
from typing import Union
import pandas as pd


def _ensure_df(obj: Union[str, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(obj, str):
        return pd.read_parquet(obj)
    return obj.copy()


def _looks_like_path(val: Union[str, os.PathLike]) -> bool:
    s = str(val)
    return s.endswith(".parquet") or ("/" in s) or ("\\" in s)

def merge_on_off(
    quant_off: Union[str, pd.DataFrame],
    quant_on: Union[str, pd.DataFrame],
    quant_whales: Union[str, pd.DataFrame, None] = None,
    ts_col: str = "timestamp",
    out_path: Union[str, os.PathLike, None] = None,
) -> pd.DataFrame:
    """Strict inner-join of off-chain/on-chain/whale frames on identical 1s timestamps.

    Parameters
    ----------
    quant_off, quant_on
        Either DataFrames or paths to parquet files already quantized to the
        1-second grid.
    quant_whales
        Optional third frame containing whale aggregates to merge on the same
        timestamp key. May be ``None``.
    ts_col
        Timestamp column name. Defaults to ``"timestamp"``.
    out_path
        Optional path to persist the merged result.

    Notes
    -----
    Backward compatible with the previous call signature where the 3rd
    positional argument was ``out_path``.
    """
    # Back-compat: third positional argument may still be the output path.
    if out_path is None and ts_col == "timestamp" and quant_whales is not None and _looks_like_path(quant_whales):
        out_path = str(quant_whales)
        quant_whales = None

    # Back-compat with tests that pass a path as the 3rd positional arg
    if out_path is None and _looks_like_path(ts_col):
        out_path = str(ts_col)
        ts_col = "timestamp"

    off_df = _ensure_df(quant_off)
    on_df = _ensure_df(quant_on)
    whale_df = _ensure_df(quant_whales) if quant_whales is not None else None

    if ts_col not in off_df.columns:
        raise ValueError(f"off-chain frame missing '{ts_col}'")
    if ts_col not in on_df.columns:
        raise ValueError(f"on-chain frame missing '{ts_col}'")
    if whale_df is not None and ts_col not in whale_df.columns:
        raise ValueError(f"whale frame missing '{ts_col}'")

    merged = pd.merge(off_df, on_df, on=ts_col, how="inner", suffixes=("_off", "_on"))

    if whale_df is not None:
        whale_df = whale_df.drop_duplicates(subset=[ts_col], keep="last")
        whale_df = whale_df.sort_values(ts_col)
        merged = pd.merge(merged, whale_df, on=ts_col, how="left")
        whale_cols = [c for c in whale_df.columns if c != ts_col]
        for col in whale_cols:
            if col.startswith("whale_"):
                merged[col] = merged[col].fillna(0.0)
        if "threshold_ltc_effective" in whale_cols:
            merged["threshold_ltc_effective"] = merged["threshold_ltc_effective"].ffill()
            merged["threshold_ltc_effective"] = merged["threshold_ltc_effective"].fillna(0.0)

    if merged.isna().any().any():
        bad = merged.columns[merged.isna().any()].tolist()
        raise ValueError(f"Merged frame contains NaN in columns: {bad}")

    merged = merged.sort_values(ts_col).drop_duplicates(subset=[ts_col], keep="last").reset_index(drop=True)

    if out_path:
        os.makedirs(os.path.dirname(str(out_path)) or ".", exist_ok=True)
        merged.to_parquet(str(out_path), index=False)

    return merged


def run(**kwargs) -> pd.DataFrame:
    """Convenience entrypoint mirroring the orchestrator's expectations."""
    return merge_on_off(**kwargs)
