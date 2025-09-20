import os
from typing import Union
import pandas as pd
from pathlib import Path

def _ensure_df(obj: Union[str, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(obj, str):
        return pd.read_parquet(obj)
    return obj.copy()

def _looks_like_path(val: Union[str, os.PathLike]) -> bool:
    s = str(val)
    return s.endswith(".parquet") or ("/" in s) or ("\\" in s)

def merge_on_off(quant_off: Union[str, pd.DataFrame],
                 quant_on: Union[str, pd.DataFrame],
                 ts_col: str = "timestamp",
                 out_path: Union[str, os.PathLike, None] = None) -> pd.DataFrame:
    """
    Strict inner-join on identical 1-second timestamps. Errors if NaNs remain.
    - Accepts DataFrames or parquet paths.
    - Backward-compatible: if ts_col looks like a path and out_path is None,
      treat ts_col as out_path and use ts_col="timestamp".
    """
    # Back-compat with tests that pass a path as the 3rd positional arg
    if out_path is None and _looks_like_path(ts_col):
        out_path = str(ts_col)
        ts_col = "timestamp"

    off_df = _ensure_df(quant_off)
    on_df  = _ensure_df(quant_on)

    if ts_col not in off_df.columns:
        raise ValueError(f"off-chain frame missing '{ts_col}'")
    if ts_col not in on_df.columns:
        raise ValueError(f"on-chain frame missing '{ts_col}'")

    merged = pd.merge(off_df, on_df, on=ts_col, how="inner", suffixes=("_off", "_on"))

    if merged.isna().any().any():
        bad = merged.columns[merged.isna().any()].tolist()
        raise ValueError(f"Merged frame contains NaN in columns: {bad}")

    merged = merged.sort_values(ts_col).drop_duplicates(subset=[ts_col], keep="last").reset_index(drop=True)

    if out_path:
        os.makedirs(os.path.dirname(str(out_path)) or ".", exist_ok=True)
        merged.to_parquet(str(out_path), index=False)

    return merged
