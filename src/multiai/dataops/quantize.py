import pandas as pd
from typing import Union

def _ensure_df(obj: Union[str, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(obj, str):
        return pd.read_parquet(obj)
    return obj.copy()

def quantize_to_1s(df_or_path: Union[str, pd.DataFrame], ts_col: str = "timestamp") -> pd.DataFrame:
    """Forward-round timestamps to next whole second and keep only the latest row per 1s bin."""
    df = _ensure_df(df_or_path)
    if ts_col not in df.columns:
        raise ValueError(f"Timestamp column '{ts_col}' not found")

    ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    if ts.isna().any():
        raise ValueError("Found unparsable timestamps")

    ns = ts.astype("int64")
    sec = 1_000_000_000
    ceil_ns = ((ns // sec) + (ns % sec > 0)) * sec
    qsec = pd.to_datetime(ceil_ns, unit="ns", utc=True).dt.tz_convert(None)

    df = df.copy()
    df["_qsec"] = qsec
    df["_orig_ts"] = ts.dt.tz_convert(None)
    df = df.sort_values(["_qsec", "_orig_ts"])
    dedup = df.drop_duplicates(subset=["_qsec"], keep="last")

    # Drop original timestamp and replace with quantized
    dedup = dedup.drop(columns=[ts_col, "_orig_ts"])
    dedup = dedup.rename(columns={"_qsec": ts_col})
    dedup = dedup.sort_values(ts_col).reset_index(drop=True)
    return dedup
