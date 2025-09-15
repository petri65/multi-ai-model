from __future__ import annotations
import pandas as pd
from pathlib import Path

def merge_on_off(on_df: pd.DataFrame, off_df: pd.DataFrame, out_path: str | Path) -> None:
    for name, df in [('on_chain', on_df), ('off_chain', off_df)]:
        if 'timestamp' not in df.columns:
            raise ValueError(f"{name} dataframe missing 'timestamp'")
    # Ensure timestamp is datetime tz-aware
    on_df = on_df.copy()
    off_df = off_df.copy()
    on_df['timestamp'] = pd.to_datetime(on_df['timestamp'], utc=True)
    off_df['timestamp'] = pd.to_datetime(off_df['timestamp'], utc=True)
    # Left merge on timestamp; off-chain on the left by convention
    merged = off_df.merge(on_df, on='timestamp', how='inner', suffixes=('_off', '_on'))
    # Validations
    if merged['timestamp'].isna().any():
        raise ValueError("NaN timestamps after merge")
    if not merged['timestamp'].is_monotonic_increasing:
        merged = merged.sort_values('timestamp').reset_index(drop=True)
    if merged.isna().any().any():
        # Hard requirement from protocol: no empty cells
        raise ValueError("Merge produced empty cells; check quantization windows and coverage")
    merged.to_parquet(out_path, index=False)
