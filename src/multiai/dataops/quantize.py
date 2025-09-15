from __future__ import annotations
import pandas as pd
from pathlib import Path

def quantize_to_1s(path_in: str | Path) -> pd.DataFrame:
    df = pd.read_parquet(path_in)

    if 'timestamp' not in df.columns:
        for alt in ['ts', 'time', 'datetime']:
            if alt in df.columns:
                df = df.rename(columns={alt: 'timestamp'})
                break

    ts = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    df = df.assign(_orig_ts=ts)

    target = ts.dt.ceil('s')
    df = df.assign(_target_ts=target)

    df = df.sort_values('_orig_ts').groupby('_target_ts', as_index=False).tail(1)

    keep_cols = [c for c in df.columns if c not in ('timestamp','_orig_ts','_target_ts')]
    df = df[keep_cols + ['_target_ts']].rename(columns={'_target_ts': 'timestamp'})

    df = df.sort_values('timestamp').reset_index(drop=True)
    return df
