import pandas as pd
from pathlib import Path
def quantize_to_1s(path_in: str|Path) -> pd.DataFrame:
    df = pd.read_parquet(path_in)
    ts = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    buckets = ts.dt.ceil('s')
    df = df.assign(_orig_ts=ts, _bucket=buckets)
    df = df.sort_values('_orig_ts').groupby('_bucket', as_index=False).tail(1)
    df = df.drop(columns=['timestamp','_orig_ts']).rename(columns={'_bucket':'timestamp'})
    return df.sort_values('timestamp').reset_index(drop=True)
