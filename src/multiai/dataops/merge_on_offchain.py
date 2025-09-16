import pandas as pd
from pathlib import Path
def merge_quantized(on_path: str|Path, off_path: str|Path) -> pd.DataFrame:
    on = pd.read_parquet(on_path)
    off = pd.read_parquet(off_path)
    on = on.rename(columns={'timestamp':'timestamp'}).set_index('timestamp')
    off = off.rename(columns={'timestamp':'timestamp'}).set_index('timestamp')
    merged = off.join(on, how='inner', lsuffix='_off', rsuffix='_on')
    merged = merged.reset_index().rename(columns={'index':'timestamp'})
    if merged.isna().any().any():
        raise ValueError("Empty cells after merge; inputs must be strictly quantized 1-second aligned.")
    return merged
