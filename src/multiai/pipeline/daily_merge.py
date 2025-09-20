import glob
import os
import pandas as pd

from multiai.dataops.quantize import quantize_to_1s
from multiai.dataops.split_object_columns import split_object_columns_if_present
from multiai.dataops.merge_on_off import merge_on_off

OBJECT_COLS = [
    "orderbook_bid", "orderbook_ask",
    "bid_depth", "ask_depth",
    "spreads", "mid_prices",
]

def _read_concat_parquets(folder: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files in {folder}")
    dfs = [pd.read_parquet(p) for p in files]
    return pd.concat(dfs, ignore_index=True)

def run_daily_merge(off_dir: str, on_dir: str, out_path: str, verbose=False):
    if verbose:
        print(f"[daily-merge] OFF: {off_dir} | ON: {on_dir} -> {out_path}")
    off = _read_concat_parquets(off_dir)
    on  = _read_concat_parquets(on_dir)

    off_q = quantize_to_1s(off, ts_col="timestamp")
    on_q  = quantize_to_1s(on,  ts_col="timestamp")

    off_q = split_object_columns_if_present(off_q, OBJECT_COLS)
    on_q  = split_object_columns_if_present(on_q,  OBJECT_COLS)

    merged = merge_on_off(off_q, on_q, ts_col="timestamp")

    if not merged["timestamp"].is_monotonic_increasing:
        merged = merged.sort_values("timestamp").reset_index(drop=True)
    merged = merged.drop_duplicates(subset=["timestamp"], keep="last")

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    merged.to_parquet(out_path, index=False)
    if verbose:
        print(f"[daily-merge] wrote {out_path} | rows={len(merged)}")
