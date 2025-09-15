from __future__ import annotations
import pandas as pd
import json

LIST_COLS = ['orderbook_bid', 'orderbook_ask', 'mid_prices', 'spreads']
LEVELS = list(range(1, 10))  # 9 levels

def _coerce_listlike(v):
    if isinstance(v, (list, tuple)):
        return list(v)
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            # fallback: split by comma
            return [x.strip() for x in v.split(',') if x.strip()]
    return []

def split_object_columns_if_present(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in LIST_COLS:
        if col in out.columns:
            series = out[col].apply(_coerce_listlike)
            for i, lvl in enumerate(LEVELS):
                new_col = f"{col}_{lvl}"
                out[new_col] = series.apply(lambda lst: lst[i] if i < len(lst) else None)
            out = out.drop(columns=[col])
    return out
