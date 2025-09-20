import json
import pandas as pd
from typing import List, Optional, Union

_DEFAULT_OBJECT_COLS = [
    "orderbook_bid", "orderbook_ask",
    "bid_depth", "ask_depth",
    "spreads", "mid_prices",
]

def split_object_columns_if_present(
    df: pd.DataFrame,
    object_cols: Optional[Union[str, List[str]]] = None
) -> pd.DataFrame:
    """
    Expand list/JSON-like columns to scalar columns; forward-fill and drop residual gaps.
    - object_cols can be None (use defaults), a single column name, or a list of names.
    - Original object column is dropped after expansion.
    - Guarantees: no new NaNs in the expanded columns.
    """
    out = df.copy()

    if object_cols is None:
        cols_to_process = [c for c in _DEFAULT_OBJECT_COLS if c in out.columns]
    elif isinstance(object_cols, str):
        cols_to_process = [object_cols] if object_cols in out.columns else []
    else:
        cols_to_process = [c for c in object_cols if c in out.columns]

    for col in cols_to_process:
        series = out[col]

        def to_list(v):
            if isinstance(v, list):
                return v
            if isinstance(v, str):
                try:
                    j = json.loads(v)
                    if isinstance(j, list):
                        return j
                except Exception:
                    return None
            return None

        lists = series.apply(to_list)
        if not lists.notna().any():
            out = out.drop(columns=[col])
            continue

        lengths = lists.dropna().apply(len)
        if lengths.empty:
            out = out.drop(columns=[col])
            continue

        target_len = int(lengths.mode().iloc[0])
        new_cols = []
        for i in range(target_len):
            name = f"{col}_{i+1}"
            out[name] = lists.apply(lambda v: (v[i] if isinstance(v, list) and len(v) > i else None))
            new_cols.append(name)

        out[new_cols] = out[new_cols].ffill()
        out = out.dropna(subset=new_cols)

        # drop original column after expansion
        out = out.drop(columns=[col])

    return out.reset_index(drop=True)
