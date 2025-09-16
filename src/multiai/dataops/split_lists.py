import pandas as pd
def explode_lists(df: pd.DataFrame) -> pd.DataFrame:
    def expand(col, base):
        if col not in df.columns: return pd.DataFrame()
        m = df[col].apply(lambda x: list(x) if isinstance(x,(list,tuple)) else [None]*9)
        return pd.DataFrame(m.tolist(), columns=[f"{base}_{i}" for i in range(1,10)])
    parts = [df.drop(columns=[c for c in ['orderbook_bid','orderbook_ask','mid_prices','spreads'] if c in df.columns], errors='ignore')]
    for base in ['orderbook_bid','orderbook_ask','mid_prices','spreads']:
        parts.append(expand(base, base))
    out = pd.concat(parts, axis=1)
    return out
