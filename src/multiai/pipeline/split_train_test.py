
import os
import pandas as pd

def _detect_ts(df):
    candidates = ["ts","timestamp","time","event_time","datetime"]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "time" in c or "ts" in c:
            return c
    return None

def run(in_path, out_train, out_test, ratio=0.8, split_timestamp=None):
    if not os.path.exists(in_path):
        raise FileNotFoundError(in_path)
    df = pd.read_parquet(in_path)
    ts_col = _detect_ts(df)
    if ts_col is None:
        raise RuntimeError("no timestamp-like column found")
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df = df.dropna(subset=[ts_col]).sort_values(ts_col).reset_index(drop=True)
    if split_timestamp is not None:
        split_ts = pd.to_datetime(split_timestamp, utc=True)
        train = df[df[ts_col] < split_ts]
        test = df[df[ts_col] >= split_ts]
    else:
        n = len(df)
        k = int(max(1, min(n-1, round(n * float(ratio)))))
        train = df.iloc[:k].copy()
        test = df.iloc[k:].copy()
    os.makedirs(os.path.dirname(out_train) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(out_test) or ".", exist_ok=True)
    train.to_parquet(out_train, index=False)
    test.to_parquet(out_test, index=False)

    try:
        from multiai.orchestrator import state as st
        st.set_artifact("train_path", out_train)
        st.set_artifact("test_path", out_test)
    except Exception:
        pass
    return {"train_rows": len(train), "test_rows": len(test), "ts_col": ts_col}
