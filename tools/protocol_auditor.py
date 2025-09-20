import sys, json, pandas as pd

RULES = {
    "one_row_per_second": True,
    "monotonic_increasing": True,
}

def main():
    if len(sys.argv)<2:
        print("Usage: python tools/protocol_auditor.py <merged.parquet>", file=sys.stderr)
        sys.exit(2)
    path = sys.argv[1]
    df = pd.read_parquet(path)
    # Required column
    if "timestamp" not in df.columns:
        print("FAIL: 'timestamp' column missing", file=sys.stderr); sys.exit(3)
    # Monotonic increasing
    if RULES["monotonic_increasing"] and not df["timestamp"].is_monotonic_increasing:
        print("FAIL: timestamps not monotonic increasing", file=sys.stderr); sys.exit(3)
    # One row per second
    if RULES["one_row_per_second"] and df["timestamp"].duplicated().any():
        print("FAIL: duplicate timestamps -> not exactly one row per second", file=sys.stderr); sys.exit(3)
    # No NaN anywhere
    if df.isna().any().any():
        cols = df.columns[df.isna().any()].tolist()
        print(f"FAIL: NaN present in columns: {cols}", file=sys.stderr); sys.exit(3)
    print("OK: protocol auditor passed")
    sys.exit(0)

if __name__ == "__main__":
    main()
