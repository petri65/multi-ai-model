import sys, yaml, pandas as pd
import numpy as np

def simple_backtest(df, ret_col="ret_1s", weight_col="kelly_weight_h60"):
    # naive equity curve
    r = df[ret_col].fillna(0.0).values * df.get(weight_col, 0.0).fillna(0.0).values
    equity = (1.0 + r).cumprod()
    pf = (r[r>0].sum() / max(1e-9, -r[r<0].sum())) if (r[r<0].sum()!=0) else np.inf
    peak = np.maximum.accumulate(equity)
    mdd = float(((peak - equity)/peak).max()) if len(equity)>0 else 0.0
    return float(pf), float(mdd)

def main():
    if len(sys.argv)<3:
        print("Usage: python tools/backtest_gates.py <features.parquet> <preds.parquet>", file=sys.stderr)
        sys.exit(2)
    features_path, preds_path = sys.argv[1], sys.argv[2]
    with open("policies/gates.yml","r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    feats = pd.read_parquet(features_path)
    preds = pd.read_parquet(preds_path)
    rows_min = int(cfg["thresholds"]["rows_min"])
    if len(feats) < rows_min or len(preds) < rows_min:
        print(f"SKIP: rows<{rows_min}, not enough data for backtest gate"); sys.exit(0)

    df = feats.merge(preds, on="timestamp", how="inner")
    if "ret_1s" not in df.columns:
        print("FAIL: ret_1s missing from features", file=sys.stderr); sys.exit(3)

    pf, mdd = simple_backtest(df, "ret_1s", "kelly_weight_h60")
    if pf < float(cfg["thresholds"]["pf_min"]):
        print(f"FAIL: profit factor {pf:.3f} < {cfg['thresholds']['pf_min']}", file=sys.stderr); sys.exit(3)
    if mdd > float(cfg["thresholds"]["mdd_max"]):
        print(f"FAIL: max drawdown {mdd:.3f} > {cfg['thresholds']['mdd_max']}", file=sys.stderr); sys.exit(3)

    print(f"OK: backtest gates passed (PF={pf:.3f}, MDD={mdd:.3f})"); sys.exit(0)

if __name__ == "__main__":
    main()
