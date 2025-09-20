import sys, yaml, pandas as pd

def main():
    if len(sys.argv)<3:
        print("Usage: python tools/calibration_gates.py <features.parquet> <preds.parquet>", file=sys.stderr)
        sys.exit(2)
    features_path, preds_path = sys.argv[1], sys.argv[2]
    with open("policies/gates.yml","r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    preds = pd.read_parquet(preds_path)
    rows_min = int(cfg["thresholds"]["rows_min"])
    if len(preds) < rows_min:
        print(f"SKIP: rows<{rows_min}, not enough data for calibration gate"); sys.exit(0)

    req = cfg.get("required_pred_columns", [])
    if missing := [c for c in req if c not in preds.columns]:
        print(f"FAIL: missing prediction columns: {missing}", file=sys.stderr); sys.exit(3)

    # coverage check
    coverage = 1.0 - (preds[req].isna().sum().sum() / (len(preds)*len(req)))
    if coverage < float(cfg["thresholds"]["coverage_min"]):
        print(f"FAIL: coverage {coverage:.3f} < {cfg['thresholds']['coverage_min']}", file=sys.stderr); sys.exit(3)

    print("OK: calibration gates passed"); sys.exit(0)

if __name__ == "__main__":
    main()
