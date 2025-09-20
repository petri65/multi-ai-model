import os
import pandas as pd

def run_predict(features_path: str, model_dir: str, out_path: str, head_h: int = 60, costs_bps: float = 20.0, verbose=False):
    df = pd.read_parquet(features_path).copy()
    df[f"pred_mu_h{head_h}"] = 0.0
    df[f"pred_sigma_h{head_h}"] = 0.01
    df[f"kelly_weight_h{head_h}"] = 0.0
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df.to_parquet(out_path, index=False)
    if verbose:
        print(f"[predict] -> {out_path} rows={len(df)}")
