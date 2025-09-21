import os, json
from math import erf, sqrt
import numpy as np
import pandas as pd
import torch
from multiai.pipeline.train_bayes_lstm import MCDropoutLSTM, resolve_device
from multiai.tools.kelly import kelly_optimal_fraction_gaussian
from multiai.tools.combiner import combine_allocations

def _mc_predict_full(model, xb, mc_samples: int):
    model.train()
    mus = []
    vars_ = []
    with torch.no_grad():
        for _ in range(mc_samples):
            mu, logvar = model(xb)
            mus.append(mu.detach().cpu().numpy())
            vars_.append(torch.exp(logvar).detach().cpu().numpy())
    mus = np.stack(mus, axis=0)        # [S,B,H]
    vars_ = np.stack(vars_, axis=0)    # [S,B,H]
    mu_hat = mus.mean(axis=0)          # [B,H]
    var_ep = mus.var(axis=0, ddof=1)   # [B,H]
    var_al = vars_.mean(axis=0)        # [B,H]
    sigma_hat = np.sqrt(var_ep + var_al)
    return mu_hat, sigma_hat

def run_predict_bayes(features_path: str, model_dir: str, out_path: str,
                      seq_len: int, mc_samples: int, device: str,
                      cost_bps_per_leg: float, sl: float, tp: float,
                      verbose: bool=False, kelly_cap: float=0.2, sigma_scale: float=1.0, combine: bool=False) -> None:
    device = resolve_device(device)
    meta = json.load(open(os.path.join(model_dir, "meta.json"), "r"))
    in_dim = meta["in_dim"]; out_dim = meta["out_dim"]; horizons = meta["horizons"]

    model = MCDropoutLSTM(in_dim=in_dim, hidden=96, num_layers=1, dropout=0.2, out_dim=out_dim)
    model.load_state_dict(torch.load(os.path.join(model_dir, "model.pt"), map_location=device))
    model.to(device)

    fdf = pd.read_parquet(features_path).sort_values("timestamp")
    X = fdf[meta["feature_columns"]].astype(float).to_numpy()
    n = len(X)
    if n <= seq_len:
        raise RuntimeError("Not enough data for prediction; need > seq_len rows.")

    batch = 1024
    rows = []
    for start in range(0, n - seq_len, batch):
        end = min(n - seq_len, start + batch)
        xs = np.stack([X[i:i+seq_len] for i in range(start, end)], axis=0).astype(np.float32, copy=False)
        xb = torch.tensor(xs, dtype=torch.float32, device=device)

        mu_hat, sigma_hat = _mc_predict_full(model, xb, mc_samples)
        sigma_hat = sigma_hat * float(sigma_scale)
        B, H = mu_hat.shape
        out_idx = fdf.index[seq_len+start:seq_len+start+B]
        base = pd.DataFrame({"timestamp": fdf.loc[out_idx, "timestamp"].values})

        for j, h in enumerate(horizons[:H]):
            mu = mu_hat[:, j]
            sig = sigma_hat[:, j]
            f_star = np.zeros(B); G_star = np.zeros(B); f_gauss = np.zeros(B)
            prob_up = np.zeros(B)
            prob_down = np.zeros(B)
            for i in range(B):
                f, G, fg = kelly_optimal_fraction_gaussian(
                    float(mu[i]), float(sig[i]), cost_bps_per_leg, sl, tp, f_cap=1.0
                )
                f_star[i] = np.clip(f, -abs(kelly_cap), abs(kelly_cap))
                G_star[i] = G
                f_gauss[i] = fg
                if sig[i] <= 1e-12:
                    prob_down[i] = 0.5
                    prob_up[i] = 0.5
                else:
                    z = (0.0 - float(mu[i])) / (float(sig[i]) * sqrt(2.0))
                    cdf0 = 0.5 * (1.0 + erf(z))
                    cdf0 = min(max(cdf0, 0.0), 1.0)
                    prob_down[i] = cdf0
                    prob_up[i] = 1.0 - cdf0
            base[f"pred_mu_h{h}"] = mu
            base[f"pred_sigma_h{h}"] = sig
            base[f"kelly_weight_h{h}"] = f_star
            base[f"kelly_G_h{h}"] = G_star
            base[f"kelly_fgauss_h{h}"] = f_gauss
            base[f"prob_up_h{h}"] = prob_up
            base[f"prob_down_h{h}"] = prob_down
            base[f"kelly_integral_h{h}"] = G_star

        fcols = [c for c in base.columns if c.startswith('kelly_weight_h')]
        if fcols:
            F = base[fcols].to_numpy()
            S = base[[c.replace('kelly_weight', 'pred_sigma') for c in fcols]].to_numpy()
            comb = [combine_allocations(F[i], S[i], cap=kelly_cap) for i in range(len(base))]
            base['kelly_weighted'] = comb
            if combine:
                base['kelly_alloc'] = comb
        rows.append(base)
        if verbose:
            print(f"[predict-bayes] chunk {start}-{end} rows={B} device={device}")

    out = pd.concat(rows, ignore_index=True)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    out.to_parquet(out_path, index=False)
    if verbose:
        print(f"[predict-bayes] -> {out_path} rows={len(out)}")
