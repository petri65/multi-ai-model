import os, json
from typing import Tuple
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

def resolve_device(requested: str) -> str:
    if requested == "auto":
        if torch.cuda.is_available():
            return "cuda"
        if torch.backends.mps.is_available():
            return "mps"
        return "cpu"
    return requested

class MCDropoutLSTM(nn.Module):
    def __init__(self, in_dim: int, hidden: int = 96, num_layers: int = 1, dropout: float = 0.2, out_dim: int = 6):
        super().__init__()
        self.lstm = nn.LSTM(input_size=in_dim, hidden_size=hidden, num_layers=num_layers,
                            batch_first=True, dropout=0.0)
        self.dropout = nn.Dropout(dropout)
        self.head = nn.Linear(hidden, 2 * out_dim)
        self.out_dim = out_dim
    def forward(self, x):
        y, _ = self.lstm(x)
        y = self.dropout(y[:, -1, :])
        y = self.head(y).view(-1, self.out_dim, 2)
        mu = y[..., 0]
        logvar = y[..., 1]
        return mu, logvar

class SeqDataset(Dataset):
    def __init__(self, X: np.ndarray, Y: np.ndarray, seq_len: int):
        self.X = X.astype(np.float32, copy=False)
        self.Y = Y.astype(np.float32, copy=False)
        self.seq_len = int(seq_len)
        self.N = len(X) - self.seq_len
        if self.N <= 0:
            raise RuntimeError("Not enough data for the chosen seq_len")
    def __len__(self):
        return self.N
    def __getitem__(self, i):
        j = i + self.seq_len
        xb = self.X[i:j]
        yb = self.Y[j]
        return torch.from_numpy(xb), torch.from_numpy(yb)

def run_train_bayes(features_path: str, targets_path: str, outdir: str,
                    seq_len: int, epochs: int, batch_size: int, lr: float,
                    device: str, verbose: bool=False) -> None:
    os.makedirs(outdir, exist_ok=True)
    device = resolve_device(device)
    if verbose:
        print(f"[train-bayes] device={device}")

    fdf = pd.read_parquet(features_path).sort_values("timestamp")
    tdf = pd.read_parquet(targets_path).sort_values("timestamp")

    if "timestamp" in fdf.columns and "timestamp" in tdf.columns:
        df = fdf.merge(tdf, on="timestamp", how="inner")
    else:
        df = fdf.join(tdf, how="inner")

    target_cols = [c for c in df.columns if c.startswith("target_ret_")]
    feat_df = df.drop(columns=[c for c in df.columns if c in target_cols or c=="timestamp"])
    feat_df = feat_df.select_dtypes(include=[np.number]).fillna(0.0)

    X = feat_df.to_numpy(dtype=np.float32, copy=False)
    # Keep horizons in canonical order if present
    horizons = [10,30,60,90,120,240]
    Ycols = [f"target_ret_{h}s" for h in horizons if f"target_ret_{h}s" in df.columns]
    Y = df[Ycols].to_numpy(dtype=np.float32, copy=False)
    out_dim = Y.shape[1]

    # Train/Val split (chronological) inside training set
    nrows = len(X)
    if nrows <= seq_len + 1:
        raise RuntimeError("Not enough rows for sequence length; reduce --seq-len or provide more data.")
    split = int(0.8 * nrows)
    Xtr, Xva = X[:split], X[split:]
    Ytr, Yva = Y[:split], Y[split:]

    tr_ds = SeqDataset(Xtr, Ytr, seq_len)
    va_ds = SeqDataset(Xva, Yva, seq_len)
    tr_dl = DataLoader(tr_ds, batch_size=min(batch_size, 512), shuffle=True,
                       num_workers=2, pin_memory=(device=="cuda"))
    va_dl = DataLoader(va_ds, batch_size=min(batch_size, 512), shuffle=False,
                       num_workers=2, pin_memory=(device=="cuda"))

    model = MCDropoutLSTM(in_dim=X.shape[1], hidden=96, num_layers=1, dropout=0.2, out_dim=out_dim).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr)

    scaler = torch.cuda.amp.GradScaler(enabled=(device=="cuda"))

    def step_epoch(loader, train=True):
        if train:
            model.train()
        else:
            model.eval()
        total = 0.0
        count = 0
        for xb, yb in loader:
            xb = xb.to(device, non_blocking=(device=="cuda"))
            yb = yb.to(device, non_blocking=(device=="cuda"))
            if train:
                opt.zero_grad(set_to_none=True)
                with torch.cuda.amp.autocast(enabled=(device=="cuda")):
                    mu, logvar = model(xb)
                    var = torch.exp(logvar).clamp_min(1e-8)
                    loss = 0.5 * ((yb - mu)**2 / var + logvar).mean()
                scaler.scale(loss).backward()
                scaler.step(opt)
                scaler.update()
            else:
                with torch.no_grad():
                    mu, logvar = model(xb)
                    var = torch.exp(logvar).clamp_min(1e-8)
                    loss = 0.5 * ((yb - mu)**2 / var + logvar).mean()
            total += loss.item() * xb.size(0)
            count += xb.size(0)
        return total / max(1, count)

    best = float("inf")
    for ep in range(epochs):
        tr = step_epoch(tr_dl, train=True)
        va = step_epoch(va_dl, train=False)
        if verbose:
            print(f"[train-bayes] epoch {ep+1}/{epochs} train={tr:.6f} val={va:.6f}")
        if va < best:
            best = va
            torch.save(model.state_dict(), os.path.join(outdir, "model.pt"))
            meta = {
                "in_dim": X.shape[1],
                "out_dim": out_dim,
                "seq_len": int(seq_len),
                "horizons": [int(c.split('_')[2].rstrip('s')) for c in Ycols],
                "feature_columns": feat_df.columns.tolist()
            }
            with open(os.path.join(outdir, "meta.json"), "w") as f:
                json.dump(meta, f, indent=2)
    if verbose:
        print(f"[train-bayes] saved -> {outdir}, best_val={best:.6f}")
