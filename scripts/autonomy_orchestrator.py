from __future__ import annotations
import os, sys, time, subprocess, json
from pathlib import Path
from datetime import datetime, timezone

REPO = Path(__file__).resolve().parents[1]
OFF_DIR = REPO/"data/offchain"
ON_DIR  = REPO/"data/onchain"
OUT_DIR = REPO/"outputs"
STATE   = REPO/".autonomy_state.json"

def ts():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

def newest(p: Path):
    files = sorted(p.glob("*.parquet"))
    return files[-1] if files else None

def load_state():
    if STATE.exists(): return json.loads(STATE.read_text())
    return {"last_branch": "", "last_run": "", "last_merged_out": ""}

def save_state(s):
    STATE.write_text(json.dumps(s, indent=2))

def run(cmd, env=None, cwd=None):
    print("RUN", " ".join(cmd))
    r = subprocess.run(cmd, cwd=cwd or str(REPO), env=env, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout); print(r.stderr, file=sys.stderr)
        raise SystemExit(r.returncode)
    return r.stdout.strip()

def ensure_dirs():
    OFF_DIR.mkdir(parents=True, exist_ok=True)
    ON_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

def daily_out_path():
    d = datetime.now(timezone.utc).strftime("%d%m%Y")
    return OUT_DIR/f"merged_{d}.parquet"

def main():
    ensure_dirs()
    st = load_state()
    off = newest(OFF_DIR)
    on  = newest(ON_DIR)
    if not off or not on:
        print("waiting for data..."); return
    merged_path = daily_out_path()
    run(["multiai","run","daily-merge","--off-dir",str(OFF_DIR),"--on-dir",str(ON_DIR),"--out",str(merged_path)])
    feat = OUT_DIR/"features.parquet"
    run(["multiai","run","build-features","--merged",str(merged_path),"--out",str(feat),"--price-col","mid_price"])
    targets = OUT_DIR/"targets.parquet"
    run(["multiai","run","build-targets","--merged",str(merged_path),"--out",str(targets),"--price-col","mid_price"])
    model_dir = OUT_DIR/"model"
    run([
        "multiai","run","train-bayes",
        "--features",str(feat),
        "--targets",str(targets),
        "--outdir",str(model_dir),
        "--seq-len","240",
        "--epochs","3",
        "--batch-size","256",
        "--lr","0.001",
        "--device","cpu"
    ])
    preds = OUT_DIR/"preds.parquet"
    run([
        "multiai","run","predict-bayes",
        "--features",str(feat),
        "--model-dir",str(model_dir),
        "--out",str(preds),
        "--seq-len","240",
        "--mc-samples","30",
        "--device","cpu",
        "--cost-bps-per-leg","20",
        "--sl","0.02",
        "--tp","0.02",
        "--kelly-cap","0.2",
        "--sigma-scale","1.0"
    ])
    branch = f"auto/update-{ts()}"
    run(["git","checkout","-b",branch])
    run(["git","add","."])
    run(["git","commit","-m",f"auto: data pipeline outputs {branch}"])
    run(["git","push","-u","origin",branch])
    title = f"auto: pipeline outputs {branch}"
    body  = f"Autonomous update at {ts()}\\n\\nIncludes merged, features, model weights, preds."
    try:
        run(["gh","pr","create","--title",title,"--body",body])
    except SystemExit:
        pass
    st["last_branch"]=branch; st["last_run"]=ts(); st["last_merged_out"]=str(merged_path); save_state(st)

if __name__=="__main__":
    main()
