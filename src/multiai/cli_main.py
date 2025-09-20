import argparse
import sys

try:
    import pandas  # noqa: F401  # Used to detect availability of optional data tooling
except ImportError:  # pragma: no cover - environment without pandas should disable data ops
    dm_available = False
    bf_available = False
else:
    dm_available = True
    bf_available = True

def cli():
    p = argparse.ArgumentParser(prog="multiai", description="Multi-AI Model Orchestrator")
    sub = p.add_subparsers(dest="cmd", required=True)

    bt = sub.add_parser("build-targets")
    bt.add_argument("--merged", required=True)
    bt.add_argument("--out", required=True)
    bt.add_argument("--price-col", default="trade_price")
    bt.add_argument("--horizons", default="10,30,60,90,120,240")
    bt.add_argument("--verbose", action="store_true")

    trb = sub.add_parser("train-bayes")
    trb.add_argument("--features", required=True)
    trb.add_argument("--targets", required=True)
    trb.add_argument("--outdir", required=True)
    trb.add_argument("--seq-len", type=int, default=240)
    trb.add_argument("--epochs", type=int, default=5)
    trb.add_argument("--batch-size", type=int, default=256)
    trb.add_argument("--lr", type=float, default=1e-3)
    trb.add_argument("--device", default="auto")
    trb.add_argument("--verbose", action="store_true")

    prb = sub.add_parser("predict-bayes", help="Predict with Bayesian LSTM + distributional Kelly")
    prb.add_argument("--features", required=True)
    prb.add_argument("--model-dir", required=True)
    prb.add_argument("--out", required=True)
    prb.add_argument("--seq-len", type=int, default=240)
    prb.add_argument("--mc-samples", type=int, default=30)
    prb.add_argument("--device", default="auto")
    prb.add_argument("--cost-bps-per-leg", type=float, default=20.0)
    prb.add_argument("--sl", type=float, default=0.02)
    prb.add_argument("--tp", type=float, default=0.02)
    prb.add_argument("--kelly-cap", type=float, default=0.2)
    prb.add_argument("--sigma-scale", type=float, default=1.0)
    prb.add_argument("--combine", action="store_true")
    prb.add_argument("--verbose", action="store_true")

    dm = sub.add_parser("daily-merge")
    dm.add_argument("--off-dir", required=True)
    dm.add_argument("--on-dir", required=True)
    dm.add_argument("--out", required=True)
    dm.add_argument("--verbose", action="store_true")

    bf = sub.add_parser("build-features")
    bf.add_argument("--merged", required=True)
    bf.add_argument("--out", required=True)
    bf.add_argument("--price-col", default="trade_price")
    bf.add_argument("--verbose", action="store_true")
    a = p.parse_args()

    if a.cmd == "build-targets":
        from multiai.pipeline.build_targets import run_build_targets
        run_build_targets(a.merged, a.out, a.price_col, [int(x) for x in a.horizons.split(",")], a.verbose)

    elif a.cmd == "train-bayes":
        from multiai.pipeline.train_bayes_lstm import run_train_bayes
        run_train_bayes(a.features, a.targets, a.outdir, a.seq_len, a.epochs, a.batch_size, a.lr, a.device, a.verbose)

    elif a.cmd == "predict-bayes":
        from multiai.pipeline.predict_bayes_lstm import run_predict_bayes
        run_predict_bayes(a.features, a.model_dir, a.out, a.seq_len, a.mc_samples, a.device,
                          a.cost_bps_per_leg, a.sl, a.tp, a.verbose, a.kelly_cap, a.sigma_scale, a.combine)

    elif a.cmd == "daily-merge":
        if not dm_available:
            print("daily-merge not available in this build", file=sys.stderr); sys.exit(2)
        from multiai.pipeline.daily_merge import run_daily_merge
        run_daily_merge(a.off_dir, a.on_dir, a.out, a.verbose)

    elif a.cmd == "build-features":
        if not bf_available:
            print("build-features not available in this build", file=sys.stderr); sys.exit(2)
        from multiai.pipeline.build_features import run_build_features
        run_build_features(a.merged, a.out, a.price_col, a.verbose)

if __name__ == "__main__":
    cli()
