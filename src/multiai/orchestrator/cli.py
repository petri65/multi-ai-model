
import sys, json, traceback, os, inspect
from datetime import datetime, timezone
from importlib import import_module
from .queue import dequeue, length, enqueue
from . import state as st
from multiai.paper_trading import SessionConfig, run as run_paper_session

def _import(module_path):
    return import_module(module_path)

def _best_callable(mod, candidates):
    for name in candidates:
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn
    # look for a class with a run() method
    for name in dir(mod):
        obj = getattr(mod, name, None)
        if inspect.isclass(obj) and hasattr(obj, "run") and callable(getattr(obj, "run")):
            return getattr(obj(), "run")
    return None

def _adapt_kwargs(fn, kwargs):
    if not callable(fn):
        return {}
    sig = inspect.signature(fn)
    out = {}
    for k, v in (kwargs or {}).items():
        if k in sig.parameters:
            out[k] = v
    return out

def _call_flexible(module_path, default_func="run", kwargs=None, extra_candidates=None):
    mod = _import(module_path)
    candidates = [default_func]
    if extra_candidates:
        candidates.extend([c for c in extra_candidates if c not in candidates])
    candidates.extend(["main", "build", "execute"])
    fn = _best_callable(mod, candidates)
    if fn is None:
        raise RuntimeError(f"no suitable entrypoint found in {module_path} among {candidates}")
    ak = _adapt_kwargs(fn, kwargs or {})
    return fn(**ak)

def _exists(p):
    return p and os.path.exists(p)

def handle_hello(payload):
    print("handled", json.dumps({"type":"hello","payload":payload}, ensure_ascii=False))
    return True

def handle_quantize(payload):
    out_path = payload.get("out_path")
    res = _call_flexible("multiai.dataops.quantize_1s", "run", payload)
    label = payload.get("label")
    if label == "on_chain":
        st.set_artifact("on_chain_q1s", out_path)
    elif label == "off_chain":
        st.set_artifact("off_chain_q1s", out_path)
    return res

def handle_split_lists(payload):
    out_path = payload.get("out_path")
    res = _call_flexible("multiai.dataops.split_object_columns", "run", payload, extra_candidates=["split","apply"])
    st.set_artifact("off_chain_q1s_split", out_path)
    return res

def handle_merge_on_off(payload):
    out_path = payload.get("out_path")
    res = _call_flexible("multiai.dataops.merge_on_offchain", "run", payload, extra_candidates=["merge","apply"])
    st.set_artifact("merged", out_path)
    return res


def handle_build_targets(payload):
    out_path = payload.get("out_path")
    # remap in_path -> merged_path for the module signature
    payload2 = {"merged_path": payload.get("in_path"), "out_path": out_path}
    res = _call_flexible("multiai.pipeline.build_targets", "run", payload2, extra_candidates=["run_build_targets","build_targets","build","apply"])
    st.set_artifact("with_targets", out_path)
    return res


def handle_build_features(payload):
    out_path = payload.get("out_path")
    merged_path = st.load().get("merged")
    price_col = payload.get("price_col")
    if not price_col:
        df_cols = []
        try:
            import pandas as _pd
            df_cols = [c.lower() for c in _pd.read_parquet(merged_path, columns=None).columns]
        except Exception:
            df_cols = []
        preferred = ["mid_price","weighted_mid_price","trade_price","price","close","last_price"]
        for c in preferred:
            if c.lower() in df_cols:
                price_col = c
                break
        if not price_col:
            price_col = "trade_price"
    payload2 = {
        "merged_path": merged_path,
        "out_path": out_path,
        "price_col": price_col
    }
    res = _call_flexible("multiai.pipeline.build_features", "run", payload2, extra_candidates=["run_build_features","build_features","build","apply"])
    st.set_artifact("with_features", out_path)
    return res

def _coerce_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in {"1","true","yes","y","on"}
    return bool(val)

def handle_train_bayes_lstm(payload):
    state = st.load()
    features_path = payload.get("features_path") or state.get("train_path")
    if not _exists(features_path):
        raise FileNotFoundError(f"training features not found: {features_path}")
    targets_path = payload.get("targets_path") or state.get("with_targets")
    if not _exists(targets_path):
        raise FileNotFoundError(f"targets parquet not found: {targets_path}")

    requested_out = payload.get("outdir") or payload.get("model_dir") or payload.get("model_out")
    if requested_out and requested_out.endswith(".pt"):
        outdir = os.path.dirname(requested_out) or "."
    else:
        outdir = requested_out
    if not outdir:
        outdir = os.path.join("outputs", "bayesian_lstm_model")

    defaults = {
        "seq_len": 60,
        "epochs": 3,
        "batch_size": 128,
        "lr": 1e-3,
        "device": "cpu",
        "verbose": False,
    }

    payload2 = {
        "features_path": features_path,
        "targets_path": targets_path,
        "outdir": outdir,
        "seq_len": int(payload.get("seq_len", defaults["seq_len"])),
        "epochs": int(payload.get("epochs", defaults["epochs"])),
        "batch_size": int(payload.get("batch_size", defaults["batch_size"])),
        "lr": float(payload.get("lr", defaults["lr"])),
        "device": payload.get("device", defaults["device"]),
        "verbose": _coerce_bool(payload.get("verbose"), defaults["verbose"]),
    }

    res = _call_flexible(
        "multiai.pipeline.train_bayes_lstm",
        "run",
        payload2,
        extra_candidates=["train", "fit", "run_train_bayes"],
    )

    st.set_artifact("model_dir", outdir)
    st.set_artifact("model_path", os.path.join(outdir, "model.pt"))
    return res

def handle_predict_bayes_lstm(payload):
    state = st.load()
    features_path = payload.get("features_path") or state.get("test_path")
    if not _exists(features_path):
        raise FileNotFoundError(f"prediction features not found: {features_path}")

    model_dir = payload.get("model_dir") or state.get("model_dir")
    if not model_dir:
        model_path = payload.get("model_path") or state.get("model_path")
        if model_path:
            model_dir = os.path.dirname(model_path)
    if not _exists(model_dir):
        raise FileNotFoundError(f"model directory not found: {model_dir}")

    out_path = payload.get("out_path") or payload.get("pred_out")
    if not out_path:
        out_path = os.path.join("outputs", "predictions.parquet")

    seq_default = 60
    meta_path = os.path.join(model_dir, "meta.json") if model_dir else None
    if meta_path and os.path.exists(meta_path):
        try:
            meta = json.load(open(meta_path, "r", encoding="utf-8"))
            seq_default = int(meta.get("seq_len", seq_default))
        except Exception:
            pass

    defaults = {
        "seq_len": seq_default,
        "mc_samples": 32,
        "device": "cpu",
        "cost_bps_per_leg": 20.0,
        "sl": 0.002,
        "tp": 0.004,
        "verbose": False,
        "kelly_cap": 0.2,
        "sigma_scale": 1.0,
        "combine": False,
    }

    payload2 = {
        "features_path": features_path,
        "model_dir": model_dir,
        "out_path": out_path,
        "seq_len": int(payload.get("seq_len", defaults["seq_len"])),
        "mc_samples": int(payload.get("mc_samples", defaults["mc_samples"])),
        "device": payload.get("device", defaults["device"]),
        "cost_bps_per_leg": float(payload.get("cost_bps_per_leg", defaults["cost_bps_per_leg"])),
        "sl": float(payload.get("sl", defaults["sl"])),
        "tp": float(payload.get("tp", defaults["tp"])),
        "verbose": _coerce_bool(payload.get("verbose"), defaults["verbose"]),
        "kelly_cap": float(payload.get("kelly_cap", defaults["kelly_cap"])),
        "sigma_scale": float(payload.get("sigma_scale", defaults["sigma_scale"])),
        "combine": _coerce_bool(payload.get("combine"), defaults["combine"]),
    }

    res = _call_flexible(
        "multiai.pipeline.predict_bayes_lstm",
        "run",
        payload2,
        extra_candidates=["predict", "apply", "run_predict_bayes"],
    )
    st.set_artifact("pred_path", out_path)
    return res

def handle_register_merged(payload):
    merged = payload.get("path")
    if not _exists(merged):
        raise FileNotFoundError(f"merged parquet not found: {merged}")
    st.set_artifact("merged", merged)
    return {"registered": merged}

def handle_split_train_test(payload):
    return _call_flexible("multiai.pipeline.split_train_test", "run", payload)

def handle_paper_trading_run(payload):
    state = st.load()
    defaults = {
        "predictions_path": state.get("pred_path"),
        "market_path": state.get("with_features"),
        "out_dir": os.path.join("outputs", "paper_trading"),
        "duration_seconds": 3600,
        "initial_capital": 100_000.0,
        "exposure_cap": 0.2,
        "hysteresis": 0.01,
        "stop_loss": 0.02,
        "take_profit": 0.04,
        "cost_bps_per_leg": 20.0,
        "price_col": "trade_price",
    }
    predictions_path = payload.get("predictions_path", defaults["predictions_path"])
    market_path = payload.get("market_path", defaults["market_path"])
    out_dir = payload.get("out_dir", defaults["out_dir"])
    if not _exists(predictions_path):
        raise FileNotFoundError(f"predictions parquet missing: {predictions_path}")
    if market_path and not _exists(market_path):
        raise FileNotFoundError(f"market parquet missing: {market_path}")

    cfg = SessionConfig(
        duration_seconds=int(payload.get("duration_seconds", defaults["duration_seconds"])),
        initial_capital=float(payload.get("initial_capital", defaults["initial_capital"])),
        exposure_cap=float(payload.get("exposure_cap", defaults["exposure_cap"])),
        hysteresis=float(payload.get("hysteresis", defaults["hysteresis"])),
        stop_loss=float(payload.get("stop_loss", defaults["stop_loss"])),
        take_profit=float(payload.get("take_profit", defaults["take_profit"])),
        cost_bps_per_leg=float(payload.get("cost_bps_per_leg", defaults["cost_bps_per_leg"])),
    )
    result = run_paper_session(
        predictions_path=predictions_path,
        market_path=market_path,
        out_dir=out_dir,
        price_col=payload.get("price_col", defaults["price_col"]),
        config=cfg,
    )
    st.set_artifact("paper_trading_log", result.log_path)
    st.set_artifact("paper_trading_equity", result.equity_path)
    st.set_artifact("paper_trading_alerts", result.alerts_path)
    st.set_artifact("paper_trading_last_run", datetime.now(timezone.utc).isoformat())
    return result.asdict()

DISPATCH = {
    "hello": handle_hello,
    "data.quantize_1s": handle_quantize,
    "data.split_lists": handle_split_lists,
    "data.merge_on_offchain": handle_merge_on_off,
    "pipeline.build_targets": handle_build_targets,
    "pipeline.build_features": handle_build_features,
    "train.bayes_lstm": handle_train_bayes_lstm,
    "predict.bayes_lstm": handle_predict_bayes_lstm,
    "data.register_merged": handle_register_merged,
    "pipeline.split_train_test": handle_split_train_test,
    "paper_trading.run": handle_paper_trading_run,
}

def next_steps():
    s = st.load()
    merged = s.get("merged")
    with_targets = s.get("with_targets")
    with_features = s.get("with_features")
    train_path = s.get("train_path")
    test_path = s.get("test_path")
    model_dir = s.get("model_dir")
    model_path = s.get("model_path")
    pred_path = s.get("pred_path")

    if not model_dir and model_path:
        model_dir = os.path.dirname(model_path)

    outputs_dir = "outputs"
    os.makedirs(outputs_dir, exist_ok=True)

    steps = []
    if _exists(s.get("on_chain_q1s")) and _exists(s.get("off_chain_q1s")):
        if not _exists(s.get("off_chain_q1s_split")):
            steps.append(("data.split_lists", {
                "in_path": s.get("off_chain_q1s"),
                "out_path": os.path.join("outputs", "off_chain_q1s_split.parquet"),
                "cols": ["orderbook_bid","orderbook_ask","bid_depth","ask_depth","mid_prices","spreads"],
                "prefix_map": {"orderbook_bid":"orderbook_bid","orderbook_ask":"orderbook_ask","bid_depth":"bid_depth","ask_depth":"ask_depth","mid_prices":"mid","spreads":"spread"}
            }))
            return steps
        if not _exists(merged):
            steps.append(("data.merge_on_offchain", {
                "left": s.get("off_chain_q1s_split"),
                "right": s.get("on_chain_q1s"),
                "out_path": os.path.join("outputs", "merged.parquet")
            }))
            return steps

    if not _exists(merged):
        return steps
    if not _exists(with_targets):
        targets_out = os.path.join(outputs_dir, "merged_with_targets.parquet")
        os.makedirs(os.path.dirname(targets_out) or ".", exist_ok=True)
        steps.append(("pipeline.build_targets", {
            "in_path": merged,
            "out_path": targets_out
        }))
        return steps
    if not _exists(with_features):
        features_out = os.path.join(outputs_dir, "merged_with_features.parquet")
        os.makedirs(os.path.dirname(features_out) or ".", exist_ok=True)
        steps.append(("pipeline.build_features", {
            "in_path": with_targets or os.path.join(outputs_dir, "merged_with_targets.parquet"),
            "out_path": features_out
        }))
        return steps
    if not (_exists(train_path) and _exists(test_path)):
        train_out = os.path.join(outputs_dir, "train.parquet")
        test_out = os.path.join(outputs_dir, "test.parquet")
        os.makedirs(os.path.dirname(train_out) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(test_out) or ".", exist_ok=True)
        steps.append(("pipeline.split_train_test", {
            "in_path": with_features or os.path.join(outputs_dir, "merged_with_features.parquet"),
            "out_train": train_out,
            "out_test": test_out,
            "ratio": 0.8
        }))
        return steps
    target_ready = _exists(with_targets)
    model_ready = model_dir and os.path.isdir(model_dir) and os.path.exists(os.path.join(model_dir, "model.pt"))
    if not model_ready:
        if not (target_ready and _exists(train_path)):
            return steps
        default_model_out = os.path.join("outputs", "bayesian_lstm_model", "model.pt")
        os.makedirs(os.path.dirname(default_model_out), exist_ok=True)
        steps.append(("train.bayes_lstm", {
            "model_out": default_model_out,
            "seq_len": 60,
            "epochs": 3,
            "batch_size": 128,
            "lr": 1e-3,
            "device": "cpu",
        }))
        return steps
    if not _exists(pred_path):
        default_pred = os.path.join("outputs", "predictions.parquet")
        os.makedirs(os.path.dirname(default_pred) or ".", exist_ok=True)
        steps.append(("predict.bayes_lstm", {
            "pred_out": default_pred,
            "seq_len": 60,
            "mc_samples": 32,
            "device": "cpu",
            "cost_bps_per_leg": 20.0,
            "sl": 0.002,
            "tp": 0.004,
            "kelly_cap": 0.2,
            "sigma_scale": 1.0,
            "combine": False,
        }))
        return steps
    last_paper = s.get("paper_trading_last_run")
    run_today = False
    if last_paper:
        try:
            last_dt = datetime.fromisoformat(last_paper)
            now = datetime.now(timezone.utc)
            run_today = last_dt.date() == now.date()
        except Exception:
            run_today = False
    if not run_today:
        market_source = with_features if _exists(with_features) else merged
        if not _exists(market_source):
            return steps
        out_dir = os.path.join(outputs_dir, "paper_trading")
        os.makedirs(out_dir, exist_ok=True)
        steps.append(("paper_trading.run", {
            "predictions_path": pred_path,
            "market_path": market_source,
            "out_dir": out_dir,
            "duration_seconds": 3600,
            "initial_capital": 100_000.0,
            "exposure_cap": 0.2,
            "hysteresis": 0.01,
            "stop_loss": 0.02,
            "take_profit": 0.04,
            "cost_bps_per_leg": 20.0,
            "price_col": "trade_price",
        }))
        return steps
    return steps

def handle(task):
    t = (task or {}).get("type")
    payload = (task or {}).get("payload", {})
    fn = DISPATCH.get(t)
    if fn is None:
        print("unhandled", json.dumps(task, ensure_ascii=False))
        return False
    try:
        res = fn(payload)
        print("ok", json.dumps({"type": t, "result": str(res)}, ensure_ascii=False))
        for nt, np in next_steps():
            enqueue(nt, np)
            print("enqueued", json.dumps({"type": nt, "payload": np}, ensure_ascii=False))
        return True
    except Exception as e:
        print("error", t, str(e))
        traceback.print_exc()
        return False

def run_once():
    task = dequeue()
    if task is None:
        print("idle")
        return 0
    ok = handle(task)
    return 0 if ok else 1

def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "run":
        sys.exit(run_once())
    print("usage: python -m multiai.orchestrator.cli run")

if __name__ == "__main__":
    main()
