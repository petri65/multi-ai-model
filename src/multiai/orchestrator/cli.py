
import sys, json, traceback, os, inspect
from importlib import import_module
from .queue import dequeue, length, enqueue
from . import state as st

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
    res = _call_flexible("multiai.pipeline.build_targets", "run", payload, extra_candidates=["build_targets","build","apply"])
    st.set_artifact("with_targets", out_path)
    return res

def handle_build_features(payload):
    out_path = payload.get("out_path")
    res = _call_flexible("multiai.pipeline.build_features", "run", payload, extra_candidates=["build_features","build","apply"])
    st.set_artifact("with_features", out_path)
    return res

def handle_train_bayes_lstm(payload):
    out_model = payload.get("model_out")
    res = _call_flexible("multiai.pipeline.train_bayes_lstm", "run", payload, extra_candidates=["train","fit"])
    if out_model:
        st.set_artifact("model_path", out_model)
    return res

def handle_predict_bayes_lstm(payload):
    out_pred = payload.get("pred_out")
    res = _call_flexible("multiai.pipeline.predict_bayes_lstm", "run", payload, extra_candidates=["predict","apply"])
    if out_pred:
        st.set_artifact("pred_path", out_pred)
    return res

def handle_register_merged(payload):
    merged = payload.get("path")
    if not _exists(merged):
        raise FileNotFoundError(f"merged parquet not found: {merged}")
    st.set_artifact("merged", merged)
    return {"registered": merged}

def handle_split_train_test(payload):
    return _call_flexible("multiai.pipeline.split_train_test", "run", payload)

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
}

def next_steps():
    s = st.load()
    merged = s.get("merged")
    with_targets = s.get("with_targets")
    with_features = s.get("with_features")
    train_path = s.get("train_path")
    test_path = s.get("test_path")
    model_path = s.get("model_path")
    pred_path = s.get("pred_path")

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
        steps.append(("pipeline.build_targets", {
            "in_path": merged,
            "out_path": os.path.join("outputs", "merged_with_targets.parquet")
        }))
        return steps
    if not _exists(with_features):
        steps.append(("pipeline.build_features", {
            "in_path": with_targets or os.path.join("outputs", "merged_with_targets.parquet"),
            "out_path": os.path.join("outputs", "merged_with_features.parquet")
        }))
        return steps
    if not (_exists(train_path) and _exists(test_path)):
        steps.append(("pipeline.split_train_test", {
            "in_path": with_features or os.path.join("outputs", "merged_with_features.parquet"),
            "out_train": os.path.join("outputs", "train.parquet"),
            "out_test": os.path.join("outputs", "test.parquet"),
            "ratio": 0.8
        }))
        return steps
    if not _exists(model_path):
        steps.append(("train.bayes_lstm", {
            "data_path": train_path,
            "model_out": os.path.join("outputs", "bayesian_lstm_model.pt")
        }))
        return steps
    if not _exists(pred_path):
        steps.append(("predict.bayes_lstm", {
            "data_path": test_path,
            "model_path": model_path or os.path.join("outputs", "bayesian_lstm_model.pt"),
            "pred_out": os.path.join("outputs", "predictions.parquet")
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
