
import sys, json, traceback, os
from importlib import import_module
from .queue import dequeue, length, enqueue
from . import state as st

def _call(module_path, func_name="run", kwargs=None):
    mod = import_module(module_path)
    fn = getattr(mod, func_name, None)
    if fn is None:
        raise RuntimeError(f"entrypoint {module_path}.{func_name} not found")
    return fn(**(kwargs or {}))

def _exists(p):
    return p and os.path.exists(p)

def handle_hello(payload):
    print("handled", json.dumps({"type":"hello","payload":payload}, ensure_ascii=False))
    return True

def handle_quantize(payload):
    out_path = payload.get("out_path")
    res = _call("multiai.dataops.quantize_1s", "run", payload)
    label = payload.get("label")
    if label == "on_chain":
        st.set_artifact("on_chain_q1s", out_path)
    elif label == "off_chain":
        st.set_artifact("off_chain_q1s", out_path)
    return res

def handle_split_lists(payload):
    out_path = payload.get("out_path")
    res = _call("multiai.dataops.split_object_columns", "run", payload)
    st.set_artifact("off_chain_q1s_split", out_path)
    return res

def handle_merge_on_off(payload):
    out_path = payload.get("out_path")
    res = _call("multiai.dataops.merge_on_offchain", "run", payload)
    st.set_artifact("merged", out_path)
    return res

def handle_build_targets(payload):
    out_path = payload.get("out_path")
    res = _call("multiai.pipeline.build_targets", "run", payload)
    st.set_artifact("with_targets", out_path)
    return res

def handle_build_features(payload):
    out_path = payload.get("out_path")
    res = _call("multiai.pipeline.build_features", "run", payload)
    st.set_artifact("with_features", out_path)
    return res

def handle_train_bayes_lstm(payload):
    out_model = payload.get("model_out")
    res = _call("multiai.pipeline.train_bayes_lstm", "run", payload)
    if out_model:
        st.set_artifact("model_path", out_model)
    return res

def handle_predict_bayes_lstm(payload):
    out_pred = payload.get("pred_out")
    res = _call("multiai.pipeline.predict_bayes_lstm", "run", payload)
    if out_pred:
        st.set_artifact("pred_path", out_pred)
    return res

DISPATCH = {
    "hello": handle_hello,
    "data.quantize_1s": handle_quantize,
    "data.split_lists": handle_split_lists,
    "data.merge_on_offchain": handle_merge_on_off,
    "pipeline.build_targets": handle_build_targets,
    "pipeline.build_features": handle_build_features,
    "train.bayes_lstm": handle_train_bayes_lstm,
    "predict.bayes_lstm": handle_predict_bayes_lstm,
}

def next_steps():
    s = st.load()
    on_q = s.get("on_chain_q1s")
    off_q = s.get("off_chain_q1s")
    off_split = s.get("off_chain_q1s_split")
    merged = s.get("merged")
    with_targets = s.get("with_targets")
    with_features = s.get("with_features")
    model_path = s.get("model_path")
    pred_path = s.get("pred_path")

    steps = []
    if not _exists(on_q) or not _exists(off_q):
        return steps
    if not _exists(off_split) and _exists(off_q):
        steps.append(("data.split_lists", {
            "in_path": off_q,
            "out_path": os.path.join("outputs", "off_chain_q1s_split.parquet"),
            "cols": ["orderbook_bid","orderbook_ask","bid_depth","ask_depth","mid_prices","spreads"],
            "prefix_map": {"orderbook_bid":"orderbook_bid","orderbook_ask":"orderbook_ask","bid_depth":"bid_depth","ask_depth":"ask_depth","mid_prices":"mid","spreads":"spread"}
        }))
        return steps
    if not _exists(merged) and _exists(off_split) and _exists(on_q):
        steps.append(("data.merge_on_offchain", {
            "left": off_split,
            "right": on_q,
            "out_path": os.path.join("outputs", "merged.parquet")
        }))
        return steps
    if not _exists(with_targets) and _exists(merged):
        steps.append(("pipeline.build_targets", {
            "in_path": merged,
            "out_path": os.path.join("outputs", "merged_with_targets.parquet")
        }))
        return steps
    if not _exists(with_features) and _exists(with_targets):
        steps.append(("pipeline.build_features", {
            "in_path": with_targets,
            "out_path": os.path.join("outputs", "merged_with_features.parquet")
        }))
        return steps
    if not _exists(model_path) and _exists(with_features):
        steps.append(("train.bayes_lstm", {
            "data_path": with_features,
            "model_out": os.path.join("outputs", "bayesian_lstm_model.pt")
        }))
        return steps
    if not _exists(pred_path) and _exists(with_features):
        steps.append(("predict.bayes_lstm", {
            "data_path": with_features,
            "model_path": s.get("model_path") or os.path.join("outputs", "bayesian_lstm_model.pt"),
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
