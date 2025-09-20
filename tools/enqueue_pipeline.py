
import os, glob, time
from multiai.orchestrator.queue import enqueue

def _latest(path):
    files = [p for p in glob.glob(path) if os.path.isfile(p)]
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]

def main():
    repo = os.getcwd()
    off = _latest(os.path.join(repo, "data", "offchain", "*.parquet"))
    on = _latest(os.path.join(repo, "data", "onchain", "*.parquet"))
    if not off or not on:
        print("missing parquet files under data/offchain or data/onchain")
        return

    out_on = os.path.join("outputs", "on_chain_q1s.parquet")
    out_off = os.path.join("outputs", "off_chain_q1s.parquet")

    print(enqueue("data.quantize_1s", {"in_path": on, "out_path": out_on, "ceil": True, "label":"on_chain"}))
    print(enqueue("data.quantize_1s", {"in_path": off, "out_path": out_off, "ceil": True, "label":"off_chain"}))
    print("seeded two quantize tasks; loop will chain the rest")

if __name__ == "__main__":
    main()
