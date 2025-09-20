
import sys
import json
from .queue import dequeue, length

def handle(task):
    t = (task or {}).get("type")
    payload = (task or {}).get("payload", {})
    if t == "hello":
        print("handled", json.dumps({"type": t, "payload": payload}, ensure_ascii=False))
        return True
    print("unhandled", json.dumps(task, ensure_ascii=False))
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
