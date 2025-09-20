import json, time, pathlib, threading
Q = pathlib.Path("orchestrator_queue.json")
_LOCK = threading.Lock()

def _load():
    if not Q.exists(): return []
    try:
        return json.loads(Q.read_text())
    except Exception:
        return []

def _save(items):
    Q.write_text(json.dumps(items, indent=2))

def enqueue(task_type: str, payload: dict):
    rec = {"time": int(time.time()), "type": task_type, "payload": payload or {}}
    with _LOCK:
        items = _load()
        items.append(rec)
        _save(items)
    return rec

def dequeue():
    with _LOCK:
        items = _load()
        if not items: return None
        rec = items.pop(0)
        _save(items)
        return rec

def length():
    with _LOCK:
        return len(_load())
