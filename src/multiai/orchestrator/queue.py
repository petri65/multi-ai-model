
import os
import json
import time
import tempfile
from typing import Any, Dict, Optional

# Queue file lives under ./logs by default; override with env if needed.
DEFAULT_QUEUE_PATH = os.environ.get("MULTIAI_QUEUE_PATH", os.path.join("logs", "task_queue.jsonl"))

def _ensure_parent(path: str) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)

def _atomic_write(path: str, data: str) -> None:
    _ensure_parent(path)
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".tmp_queue_", dir=d, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(data)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _read_lines(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln for ln in (l.strip() for l in f.readlines()) if ln]

def enqueue(task_type: str, payload: Dict[str, Any], queue_path: Optional[str] = None) -> Dict[str, Any]:
    """Append a task (JSONL). Returns the enqueued record."""
    qp = queue_path or DEFAULT_QUEUE_PATH
    _ensure_parent(qp)
    record = {
        "time": int(time.time()),
        "type": str(task_type),
        "payload": payload or {},
    }
    line = json.dumps(record, ensure_ascii=False)
    with open(qp, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    return record

def length(queue_path: Optional[str] = None) -> int:
    qp = queue_path or DEFAULT_QUEUE_PATH
    return len(_read_lines(qp))

def dequeue(queue_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Pop the oldest task. Returns None if empty. Uses an atomic rewrite to avoid partial writes."""
    qp = queue_path or DEFAULT_QUEUE_PATH
    lines = _read_lines(qp)
    if not lines:
        return None
    head = lines[0]
    rest = lines[1:]
    # Atomic rewrite
    _atomic_write(qp, "".join(l + "\n" for l in rest))
    try:
        return json.loads(head)
    except json.JSONDecodeError:
        # Skip malformed head and continue (best-effort)
        if rest:
            _atomic_write(qp, "".join(l + "\n" for l in rest[1:]))
            try:
                return json.loads(rest[0])
            except Exception:
                return None
        return None
