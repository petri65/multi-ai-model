from importlib import import_module
_q = import_module("multiai.orchestrator.queue")
try:
    enqueue = _q.enqueue
    dequeue = _q.dequeue
    length = _q.length
except AttributeError as e:
    raise ImportError(f"queue module missing expected symbol: {e}")
__all__ = ["enqueue", "dequeue", "length"]
