set -euo pipefail
cd "$(dirname "$0")/.."
. .venv/bin/activate
export PYTHONUNBUFFERED=1
export PYTHONPATH="$(pwd)/src"
export MULTIAI_QUEUE_PATH="$(pwd)/logs/task_queue.jsonl"

python - <<'PY'
import os, sys, importlib
q = importlib.import_module("multiai.orchestrator.queue")
c = importlib.import_module("multiai.orchestrator.cli")
print("exe", sys.executable)
print("pp", os.environ.get("PYTHONPATH"))
print("queue_file", q.__file__, "has_dequeue", hasattr(q,"dequeue"))
print("cli_file", c.__file__)
PY

python -m multiai.orchestrator.cli run || true
sleep 2
