
set -euo pipefail
cd "$(dirname "$0")/.."
. .venv/bin/activate
export PYTHONUNBUFFERED=1
export PYTHONPATH="$(pwd)/src"
export MULTIAI_QUEUE_PATH="$(pwd)/logs/task_queue.jsonl"
while true; do
  python -m multiai.orchestrator.cli run || true
  sleep 2
done
