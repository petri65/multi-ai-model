#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="src:${PYTHONPATH}"
export MULTIAI_DEVICE=auto
mkdir -p logs
LOGFILE="logs/orchestrator.log"
touch "$LOGFILE"
while true; do
  python -m multiai.orchestrator.cli run >> "$LOGFILE" 2>&1 || true
  if ! git diff --quiet || ! git diff --cached --quiet; then
    git add -A
    git commit -m "orchestrator: local change [auto]" || true
    make guarded-merge || true
  fi
  sleep 30
done
