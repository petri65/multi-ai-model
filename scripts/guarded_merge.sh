#!/usr/bin/env bash
set -euo pipefail

TARGET=${1:-main}
SRC=${2:-$(git rev-parse --abbrev-ref HEAD)}

echo "[guarded-merge] target=$TARGET from=$SRC"

export PYTHONPATH="src:${PYTHONPATH}"

# 1) Ensure clean tree
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "[guarded-merge] working tree not clean"; exit 2
fi

# 2) Update and rebase onto target
git fetch origin "$TARGET" || true
git checkout "$SRC"
git rebase "origin/$TARGET" || true

# 3) Local validators (flags explicit)
python -m tools.protocol_auditor --policy core_protocol.md --rules policies/rules.yml
python -m tools.llama_guard       --policy core_protocol.md
python -m tools.math_trigger --diff-only | xargs -r -n1 python -m tools.gpt_math_validate --require-pass
pytest -q

# 4) Attestation
python - <<'PY'
from multiai.orchestrator.attest import write_attestation
paths = ["core_protocol.md","policies/rules.yml"]
write_attestation(paths, out_path="ai_attestation.json", tool="guarded-merge")
print("[attestation] ai_attestation.json written")
PY
git add ai_attestation.json
git commit -m "attestation: guarded merge" || true

# 5) Fast-forward merge locally
git checkout "$TARGET" || git checkout -b "$TARGET"
git pull --ff-only origin "$TARGET" || true
git merge --ff-only "$SRC"

# 6) Push with explicit allow (pre-push hook checks this)
export GUARDED_MERGE_OK=1
git push origin "$TARGET"
echo "[guarded-merge] success"
