#!/usr/bin/env bash
set -euo pipefail
TARGET=${1:-main}
SRC=${2:-$(git rev-parse --abbrev-ref HEAD)}
echo "[guarded-merge] target=$TARGET from=$SRC"
export PYTHONPATH="src:${PYTHONPATH:-}"
git fetch origin "$TARGET" || true
if ! git diff --quiet || ! git diff --cached --quiet; then
  git add -A
  git commit -m "guarded: stage working changes" || true
fi
git checkout "$SRC"
git rebase "origin/$TARGET" || true
python -m tools.protocol_auditor --policy core_protocol.md --rules policies/rules.yml
python -m tools.llama_guard --policy core_protocol.md
python -m tools.math_trigger --diff-only | xargs -r -n1 python -m tools.gpt_math_validate --require-pass
pytest -q
python - <<'PY'
from multiai.orchestrator.attest import write_attestation
paths = ["core_protocol.md","policies/rules.yml"]
write_attestation(paths, out_path="ai_attestation.json", tool="guarded-merge")
print("[attestation] ai_attestation.json written")
PY
git add ai_attestation.json
git commit -m "attestation: guarded merge" || true
PR_BRANCH="guarded/$(date +%Y%m%d-%H%M%S)"
git branch -f "$PR_BRANCH" "$SRC"
git checkout "$PR_BRANCH"
git push -u origin "$PR_BRANCH"
url=$(git config --get remote.origin.url)
case "$url" in
  git@github.com:*) url="https://github.com/${url#git@github.com:}"; url="${url%.git}";;
  https://github.com/*) url="${url%.git}";;
esac
echo "[guarded-merge] Pushed branch: $(git rev-parse --abbrev-ref HEAD)"
echo "[guarded-merge] Open PR: ${url}/compare/${TARGET}...$(git rev-parse --abbrev-ref HEAD)?expand=1"
echo "[guarded-merge] success"
