from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Iterable, Mapping, Sequence

DEFAULT_TOOL = "guarded-merge"
DEFAULT_ATTESTATION_PATH = "ai_attestation.json"
GOVERNANCE_LOG_PATH = Path("GOVERNANCE_LOG.md")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    return _sha256_bytes(path.read_bytes())


def _materialise_entries(paths: Iterable[str]) -> list[dict[str, str | bool]]:
    entries: list[dict[str, str | bool]] = []
    for raw in paths:
        if not raw:
            continue
        p = Path(raw)
        info: dict[str, str | bool] = {"path": str(p), "exists": p.exists()}
        file_hash = _hash_file(p)
        if file_hash:
            info["sha256"] = file_hash
        entries.append(info)
    return entries


def _ensure_parent(path: Path) -> None:
    if path.parent:
        path.parent.mkdir(parents=True, exist_ok=True)


def _append_governance_log(timestamp: str, job_id: str, digest: str) -> None:
    header = "# Governance Log\n\n| Timestamp | Job ID | Attestation Digest |\n| --- | --- | --- |\n"
    if not GOVERNANCE_LOG_PATH.exists():
        GOVERNANCE_LOG_PATH.write_text(header, encoding="utf-8")
    with GOVERNANCE_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"| {timestamp} | {job_id} | {digest} |\n")


def write_attestation(
    job_id: str,
    *,
    validators: Sequence[Mapping[str, str]],
    rule_paths: Sequence[str],
    diff_paths: Sequence[str],
    execution_logs: Sequence[Mapping[str, object]],
    prompt: str,
    out_path: str = DEFAULT_ATTESTATION_PATH,
    tool: str = DEFAULT_TOOL,
    signature_secret: str | None = None,
) -> str:
    """Emit an attestation payload and append the digest to the governance log."""

    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    prompt_digest = _sha256_bytes(prompt.encode("utf-8"))

    payload = {
        "job_id": job_id,
        "tool": tool,
        "timestamp": timestamp,
        "validators": list(validators),
        "rules": _materialise_entries(rule_paths),
        "diffs": _materialise_entries(diff_paths),
        "execution_logs": list(execution_logs),
        "prompt_sha256": prompt_digest,
    }

    digest = _sha256_bytes(json.dumps(payload, sort_keys=True).encode("utf-8"))
    secret = signature_secret or os.environ.get("MULTIAI_ATTESTATION_SECRET", "")
    signature = _sha256_bytes((digest + secret).encode("utf-8"))

    payload["digest"] = digest
    payload["signature"] = signature

    out = Path(out_path)
    _ensure_parent(out)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    _append_governance_log(timestamp, job_id, digest)
    return str(out)


__all__ = ["write_attestation", "DEFAULT_ATTESTATION_PATH", "GOVERNANCE_LOG_PATH"]
