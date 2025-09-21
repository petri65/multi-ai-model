from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from . import attest, github_app, locks, prompt_guard

REPO_ROOT = Path(__file__).resolve().parents[3]
TOOLS_DIR = REPO_ROOT / "tools"
DEFAULT_POLICY_PATH = str(Path("policies") / "gates.yml")
DEFAULT_RULES_PATH = str(Path("policies") / "rules.yml")

VALIDATOR_VERSIONS: Dict[str, str] = {
    "llama_guard": "1.0",
    "protocol_auditor": "1.0",
    "gpt_math_validate": "1.0",
}


@dataclass
class ChangeProposal:
    job_id: str
    shards: Sequence[str]
    title: str
    prompt: str
    description: str = ""
    diff_paths: Sequence[str] = field(default_factory=list)
    branch: Optional[str] = None
    requires_math: bool = False


class Orchestrator:
    def __init__(
        self,
        *,
        lease_manager: Optional[locks.LeaseManager] = None,
        policy_path: str = DEFAULT_POLICY_PATH,
        rules_path: str = DEFAULT_RULES_PATH,
    ) -> None:
        self._lease_manager = lease_manager or locks.LeaseManager()
        self._policy_path = policy_path
        self._rules_path = rules_path
        self._current_cp: Optional[ChangeProposal] = None
        self._sanitized_prompt: str = ""
        self._leases: Dict[str, locks.Lease] = {}
        self._execution_logs: List[Dict[str, object]] = []
        self._validator_status: Dict[str, str] = {}

    # -- lifecycle ------------------------------------------------------

    def _reset(self) -> None:
        self._current_cp = None
        self._sanitized_prompt = ""
        self._leases = {}
        self._execution_logs = []
        self._validator_status = {}

    def prepare(self, cp: ChangeProposal) -> None:
        if self._current_cp is not None:
            raise RuntimeError("another change proposal is already active")
        sanitized = prompt_guard.sanitize(cp.prompt or cp.description)
        self._current_cp = cp
        self._sanitized_prompt = sanitized
        if cp.shards:
            self._leases = self._lease_manager.acquire(cp.shards, holder=cp.job_id)
        self._log_event(
            "lease_acquired",
            {
                "shards": list(cp.shards),
                "holder": cp.job_id,
            },
        )

    def validate_local(self) -> None:
        cp = self._require_cp()
        prompt_bytes = self._sanitized_prompt.encode("utf-8")
        self._execution_logs = []
        self._validator_status = {}

        self._run_validator(
            "llama_guard",
            [sys.executable, str(TOOLS_DIR / "llama_guard.py"), "--policy", self._rules_path],
            prompt_bytes,
        )
        self._run_validator(
            "protocol_auditor",
            [
                sys.executable,
                str(TOOLS_DIR / "protocol_auditor.py"),
                "--policy",
                self._policy_path,
                "--rules",
                self._rules_path,
            ],
            prompt_bytes,
        )
        if self._should_run_math(cp):
            paths = [str(Path(p)) for p in (cp.diff_paths or [])]
            self._run_validator(
                "gpt_math_validate",
                [sys.executable, str(TOOLS_DIR / "gpt_math_validate.py"), *paths],
                None,
            )
        # renew leases after validation pass to keep TTL fresh
        if self._leases:
            self._lease_manager.renew(cp.shards, holder=cp.job_id)

    def open_pr(self) -> str:
        cp = self._require_cp()
        attestation_path = attest.write_attestation(
            cp.job_id,
            validators=self._build_validator_report(),
            rule_paths=[self._policy_path, self._rules_path],
            diff_paths=[str(Path(p)) for p in (cp.diff_paths or [])],
            execution_logs=self._execution_logs,
            prompt=self._sanitized_prompt,
        )
        branch_name = cp.branch or f"ai/{cp.job_id}"
        github_app.push_branch(
            branch_name,
            attestation_path=attestation_path,
            title=cp.title,
            body=cp.description,
        )
        self._release_leases()
        self._reset()
        return attestation_path

    def abort(self) -> None:
        self._release_leases()
        self._reset()

    # -- helpers --------------------------------------------------------

    def _require_cp(self) -> ChangeProposal:
        if self._current_cp is None:
            raise RuntimeError("no active change proposal")
        return self._current_cp

    def _log_event(self, name: str, data: Optional[Dict[str, object]] = None) -> None:
        self._execution_logs.append(
            {
                "timestamp": time.time(),
                "event": name,
                "payload": data or {},
            }
        )

    def _run_validator(self, name: str, cmd: List[str], input_bytes: Optional[bytes]) -> None:
        started = time.time()
        proc = subprocess.run(
            cmd,
            input=input_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        finished = time.time()
        log_entry = {
            "timestamp": finished,
            "validator": name,
            "command": cmd,
            "returncode": proc.returncode,
            "stdout": proc.stdout.decode("utf-8", errors="ignore").strip(),
            "stderr": proc.stderr.decode("utf-8", errors="ignore").strip(),
            "duration": finished - started,
        }
        self._execution_logs.append(log_entry)
        if proc.returncode != 0:
            self._validator_status[name] = "fail"
            raise RuntimeError(f"validator {name} failed: {log_entry['stderr'] or log_entry['stdout']}")
        self._validator_status[name] = "pass"

    def _should_run_math(self, cp: ChangeProposal) -> bool:
        if cp.requires_math:
            return True
        for path in cp.diff_paths or []:
            lower = path.lower()
            if lower.endswith((".py", ".ipynb")):
                return True
            if any(token in lower for token in ("math", "calc", "formula", "model")):
                return True
        return False

    def _build_validator_report(self) -> List[Dict[str, str]]:
        report: List[Dict[str, str]] = []
        for name, version in VALIDATOR_VERSIONS.items():
            status = self._validator_status.get(name, "skipped")
            report.append({
                "name": name,
                "version": version,
                "status": status,
            })
        return report

    def _release_leases(self) -> None:
        if not self._current_cp or not self._leases:
            return
        try:
            self._lease_manager.release(self._current_cp.shards, holder=self._current_cp.job_id)
            self._log_event(
                "lease_released",
                {"shards": list(self._current_cp.shards), "holder": self._current_cp.job_id},
            )
        finally:
            self._leases = {}


__all__ = ["ChangeProposal", "Orchestrator"]
