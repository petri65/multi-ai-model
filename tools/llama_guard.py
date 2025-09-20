from __future__ import annotations

import argparse
import sys
from typing import Iterable, List


class PolicyViolation(RuntimeError):
    """Raised when the prompt violates the guard policy."""


_DEFAULT_BLOCKED = (
    "prompt injection",
    "ignore previous",
    "override safety",
    "jailbreak",
    "rm -rf",
    "drop table",
)


def _load_policy(path: str) -> List[str]:
    blocked: List[str] = []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if ":" in stripped:
                    _, value = stripped.split(":", 1)
                    stripped = value.strip()
                blocked.append(stripped)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"policy file not found: {path}") from exc
    return blocked or list(_DEFAULT_BLOCKED)


def _enforce(prompt: str, blocked_terms: Iterable[str]) -> None:
    lowered = prompt.lower()
    for term in blocked_terms:
        term_lower = term.lower().strip()
        if not term_lower:
            continue
        if term_lower in lowered:
            raise PolicyViolation(f"blocked term detected: {term}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Llama Guard policy checks")
    parser.add_argument("--policy", required=True, help="Path to guard policy file")
    args = parser.parse_args()

    prompt = sys.stdin.read()
    if not prompt.strip():
        print("llama_guard: empty prompt supplied", file=sys.stderr)
        raise SystemExit(2)

    try:
        blocked_terms = _load_policy(args.policy)
        _enforce(prompt, blocked_terms)
    except PolicyViolation as exc:
        print(f"llama_guard: policy violation - {exc}", file=sys.stderr)
        raise SystemExit(3)

    print("llama_guard: pass")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
