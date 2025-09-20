from __future__ import annotations

import argparse
import sys
from typing import Dict, List, Tuple


class ProtocolViolation(RuntimeError):
    """Raised when the orchestrator task violates governance rules."""


_KEYWORD_MAP: Dict[str, Tuple[str, ...]] = {
    "egress.secrets": ("secret", "credential", "password"),
    "egress.raw_data": ("raw dump", "export dataset", "full database"),
    "gpu_required": ("cpu only",),
}


def _load_denied_capabilities(path: str) -> List[str]:
    denied: List[str] = []
    stack: List[Tuple[int, str]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            if not raw_line.strip() or raw_line.lstrip().startswith("#"):
                continue
            indent = len(raw_line) - len(raw_line.lstrip(" "))
            stripped = raw_line.strip()
            if ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            key = key.strip()
            value = value.strip()
            while stack and stack[-1][0] >= indent:
                stack.pop()
            if not value:
                stack.append((indent, key))
                continue
            if "deny" in value.lower():
                path_components = [frame[1] for frame in stack] + [key]
                denied.append(".".join(path_components))
    return denied


def _enforce(prompt: str, policy_path: str, rules_path: str) -> None:
    denied = set(_load_denied_capabilities(rules_path))
    if not denied:
        return
    lowered = prompt.lower()
    for capability in denied:
        keywords = _KEYWORD_MAP.get(capability, ())
        for kw in keywords:
            if kw in lowered:
                raise ProtocolViolation(
                    f"capability '{capability}' is denied but keyword '{kw}' was requested"
                )
    # Basic policy sanity: require mention of policy identifier to ensure context.
    policy_text = open(policy_path, "r", encoding="utf-8").read().lower()
    if "thresholds" in policy_text and "threshold" not in lowered:
        raise ProtocolViolation("prompt missing acknowledgement of gate thresholds")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run protocol governance audit")
    parser.add_argument("--policy", required=True, help="Path to governance policy file")
    parser.add_argument("--rules", required=True, help="Path to protocol rules file")
    args = parser.parse_args()

    prompt = sys.stdin.read()
    if not prompt.strip():
        print("protocol_auditor: empty prompt supplied", file=sys.stderr)
        raise SystemExit(2)

    try:
        _enforce(prompt, args.policy, args.rules)
    except ProtocolViolation as exc:
        print(f"protocol_auditor: violation - {exc}", file=sys.stderr)
        raise SystemExit(3)

    print("protocol_auditor: pass")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
