from __future__ import annotations

import re
from typing import Iterable


class PromptRejected(ValueError):
    """Raised when a prompt violates guardrails."""


_DEFAULT_MAX_LENGTH = 4096
_BLOCKED_PATTERNS: tuple[str, ...] = (
    r"\b(?:rm\s+-rf|drop\s+table|sudo\s+rm)\b",
    r"\b(?:bypass|jailbreak|ignore\s+policy)\b",
    r"\b(?:exfiltrate|leak\s+data|steal\s+secrets)\b",
)


def _strip_control(text: str) -> str:
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", text)


def sanitize(text: str, *, max_length: int = _DEFAULT_MAX_LENGTH, blocked_patterns: Iterable[str] | None = None) -> str:
    """Normalize and vet prompts before dispatching them to models."""
    if text is None:
        raise PromptRejected("prompt is required")

    cleaned = _strip_control(str(text)).strip()
    if not cleaned:
        raise PromptRejected("prompt cannot be empty")

    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rstrip()

    patterns = tuple(blocked_patterns) if blocked_patterns else _BLOCKED_PATTERNS
    lowered = cleaned.lower()
    for pattern in patterns:
        if re.search(pattern, lowered, flags=re.IGNORECASE):
            raise PromptRejected(f"prompt rejected by guard pattern: {pattern}")

    return cleaned


__all__ = ["PromptRejected", "sanitize"]
