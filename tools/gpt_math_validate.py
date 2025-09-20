from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Iterable


class MathValidationError(RuntimeError):
    """Raised when math validation fails."""


_SUSPICIOUS_TOKENS = ("TODO", "nan", "div0", "??")


def _validate_file(path: pathlib.Path) -> None:
    if not path.exists():
        raise MathValidationError(f"missing file: {path}")
    text = path.read_text(encoding="utf-8", errors="ignore")
    lowered = text.lower()
    for token in _SUSPICIOUS_TOKENS:
        if token.lower() in lowered:
            raise MathValidationError(f"math validation failed: token '{token}' in {path}")


def _validate(paths: Iterable[str]) -> None:
    for raw in paths:
        if not raw:
            continue
        _validate_file(pathlib.Path(raw))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run GPT math validator checks")
    parser.add_argument("paths", nargs="*", help="Paths to inspect for math integrity")
    args = parser.parse_args()

    try:
        _validate(args.paths)
    except MathValidationError as exc:
        print(f"GPT-Math-Validate: {exc}", file=sys.stderr)
        raise SystemExit(3)

    print("GPT-Math-Validate: pass")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
