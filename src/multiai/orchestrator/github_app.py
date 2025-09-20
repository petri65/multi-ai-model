from __future__ import annotations

import json
import sys
from typing import Optional


def push_branch(branch: str, *, attestation_path: Optional[str] = None, title: Optional[str] = None) -> None:
    """Stub GitHub App client that records push metadata."""
    if not branch:
        raise ValueError("branch name is required for push")

    payload = {
        "branch": branch,
        "title": title or "",
        "attestation": attestation_path or "",
    }
    sys.stdout.write("github_app.push_branch " + json.dumps(payload) + "\n")
