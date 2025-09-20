import json, time, hashlib, pathlib
def write_attestation(paths, out_path="ai_attestation.json", tool="guarded-merge"):
    rec = {"tool": tool, "time": int(time.time()), "artifacts": []}
    for p in paths:
        pp = pathlib.Path(p)
        if pp.exists() and pp.is_file():
            h = hashlib.sha256(pp.read_bytes()).hexdigest()
            rec["artifacts"].append({"path": str(pp), "sha256": h})
    pathlib.Path(out_path).write_text(json.dumps(rec, indent=2), encoding="utf-8")
    return out_path
if __name__ == "__main__":
    write_attestation(["core_protocol.md","policies/rules.yml"])
