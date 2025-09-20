import json, hashlib, os, sys, time

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    # Emit ai_attestation.json in CWD summarizing key files if they exist
    files = []
    for p in ["core_protocol.md","policies/rules.yml","policies/gates.yml","src/multiai/cli.py"]:
        if os.path.exists(p):
            files.append({"path": p, "sha256": sha256_file(p)})
    att = {
        "timestamp": int(time.time()),
        "files": files,
        "validators": {
            "protocol_auditor": "phase1",
            "calibration_gates": "phase1",
            "backtest_gates": "phase1"
        }
    }
    with open("ai_attestation.json","w",encoding="utf-8") as f:
        json.dump(att, f, indent=2)
    print("Wrote ai_attestation.json")

if __name__ == "__main__":
    main()
