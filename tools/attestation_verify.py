import json, sys

def main():
    path = "ai_attestation.json"
    try:
        with open(path,"r",encoding="utf-8") as f:
            obj = json.load(f)
    except Exception as e:
        print(f"FAIL: cannot read {path}: {e}", file=sys.stderr); sys.exit(3)

    # Minimal checks
    if "files" not in obj or not isinstance(obj["files"], list):
        print("FAIL: attestation missing 'files' list", file=sys.stderr); sys.exit(3)
    if "validators" not in obj:
        print("FAIL: attestation missing 'validators'", file=sys.stderr); sys.exit(3)

    print("OK: attestation verified"); sys.exit(0)

if __name__ == "__main__":
    main()
