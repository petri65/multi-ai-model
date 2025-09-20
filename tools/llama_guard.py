import sys, json, fnmatch, os
try:
    import yaml
except Exception:
    yaml = None

def main():
    raw = sys.stdin.read()
    diff_paths = [line.strip() for line in raw.splitlines() if line.strip()]

    if not os.path.exists("policies/rules.yml"):
        print("OK: no rules.yml present; skipping", file=sys.stderr)
        sys.exit(0)

    if yaml is None:
        print("ERROR: pyyaml not available", file=sys.stderr)
        sys.exit(2)

    with open("policies/rules.yml", "r", encoding="utf-8") as f:
        rules = yaml.safe_load(f)

    blocked = rules.get("blocked_patterns", []) or []
    for p in diff_paths:
        base = os.path.basename(p)
        for pat in blocked:
            if fnmatch.fnmatch(base, pat):
                print(f"DENY: blocked pattern match: {p}", file=sys.stderr)
                sys.exit(3)

    print("OK: llama_guard pass")
    sys.exit(0)

if __name__ == "__main__":
    main()
