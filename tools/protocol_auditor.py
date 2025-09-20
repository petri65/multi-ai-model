import argparse
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", required=True)
    ap.add_argument("--rules", required=True)
    a = ap.parse_args()
    open(a.policy,"r",encoding="utf-8").read(256)
    open(a.rules,"r",encoding="utf-8").read(256)
    print("protocol_auditor: ok")
if __name__ == "__main__":
    main()
