import sys, argparse, re
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--policy", required=True)
    ap.add_argument("--rules", required=True)
    a=ap.parse_args()
    pol=open(a.policy,"r",encoding="utf-8").read()
    rul=open(a.rules,"r",encoding="utf-8").read()
    need=["seq_len: 240","horizons","gpu_policy"]
    bad=[k for k in need if k not in rul]
    if "policy_file:" not in pol and "Machine-Readable" not in pol and "policies/rules.yml" not in pol:
        bad.append("policy-file-link")
    if bad:
        print("protocol_auditor: missing keys:", ",".join(bad))
        sys.exit(2)
    print("protocol_auditor: ok")
if __name__=="__main__": main()
