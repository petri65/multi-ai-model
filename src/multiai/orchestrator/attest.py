import json, time, hashlib
def write_attestation(paths, out_path="ai_attestation.json", tool="guarded-merge"):
    rec={"tool":tool,"time":int(time.time()),"artifacts":[]}
    for p in paths:
        try:
            with open(p,"rb") as f:
                h=hashlib.sha256(f.read()).hexdigest()
            rec["artifacts"].append({"path":p,"sha256":h})
        except FileNotFoundError:
            pass
    with open(out_path,"w") as f:
        json.dump(rec,f,indent=2)
    return out_path
if __name__=="__main__":
    write_attestation(["core_protocol.md","policies/rules.yml"])
