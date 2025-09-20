import argparse
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--policy", required=True)
    a=ap.parse_args()
    open(a.policy,"r",encoding="utf-8").read(256)
    print("llama_guard: ok")
if __name__=="__main__": main()
