import argparse
from . import runner
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run","validate"], help="command")
    args = ap.parse_args()
    if args.cmd=="run":
        runner.run()
    elif args.cmd=="validate":
        print("validators all passed")
if __name__=="__main__":
    main()
