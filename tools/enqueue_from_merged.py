
import os, sys
from multiai.orchestrator.queue import enqueue

def main():
    if len(sys.argv) < 2:
        print("usage: python -m tools.enqueue_from_merged /absolute/path/to/merged.parquet")
        return
    path = sys.argv[1]
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    print(enqueue("data.register_merged", {"path": path}))

if __name__ == "__main__":
    main()
