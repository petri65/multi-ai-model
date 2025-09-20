
from multiai.orchestrator.queue import enqueue, dequeue, length

def main():
    print("[smoke] initial length:", length())
    rec = enqueue("hello", {"note": "smoke-test"})
    print("[smoke] enqueued:", rec)
    print("[smoke] length after enqueue:", length())
    got = dequeue()
    print("[smoke] dequeued:", got)
    print("[smoke] length after dequeue:", length())

if __name__ == "__main__":
    main()
