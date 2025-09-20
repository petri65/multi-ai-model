
import os, json

STATE_DIR = os.environ.get("MULTIAI_STATE_DIR") or "state"
STATE_PATH = os.path.join(STATE_DIR, "pipeline.json")

def _ensure_dir(p):
    d = os.path.dirname(p) or "."
    os.makedirs(d, exist_ok=True)

def load():
    if not os.path.exists(STATE_PATH):
        return {}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {}

def save(d):
    _ensure_dir(STATE_PATH)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

def set_artifact(key, path):
    s = load()
    s[key] = path
    save(s)

def get_artifact(key):
    return load().get(key)
