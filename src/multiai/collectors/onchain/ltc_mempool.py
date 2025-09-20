from __future__ import annotations
import time
from datetime import datetime, timezone
import httpx
from multiai.collectors.rotate import RollingParquetWriter

def utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp()*1000)

def rpc_call(client: httpx.Client, method: str, params=None):
    j = {"jsonrpc":"2.0","id":1,"method":method,"params":params or []}
    r = client.post("/", json=j, timeout=5)
    r.raise_for_status(); out = r.json()
    if "error" in out and out["error"]:
        raise RuntimeError(out["error"])
    return out.get("result")

def run_litecoin_mempool(rpc_url: str, rpc_user: str, rpc_pass: str, out_dir: str, rotate_minutes: int = 90):
    writer = RollingParquetWriter(out_dir, prefix="onchain_LTC", rotate_minutes=rotate_minutes)
    auth = (rpc_user, rpc_pass)
    with httpx.Client(base_url=rpc_url, auth=auth) as c:
        last_emit = 0
        while True:
            now_ms = utc_ms()
            if now_ms - last_emit >= 200:
                info = rpc_call(c, "getmempoolinfo")
                row = {"timestamp": now_ms}
                row["tx_count"] = info.get("size")
                row["bytes"] = info.get("bytes")
                row["usage"] = info.get("usage")
                row["mempoolminfee"] = info.get("mempoolminfee")
                row["minrelaytxfee"] = info.get("minrelaytxfee")
                writer.write_row(row)
                last_emit = now_ms
            time.sleep(0.05)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rpc-url", default="http://127.0.0.1:9332")
    ap.add_argument("--rpc-user", required=True)
    ap.add_argument("--rpc-pass", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--rotate-minutes", type=int, default=90)
    args = ap.parse_args()
    run_litecoin_mempool(args.rpc_url, args.rpc_user, args.rpc_pass, args.outdir, rotate_minutes=args.rotate_minutes)

if __name__ == "__main__":
    main()
