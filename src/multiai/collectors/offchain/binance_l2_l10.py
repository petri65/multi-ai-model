from __future__ import annotations
import asyncio, json, time
from datetime import datetime, timezone
from typing import Dict, Any
import websockets
from multiai.collectors.rotate import RollingParquetWriter

WS = "wss://stream.binance.com:9443/stream"

def utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp()*1000)

async def run_binance_l2(symbol: str, out_dir: str, rotate_minutes: int = 90):
    sym = symbol.lower()
    streams = f"{sym}@depth10@100ms/{sym}@trade"
    uri = f"{WS}?streams={streams}"
    writer = RollingParquetWriter(out_dir, prefix=f"offchain_{symbol}", rotate_minutes=rotate_minutes)
    latest: Dict[str, Any] = {"bids": [], "asks": [], "last_price": None, "last_qty": None}
    try:
        async with websockets.connect(uri, ping_interval=10, ping_timeout=10, max_queue=1000) as ws:
            last_emit = 0
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    data = json.loads(msg)
                    s = data.get("stream", "")
                    p = data.get("data", {})
                    if s.endswith("@depth10@100ms"):
                        latest["bids"] = p.get("bids", [])
                        latest["asks"] = p.get("asks", [])
                    elif s.endswith("@trade"):
                        latest["last_price"] = float(p.get("p")) if "p" in p else latest["last_price"]
                        latest["last_qty"] = float(p.get("q")) if "q" in p else latest["last_qty"]
                except asyncio.TimeoutError:
                    pass
                now_ms = utc_ms()
                if now_ms - last_emit >= 200:
                    row = {"timestamp": now_ms, "symbol": symbol}
                    bids = latest.get("bids") or []
                    asks = latest.get("asks") or []
                    for i in range(10):
                        if i < len(bids):
                            row[f"bid_px_{i+1}"] = float(bids[i][0]); row[f"bid_sz_{i+1}"] = float(bids[i][1])
                        else:
                            row[f"bid_px_{i+1}"] = None; row[f"bid_sz_{i+1}"] = None
                        if i < len(asks):
                            row[f"ask_px_{i+1}"] = float(asks[i][0]); row[f"ask_sz_{i+1}"] = float(asks[i][1])
                        else:
                            row[f"ask_px_{i+1}"] = None; row[f"ask_sz_{i+1}"] = None
                    lp = latest.get("last_price")
                    row["last_price"] = float(lp) if lp is not None else None
                    lq = latest.get("last_qty")
                    row["last_qty"] = float(lq) if lq is not None else None
                    if row.get("bid_px_1") is not None and row.get("ask_px_1") is not None:
                        row["mid_price"] = 0.5*(row["bid_px_1"]+row["ask_px_1"])
                        row["spread"] = row["ask_px_1"]-row["bid_px_1"]
                    else:
                        row["mid_price"] = None; row["spread"] = None
                    writer.write_row(row)
                    last_emit = now_ms
    finally:
        writer.close()

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="LTCUSDT")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--rotate-minutes", type=int, default=90)
    args = ap.parse_args()
    asyncio.run(run_binance_l2(args.symbol, args.outdir, rotate_minutes=args.rotate_minutes))

if __name__ == "__main__":
    main()
