from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Deque, Dict, List, Optional

import httpx

from multiai.collectors.onchain import ltc_mempool
from multiai.collectors.rotate import RollingParquetWriter

WINDOW_SECONDS_DEFAULT = 10 * 60  # 10 minutes


def utc_ms() -> int:
    """Return the current UTC time in milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


@dataclass
class WhaleEvent:
    txid: str
    timestamp_ms: int
    value_ltc_total: float
    inputs_count: int
    outputs_count: int
    block_height: int
    first_seen_ms: int

    def to_row(self) -> Dict[str, object]:
        return {
            "timestamp": self.timestamp_ms,
            "timestamp_ms": self.timestamp_ms,
            "txid": self.txid,
            "value_ltc_total": float(self.value_ltc_total),
            "inputs_count": int(self.inputs_count),
            "outputs_count": int(self.outputs_count),
            "block_height": int(self.block_height),
            "first_seen_ms": int(self.first_seen_ms),
        }


class WhaleRingBuffer:
    def __init__(self, window_seconds: int = WINDOW_SECONDS_DEFAULT) -> None:
        self.window_ms = window_seconds * 1000
        self._events: Deque[WhaleEvent] = deque()

    def add(self, event: WhaleEvent) -> None:
        self._events.append(event)

    def prune(self, now_ms: int) -> List[WhaleEvent]:
        cutoff = now_ms - self.window_ms
        removed: List[WhaleEvent] = []
        while self._events and self._events[0].timestamp_ms < cutoff:
            removed.append(self._events.popleft())
        return removed

    def snapshot(self) -> List[WhaleEvent]:
        return list(self._events)


class WhaleTracker:
    def __init__(
        self,
        threshold_ltc: float,
        top_n: int = 5,
        *,
        window_seconds: int = WINDOW_SECONDS_DEFAULT,
        event_writer: Optional[RollingParquetWriter] = None,
        metrics_writer: Optional[RollingParquetWriter] = None,
        time_fn: Callable[[], int] = utc_ms,
    ) -> None:
        if threshold_ltc <= 0:
            raise ValueError("threshold_ltc must be positive")
        if top_n <= 0:
            raise ValueError("top_n must be positive")
        self.threshold_ltc = float(threshold_ltc)
        self.top_n = int(top_n)
        self._buffer = WhaleRingBuffer(window_seconds)
        self._event_writer = event_writer
        self._metrics_writer = metrics_writer
        self._time_fn = time_fn
        self._events_by_txid: Dict[str, WhaleEvent] = {}
        self._first_seen: Dict[str, int] = {}
        self._last_tick_ts: Optional[int] = None
        self._last_metrics: Optional[Dict[str, object]] = None

    @staticmethod
    def _total_output_value(tx: Dict[str, object]) -> float:
        total = 0.0
        for vout in tx.get("vout", []) or []:
            value = None
            if isinstance(vout, dict):
                value = vout.get("value")
            if value is None:
                continue
            try:
                total += float(value)
            except (TypeError, ValueError):
                continue
        return total

    def _counts(self, tx: Dict[str, object]) -> tuple[int, int]:
        vin = tx.get("vin", []) or []
        vout = tx.get("vout", []) or []
        return len(vin), len(vout)

    def _handle_tx(
        self,
        tx: Dict[str, object],
        *,
        seen_ms: Optional[int] = None,
        block_height: Optional[int] = None,
    ) -> Optional[WhaleEvent]:
        txid = tx.get("txid") or tx.get("hash")
        if not txid:
            return None
        total_value = self._total_output_value(tx)
        if total_value < self.threshold_ltc:
            return None
        now_ms = int(seen_ms if seen_ms is not None else self._time_fn())
        first_seen = self._first_seen.setdefault(txid, now_ms)
        event = self._events_by_txid.get(txid)
        if event is None:
            inputs_count, outputs_count = self._counts(tx)
            event = WhaleEvent(
                txid=txid,
                timestamp_ms=first_seen,
                value_ltc_total=total_value,
                inputs_count=inputs_count,
                outputs_count=outputs_count,
                block_height=int(block_height) if block_height is not None else -1,
                first_seen_ms=first_seen,
            )
            self._events_by_txid[txid] = event
            self._buffer.add(event)
            if self._event_writer is not None:
                self._event_writer.write_row(event.to_row())
        else:
            # Update totals if they changed and enrich block height when known.
            event.value_ltc_total = max(event.value_ltc_total, total_value)
            if block_height is not None and block_height >= 0:
                new_height = int(block_height)
                height_changed = event.block_height != new_height
                event.block_height = new_height
                if height_changed and self._event_writer is not None:
                    # Emit an updated row reflecting confirmation metadata.
                    self._event_writer.write_row(event.to_row())
        return event

    def process_mempool_tx(self, tx: Dict[str, object], *, seen_ms: Optional[int] = None) -> Optional[WhaleEvent]:
        return self._handle_tx(tx, seen_ms=seen_ms, block_height=-1)

    def process_block(self, block: Dict[str, object], *, seen_ms: Optional[int] = None) -> List[WhaleEvent]:
        now_ms = int(seen_ms if seen_ms is not None else self._time_fn())
        block_height = block.get("height")
        results: List[WhaleEvent] = []
        for tx in block.get("tx", []) or []:
            if isinstance(tx, dict):
                evt = self._handle_tx(tx, seen_ms=now_ms, block_height=block_height)
                if evt is not None:
                    results.append(evt)
        return results

    def _grid_second(self, ts_ms: int) -> int:
        return (ts_ms // 1000) * 1000

    def tick(self, now_ms: Optional[int] = None) -> Dict[str, object]:
        ts = int(now_ms if now_ms is not None else self._time_fn())
        grid_ts = self._grid_second(ts)
        if self._last_tick_ts == grid_ts and self._last_metrics is not None:
            return dict(self._last_metrics)
        removed = self._buffer.prune(grid_ts)
        for ev in removed:
            self._events_by_txid.pop(ev.txid, None)
            self._first_seen.pop(ev.txid, None)
        events = self._buffer.snapshot()
        count = len(events)
        total_value = sum(e.value_ltc_total for e in events)
        avg_value = total_value / count if count else 0.0
        max_value = max((e.value_ltc_total for e in events), default=0.0)
        top_values = sorted((e.value_ltc_total for e in events), reverse=True)
        top_sum = sum(top_values[: self.top_n]) if top_values else 0.0
        metrics = {
            "timestamp": grid_ts,
            "whale_tx_count_10m": int(count),
            "whale_total_value_ltc_10m": float(total_value),
            "whale_avg_value_ltc_10m": float(avg_value),
            "whale_max_value_ltc_10m": float(max_value),
            "whale_topN_sum_ltc_10m": float(top_sum),
            "threshold_ltc_effective": float(self.threshold_ltc),
        }
        if self._metrics_writer is not None:
            self._metrics_writer.write_row(metrics)
        self._last_tick_ts = grid_ts
        self._last_metrics = metrics
        return dict(metrics)

    def close(self) -> None:
        if self._event_writer is not None:
            self._event_writer.close()
        if self._metrics_writer is not None:
            self._metrics_writer.close()


def run_ltc_whale_tracker(
    rpc_url: str,
    rpc_user: str,
    rpc_pass: str,
    out_dir: str,
    *,
    threshold_ltc: float = 10.0,
    top_n: int = 5,
    rotate_minutes: int = 90,
    poll_interval_ms: int = 200,
) -> None:
    """Long-running whale tracker loop for Litecoin."""
    event_writer = RollingParquetWriter(out_dir, prefix="whale_events", rotate_minutes=rotate_minutes)
    metrics_writer = RollingParquetWriter(out_dir, prefix="whale_metrics", rotate_minutes=rotate_minutes)
    tracker = WhaleTracker(
        threshold_ltc=threshold_ltc,
        top_n=top_n,
        event_writer=event_writer,
        metrics_writer=metrics_writer,
    )
    auth = (rpc_user, rpc_pass)
    last_mempool_poll = 0
    last_second = None
    best_hash: Optional[str] = None
    try:
        with httpx.Client(base_url=rpc_url, auth=auth) as client:
            while True:
                now = utc_ms()
                if now - last_mempool_poll >= poll_interval_ms:
                    try:
                        mempool = ltc_mempool.rpc_call(client, "getrawmempool", [True]) or {}
                    except Exception:
                        mempool = {}
                    if isinstance(mempool, dict):
                        for txid, tx in mempool.items():
                            if isinstance(tx, dict):
                                tx.setdefault("txid", txid)
                                tracker.process_mempool_tx(tx, seen_ms=now)
                    last_mempool_poll = now
                try:
                    current_hash = ltc_mempool.rpc_call(client, "getbestblockhash")
                except Exception:
                    current_hash = None
                if current_hash and current_hash != best_hash:
                    try:
                        block = ltc_mempool.rpc_call(client, "getblock", [current_hash, 2]) or {}
                    except Exception:
                        block = {}
                    tracker.process_block(block, seen_ms=now)
                    best_hash = current_hash
                sec = now // 1000
                if last_second is None or sec > last_second:
                    tracker.tick(sec * 1000)
                    last_second = sec
                time.sleep(max(poll_interval_ms / 1000.0 / 4.0, 0.05))
    finally:
        tracker.close()


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Litecoin whale tracker")
    ap.add_argument("--rpc-url", default="http://127.0.0.1:9332")
    ap.add_argument("--rpc-user", required=True)
    ap.add_argument("--rpc-pass", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--threshold-ltc", type=float, default=10.0)
    ap.add_argument("--top-n", type=int, default=5)
    ap.add_argument("--rotate-minutes", type=int, default=90)
    args = ap.parse_args()
    run_ltc_whale_tracker(
        args.rpc_url,
        args.rpc_user,
        args.rpc_pass,
        args.outdir,
        threshold_ltc=args.threshold_ltc,
        top_n=args.top_n,
        rotate_minutes=args.rotate_minutes,
    )


if __name__ == "__main__":
    main()
