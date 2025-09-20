import pandas as pd
import pytest

from multiai.collectors.onchain.whale_tracker import WhaleTracker
from multiai.collectors.rotate import RollingParquetWriter


class FakeClock:
    def __init__(self, start_ms: int) -> None:
        self._now = start_ms

    def set(self, ts_ms: int) -> None:
        self._now = ts_ms

    def __call__(self) -> int:
        return self._now


def _mk_tx(txid: str, value: float, *, inputs: int = 1, outputs: int = 1) -> dict:
    per_output = value / outputs
    return {
        "txid": txid,
        "vin": [{}] * inputs,
        "vout": [{"value": per_output} for _ in range(outputs)],
    }


def test_whale_tracker_detection_metrics_and_parquet(tmp_path):
    base_ms = 1_700_000_000_000
    clock = FakeClock(base_ms)
    writer_dir = tmp_path / "whales"
    event_writer = RollingParquetWriter(writer_dir, prefix="whale_events", rotate_minutes=1)
    metrics_writer = RollingParquetWriter(writer_dir, prefix="whale_metrics", rotate_minutes=1)
    tracker = WhaleTracker(
        threshold_ltc=50.0,
        top_n=2,
        event_writer=event_writer,
        metrics_writer=metrics_writer,
        time_fn=clock,
    )

    tracker.process_mempool_tx(_mk_tx("tx1", 60.0, inputs=2, outputs=2), seen_ms=base_ms)
    metrics = tracker.tick(base_ms)
    assert metrics["timestamp"] % 1000 == 0
    assert metrics["whale_tx_count_10m"] == 1
    assert metrics["whale_total_value_ltc_10m"] == pytest.approx(60.0)
    assert metrics["whale_avg_value_ltc_10m"] == pytest.approx(60.0)
    assert metrics["whale_max_value_ltc_10m"] == pytest.approx(60.0)
    assert metrics["whale_topN_sum_ltc_10m"] == pytest.approx(60.0)
    assert metrics["threshold_ltc_effective"] == pytest.approx(50.0)

    # Values below threshold are ignored.
    tracker.process_mempool_tx(_mk_tx("tiny", 5.0), seen_ms=base_ms + 500)
    metrics = tracker.tick(base_ms + 500)
    assert metrics["whale_tx_count_10m"] == 1

    # Add a second whale 30 seconds later.
    clock.set(base_ms + 30_000)
    tracker.process_mempool_tx(_mk_tx("tx2", 80.0, inputs=3, outputs=4), seen_ms=clock())
    metrics = tracker.tick(clock())
    assert metrics["whale_tx_count_10m"] == 2
    assert metrics["whale_total_value_ltc_10m"] == pytest.approx(140.0)
    assert metrics["whale_avg_value_ltc_10m"] == pytest.approx(70.0)
    assert metrics["whale_max_value_ltc_10m"] == pytest.approx(80.0)
    assert metrics["whale_topN_sum_ltc_10m"] == pytest.approx(140.0)

    # Confirmation should not double-count but should update block height metadata.
    block = {"height": 2_500_000, "tx": [_mk_tx("tx1", 60.0)]}
    clock.set(base_ms + 40_000)
    tracker.process_block(block, seen_ms=clock())
    metrics = tracker.tick(clock())
    assert metrics["whale_tx_count_10m"] == 2

    # After 10 minutes the first whale falls out of the window.
    clock.set(base_ms + 601_000)
    metrics = tracker.tick(clock())
    assert metrics["whale_tx_count_10m"] == 1
    assert metrics["whale_total_value_ltc_10m"] == pytest.approx(80.0)

    # And eventually the buffer empties entirely.
    clock.set(base_ms + 661_000)
    metrics = tracker.tick(clock())
    assert metrics["whale_tx_count_10m"] == 0
    assert metrics["whale_total_value_ltc_10m"] == 0.0
    assert metrics["whale_max_value_ltc_10m"] == 0.0

    tracker.close()

    event_files = sorted(writer_dir.glob("whale_events_*.parquet"))
    assert event_files, "whale event parquet should be written"
    events_df = pd.read_parquet(event_files[0])
    assert {"txid", "timestamp_ms", "value_ltc_total", "inputs_count", "outputs_count", "block_height", "first_seen_ms"} <= set(events_df.columns)
    assert set(events_df["txid"]) == {"tx1", "tx2"}
    tx1_rows = events_df[events_df["txid"] == "tx1"]
    assert (tx1_rows["block_height"] == -1).any()
    assert tx1_rows["block_height"].iloc[-1] == 2_500_000

    metrics_files = sorted(writer_dir.glob("whale_metrics_*.parquet"))
    assert metrics_files, "per-second whale metrics parquet should be written"
    metrics_df = pd.read_parquet(metrics_files[0])
    expected_cols = {
        "timestamp",
        "whale_tx_count_10m",
        "whale_total_value_ltc_10m",
        "whale_avg_value_ltc_10m",
        "whale_max_value_ltc_10m",
        "whale_topN_sum_ltc_10m",
        "threshold_ltc_effective",
    }
    assert expected_cols <= set(metrics_df.columns)
    assert (metrics_df["timestamp"] % 1000 == 0).all()
