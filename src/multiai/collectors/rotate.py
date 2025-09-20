from __future__ import annotations
import time, math
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class RollingParquetWriter:
    def __init__(self, out_dir: str|Path, prefix: str, rotate_minutes: int = 90, flush_rows: int = 250):
        self.out_dir = Path(out_dir); self.out_dir.mkdir(parents=True, exist_ok=True)
        self.prefix = prefix; self.rotate_s = rotate_minutes*60
        self.flush_rows = flush_rows
        self._rows = []
        self._start_epoch = None
        self._writer = None

    def _rotate_needed(self, ts_ms: int) -> bool:
        if self._start_epoch is None: return True
        return (ts_ms//1000) - self._start_epoch >= self.rotate_s

    def _start_new_file(self, ts_ms: int):
        self._close()
        self._start_epoch = (ts_ms//1000)
        ts = time.gmtime(self._start_epoch)
        name = f"{self.prefix}_{ts.tm_year:04d}{ts.tm_mon:02d}{ts.tm_mday:02d}_{ts.tm_hour:02d}{ts.tm_min:02d}.parquet"
        self._path = self.out_dir/name
        self._writer = None

    def write_row(self, row: dict):
        ts_ms = int(row["timestamp"])
        if self._rotate_needed(ts_ms): self._start_new_file(ts_ms)
        self._rows.append(row)
        if len(self._rows) >= self.flush_rows: self._flush()

    def _flush(self):
        if not self._rows: return
        df = pd.DataFrame(self._rows)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self._path, table.schema)
        self._writer.write_table(table)
        self._rows.clear()

    def _close(self):
        if self._writer is not None:
            self._writer.close(); self._writer = None
        self._rows.clear()

    def close(self):
        self._flush(); self._close()
