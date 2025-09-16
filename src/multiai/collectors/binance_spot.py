from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
def collect_tick_snapshot(orderbook, trades) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    row = {"timestamp": int(now.timestamp()*1000)}
    return pd.DataFrame([row])
