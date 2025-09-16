from datetime import datetime, timezone
import pandas as pd
def collect_mempool_snapshot(node) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    row = {"timestamp": int(now.timestamp()*1000)}
    return pd.DataFrame([row])
