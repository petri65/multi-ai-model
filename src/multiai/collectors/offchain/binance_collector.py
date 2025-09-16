from datetime import datetime, timezone
import pandas as pd
def collect_snapshot() -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    return pd.DataFrame([{"timestamp": int(now.timestamp()*1000)}])
