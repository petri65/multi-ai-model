import pandas as pd
from multiai.dataops.quantize import quantize_to_1s

def test_forward_round_and_latest_kept():
    df = pd.DataFrame({
        "timestamp": [
            "2025-01-01T00:00:00.100Z",
            "2025-01-01T00:00:00.900Z",
            "2025-01-01T00:00:01.001Z",
        ],
        "val":[1,2,3]
    })
    out = quantize_to_1s(df, ts_col="timestamp")
    # 00.100 -> 01, 00.900 -> 01 (latest is 00.900 row), 01.001 -> 02
    assert len(out)==2
    assert out.iloc[0]["timestamp"].strftime("%H:%M:%S")=="00:00:01"
    assert out.iloc[0]["val"]==2
    assert out.iloc[1]["timestamp"].strftime("%H:%M:%S")=="00:00:02"
