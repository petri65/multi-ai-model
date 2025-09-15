import pandas as pd
from multiai.dataops.quantize import quantize_to_1s

def test_quantize_forward_snap_and_latest_kept(tmp_path):
    df = pd.DataFrame({
        'timestamp': pd.to_datetime([
            '2025-01-01T00:00:00.100Z',
            '2025-01-01T00:00:00.900Z',  # same second, later -> should win
            '2025-01-01T00:00:01.050Z',
        ], utc=True),
        'v': [1, 2, 3]
    })
    p = tmp_path / 'in.parquet'
    df.to_parquet(p, index=False)
    out = quantize_to_1s(p)
    assert list(out['timestamp'].dt.floor('s')) == [
        pd.Timestamp('2025-01-01T00:00:01Z'),
        pd.Timestamp('2025-01-01T00:00:02Z'),
    ]
    assert out['v'].tolist() == [2,3]
