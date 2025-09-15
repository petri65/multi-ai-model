import pandas as pd
from multiai.dataops.merge_on_off import merge_on_off

def test_merge_no_nans(tmp_path):
    on = pd.DataFrame({
        'timestamp': pd.to_datetime(['2025-01-01T00:00:01Z','2025-01-01T00:00:02Z'], utc=True),
        'a_on':[10,11]
    })
    off = pd.DataFrame({
        'timestamp': pd.to_datetime(['2025-01-01T00:00:01Z','2025-01-01T00:00:02Z'], utc=True),
        'b_off':[20,21]
    })
    outp = tmp_path / 'm.parquet'
    merge_on_off(on, off, outp)
    m = pd.read_parquet(outp)
    assert m.isna().any().any() == False
    assert list(m.columns)[0] == 'timestamp'
