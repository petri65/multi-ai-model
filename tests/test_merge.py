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
    assert not m.isna().any().any()
    assert list(m.columns)[0] == 'timestamp'


def test_merge_with_whales():
    ts = pd.to_datetime(['2025-01-01T00:00:01Z', '2025-01-01T00:00:02Z'], utc=True)
    off = pd.DataFrame({
        'timestamp': ts,
        'b_off': [20, 21],
    })
    on = pd.DataFrame({
        'timestamp': ts,
        'a_on': [10, 11],
    })
    whales = pd.DataFrame({
        'timestamp': ts,
        'whale_tx_count_10m': [0, 2],
        'whale_total_value_ltc_10m': [0.0, 140.0],
        'whale_avg_value_ltc_10m': [0.0, 70.0],
        'whale_max_value_ltc_10m': [0.0, 80.0],
        'whale_topN_sum_ltc_10m': [0.0, 140.0],
        'threshold_ltc_effective': [50.0, 50.0],
    })

    merged = merge_on_off(off, on, whales)

    for col in ['whale_tx_count_10m', 'whale_total_value_ltc_10m', 'whale_avg_value_ltc_10m']:
        assert col in merged.columns
    assert merged['whale_tx_count_10m'].tolist() == [0, 2]
    assert not merged.isna().any().any()
