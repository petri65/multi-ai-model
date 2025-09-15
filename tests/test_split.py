import pandas as pd
from multiai.dataops.split_object_columns import split_object_columns_if_present

def test_split_creates_9_and_drops_original():
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(['2025-01-01T00:00:00Z']),
        'orderbook_bid': [[1,2,3,4,5,6,7,8,9]],
    })
    out = split_object_columns_if_present(df)
    assert 'orderbook_bid' not in out.columns
    cols = [c for c in out.columns if c.startswith('orderbook_bid_')]
    assert len(cols) == 9
