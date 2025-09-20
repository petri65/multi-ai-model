import pandas as pd
from multiai.dataops.split_object_columns import split_object_columns_if_present
from multiai.dataops.merge_on_off import merge_on_off

def test_split_object_and_merge_clean():
    off = pd.DataFrame({
        "timestamp":["2025-01-01T00:00:01Z","2025-01-01T00:00:02Z"],
        "orderbook_bid": ["[10.0, 9.9]", "[10.1, 10.0]"],
        "orderbook_ask": ["[10.2, 10.3]", "[10.25, 10.35]"]
    })
    on = pd.DataFrame({
        "timestamp":["2025-01-01T00:00:01Z","2025-01-01T00:00:02Z"],
        "mid_prices": ["[10.1, 10.15]","[10.17, 10.2]"]
    })
    off_s = split_object_columns_if_present(off, ["orderbook_bid","orderbook_ask"])
    on_s = split_object_columns_if_present(on, ["mid_prices"])
    merged = merge_on_off(off_s, on_s, ts_col="timestamp")
    assert not merged.isna().any().any()
    assert len(merged)==2
