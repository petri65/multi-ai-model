import pandas as pd
import numpy as np
from multiai.pipeline.build_targets import run_build_targets

def test_build_targets_from_trade_price(tmp_path):
    ts = pd.date_range('2025-01-01', periods=8, freq='1s', tz='UTC')
    price = pd.Series([100,101,102,103,104,105,106,107], index=ts)
    df = pd.DataFrame({'timestamp': ts, 'trade_price': price.values})
    src = tmp_path / 'merged.parquet'
    out = tmp_path / 'targets.parquet'
    df.to_parquet(src, index=False)
    run_build_targets(str(src), str(out), price_col='trade_price', horizons=(1,2), verbose=True)
    t = pd.read_parquet(out)
    assert 'target_ret_1s' in t.columns and 'target_ret_2s' in t.columns
    assert len(t) == 6  # dropped last 2 rows
    # check first row values
    expected1 = np.log(101/100)
    expected2 = np.log(102/100)
    assert np.isclose(t.loc[0,'target_ret_1s'], expected1)
    assert np.isclose(t.loc[0,'target_ret_2s'], expected2)
