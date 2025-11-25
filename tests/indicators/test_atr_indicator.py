import pytest
from datetime import datetime, timedelta

from alchemist.datas.bar_data import BarData
from alchemist.indicators.atr_indicator import ATRIndicator


@pytest.fixture
def bar_data(freq='1m', max_len=50):
    return BarData(max_len=max_len, freq=freq)


def test_atr_indicator(bar_data):
    indicator = ATRIndicator(bar_data.high, bar_data.low, bar_data.close, min_period=2, ts_line=bar_data.ts)

    bars = [
        (10.0, 9.0, 9.5),   # high, low, close
        (12.0, 8.0, 11.0),  # tr = max(4, 12-9.5=2.5, 9.5-8=1.5) = 4
        (13.0, 11.0, 12.0), # tr = max(2, 13-11=2, 11-11=0) = 2
    ]
    base_ts = datetime(2024, 1, 1, 9, 30)
    for i, (high, low, close) in enumerate(bars):
        ts = base_ts + timedelta(minutes=i)
        bar_data.on_bar_update(ts, close, high, low, close, volume=1000)

    assert indicator.atr[0] == pytest.approx(4.0, rel=1e-6)
    assert indicator.atr[-1] == pytest.approx(3.0, rel=1e-6)
