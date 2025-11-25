import pytest
from datetime import datetime, timedelta

from alchemist.datas.bar_data import BarData
from alchemist.indicators.std_indicator import StdIndicator


@pytest.fixture
def bar_data(freq='1m', max_len=50):
    return BarData(max_len=max_len, freq=freq)


def _add_closes(bar_data: BarData, closes):
    base_ts = datetime(2024, 1, 1, 9, 30)
    for i, close in enumerate(closes):
        ts = base_ts + timedelta(minutes=i)
        bar_data.on_bar_update(ts, close, close, close, close, volume=1000)


def test_std_indicator(bar_data):
    closes = [1.0, 2.0, 3.0]
    period = 3
    indicator = StdIndicator(bar_data.close, min_period=period, ts_line=bar_data.ts)
    _add_closes(bar_data, closes)

    expected_std = pytest.approx(0.8164965809, rel=1e-6)  # np.std population
    assert indicator.std[-1] == expected_std
