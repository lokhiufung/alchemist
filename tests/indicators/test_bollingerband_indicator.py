import math
import pytest
from datetime import datetime, timedelta

from alchemist.datas.bar_data import BarData
from alchemist.indicators.bollingerband_indicator import BollingerbandIndicator


@pytest.fixture
def bar_data(freq='1m', max_len=50):
    return BarData(max_len=max_len, freq=freq)


def _add_closes(bar_data: BarData, closes):
    base_ts = datetime(2024, 1, 1, 9, 30)
    for i, close in enumerate(closes):
        ts = base_ts + timedelta(minutes=i)
        bar_data.on_bar_update(ts, close, close, close, close, volume=1000)


def test_bollingerband_indicator(bar_data):
    closes = [10.0, 20.0]
    period = 2
    indicator = BollingerbandIndicator(bar_data.close, min_period=period, num_std_dev=2, ts_line=bar_data.ts)
    _add_closes(bar_data, closes)

    mean = sum(closes) / period
    variance = sum((x - mean) ** 2 for x in closes) / period
    std = math.sqrt(variance)

    assert indicator.top[-1] == pytest.approx(mean + 2 * std, rel=1e-6)
    assert indicator.bot[-1] == pytest.approx(mean - 2 * std, rel=1e-6)
    assert indicator.width[-1] == pytest.approx(4 * std, rel=1e-6)
