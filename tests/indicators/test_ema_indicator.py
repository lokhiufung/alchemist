import pytest
from datetime import datetime, timedelta

from alchemist.datas.bar_data import BarData
from alchemist.indicators.ema_indicator import EmaIndicator


@pytest.fixture
def bar_data(freq='1m', max_len=50):
    return BarData(max_len=max_len, freq=freq)


def _add_closes(bar_data: BarData, closes):
    base_ts = datetime(2024, 1, 1, 9, 30)
    for i, close in enumerate(closes):
        ts = base_ts + timedelta(minutes=i)
        bar_data.on_bar_update(ts, close, close, close, close, volume=1000)


def test_ema_indicator(bar_data):
    closes = [10.0, 20.0, 30.0]
    period = 2
    indicator = EmaIndicator(bar_data.close, min_period=period, ts_line=bar_data.ts)
    _add_closes(bar_data, closes)

    assert len(indicator.ema) == 2  # one value after seeding, one after update
    first_expected = sum(closes[:period]) / period
    second_expected = (closes[2] - first_expected) * (2 / (period + 1)) + first_expected
    assert indicator.ema[0] == pytest.approx(first_expected, rel=1e-6)
    assert indicator.ema[-1] == pytest.approx(second_expected, rel=1e-6)
