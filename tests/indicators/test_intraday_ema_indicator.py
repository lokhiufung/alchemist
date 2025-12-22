import math
from datetime import datetime, timedelta

import pytest

from alchemist.datas.bar_data import BarData
from alchemist.indicators.ema_indicator import IntradayEmaIndicator


@pytest.fixture
def bar_data():
    return BarData(max_len=20, freq='1m')


def _add_closes(bar_data: BarData, closes, start_ts: datetime):
    for i, close in enumerate(closes):
        ts = start_ts + timedelta(minutes=i)
        bar_data.on_bar_update(ts, close, close, close, close, volume=1000)


def test_intraday_ema_matches_shifted_series(bar_data: BarData):
    closes = [10.0, 20.0, 30.0, 40.0]
    indicator = IntradayEmaIndicator(bar_data.close, period=3, ts_line=bar_data.ts)
    _add_closes(bar_data, closes, datetime(2024, 1, 1, 9, 30))

    assert len(indicator.ema) == len(closes)
    assert math.isnan(indicator.ema[0])

    alpha = 2 / (3 + 1)
    expected_second = closes[0]
    expected_third = alpha * closes[1] + (1 - alpha) * expected_second
    expected_fourth = alpha * closes[2] + (1 - alpha) * expected_third

    assert indicator.ema[1] == pytest.approx(expected_second, rel=1e-6)
    assert indicator.ema[2] == pytest.approx(expected_third, rel=1e-6)
    assert indicator.ema[3] == pytest.approx(expected_fourth, rel=1e-6)


def test_intraday_ema_resets_each_day(bar_data: BarData):
    indicator = IntradayEmaIndicator(bar_data.close, period=2, ts_line=bar_data.ts)
    start = datetime(2024, 1, 1, 9, 30)

    _add_closes(bar_data, [100.0, 110.0], start)
    _add_closes(bar_data, [90.0, 80.0], start + timedelta(days=1))

    assert len(indicator.ema) == 4
    assert math.isnan(indicator.ema[0])
    assert indicator.ema[1] == pytest.approx(100.0, rel=1e-6)
    assert math.isnan(indicator.ema[2])
    assert indicator.ema[3] == pytest.approx(90.0, rel=1e-6)
