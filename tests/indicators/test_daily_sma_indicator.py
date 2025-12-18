import math
from datetime import datetime, timedelta

import pytest

from alchemist.datas.bar_data import BarData
from alchemist.indicators.sma_indicator import DailySmaIndicator


@pytest.fixture
def bar_data():
    return BarData(max_len=20, freq='1m')


def _add_closes(bar_data: BarData, closes, start_ts: datetime):
    for i, close in enumerate(closes):
        ts = start_ts + timedelta(minutes=i)
        bar_data.on_bar_update(ts, close, close, close, close, volume=1000)


def test_daily_sma_within_single_day(bar_data: BarData):
    indicator = DailySmaIndicator(bar_data.close, period=3, ts_line=bar_data.ts)
    start = datetime(2024, 1, 1, 9, 30)

    _add_closes(bar_data, [10.0, 20.0, 30.0, 40.0], start)

    assert len(indicator.sma) == 4
    assert math.isnan(indicator.sma[0])
    assert math.isnan(indicator.sma[1])
    assert indicator.sma[2] == pytest.approx(20.0, rel=1e-6)
    assert indicator.sma[3] == pytest.approx(30.0, rel=1e-6)


def test_daily_sma_resets_each_day(bar_data: BarData):
    indicator = DailySmaIndicator(bar_data.close, period=2, ts_line=bar_data.ts)
    start = datetime(2024, 1, 1, 9, 30)

    _add_closes(bar_data, [100.0, 110.0], start)
    _add_closes(bar_data, [200.0, 220.0], start + timedelta(days=1))

    assert len(indicator.sma) == 4
    assert math.isnan(indicator.sma[0])
    assert indicator.sma[1] == pytest.approx(105.0, rel=1e-6)
    assert math.isnan(indicator.sma[2])
    assert indicator.sma[3] == pytest.approx(210.0, rel=1e-6)
