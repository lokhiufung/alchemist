from datetime import datetime, timedelta

import pytest

from alchemist.datas.bar_data import BarData
from alchemist.indicators.running_max_indicator import DailyRunningMaxIndicator


@pytest.fixture
def bar_data():
    return BarData(max_len=20, freq='1m')


@pytest.fixture
def running_max_indicator(bar_data: BarData):
    return DailyRunningMaxIndicator(bar_data.close, ts_line=bar_data.ts)


def _add_closes(bar_data: BarData, closes, start_ts: datetime):
    for i, close in enumerate(closes):
        ts = start_ts + timedelta(minutes=i)
        bar_data.on_bar_update(ts, close, close, close, close, volume=1000)


def test_running_max_requires_ts_line(bar_data: BarData):
    with pytest.raises(ValueError):
        DailyRunningMaxIndicator(bar_data.close, ts_line=None)


def test_running_max_tracks_high_within_day(running_max_indicator: DailyRunningMaxIndicator, bar_data: BarData):
    closes = [100.0, 105.0, 103.0, 110.0]
    _add_closes(bar_data, closes, datetime(2024, 1, 1, 9, 30))

    assert len(running_max_indicator.daily_max) == len(closes)
    assert running_max_indicator.daily_max[0] == pytest.approx(100.0)
    assert running_max_indicator.daily_max[1] == pytest.approx(105.0)
    assert running_max_indicator.daily_max[2] == pytest.approx(105.0)
    assert running_max_indicator.daily_max[3] == pytest.approx(110.0)


def test_running_max_resets_each_day(running_max_indicator: DailyRunningMaxIndicator, bar_data: BarData):
    start = datetime(2024, 1, 1, 9, 30)
    _add_closes(bar_data, [100.0, 105.0], start)
    _add_closes(bar_data, [90.0, 95.0, 92.0], start + timedelta(days=1))

    assert len(running_max_indicator.daily_max) == 5
    assert running_max_indicator.daily_max[0] == pytest.approx(100.0)
    assert running_max_indicator.daily_max[1] == pytest.approx(105.0)
    assert running_max_indicator.daily_max[2] == pytest.approx(90.0)
    assert running_max_indicator.daily_max[3] == pytest.approx(95.0)
    assert running_max_indicator.daily_max[4] == pytest.approx(95.0)
