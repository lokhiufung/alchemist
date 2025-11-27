from datetime import datetime, timedelta

import pytest

from alchemist.datas.bar_data import BarData
from alchemist.indicators.open_price_indicator import OpenPriceIndicator


@pytest.fixture
def bar_data():
    return BarData(max_len=10, freq='1m')


def _push_bar(data: BarData, ts, open_, high, low, close, volume):
    data.on_bar_update(ts, open_, high, low, close, volume)


def test_open_price_tracks_first_bar_each_day(bar_data: BarData):
    indicator = OpenPriceIndicator(bar_data.open, ts_line=bar_data.ts)

    start = datetime(2024, 1, 1, 9, 30)
    bars = [
        (start, 100.0, 101.0, 99.0, 100.5, 1000),
        (start + timedelta(minutes=1), 101.0, 102.0, 100.0, 101.5, 1200),
        (start + timedelta(days=1), 110.0, 111.0, 109.0, 110.5, 1300),
        (start + timedelta(days=1, minutes=1), 111.0, 112.0, 110.0, 111.5, 1400),
    ]

    for ts, o, h, l, c, v in bars:
        _push_bar(bar_data, ts, o, h, l, c, v)

    expected = [100.0, 100.0, 110.0, 110.0]
    assert len(indicator.open_price) == len(expected)
    assert indicator.open_price[:] == expected


def test_open_price_requires_ts_line(bar_data: BarData):
    with pytest.raises(ValueError):
        OpenPriceIndicator(bar_data.open, ts_line=None)
