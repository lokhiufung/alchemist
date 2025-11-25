import math
from datetime import datetime, timedelta

import pytest

from alchemist.datas.bar_data import BarData
from alchemist.indicators.vwap_indicator import DailyVwapIndicator


@pytest.fixture
def bar_data(freq='1m', max_len=50):
    return BarData(max_len=max_len, freq=freq)


def _add_bars(bar_data: BarData, bars):
    for i, bar in enumerate(bars):
        ts = bar['ts']
        bar_data.on_bar_update(
            ts,
            bar['open'],
            bar['high'],
            bar['low'],
            bar['close'],
            bar['volume'],
        )


def test_daily_vwap_indicator(bar_data):
    # Start at 6:00 so VWAP accumulation begins immediately
    bars = [
        {'ts': datetime(2024, 1, 1, 6, 0, 0), 'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.0, 'volume': 10},
        {'ts': datetime(2024, 1, 1, 6, 1, 0), 'open': 101.0, 'high': 102.0, 'low': 100.0, 'close': 101.0, 'volume': 20},
        {'ts': datetime(2024, 1, 1, 6, 2, 0), 'open': 99.0, 'high': 100.0, 'low': 98.0, 'close': 99.5, 'volume': 30},
    ]

    indicator = DailyVwapIndicator(
        bar_data.high,
        bar_data.low,
        bar_data.close,
        bar_data.volume,
        ts_line=bar_data.ts,
        start_hour=6,
    )

    _add_bars(bar_data, bars)

    # Compute expected VWAPs
    expected = []
    cum_vol = 0.0
    cum_vol_price = 0.0
    for bar in bars:
        typical_price = (bar['high'] + bar['low'] + bar['close']) / 3
        cum_vol += bar['volume']
        cum_vol_price += typical_price * bar['volume']
        expected.append(cum_vol_price / cum_vol)

    assert len(indicator.vwap) == len(expected)
    for got, exp in zip(indicator.vwap, expected):
        assert got == pytest.approx(exp, rel=1e-6)

