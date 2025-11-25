import math
from datetime import datetime, timedelta

import pytest

from alchemist.datas.bar_data import BarData
from alchemist.data_line import DataLine
from alchemist.indicators.rsi_indicator import RsiIndicator


@pytest.fixture
def bar_data(max_len=50, freq='1m'):
    return BarData(max_len=max_len, freq=freq)


@pytest.fixture
def rsi_indicator(bar_data, min_period=5):
    return RsiIndicator(bar_data.close, min_period=min_period, ts_line=bar_data.ts)


def _add_closes(bar_data, closes):
    base_ts = datetime(2024, 1, 1, 9, 30)
    for index, close in enumerate(closes):
        ts = base_ts + timedelta(minutes=index)
        bar_data.on_bar_update(ts, close, close, close, close, volume=1000)


def _expected_rsi(closes, period):
    recent = closes[-(period + 1):]
    gains = []
    losses = []
    for previous, current in zip(recent[:-1], recent[1:]):
        delta = current - previous
        if delta > 0:
            gains.append(delta)
            losses.append(0.0)
        elif delta < 0:
            gains.append(0.0)
            losses.append(-delta)
        else:
            gains.append(0.0)
            losses.append(0.0)

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0.0 and avg_gain == 0.0:
        return 50.0
    if avg_loss == 0.0:
        return 100.0
    if avg_gain == 0.0:
        return 0.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def test_rsi_initialization(rsi_indicator):
    assert rsi_indicator.min_period == 5
    assert hasattr(rsi_indicator, 'rsi')
    assert isinstance(rsi_indicator.rsi, DataLine)
    assert len(rsi_indicator.rsi) == 0


def test_rsi_requires_additional_bar(rsi_indicator, bar_data):
    closes = [100, 101, 102, 103, 104]
    _add_closes(bar_data, closes)

    assert len(rsi_indicator.rsi) == 1
    assert math.isnan(rsi_indicator.rsi[-1])


def test_rsi_calculates_expected_value(rsi_indicator, bar_data):
    closes = [44.0, 44.15, 43.9, 44.35, 44.9, 45.6]
    _add_closes(bar_data, closes)

    expected = _expected_rsi(closes, period=5)
    assert rsi_indicator.rsi[-1] == pytest.approx(expected, rel=1e-6)


def test_rsi_hits_upper_bound_when_all_gains(rsi_indicator, bar_data):
    closes = [100, 101, 102, 103, 104, 105]
    _add_closes(bar_data, closes)

    assert rsi_indicator.rsi[-1] == pytest.approx(100.0)


def test_rsi_hits_lower_bound_when_all_losses(rsi_indicator, bar_data):
    closes = [105, 104, 103, 102, 101, 100]
    _add_closes(bar_data, closes)

    assert rsi_indicator.rsi[-1] == pytest.approx(0.0)


def test_rsi_returns_midpoint_when_no_change(rsi_indicator, bar_data):
    closes = [100, 100, 100, 100, 100, 100]
    _add_closes(bar_data, closes)

    assert rsi_indicator.rsi[-1] == pytest.approx(50.0)
