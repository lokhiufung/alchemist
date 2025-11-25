import math
import pytest
from datetime import datetime, timedelta

from alchemist.datas.bar_data import BarData
from alchemist.indicators.log_return_indicator import LogReturnIndicator


@pytest.fixture
def bar_data(freq='1m', max_len=50):
    return BarData(max_len=max_len, freq=freq)


def _add_closes(bar_data: BarData, closes):
    base_ts = datetime(2024, 1, 1, 9, 30)
    for i, close in enumerate(closes):
        ts = base_ts + timedelta(minutes=i)
        bar_data.on_bar_update(ts, close, close, close, close, volume=1000)


def test_log_return_indicator(bar_data):
    closes = [100.0, 110.0]
    indicator = LogReturnIndicator(bar_data.close, ts_line=bar_data.ts)
    _add_closes(bar_data, closes)

    expected_log_return = math.log(110.0 / 100.0)
    assert indicator.log_return[-1] == pytest.approx(expected_log_return, rel=1e-6)
