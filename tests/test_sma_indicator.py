import datetime

import pytest

from alchemist.datas.tick_data import TickData
from alchemist.datas.bar_data import BarData
from alchemist.indicators.sma_indicator import SmaIndicator


@pytest.mark.parametrize(
        'period, ticks, expected_mus, expected_len',
        [
            (
                2,
                [
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0), 'price': 100.1, 'size': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1), 'price': 100.1, 'size': 1},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1, 250000), 'price': 120.1, 'size': 1},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1, 500000), 'price': 90.1, 'size': 1},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2), 'price': 98.1, 'size': 2},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2, 250000), 'price': 102.1, 'size': 1},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2, 500000), 'price': 88.1, 'size': 1},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2, 750000), 'price': 90.1, 'size': 1},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3), 'price': 90.1, 'size': 2},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3, 250000), 'price': 95.1, 'size': 2},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3, 500000), 'price': 85.1, 'size': 2},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3, 750000), 'price': 88.1, 'size': 1},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4), 'price': 88.1, 'size': 3},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4, 250000), 'price': 92.1, 'size': 2},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4, 500000), 'price': 82.1, 'size': 2},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4, 750000), 'price': 85.1, 'size': 2}
                ],
                [95.1, 90.1, 89.1],
                3
            )
        ]
)
def test_on_tick_update(period, ticks, expected_mus, expected_len):
    ...
    # """
    # Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 0), open_=100.1, high=100.1, low=100.1, close=100.1, volume=10),
    # Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 1), open_=100.1, high=120.1, low=90.1, close=90.1, volume=3),
    # Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 2), open_=98.1, high=102.1, low=88.1, close=90.1, volume=5),
    # Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 3), open_=90.1, high=95.1, low=85.1, close=88.1, volume=7),
    # """
    # datas = [BarData(max_len=10000, freq='1s')]
    # indicator = SmaIndicator(datas[0], min_period=period)

    # for tick in ticks:
    #     datas[0].on_tick_update(ts=tick['ts'], price=tick['price'], size=tick['size'])

    # assert list(indicator.sma.values) == expected_mus
    # assert len(indicator.sma) == expected_len


@pytest.mark.parametrize(
        'period, bars, expected_mus, expected_len',
        [
            (
                5,
                [
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 0), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3), 'open': 90.1, 'high': 95.1, 'low': 85.1, 'close': 88.1, 'volume': 7},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4), 'open': 88.1, 'high': 92.1, 'low': 82.1, 'close': 85.1, 'volume': 9},
                ],
                [92.3],
                1
            ),
            (
                5,
                [
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 0), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3), 'open': 90.1, 'high': 95.1, 'low': 85.1, 'close': 88.1, 'volume': 7},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4), 'open': 88.1, 'high': 92.1, 'low': 82.1, 'close': 85.1, 'volume': 9},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 5), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 6), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 7), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 8), 'open': 90.1, 'high': 95.1, 'low': 85.1, 'close': 88.1, 'volume': 7},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 9), 'open': 88.1, 'high': 92.1, 'low': 82.1, 'close': 85.1, 'volume': 9},
                ],
                [92.3, 92.3, 92.3, 92.3, 92.3, 92.3],
                6
            )
        ],
    )
def test_on_bar_update(period, bars, expected_mus, expected_len):
    datas = [BarData(max_len=10000, freq='1s')]
    indicator = SmaIndicator(datas[0].close, min_period=period, ts_line=datas[0].ts)

    for bar in bars:
        datas[0].on_bar_update(ts=bar['ts'], open_=bar['open'], high=bar['high'], low=bar['low'], close=bar['close'], volume=bar['volume'])

    assert list(indicator.sma.values) == expected_mus
    assert len(indicator.sma) == expected_len
        
        



        
