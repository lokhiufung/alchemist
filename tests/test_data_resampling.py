import datetime

import pytest

from alchemist.datas.bar_data import BarData
from alchemist.datas.common import Bar
from alchemist.datas.resampler import TimeBarResampler
from alchemist.datas.frequency import Frequency


@pytest.mark.parametrize(
        'ticks, expected_bars, expected_len',
        [
            (
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
                [
                    Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 0), open_=100.1, high=100.1, low=100.1, close=100.1, volume=10),
                    Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 1), open_=100.1, high=120.1, low=90.1, close=90.1, volume=3),
                    Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 2), open_=98.1, high=102.1, low=88.1, close=90.1, volume=5),
                    Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 3), open_=90.1, high=95.1, low=85.1, close=88.1, volume=7),
                ],
                4
            )
        ]
)
def test_resample_tick_to_sec_bar(ticks, expected_bars, expected_len):
    """
    Test resampling of high-frequency tick data into 1-second bars.

    Scenario:
      - Provide a list of tick records with timestamps, prices, and sizes.
      - Use TimeBarResampler to aggregate the tick data into second-level bars.
    
    Expected Outcome:
      - The resulting BarData object has the correct OHLCV values 
        and matches the expected list of bars.
      - The final length of the bars list equals the expected length.
    """
    frequency = Frequency(freq='1s')
    data = BarData(max_len=10000, freq=frequency.freq)
    resampler = TimeBarResampler(frequency=frequency)

    for tick in ticks:
        resampler.on_tick_update(freq=frequency.freq, ts=tick['ts'], price=tick['price'], size=tick['size'])
        resampled_bar = resampler.flush()
        if resampled_bar:
            data.on_bar_update(
                ts=resampled_bar.ts,
                open_=resampled_bar.open,
                high=resampled_bar.high,
                low=resampled_bar.low,
                close=resampled_bar.close,
                volume=resampled_bar.volume
            )

    assert len(data) == expected_len
    # checking if the bars are the same as expected
    for bar, expected_bar in zip(data, expected_bars):
        assert bar.open == expected_bar.open
        assert bar.high == expected_bar.high
        assert bar.low == expected_bar.low
        assert bar.close == expected_bar.close
        assert bar.volume == expected_bar.volume


def test_resample_hour_bar_to_day_bar():
    """
    Ensure 1-hour bars roll up into a single 1-day bar with correct
    open/high/low/close/volume and day alignment.
    """
    target_frequency = Frequency(freq='1d')
    data = BarData(max_len=10, freq='1d')
    resampler = TimeBarResampler(frequency=target_frequency)

    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    bars = []
    for hour in range(24):
        ts = start + datetime.timedelta(hours=hour)
        bars.append(
            {
                'ts': ts,
                'open': 100 + hour,
                'high': 101 + hour,
                'low': 99 + hour,
                'close': 100.5 + hour,
                'volume': 1000 + hour,
            }
        )

    for bar in bars:
        resampler.on_bar_update(
            freq='1h',
            ts=bar['ts'],
            open_=bar['open'],
            high=bar['high'],
            low=bar['low'],
            close=bar['close'],
            volume=bar['volume'],
        )
        resampled_bar = resampler.flush()
        if resampled_bar:
            data.on_bar_update(
                ts=resampled_bar.ts,
                open_=resampled_bar.open,
                high=resampled_bar.high,
                low=resampled_bar.low,
                close=resampled_bar.close,
                volume=resampled_bar.volume,
            )

    assert len(data) == 1
    bar = data[-1]
    assert bar.ts == datetime.datetime(2024, 1, 1, 0, 0, 0)
    assert bar.open == bars[0]['open']
    assert bar.high == max(b['high'] for b in bars)
    assert bar.low == min(b['low'] for b in bars)
    assert bar.close == bars[-1]['close']
    assert bar.volume == sum(b['volume'] for b in bars)


# test for resampling 5 sec bars into 1 min bars
@pytest.mark.parametrize(
        'bars, expected_bars, expected_len',
        [
            (
                [
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 0), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 5), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 10), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 15), 'open': 90.1, 'high': 95.1, 'low': 85.1, 'close': 88.1, 'volume': 7},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 20), 'open': 88.1, 'high': 92.1, 'low': 82.1, 'close': 85.1, 'volume': 9},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 25), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 30), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 35), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 40), 'open': 90.1, 'high': 95.1, 'low': 85.1, 'close': 88.1, 'volume': 7},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 45), 'open': 88.1, 'high': 92.1, 'low': 82.1, 'close': 85.1, 'volume': 9},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 50), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 55), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 1, 0), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                ],
                [
                    Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 0), open_=100.1, high=120.1, low=82.1, close=100.1, volume=88),
                ],
                1
            )
        ]
)
def test_resample_sec_bar_to_minute_bar(bars, expected_bars, expected_len):
    """
    Test rolling up second-level bars into minute-level bars.

    Scenario:
      - Feed 5-second bars into the resampler configured for 1-minute bars.
      - Check that OHLCV values are correctly aggregated over the entire minute period.
    
    Expected Outcome:
      - The BarData object contains minute-level bars whose 
        open, high, low, close, and volume match the aggregated values.
      - The total bar count equals the expected length.
    """
    frequency = Frequency(freq='1m')
    data = BarData(max_len=10000, freq='1m')
    resampler = TimeBarResampler(frequency=frequency)

    for bar in bars:
        # from 5s bars to 1m bars
        resampler.on_bar_update(freq='5s', ts=bar['ts'], open_=bar['open'], high=bar['high'], low=bar['low'], close=bar['close'], volume=bar['volume'])
        print(resampler.current_bar)
        print(resampler._buffer)
        resampled_bar = resampler.flush()
        if resampled_bar:
            data.on_bar_update(
                ts=resampled_bar.ts,
                open_=resampled_bar.open,
                high=resampled_bar.high,
                low=resampled_bar.low,
                close=resampled_bar.close,
                volume=resampled_bar.volume
            )

    assert len(data) == expected_len
    # checking if the bars are the same as expected
    for bar, expected_bar in zip(data, expected_bars):
        assert bar.open == expected_bar.open
        assert bar.high == expected_bar.high
        assert bar.low == expected_bar.low
        assert bar.close == expected_bar.close
        assert bar.volume == expected_bar.volume


@pytest.mark.parametrize(
        'bars, expected_bars, expected_len',
        [
            (
                [
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 0), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 5), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
                    {'ts': datetime.datetime(2024, 1, 1, 0, 0, 10), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
                ],
                [
                    Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 0), open_=100.1, high=100.1, low=100.1, close=100.1, volume=10),
                    Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 5), open_=100.1, high=120.1, low=90.1, close=98.1, volume=3),
                    Bar(ts=datetime.datetime(2024, 1, 1, 0, 0, 10), open_=98.1, high=102.1, low=88.1, close=90.1, volume=5),
                ],
                3
            )
        ]
)
def test_resample_sec_bar_to_sec_bar(bars, expected_bars, expected_len):
    """
    Test passing same-frequency bars (e.g., 5s to 5s) through the resampler.

    Scenario:
      - Provide bars already at a 5-second frequency.
      - Use TimeBarResampler configured for '5s' to see if it passes data through unchanged.
    
    Expected Outcome:
      - Each bar in the final BarData matches the original bar's OHLCV fields.
      - The length of BarData remains consistent with the input.
    """
    frequency = Frequency(freq='5s')
    data = BarData(max_len=10000, freq='5s')
    resampler = TimeBarResampler(frequency=frequency)

    for bar in bars:
        # from 5s bars to 5 sec bars
        resampler.on_bar_update(freq='5s', ts=bar['ts'], open_=bar['open'], high=bar['high'], low=bar['low'], close=bar['close'], volume=bar['volume'])
        resampled_bar = resampler.flush()
        if resampled_bar:
            data.on_bar_update(
                ts=resampled_bar.ts,
                open_=resampled_bar.open,
                high=resampled_bar.high,
                low=resampled_bar.low,
                close=resampled_bar.close,
                volume=resampled_bar.volume
            )

    assert len(data) == expected_len
    # checking if the bars are the same as expected
    for bar, expected_bar in zip(data, expected_bars):
        assert bar.open == expected_bar.open
        assert bar.high == expected_bar.high
        assert bar.low == expected_bar.low
        assert bar.close == expected_bar.close
        assert bar.volume == expected_bar.volume
