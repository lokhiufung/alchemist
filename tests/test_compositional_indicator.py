import pytest
import datetime
from alchemist.datas.bar_data import BarData
from alchemist.indicators.pearson_r_indicator import PearsonRIndicator
from alchemist.indicators.vwap_indicator import DailyVwapIndicator


@pytest.mark.parametrize(
    'period, bars_1, bars_2, expected_pearson_r, expected_len',
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
            [
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 0), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3), 'open': 90.1, 'high': 95.1, 'low': 85.1, 'close': 88.1, 'volume': 7},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4), 'open': 88.1, 'high': 92.1, 'low': 82.1, 'close': 85.1, 'volume': 9},
            ],
            [1.0],  # Pearson's r should be 1 for identical data sets
            1
        ),
        (
            3,
            [
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 0), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3), 'open': 90.1, 'high': 95.1, 'low': 85.1, 'close': 88.1, 'volume': 7},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4), 'open': 88.1, 'high': 92.1, 'low': 82.1, 'close': 85.1, 'volume': 9},
            ],
            [
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 0), 'open': 110.1, 'high': 115.1, 'low': 105.1, 'close': 110.1, 'volume': 8},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1), 'open': 110.1, 'high': 130.1, 'low': 100.1, 'close': 108.1, 'volume': 4},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2), 'open': 108.1, 'high': 112.1, 'low': 98.1, 'close': 100.1, 'volume': 6},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3), 'open': 100.1, 'high': 105.1, 'low': 95.1, 'close': 98.1, 'volume': 7},
                {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4), 'open': 98.1, 'high': 102.1, 'low': 92.1, 'close': 95.1, 'volume': 9},
            ],
            [1.0, 1.0, 1.0],  # Pearson's r might be negative for inverse relations
            3
        )
    ]
)
def test_pearson_r_indicator(period, bars_1, bars_2, expected_pearson_r, expected_len):
    # Set up the two BarData instances
    data_1 = BarData(max_len=10000, freq='1s', index='data_1')
    data_2 = BarData(max_len=10000, freq='1s', index='data_2')
    
    # Create the PearsonRIndicator instance
    indicator = PearsonRIndicator(data_1.close, data_2.close, min_period=period, ts_line=data_2.ts)
    
    # Add bars to both BarData instances
    for bar_1, bar_2 in zip(bars_1, bars_2):
        data_1.on_bar_update(bar_1['ts'], bar_1['open'], bar_1['high'], bar_1['low'], bar_1['close'], bar_1['volume'])
        data_2.on_bar_update(bar_2['ts'], bar_2['open'], bar_2['high'], bar_2['low'], bar_2['close'], bar_2['volume'])
    
    # Check if the calculated Pearson R values match the expected ones
    assert len(indicator.pearson_r) == expected_len
    for i, expected_r in enumerate(expected_pearson_r):
        assert abs(indicator.pearson_r[i] - expected_r) < 0.01, f"Expected Pearson's r {expected_r}, got {indicator.pearson_r[i]}"


# @pytest.mark.parametrize()
# def test_divergence_indicator():
#     # Set up the two BarData instances
#     data_2 = BarData(max_len=10000, freq='1s', index='data_2')
#     data_1 = BarData(max_len=10000, freq='1s', index='data_1')
    
#     # Create the PearsonRIndicator instance
#     indicator = DivergenceIndicator({'period': 1}, data_1, data_2)
    
#     # Add bars to both BarData instances
#     for bar_1, bar_2 in zip(bars_1, bars_2):
#         data_1.update_from_bar(bar_1['ts'], bar_1['open'], bar_1['high'], bar_1['low'], bar_1['close'], bar_1['volume'])
#         data_2.update_from_bar(bar_2['ts'], bar_2['open'], bar_2['high'], bar_2['low'], bar_2['close'], bar_2['volume'])
    

def test_daily_vwap_indicator_calculation():
    data = BarData(max_len=10000, freq='1m', index='data_1')
    daily_vwap_indicator = DailyVwapIndicator(
        data.high,
        data.low,
        data.close,
        data.volume,
        ts_line=data.ts,
        start_hour=18,
    )
    
    daily_vwap_indicator.start_vwap_calculation = True
    daily_vwap_indicator.current_date = datetime.date(2024, 1, 1)

    bars = [
        {'ts': datetime.datetime(2024, 1, 1, 0, 0, 0), 'open': 100.1, 'high': 100.1, 'low': 100.1, 'close': 100.1, 'volume': 10},
        {'ts': datetime.datetime(2024, 1, 1, 0, 0, 1), 'open': 100.1, 'high': 120.1, 'low': 90.1, 'close': 98.1, 'volume': 3},
        {'ts': datetime.datetime(2024, 1, 1, 0, 0, 2), 'open': 98.1, 'high': 102.1, 'low': 88.1, 'close': 90.1, 'volume': 5},
        {'ts': datetime.datetime(2024, 1, 1, 0, 0, 3), 'open': 90.1, 'high': 95.1, 'low': 85.1, 'close': 88.1, 'volume': 7},
        {'ts': datetime.datetime(2024, 1, 1, 0, 0, 4), 'open': 88.1, 'high': 92.1, 'low': 82.1, 'close': 85.1, 'volume': 9},
    ]

    for bar in bars:
        data.on_bar_update(bar['ts'], bar['open'], bar['high'], bar['low'], bar['close'], bar['volume'])


    
    

    
    
