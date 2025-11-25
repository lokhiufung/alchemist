"""
We will use pytest to structure our tests. The tests will cover the following scenarios:
1.	Initialization: Verify that the indicator initializes with the correct parameters and data lines.
2.	SMA Calculation with Sufficient Data: Ensure that the SMA is correctly calculated when enough data is present.
3.	SMA Update with New Data: Check that the SMA updates correctly as new bars are added.
4.	Handling Insufficient Data: Confirm that the indicator does not calculate the SMA when there isn’t enough data.
5.	Edge Cases: Test scenarios like non-consecutive bar timestamps or identical closing prices.
"""

# tests/indicators/test_sma_indicator.py

import pytest
from datetime import datetime, timedelta

from alchemist.datas.bar_data import BarData
from alchemist.datas.common import Bar
from alchemist.indicators.sma_indicator import SmaIndicator
from alchemist.data_line import DataLine
from alchemist.products.stock_product import StockProduct


@pytest.fixture
def sample_product():
    """Fixture to create a sample StockProduct."""
    return StockProduct(exch="NYSE", name="AAPL", base_currency="USD")

@pytest.fixture
def bar_data(max_len=10, freq='1m'):
    """Fixture to create a BarData instance."""
    return BarData(max_len=max_len, freq=freq)

@pytest.fixture
def sma_indicator(bar_data, min_period=5):
    """Fixture to create an SMAIndicator instance."""
    return SmaIndicator(bar_data.close, min_period=min_period, ts_line=bar_data.ts)

def test_sma_initialization(sma_indicator):
    """Test that SMAIndicator initializes correctly."""
    assert sma_indicator.min_period == 5
    assert hasattr(sma_indicator, 'sma')
    assert isinstance(sma_indicator.sma, DataLine)
    assert sma_indicator.sma.name == 'sma'
    assert len(sma_indicator.sma) == 0  # Initially empty

def test_sma_not_ready(sma_indicator, bar_data):
    """Test that SMAIndicator does not calculate SMA until enough data is present."""
    # Add fewer bars than the period
    for i in range(3):
        bar = Bar(
            ts=datetime(2024, 1, 1, 12, 0) + timedelta(minutes=i),
            open_=100.0 + i,
            high=101.0 + i,
            low=99.0 + i,
            close=100.0 + i,
            volume=1000
        )
        bar_data.on_bar_update(bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume)
    
    assert len(sma_indicator.sma) == 0  # Not enough data yet

def test_sma_calculation(sma_indicator, bar_data):
    """Test that SMAIndicator calculates SMA correctly after enough data."""
    # Add bars equal to the period
    for i in range(5):
        bar = Bar(
            ts=datetime(2024, 1, 1, 12, 0) + timedelta(minutes=i),
            open_=100.0 + i,
            high=101.0 + i,
            low=99.0 + i,
            close=100.0 + i,
            volume=1000
        )
        bar_data.on_bar_update(bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume)
    
    assert len(sma_indicator.sma) == 1
    expected_sma = (100 + 101 + 102 + 103 + 104) / 5
    assert sma_indicator.sma[-1] == expected_sma

def test_sma_update(sma_indicator, bar_data):
    """Test that SMAIndicator updates SMA correctly as new data comes in."""
    # Add initial bars to reach the period
    for i in range(5):
        bar = Bar(
            ts=datetime(2024, 1, 1, 12, 0) + timedelta(minutes=i),
            open_=100.0 + i,
            high=101.0 + i,
            low=99.0 + i,
            close=100.0 + i,
            volume=1000
        )
        bar_data.on_bar_update(bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume)
    
    assert len(sma_indicator.sma) == 1
    expected_sma = (100 + 101 + 102 + 103 + 104) / 5
    assert sma_indicator.sma[-1] == expected_sma
    
    # Add a new bar, SMA should update
    new_bar = Bar(
        ts=datetime(2024, 1, 1, 12, 5),
        open_=105.0,
        high=106.0,
        low=104.0,
        close=105.0,
        volume=1000
    )
    bar_data.on_bar_update(new_bar.ts, new_bar.open, new_bar.high, new_bar.low, new_bar.close, new_bar.volume)
    
    assert len(sma_indicator.sma) == 2
    new_expected_sma = (101 + 102 + 103 + 104 + 105) / 5
    assert sma_indicator.sma[-1] == new_expected_sma

def test_sma_nan_when_insufficient_data(sma_indicator, bar_data):
    """Test that SMAIndicator returns nan when not enough data is present."""
    # Add exactly one bar
    bar = Bar(
        ts=datetime(2024, 1, 1, 12, 0),
        open_=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1000
    )
    bar_data.on_bar_update(bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume)
    
    # Since period=5, no SMA should be calculated
    assert len(sma_indicator.sma) == 0
    
    # Optionally, depending on implementation, the indicator might append nan
    # Uncomment the following lines if your implementation does so
    # assert len(sma_indicator.sma) == 1
    # assert math.isnan(sma_indicator.sma[-1])

def test_sma_with_non_consecutive_bars(sma_indicator, bar_data):
    """Test SMAIndicator with non-consecutive bar timestamps."""
    # Add bars with gaps (e.g., missing minutes)
    bar1 = Bar(
        ts=datetime(2024, 1, 1, 12, 0),
        open_=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1000
    )
    bar2 = Bar(
        ts=datetime(2024, 1, 1, 12, 2),  # Skipped 12:01
        open_=101.0,
        high=102.0,
        low=100.0,
        close=101.0,
        volume=1000
    )
    bar3 = Bar(
        ts=datetime(2024, 1, 1, 12, 3),
        open_=102.0,
        high=103.0,
        low=101.0,
        close=102.0,
        volume=1000
    )
    bar4 = Bar(
        ts=datetime(2024, 1, 1, 12, 4),
        open_=103.0,
        high=104.0,
        low=102.0,
        close=103.0,
        volume=1000
    )
    bar5 = Bar(
        ts=datetime(2024, 1, 1, 12, 5),
        open_=104.0,
        high=105.0,
        low=103.0,
        close=104.0,
        volume=1000
    )
    
    bars = [bar1, bar2, bar3, bar4, bar5]
    for bar in bars:
        bar_data.on_bar_update(bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume)
    
    # Check if SMA was calculated correctly
    # Assuming it takes the last 5 closes: 100,101,102,103,104
    assert len(sma_indicator.sma) == 1
    expected_sma = (100 + 101 + 102 + 103 + 104) / 5
    assert sma_indicator.sma[-1] == expected_sma
