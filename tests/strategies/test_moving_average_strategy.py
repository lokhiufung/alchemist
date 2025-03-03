# tests/strategies/test_moving_average_strategy.py
import pytest
import ray
from unittest.mock import MagicMock, patch
from datetime import timedelta, datetime

from alchemist.strategies.moving_average_strategy.moving_average_strategy import MovingAverageStrategy
from alchemist.products.stock_product import StockProduct
from alchemist.datas.common import Bar
from alchemist.data_card import DataCard


# Ensure Ray is initialized via conftest.py


@pytest.fixture
def sample_product():
    """Fixture to create a sample BaseProduct."""
    return StockProduct(exch="NYSE", name="AAPL", base_currency="USD")


@pytest.fixture
def sample_bar():
    """Fixture to create a sample Bar."""
    return Bar(
        ts=datetime(2024, 1, 1, 12, 0),
        open_=100.0,
        high=105.0,
        low=95.0,
        close=102.0,
        volume=1000
    )


@pytest.fixture
def moving_average_strategy(sample_product):
    """Fixture to initialize the MovingAverageStrategy actor."""
    strategy = MovingAverageStrategy.remote(
        name="ma_strategy",
        zmq_send_port=5555,
        zmq_recv_ports=[5556],
        products=[sample_product],
        data_cards=[DataCard(product=sample_product, freq='5m', aggregation='ohlcv')],  # Assuming no data cards are needed for this test
        data_pipeline=None,
        max_len=1000,
        params={"period": 5},
        monitor_actor=None
    )

    ray.get(strategy.update_balance.remote('USD', 10000))

    return strategy


def test_initialization(moving_average_strategy):
    """
    Test that the MovingAverageStrategy initializes correctly.
    """
    # Retrieve internal state if accessible
    # For example, moving_average should have window=5
    # This depends on how the strategy exposes its state
    # Here, we assume there's a method to get internal state for testing purposes
    params = ray.get(moving_average_strategy.get_params.remote())
    assert params['period'] == 5  # default is 10, now it should be changed to 5 at initialization


def test_buy_signal(moving_average_strategy, sample_product, sample_bar):
    """
    Test that a buy signal is generated when price crosses above the moving average.
    """
    # Mock dependencies
    # Simulate adding bars below the moving average
    for i in range(5):
        bar = Bar(
            ts=sample_bar.ts + timedelta(minutes=i),
            open_=100.0,
            high=100.0,
            low=100.0,
            close=100.0 + i,  # Increasing close prices
            volume=1000
        )
        ray.get(moving_average_strategy._on_bar.remote(
            'mock_gateway', sample_product.exch, sample_product.name, '5m', bar.ts, bar.open, bar.high, bar.low, sample_bar.close, bar.volume
        ))

    # The 6th bar should cross above the moving average
    crossing_bar = Bar(
        ts=sample_bar.ts + timedelta(minutes=5),
        open_=105.0,
        high=105.0,
        low=100.0,
        close=110.0,
        volume=1500
    )
    ray.get(moving_average_strategy._on_bar.remote(
        'mock_gateway', sample_product.exch, sample_product.name, '5m', crossing_bar.ts, crossing_bar.open, crossing_bar.high, crossing_bar.low, crossing_bar.close, crossing_bar.volume
    ))

    # get all submitted orders and check
    submitted_orders = ray.get(moving_average_strategy.get_submitted_orders.remote())
    assert len(submitted_orders) == 1
    # Check if the order was submitted correctly
    submitted_order = list(submitted_orders.values())[0]
    assert submitted_order.gateway == 'mock_gateway'
    assert submitted_order.product.name == sample_product.name
    assert submitted_order.price == 110.0
    assert submitted_order.size == 1  # Assuming size=1
    assert submitted_order.order_type == 'MARKET'
    assert submitted_order.time_in_force == 'GTC'
    assert submitted_order.side == 1

def test_sell_signal(moving_average_strategy, sample_product, sample_bar):
    """
    Test that a sell signal is generated when price crosses below the moving average.
    """
    # Mock dependencies
    # First, create a position by simulating a buy signal
    buy_bar = Bar(
        ts=sample_bar.ts,
        open_=100.0,
        high=100.0,
        low=100.0,
        close=105.0,
        volume=1000
    )
    ray.get(moving_average_strategy._on_bar.remote(
            'mock_gateway', sample_product.exch, sample_product.name, '5m', buy_bar.ts, buy_bar.open, buy_bar.high, buy_bar.low, buy_bar.close, buy_bar.volume
        ))

    # Simulate bars above the moving average
    for i in range(5):
        bar = Bar(
            ts=buy_bar.ts + timedelta(minutes=i),
            open_=105.0,
            high=105.0,
            low=105.0,
            close=105.0 + i,  # Increasing close prices
            volume=1000
        )
        ray.get(moving_average_strategy._on_bar.remote(
            'mock_gateway', sample_product.exch, sample_product.name, '5m', bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume
        ))

    # The next bar should cross below the moving average
    crossing_bar = Bar(
        ts=buy_bar.ts + timedelta(minutes=5),
        open_=110.0,
        high=110.0,
        low=100.0,
        close=100.0,  # Dropping below the moving average
        volume=1500
    )
    ray.get(moving_average_strategy._on_bar.remote(
        'mock_gateway', sample_product.exch, sample_product.name, '5m', crossing_bar.ts, crossing_bar.open, crossing_bar.high, crossing_bar.low, crossing_bar.close, crossing_bar.volume
    ))

    # get all submitted orders and check
    submitted_orders = ray.get(moving_average_strategy.get_submitted_orders.remote())
    assert len(submitted_orders) == 1
    # Check if the order was submitted correctly
    submitted_order = list(submitted_orders.values())[0]
    assert submitted_order.gateway == 'mock_gateway'
    assert submitted_order.product.name == sample_product.name
    assert submitted_order.price == 100.0
    assert submitted_order.size == 1  # Assuming size=1
    assert submitted_order.order_type == 'MARKET'
    assert submitted_order.time_in_force == 'GTC'
    assert submitted_order.side == -1
        

def test_no_signal(moving_average_strategy, sample_product, sample_bar):
    """
    Test that no signal is generated when price does not cross the moving average.
    """
    # Mock dependencies
    # Simulate adding bars without crossing the moving average
    for i in range(5):
        bar = Bar(
            ts=sample_bar.ts + timedelta(minutes=i),
            open_=100.0,
            high=105.0,
            low=95.0,
            close=100.0 + i,  # Gradually increasing
            volume=1000
        )
        ray.get(moving_average_strategy._on_bar.remote(
            'mock_gateway', sample_product.exch, sample_product.name, '5m', bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume
        ))

    # The 6th bar does not cross the moving average
    stable_bar = Bar(
        ts=sample_bar.ts + timedelta(minutes=5),
        open_=105.0,
        high=105.0,
        low=100.0,
        close=104.0,  # Slight increase, but no crossing
        volume=1500
    )
    ray.get(moving_average_strategy._on_bar.remote(
        'mock_gateway', sample_product.exch, sample_product.name, '5m', stable_bar.ts, stable_bar.open, stable_bar.high, stable_bar.low, stable_bar.close, stable_bar.volume
    ))

    # get all submitted orders and check
    submitted_orders = ray.get(moving_average_strategy.get_submitted_orders.remote())
    assert len(submitted_orders) == 0
    