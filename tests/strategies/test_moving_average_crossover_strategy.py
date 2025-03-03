# tests/strategies/test_moving_average_crossover_strategy.py

import pytest
import ray
from unittest.mock import MagicMock
from datetime import datetime, timedelta

from alchemist.strategies.moving_average_crossover_strategy.moving_average_crossover_strategy import MovingAverageCrossoverStrategy
from alchemist.products.stock_product import StockProduct
from alchemist.datas.common import Bar
from alchemist.data_card import DataCard
from alchemist.order import Order


# Ensure Ray is initialized via conftest.py


@pytest.fixture
def sample_product():
    """Fixture to create a sample StockProduct."""
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
def moving_average_crossover_strategy(sample_product):
    """Fixture to initialize the MovingAverageCrossoverStrategy actor with a mocked OrderManager."""
    strategy = MovingAverageCrossoverStrategy.remote(
        name="ma_crossover_strategy",
        zmq_send_port=5555,
        zmq_recv_ports=[5556],
        products=[sample_product],
        data_cards=[DataCard(product=sample_product, freq='5m', aggregation='ohlcv')],
        max_len=1000,
        params={
            "fast_period": 5,
            "slow_period": 20
        },
        monitor_actor=None,
    )

    ray.get(strategy.update_balance.remote('USD', 10000))

    return strategy


def test_initialization(moving_average_crossover_strategy):
    """
    Test that the MovingAverageCrossoverStrategy initializes correctly.
    """
    # Retrieve strategy parameters via a remote method
    params = ray.get(moving_average_crossover_strategy.get_params.remote())
    assert params["fast_period"] == 5
    assert params["slow_period"] == 20


def test_buy_signal(moving_average_crossover_strategy, sample_product, sample_bar):
    """
    Test that a buy signal is generated when fast MA crosses above slow MA.
    """
    # Simulate adding bars to build up fast and slow MAs
    for i in range(21):
        # Create synthetic bars where fast MA will eventually cross above slow MA
        # Introducing a jump at the 15th bar to trigger crossover
        if i == 20:
            # simulate a penatration bar
            close_price = 150
        elif i == 19:
            close_price = 50.0
        else:
            close_price = 100.0 + i

        bar = Bar(
            ts=sample_bar.ts + timedelta(minutes=i),
            open_=close_price - 1,
            high=close_price + 1,
            low=close_price - 2,
            close=close_price,
            volume=1000
        )
        ray.get(moving_average_crossover_strategy._on_bar.remote(
            'test_gateway', sample_product.exch, sample_product.name, '5m', bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume
        ))

    retrieved_orders = ray.get(moving_average_crossover_strategy.get_submitted_orders.remote())

    # Assert that one buy order was submitted
    buy_orders = [order for order in retrieved_orders.values() if order.side == 1]
    assert len(buy_orders) == 1

    # Verify the details of the buy order
    buy_order = buy_orders[0]
    assert buy_order.gateway == 'mock_gateway'
    assert buy_order.product.name == sample_product.name
    assert buy_order.price == 150.0  # Price at crossover
    assert buy_order.size == 1  # Assuming size=1
    assert buy_order.order_type == 'MARKET'
    assert buy_order.time_in_force == 'GTC'


def test_sell_signal(moving_average_crossover_strategy, sample_product, sample_bar):
    """
    Test that a sell signal is generated when fast MA crosses below slow MA.
    """
    # Generate initial bars to trigger buy signal
    for i in range(21):
        # Introduce a jump at the 15th bar to trigger crossover
        if i == 15:
            close_price = 150.0  # Jump to ensure fast MA crosses above slow MA
        elif i == 20:
            close_price = 40.0
        else:
            close_price = 100.0 - i

        bar = Bar(
            ts=sample_bar.ts + timedelta(minutes=i),
            open_=close_price - 1,
            high=close_price + 1,
            low=close_price - 2,
            close=close_price,
            volume=1000
        )
        ray.get(moving_average_crossover_strategy._on_bar.remote(
            'test_gateway', sample_product.exch, sample_product.name, '5m', bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume
        ))

    # Retrieve submitted orders
    retrieved_orders = ray.get(moving_average_crossover_strategy.get_submitted_orders.remote())

    # Assert that one sell order was submitted
    sell_orders = [order for order in retrieved_orders.values() if order.side == -1]
    assert len(sell_orders) == 1

    # Verify the details of the sell order
    sell_order = sell_orders[0]
    assert sell_order.gateway == 'mock_gateway'
    assert sell_order.product.name == sample_product.name
    assert sell_order.price == 40.0  # Price at crossover
    assert sell_order.size == 1  # Assuming size=1
    assert sell_order.order_type == 'MARKET'
    assert sell_order.time_in_force == 'GTC'


def test_no_signal(moving_average_crossover_strategy, sample_product, sample_bar):
    """
    Test that no signal is generated when there is no crossover.
    """
    # Simulate a consistent upward trend without any crossover
    for i in range(20):
        close_price = 100.0 + i  # Gradually increasing without any jumps

        bar = Bar(
            ts=sample_bar.ts + timedelta(minutes=i),
            open_=close_price - 1,
            high=close_price + 1,
            low=close_price - 2,
            close=close_price,
            volume=1000
        )
        ray.get(moving_average_crossover_strategy._on_bar.remote(
            'test_gateway', sample_product.exch, sample_product.name, '5m', bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume
        ))

    # Retrieve submitted orders
    retrieved_orders = ray.get(moving_average_crossover_strategy.get_submitted_orders.remote())

    # Assert that no orders were submitted
    assert len(retrieved_orders) == 0