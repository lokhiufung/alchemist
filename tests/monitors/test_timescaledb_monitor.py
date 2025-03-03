# tests/monitors/test_timescaledb_monitor.py

import pytest
import ray
from unittest.mock import MagicMock, patch
from datetime import datetime

from sqlalchemy.orm import Session

from alchemist.monitors.timescaledb_monitor import TimescaleDBMonitor
# from alchemist.timescaledb.models import Signal, Trade, Order, Position, Portfolio
# from alchemist.buffer import Buffer


@pytest.fixture
def timescaledb_monitor():
    """Fixture to create an instance of TimescaleDBMonitor with mocked dependencies."""
    # Initialize the TimescaleDBMonitor actor with a mock DB URL
    monitor = TimescaleDBMonitor.remote(
        db_url="postgresql://user:password@localhost:5432/test_db",
        flush_interval=1.0,  # Set a low flush interval for testing,
        is_test=True,
    )
    yield monitor


@pytest.fixture
def sample_signal():
    """Fixture to create a sample signal entry."""
    return {
        "strategy_name": "test_strategy",
        "ts": datetime.now(),
        "signal_name": "test_signal",
        "value": 1.23,
    }


@pytest.fixture
def sample_trade():
    """Fixture to create a sample trade entry."""
    return {
        "strategy_name": "test_strategy",
        "ts": datetime.utcnow(),
        "order_id": "order123",
        "product": "AAPL",
        "filled_price": 150.0,
        "filled_size": 10.0,
        "side": "buy",
    }


@pytest.fixture
def sample_order():
    """Fixture to create a sample order entry."""
    return {
        "strategy_name": "test_strategy",
        "ts": datetime.utcnow(),
        "order_id": "order123",
        "order_type": "market",
        "time_in_force": "GTC",
        "product": "AAPL",
        "status": "filled",
        "size": 10.0,
        "price": 150.0,
        "side": "buy",
    }


@pytest.fixture
def sample_position():
    """Fixture to create a sample position entry."""
    return {
        "strategy_name": "test_strategy",
        "ts": datetime.utcnow(),
        "product": "AAPL",
        "size": 10.0,
        "side": "long",
        "avg_price": 150.0,
        "realized_pnl": 50.0,
        "unrealized_pnl": 25.0,
    }


@pytest.fixture
def sample_portfolio():
    """Fixture to create a sample portfolio entry."""
    return {
        "strategy_name": "test_strategy",
        "ts": datetime.utcnow(),
        "balance": 10000.0,
        "margin_used": 500.0,
        "total_value": 10500.0,
    }


def test_log_signal(timescaledb_monitor, sample_signal):
    """Test that log_signal correctly adds a signal to the buffer."""
    timescaledb_monitor.log_signal.remote(
        strategy_name=sample_signal["strategy_name"],
        ts=sample_signal["ts"],
        signal_name=sample_signal["signal_name"],
        value=sample_signal["value"],
    )

    # Access the internal buffer (assuming Buffer has a method to get data for testing)
    buffer_data = ray.get(timescaledb_monitor._pop.remote("signals"))
    assert sample_signal in buffer_data, "Signal was not added to the buffer."


def test_log_trade(timescaledb_monitor, sample_trade):
    """Test that log_trade correctly adds a trade to the buffer."""
    timescaledb_monitor.log_trade.remote(
        strategy_name=sample_trade["strategy_name"],
        ts=sample_trade["ts"],
        oid=sample_trade["order_id"],
        pdt=sample_trade["product"],
        filled_price=sample_trade["filled_price"],
        filled_size=sample_trade["filled_size"],
        side=sample_trade["side"],
    )

    buffer_data = ray.get(timescaledb_monitor._pop.remote("trades"))
    assert sample_trade in buffer_data, "Trade was not added to the buffer."


def test_log_order(timescaledb_monitor, sample_order):
    """Test that log_order correctly adds an order to the buffer."""
    timescaledb_monitor.log_order.remote(
        strategy_name=sample_order["strategy_name"],
        ts=sample_order["ts"],
        oid=sample_order["order_id"],
        order_type=sample_order["order_type"],
        time_in_force=sample_order["time_in_force"],
        pdt=sample_order["product"],
        status=sample_order["status"],
        size=sample_order["size"],
        price=sample_order["price"],
        side=sample_order["side"],
    )

    buffer_data = ray.get(timescaledb_monitor._pop.remote("orders"))
    assert sample_order in buffer_data, "Order was not added to the buffer."


def test_log_position(timescaledb_monitor, sample_position):
    """Test that log_position correctly adds a position to the buffer."""
    timescaledb_monitor.log_position.remote(
        strategy_name=sample_position["strategy_name"],
        ts=sample_position["ts"],
        pdt=sample_position["product"],
        size=sample_position["size"],
        side=sample_position["side"],
        avg_price=sample_position["avg_price"],
        realized_pnl=sample_position["realized_pnl"],
        unrealized_pnl=sample_position["unrealized_pnl"],
    )

    buffer_data = ray.get(timescaledb_monitor._pop.remote("positions"))
    assert sample_position in buffer_data, "Position was not added to the buffer."


def test_log_portfolio(timescaledb_monitor, sample_portfolio):
    """Test that log_portfolio correctly adds a portfolio to the buffer."""
    timescaledb_monitor.log_portfolio.remote(
        strategy_name=sample_portfolio["strategy_name"],
        ts=sample_portfolio["ts"],
        balance=sample_portfolio["balance"],
        margin_used=sample_portfolio["margin_used"],
        total_value=sample_portfolio["total_value"],
    )

    buffer_data = ray.get(timescaledb_monitor._pop.remote("portfolios"))
    assert sample_portfolio in buffer_data, "Portfolio was not added to the buffer."

