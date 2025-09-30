import pytest
from unittest.mock import Mock, MagicMock
import pandas as pd
from alchemist.managers.backtest_manager import BacktestManager
from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.order import Order
from alchemist.products.stock_product import StockProduct
from alchemist.commission import Commission
from alchemist.datas.common import Bar

@pytest.fixture
def portfolio_manager():
    pm = PortfolioManager()
    pm.update_balance('USD', 100000)
    return pm

@pytest.fixture
def commission():
    return Commission(commission=1.0)

@pytest.fixture
def strategy():
    mock_strategy = Mock()
    mock_strategy.name = 'test_strategy'
    return mock_strategy

@pytest.fixture
def backtest_manager(strategy, portfolio_manager, commission):
    return BacktestManager(strategy=strategy, pm=portfolio_manager, commission=commission)

@pytest.fixture
def sample_product():
    return StockProduct(name='NVDA', base_currency='USD', exch='NASDAQ')

def test_init(backtest_manager):
    assert backtest_manager.portfolio_history == []
    assert backtest_manager.transaction_log == []
    assert isinstance(backtest_manager.pm, PortfolioManager)
    assert backtest_manager.commission is not None

def test_place_order(backtest_manager, sample_product, strategy):
    order = Order(
        gateway='test_gateway',
        side=1,
        size=1.0,
        price=50000,
        order_type='MARKET',
        strategy=strategy.name,
        product=sample_product,
    )
    
    backtest_manager.place_order(order)
    assert order.oid in backtest_manager.open_orders

def test_execute_order(backtest_manager, sample_product, strategy):
    order = Order(
        gateway='test_gateway',
        strategy=strategy.name,
        product=sample_product,
        side=1, 
        size=1.0,
        order_type='MARKET',
        price=50000
    )

    bar = Bar(
        ts=pd.Timestamp('2023-01-01'),
        open_=50000,
        high=51000,
        low=49000,
        close=50500,
        volume=100
    )

    mock_callback = Mock()
    backtest_manager.execute_order(
        gateway='test_gateway',
        exch='NASDAQ',
        pdt='NVDA',
        order=order,
        bar=bar,
        on_order_status_update=mock_callback
    )

    # Check transaction log
    assert len(backtest_manager.transaction_log) == 1
    assert backtest_manager.transaction_log[0]['product'] == 'NVDA'
    assert backtest_manager.transaction_log[0]['side'] == 1
    assert backtest_manager.transaction_log[0]['size'] == 1.0

def test_on_bar(backtest_manager, sample_product, strategy):
    # Add an open order
    order = Order(
        gateway='test_gateway',
        strategy=strategy.name,
        product=sample_product,
        side=1,
        size=1.0, 
        order_type='MARKET',
        price=50000
    )
    backtest_manager.open_orders[order.oid] = order

    # Mock strategy.get_product
    backtest_manager.strategy.get_product = MagicMock(return_value=sample_product)

    mock_callback = Mock()
    backtest_manager.on_bar(
        gateway='test_gateway',
        exch='NASDAQ',
        pdt='NVDA',
        freq='1m',
        ts=pd.Timestamp('2023-01-01'),
        open_=50000,
        high=51000,
        low=49000,
        close=50500,
        volume=100,
        on_order_status_update=mock_callback
    )

    # Check portfolio history updated
    assert len(backtest_manager.portfolio_history) == 1

def test_export_data(backtest_manager, tmp_path):
    # Add some test data
    backtest_manager.portfolio_history.append({
        'ts': pd.Timestamp('2023-01-01'),
        'portfolio_value': 100000
    })
    backtest_manager.transaction_log.append({
        'ts': pd.Timestamp('2023-01-01'),
        'oid': '123',
        'product': 'NVDA',
        'side': 1,
        'size': 1.0,
        'filled_price': 50000,
        'order_type': 'MARKET',
        'status': 'FILLED'
    })

    # Export to temporary directory
    path_prefix = str(tmp_path / 'test')
    backtest_manager.export_data(path_prefix)

    # Verify files were created
    assert (tmp_path / 'test_portfolio.csv').exists()
    assert (tmp_path / 'test_transactions.csv').exists()

def test_invalid_order_type_warning(backtest_manager, sample_product, caplog, strategy):
    order = Order(
        gateway='test_gateway',
        strategy=strategy.name,
        product=sample_product,
        side=1,
        size=1.0,
        order_type='LIMIT',
        price=50000
    )

    bar = Bar(
        ts=pd.Timestamp('2023-01-01'),
        open_=50000,
        high=51000,
        low=49000,
        close=50500,
        volume=100
    )

    backtest_manager.execute_order(
        gateway='test_gateway',
        exch='NASDAQ',
        pdt='NVDA',
        order=order,
        bar=bar,
        on_order_status_update=Mock()
    )

    assert "Only MARKET order is supported for backtesting" in caplog.text