import pytest
from unittest.mock import Mock, MagicMock
import pandas as pd
from alchemist.managers.backtest_manager import BacktestManager
from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.order import Order
from alchemist.products.future_product import FutureProduct
from alchemist.commission import FutureContractCommission
from alchemist.datas.common import Bar

@pytest.fixture
def portfolio_manager():
    pm = PortfolioManager()
    pm.update_balance('USD', 100000)
    return pm

@pytest.fixture
def commission():
    # MES-like commission: Margin 2000, Multiplier 5
    # Tick Size 0.25, Tick Value 1.25 (implied by multiplier 5)
    return FutureContractCommission(
        commission=2.0, 
        initial_margin=2000.0, 
        maintainance_margin=1800.0, 
        multiplier=5.0,
        tick_size=0.25,
        tick_value=1.25
    )

@pytest.fixture
def strategy():
    mock_strategy = Mock()
    mock_strategy.name = 'test_strategy'
    return mock_strategy

@pytest.fixture
def backtest_manager(strategy, portfolio_manager, commission):
    return BacktestManager(strategy=strategy, pm=portfolio_manager, commission=commission)

@pytest.fixture
def mes_product():
    return FutureProduct(name='MES', base_currency='USD', exch='CME', contract_month='2023-03', margin=2019)

def test_futures_margin_deduction(backtest_manager, mes_product, strategy):
    # Open Long 1 contract
    order = Order(
        gateway='test_gateway',
        strategy=strategy.name,
        product=mes_product,
        side=1,
        size=1.0,
        order_type='MARKET',
        price=2000.0
    )
    
    bar = Bar(
        ts=pd.Timestamp('2023-01-01'),
        open_=2000.0, high=2005.0, low=1995.0, close=2000.0, volume=100
    )
    
    backtest_manager.execute_order(
        gateway='test_gateway', exch='CME', pdt='MES',
        order=order, bar=bar, on_order_status_update=Mock()
    )
    
    # Check Balance: 100000 - 2000 (margin) - 2 (commission) = 97998
    current_balance = backtest_manager.pm.get_balance('USD')
    assert current_balance == 100000 - 2000 - 2

def test_futures_realized_pnl(backtest_manager, mes_product, strategy):
    # 1. Open Long 1 at 2000
    order_open = Order(
        gateway='test_gateway', strategy=strategy.name, product=mes_product,
        side=1, size=1.0, order_type='MARKET', price=2000.0
    )
    bar_open = Bar(
        ts=pd.Timestamp('2023-01-01 10:00'),
        open_=2000.0, high=2005.0, low=1995.0, close=2000.0, volume=100
    )
    backtest_manager.execute_order(
        gateway='test_gateway', exch='CME', pdt='MES',
        order=order_open, bar=bar_open, on_order_status_update=Mock()
    )
    
    # 2. Close Short 1 at 2150
    order_close = Order(
        gateway='test_gateway', strategy=strategy.name, product=mes_product,
        side=-1, size=1.0, order_type='MARKET', price=2150.0
    )
    bar_close = Bar(
        ts=pd.Timestamp('2023-01-01 11:00'),
        open_=2150.0, high=2155.0, low=2145.0, close=2150.0, volume=100
    )
    backtest_manager.execute_order(
        gateway='test_gateway', exch='CME', pdt='MES',
        order=order_close, bar=bar_close, on_order_status_update=Mock()
    )
    
    # Expected PnL: (2150 - 2000) * 5 = 750
    # Expected Balance: 100000 - 2 (comm open) - 2 (comm close) + 750 (profit) = 100746
    # Note: Margin is returned.
    current_balance = backtest_manager.pm.get_balance('USD')
    assert current_balance == 100000 - 4 + 750

def test_futures_unrealized_pnl(backtest_manager, mes_product, strategy):
    # 1. Open Long 1 at 2000
    order_open = Order(
        gateway='test_gateway', strategy=strategy.name, product=mes_product,
        side=1, size=1.0, order_type='MARKET', price=2000.0
    )
    bar_open = Bar(
        ts=pd.Timestamp('2023-01-01 10:00'),
        open_=2000.0, high=2005.0, low=1995.0, close=2000.0, volume=100
    )
    backtest_manager.execute_order(
        gateway='test_gateway', exch='CME', pdt='MES',
        order=order_open, bar=bar_open, on_order_status_update=Mock()
    )
    
    # 2. Update with new bar at 2100
    # We need to simulate on_bar updating unrealized pnl
    # But execute_order also updates position at the end.
    # Let's just check after execute_order first, but wait, execute_order uses bar.close for unrealized pnl.
    # In step 1, close is 2000, so unrealized is 0.
    
    # Now simulate on_bar with price 2100
    backtest_manager.strategy.get_product = MagicMock(return_value=mes_product)
    backtest_manager.on_bar(
        gateway='test_gateway', exch='CME', pdt='MES', freq='1m',
        ts=pd.Timestamp('2023-01-01 10:01'),
        open_=2100.0, high=2105.0, low=2095.0, close=2100.0, volume=100,
        on_order_status_update=Mock()
    )
    
    pos = backtest_manager.pm.get_position(mes_product)
    # Expected Unrealized PnL: (2100 - 2000) * 5 = 500
    assert pos.unrealized_pnl == 500.0
