import pytest

from unittest.mock import MagicMock

from alchemist.order import Order
from alchemist.position import Position
from alchemist.products.future_product import FutureProduct
from alchemist.managers.order_manager import OrderManager
from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.zeromq import ZeroMQ


@pytest.fixture
def mock_zmq():
    mock_zmq = MagicMock(spec=ZeroMQ)
    return mock_zmq


@pytest.fixture
def portfolio_manager():
    pm = PortfolioManager()
    return pm


def test_check_enough_balance_to_buy(mock_zmq, portfolio_manager):
    # set balance to 100000
    portfolio_manager.balances['USD'] = 100000
    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    # case 1: you place an order that is more than your balance
    order = Order(
        gateway='test_gateway',
        strategy='test_strategy',
        side=1,
        price=1000.2,
        size=100,  # This would require 1000.2 * 100 = 100020
        order_type='limit',
        product=FutureProduct(name='test_product', base_currency='USD', exch='test_exch', contract_month='2024-08', margin=2109)
    )
    validation_result = om._validate_order(order)
    assert not validation_result['passed']
    assert validation_result['reason'] == f'Not enough balance: balance=100000.0 required_balance=100020.0'


def test_check_enough_balance_to_reduce_position(mock_zmq, portfolio_manager):
    product = FutureProduct(name='test_product', base_currency='USD', exch='test_exch', contract_month='2024-08', margin=2109)
    position = Position(product=product, side=1, size=10, last_price=1005.2, avg_price=1005.2, realized_pnl=None, unrealized_pnl=None)

    # set balance to 100000
    portfolio_manager.balances['USD'] = 100000
    # set position to 10
    portfolio_manager.positions['test_exch'] = {}
    portfolio_manager.positions['test_exch']['test_product'] = position

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    # case 2: you place an order that only reduce your position
    order = Order(
        gateway='test_gateway',
        strategy='test_strategy',
        side=-1,
        price=1000.2,
        size=5, 
        order_type='limit',
        product=product,
    )
    validation_result = om._validate_order(order)
    assert validation_result['passed']


def test_reserve_balance_on_order_placement(mock_zmq, portfolio_manager):
    """
    Test that placing an order that passes validation immediately reserves capital in PortfolioManager.
    """
    # set balance to 100000
    portfolio_manager.balances['USD'] = 100000

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    product = FutureProduct(name='ES', base_currency='USD', exch='CME', contract_month='2024-09', margin=2019)
    order = Order(
        gateway='test_gateway',
        strategy='test_strategy',
        side=1,
        price=1000.0,
        size=10,
        order_type='LIMIT',
        product=product
    )
    # Validation should pass (cost = 10,000; we have 100,000)
    result = om._validate_order(order)
    assert result['passed']

    # Place the order, expecting capital to be reserved
    om.place_order(order)
    
    # Check if capital is reserved
    reserved = portfolio_manager.get_reserved_balance('USD')
    assert reserved == 10000.0, f"Expected 10000.0 reserved, got {reserved}"
    assert portfolio_manager.get_available_balance('USD') == 90000.0, "Available balance should reflect reserved capital."

def test_release_balance_on_order_canceled(mock_zmq, portfolio_manager: PortfolioManager):
    """
    Test that when an order is canceled, reserved capital is released.
    """
    # set balance to 100000
    portfolio_manager.balances['USD'] = 100000

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    product = FutureProduct(name='ES', base_currency='USD', exch='CME', contract_month='2024-09', margin=2019)
    order = Order(
        gateway='test_gateway',
        strategy='test_strategy',
        side=1,
        price=500.0,
        size=10,
        order_type='limit',
        product=product
    )
    # place and submit order
    om.place_order(order)
    # Simulate gateway confirms order is OPENED
    om.on_order_status_update('test_gateway', {'data':{'oid': order.oid, 'status':'OPENED'}})

    # Capital now reserved: 500*10 = 5000
    assert portfolio_manager.get_reserved_balance('USD') == 5000.0

    # Now simulate a CANCELED message from the gateway
    om.on_order_status_update('test_gateway', {'data':{'oid': order.oid, 'status':'CANCELED'}})
    # All reserved capital should be released
    assert portfolio_manager.get_reserved_balance('USD') == 0.0
    assert portfolio_manager.get_available_balance('USD') == 100000.0

def test_release_balance_on_order_rejected(mock_zmq, portfolio_manager: PortfolioManager):
    """
    Test that when an order is rejected by the gateway, reserved capital is released.
    """
    
    # set balance to 100000
    portfolio_manager.balances['USD'] = 100000

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    product = FutureProduct(name='ES', base_currency='USD', exch='CME', contract_month='2024-09', margin=2019)
    order = Order(
        gateway='test_gateway',
        strategy='test_strategy',
        side=1,
        price=1000.0,
        size=5,
        order_type='LIMIT',
        product=product
    )
    om.place_order(order)
    # Initially reserved: 1000*5 = 5000
    assert portfolio_manager.get_reserved_balance('USD') == 5000.0

    # Simulate gateway rejects the order
    om.on_order_status_update('test_gateway', {'data':{'oid': order.oid, 'status':'REJECTED', 'reason': 'Some reason'}})
    # Capital should be fully released
    assert portfolio_manager.get_reserved_balance('USD') == 0.0
    assert portfolio_manager.get_available_balance('USD') == 100000.0

def test_partial_fill_adjustment(mock_zmq, portfolio_manager):
    """
    Test handling of partial fills:
    For partial fills, we should release capital proportionally to the filled quantity.
    """
    # set balance to 100000
    portfolio_manager.balances['USD'] = 100000

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    product = FutureProduct(name='ES', base_currency='USD', exch='CME', contract_month='2024-09', margin=2019)
    order = Order(
        gateway='test_gateway',
        strategy='test_strategy',
        side=1,
        price=200.0,
        size=50,
        order_type='LIMIT',
        product=product
    )
    # total required = 200 * 50 = 10,000
    om.place_order(order)
    om.on_order_status_update('test_gateway', {'data':{'oid': order.oid, 'status':'OPENED'}})
    assert portfolio_manager.get_reserved_balance('USD') == 10000.0

    # Now a partial fill occurs: 20 units filled at price 200
    # Logic: release capital proportional to filled quantity (20/50 = 40% of total)
    # The partial fill scenario depends on your exact code for partial fills
    # Let's assume your code calls release_balance for partial amount:
    # If partial fill adjusts reservation, you need something like:
    # reserved for remaining = 30 * 200 = 6000
    # released = 10000 - 6000 = 4000

    # Simulate PARTIAL_FILLED from the gateway
    om.on_order_status_update('test_gateway', {'data':{'oid': order.oid, 'status':'PARTIAL_FILLED', 'last_filled_price':200.0, 'last_filled_size':20}})

    # Check if reservation adjusted
    # Depending on implementation, if you haven't implemented partial logic:
    # this test will fail. You must implement partial release logic.
    # For now, we assume you've done that:
    expected_remaining = 6000.0  # after partial fill
    currently_reserved = portfolio_manager.get_reserved_balance('USD')
    assert currently_reserved == expected_remaining, f"Expected {expected_remaining}, got {currently_reserved}"

    # Another partial fill completes the order (30 units)
    # Now the order is fully FILLED, all reserved capital should be released.
    om.on_order_status_update('test_gateway', {'data':{'oid': order.oid, 'status':'FILLED', 'last_filled_price':200.0, 'last_filled_size':30}})
    assert portfolio_manager.get_reserved_balance('USD') == 0.0
    assert portfolio_manager.get_available_balance('USD') == 100000.0