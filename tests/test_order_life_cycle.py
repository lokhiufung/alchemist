import time
import pytest
from unittest.mock import MagicMock

from alchemist.managers.order_manager import OrderManager
from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.standardized_messages import create_order_update_message
from alchemist.zeromq import ZeroMQ
from alchemist.order import Order
from alchemist.products.future_product import FutureProduct


@pytest.fixture
def mock_zmq():
    mock_zmq = MagicMock(spec=ZeroMQ)
    return mock_zmq

# add a portfolio manager fixture
@pytest.fixture
def portfolio_manager():
    return PortfolioManager()



def test_open_order_update(mock_zmq, portfolio_manager):
    """
    Description:
        Scenario:
            The order manager have send an order to the gateway. And a moment later the order manager receives a `opened` message from the gateway.
        Expected result:
            The order manager should change the order status of the corresponding order to `opened`. And the order should be moved from open_orders to submitted_orders.
        Setup:
            There order manager should have the order in the submitted_orders.
    """
    gateway = 'test_gateway'
    strategy ='test_strategy'
    exch = 'test_exch'
    pdt = 'test_pdt'
    oid = '1'
    product = FutureProduct(name=pdt, base_currency='USD', exch=exch, contract_month='2024-08')

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    order = Order(gateway, strategy, 1, 100.1, 10, 'limit', product, oid=oid)
    # append the submitted
    om.submitted_orders[order.oid] = order
    
    ts = time.time()
    _, _, (gateway, strategy, order_update) = create_order_update_message(
        ts=ts,
        gateway=gateway,
        strategy=strategy,
        exch=exch,
        pdt=pdt,
        oid=oid,
        status='OPENED',
        average_filled_price=None,
        last_filled_price=None,
        last_filled_size=None,
        amend_price=None,
        amend_size=None,
        create_ts=time.time(),
        target_price=100.1,
    )
    om.on_order_status_update(gateway, order_update)

    # result
    assert om.open_orders[oid].status == 'OPENED'
    assert oid not in om.submitted_orders
    assert oid in om.open_orders


def test_filled_order_update_when_opened(mock_zmq, portfolio_manager):
    """
    Description:
        Scenario:
            We have successfully opened an order. Now we receive a filled order message from the gateway.
        Expected result:
            The order manager removes the order from the open_orders.
        Setup:
            There order manager should have the order in the open_orders.
    """
    gateway = 'test_gateway'
    strategy ='test_strategy'
    exch = 'test_exch'
    pdt = 'test_pdt'
    oid = '1'
    product = FutureProduct(name=pdt, base_currency='USD', exch=exch, contract_month='2024-08')

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    order = Order(gateway, strategy, 1, 100.1, 10, 'LIMIT', product, oid=oid)
    # append the open_orders
    om.open_orders[order.oid] = order
    
    ts = time.time()
    _, _, (gateway, strategy, order_update) = create_order_update_message(
        ts=ts,
        gateway=gateway,
        strategy=strategy,
        exch=exch,
        pdt=pdt,
        oid=oid,
        status='FILLED',
        average_filled_price=100.1,
        last_filled_price=100.1,
        last_filled_size=10,
        amend_price=None,
        amend_size=None,
        create_ts=ts,
        target_price=100.1,
    )
    om.on_order_status_update(gateway, order_update)

    # result
    assert oid not in om.open_orders


def test_partial_filled_order_update(mock_zmq, portfolio_manager):
    """
    Description:
        Scenario:
            We have successfully opened an order. Now we receive a partially-filled order message from the gateway.
        Expected result:
            The order manager maintains the order in the open_orders, but we should update the last_traded_price and last_traded_size.
        Setup:
            There order manager should have the order in the open_orders.
    """
    gateway = 'test_gateway'
    strategy ='test_strategy'
    exch = 'test_exch'
    pdt = 'test_pdt'
    oid = '1'
    product = FutureProduct(name=pdt, base_currency='USD', exch=exch, contract_month='2024-08')

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    order = Order(gateway, strategy, 1, 100.1, 10, 'LIMIT', product, oid=oid)
    order.on_opened()  # assume that we have opened the order
    # append the open_orders
    om.open_orders[order.oid] = order
    
    ts = time.time()
    _, _, (gateway, strategy, order_update) = create_order_update_message(
        ts=time.time(),
        gateway=gateway,
        strategy=strategy,
        exch=exch,
        pdt=pdt,
        oid=oid,
        status='PARTIAL_FILLED',
        average_filled_price=100.1,
        last_filled_price=100.1,
        last_filled_size=5,
        amend_price=None,
        amend_size=None,
        create_ts=ts,
        target_price=100.1,
    )
    om.on_order_status_update(gateway, order_update)

    # result
    assert om.open_orders[oid].status == 'PARTIALLY_FILLED'
    assert om.open_orders[oid].last_traded_price == 100.1
    assert om.open_orders[oid].last_traded_size == 5


def test_cancel_order_update(mock_zmq, portfolio_manager):
    """
    Description:
        Scenario:
            We submitted a cancel order request. Now we receive a cancel order message from the gateway.
        Expected result:
            The order manager removes the order in the open_orders.
        Setup:
            There order manager should have the order in the open_orders.
    """
    gateway = 'test_gateway'
    strategy ='test_strategy'
    exch = 'test_exch'
    pdt = 'test_pdt'
    oid = '1'
    product = FutureProduct(name=pdt, base_currency='USD', exch=exch, contract_month='2024-08')

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    order = Order(gateway, strategy, 1, 100.1, 10, 'LIMIT', product, oid=oid)
    order.on_opened()  # assume that we have opened the order
    # append the open_orders
    om.open_orders[order.oid] = order
    
    ts = time.time()
    _, _, (gateway, strategy, order_update) = create_order_update_message(
        ts=ts,
        gateway=gateway,
        strategy=strategy,
        exch=exch,
        pdt=pdt,
        oid=oid,
        status='CANCELED',
        average_filled_price=None,
        last_filled_price=None,
        last_filled_size=None,
        amend_price=None,
        amend_size=None,
        create_ts=ts,
        target_price=100.1,
    )
    om.on_order_status_update(gateway, order_update)

    # result
    assert oid not in om.open_orders


def test_amend_order_update(mock_zmq, portfolio_manager):
    """
    Description:
        Scenario:
            We submitted an amend order request. Now we receive a amend order message from the gateway.
        Expected result:
            The order manager maintains the order in the open_orders. We should update the price and size of the order using the message
        Setup:
            There order manager should have the order in the open_orders.
    """
    gateway = 'test_gateway'
    strategy ='test_strategy'
    exch = 'test_exch'
    pdt = 'test_pdt'
    oid = '1'
    product = FutureProduct(name=pdt, base_currency='USD', exch=exch, contract_month='2024-08')

    om = OrderManager(zmq=mock_zmq, portfolio_manager=portfolio_manager)
    order = Order(gateway, strategy, 1, 100.1, 10, 'limit', product, oid=oid)
    order.on_opened()  # assume that we have opened the order
    # append the open_orders
    om.open_orders[order.oid] = order
    
    ts = time.time()
    _, _, (gateway, strategy, order_update) = create_order_update_message(
        ts=ts,
        gateway=gateway,
        strategy=strategy,
        exch=exch,
        pdt=pdt,
        oid=oid,
        status='AMENDED',
        average_filled_price=None,
        last_filled_price=None,
        last_filled_size=None,
        amend_price=90.1,
        amend_size=5,
        create_ts=ts,
        target_price=90.1,
    )
    om.on_order_status_update(gateway, order_update)

    # result
    assert oid in om.open_orders
    assert om.open_orders[oid].status == 'AMENDED'
    assert om.open_orders[oid].price == 90.1
    assert om.open_orders[oid].size == 5

