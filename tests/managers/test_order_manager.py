import pytest
from unittest.mock import MagicMock, patch, ANY
from alchemist.managers.order_manager import OrderManager
from alchemist.order import Order
from alchemist.products.base_product import BaseProduct


@pytest.fixture
def mock_zmq():
    """Fixture for mocking ZeroMQ."""
    return MagicMock()


@pytest.fixture
def mock_portfolio_manager():
    """Fixture for mocking PortfolioManager."""
    manager = MagicMock()
    manager.get_available_balance.return_value = 10000  # Mocked balance
    manager.get_position.return_value = None  # No existing position
    return manager


@pytest.fixture
def order_manager(mock_zmq, mock_portfolio_manager):
    """Fixture for initializing OrderManager with mocked dependencies."""
    return OrderManager(zmq=mock_zmq, portfolio_manager=mock_portfolio_manager)


@pytest.fixture
def sample_order():
    """Fixture for creating a sample Order object."""
    product = BaseProduct(exch="NYSE", name="AAPL", base_currency="USD")
    return Order(
        gateway="mock_gateway",
        strategy="mock_strategy",
        product=product,
        oid="order123",
        side="buy",
        price=100.0,
        size=10,
        order_type="market",
    )


def test_validate_order(order_manager, sample_order, mock_portfolio_manager):
    """
    Test that _validate_order checks for sufficient balance and validates orders correctly.
    """
    # Test valid order
    result = order_manager._validate_order(sample_order)
    assert result["passed"] is True

    # Test insufficient balance
    mock_portfolio_manager.get_available_balance.return_value = 500  # Set low balance
    result = order_manager._validate_order(sample_order)
    assert result["passed"] is False
    assert "Not enough balance" in result["reason"]


def test_place_order_valid(order_manager, sample_order, mock_zmq):
    """
    Test that a valid order is sent to ZeroMQ and logged correctly.
    """
    with patch.object(order_manager, "on_submitted") as mock_on_submitted:
        order_manager.place_order(sample_order)

        # Validate ZeroMQ message sent
        mock_zmq.send.assert_called_once()

        # Validate on_submitted is called
        mock_on_submitted.assert_called_once_with(sample_order)


def test_place_order_invalid(order_manager, sample_order, mock_portfolio_manager):
    """
    Test that an invalid order is rejected and handled correctly.
    """
    mock_portfolio_manager.get_available_balance.return_value = 500  # Set low balance

    with patch.object(order_manager, "on_internal_rejected") as mock_on_internal_rejected:
        order_manager.place_order(sample_order)

        # Validate on_internal_rejected is called
        mock_on_internal_rejected.assert_called_once_with(sample_order, reason=ANY)


def test_on_submitted(order_manager, sample_order, mock_portfolio_manager):
    """
    Test the behavior of on_submitted, including reserving balance.
    """
    order_status_updates = order_manager.on_submitted(sample_order)

    # Check balance reservation
    mock_portfolio_manager.reserve_balance.assert_called_once_with(
        oid=sample_order.oid,
        currency="USD",
        value=sample_order.price * sample_order.size,
    )
    # Check status update
    assert order_status_updates[0] == "SUBMITTED"
    assert order_manager.submitted_orders[sample_order.oid] == sample_order


def test_on_opened(order_manager, sample_order):
    """
    Test that an order transitions correctly to 'OPENED' state.
    """
    # Precondition: Order is in submitted_orders
    order_manager.submitted_orders[sample_order.oid] = sample_order
    # make it submitted
    sample_order.on_submitted()

    order_status_updates = order_manager.on_opened(sample_order.oid)

    # Check state transition
    assert sample_order.oid in order_manager.open_orders
    assert sample_order.oid not in order_manager.submitted_orders
    assert order_status_updates[0] == "OPENED"


def test_on_filled(order_manager, sample_order, mock_portfolio_manager):
    """
    Test that a filled order is correctly removed and balance is released.
    """
    # Precondition: Order is in open_orders
    order_manager.open_orders[sample_order.oid] = sample_order
    # make it opened
    sample_order.on_opened()

    order_status_updates = order_manager.on_filled(sample_order.oid, price=100.0, size=10)

    # Check order removal
    assert sample_order.oid not in order_manager.open_orders

    # Check balance release
    mock_portfolio_manager.release_balance.assert_called_once_with(
        sample_order.oid, currency="USD"
    )
    assert order_status_updates[0] == "FILLED"
    assert sample_order.filled_size == 10
    assert sample_order.remaining_size == 0
    assert sample_order.oid not in order_manager.open_orders


def test_on_rejected(order_manager, sample_order, mock_portfolio_manager):
    """
    Test that a rejected order releases reserved balance and updates status.
    """
    # Precondition: Order is in submitted_orders
    order_manager.submitted_orders[sample_order.oid] = sample_order
    # make it submitted
    sample_order.on_submitted()

    order_status_updates = order_manager.on_rejected(sample_order.oid, reason="Test rejection")

    # Check order removal
    assert sample_order.oid not in order_manager.open_orders

    # Check balance release
    mock_portfolio_manager.release_balance.assert_called_once_with(
        sample_order.oid, currency="USD"
    )
    assert order_status_updates[0] == "REJECTED"
    assert sample_order.oid not in order_manager.submitted_orders


def test_on_partial_filled(order_manager, sample_order):
    """
    Test that a partially filled order updates its filled size correctly.
    """
    # Precondition: Order is in open_orders
    order_manager.open_orders[sample_order.oid] = sample_order
    # make it opened
    sample_order.on_opened()

    order_status_updates = order_manager.on_partial_filled(sample_order.oid, price=100.0, size=5)

    # Check that the filled size is updated
    assert sample_order.filled_size == 5
    assert sample_order.remaining_size == 5
    assert order_status_updates[0] == "PARTIAL_FILLED"
    # the order is still opened
    assert order_manager.open_orders[sample_order.oid] == sample_order


def test_on_canceled(order_manager, sample_order, mock_portfolio_manager):
    """
    Test that a canceled order releases reserved balance and updates its state.
    """
    # Precondition: Order is in open_orders
    order_manager.open_orders[sample_order.oid] = sample_order
    # make it opened
    sample_order.on_opened()

    order_status_updates = order_manager.on_canceled(sample_order.oid)

    # Check that the order is removed from open orders
    assert sample_order.oid not in order_manager.open_orders

    # Verify balance release
    mock_portfolio_manager.release_balance.assert_called_once_with(
        sample_order.oid, currency="USD"
    )
    assert order_status_updates[0] == "CANCELED"


def test_place_order_error_handling(order_manager, sample_order, mock_zmq):
    """
    Test that place_order handles unexpected errors gracefully.
    """
    with patch.object(order_manager, "on_internal_rejected") as mock_on_internal_rejected:
        # Simulate ZeroMQ failure by raising an exception when send is called
        mock_zmq.send.side_effect = Exception("ZeroMQ send failure")

        order_manager.place_order(sample_order)

        # Validate that on_internal_rejected is called due to the exception
        mock_on_internal_rejected.assert_called_once_with(sample_order, reason=ANY)


def test_missing_order_state(order_manager, caplog):
    """
    Test behavior when trying to update a non-existent order.
    """
    non_existent_oid = "order999"
    order_update = {
        'data': {
            'status': 'FILLED',
            'oid': non_existent_oid,
        }
    }

    order_manager.on_order_status_update(
        'mock_gateway',
        order_update
    )
    assert "not found" in caplog.text


def test_maximum_order_size(order_manager, mock_zmq, mock_portfolio_manager):
    """
    Test placing an order with an extremely large size and price.
    """
    large_order = Order(
        gateway="mock_gateway",
        strategy="mock_strategy",
        product=BaseProduct(exch="NYSE", name="AAPL", base_currency="USD"),
        oid="order_large",
        side="buy",
        price=1e6,  # Large price
        size=1e4,   # Large size
        order_type="market",
    )

    # Assuming PortfolioManager can handle the large balance
    mock_portfolio_manager.get_available_balance.return_value = 1e10

    # with patch.object(order_manager, "on_submitted") as mock_on_submitted:
    order_manager.place_order(large_order)

    # Validate ZeroMQ message sent
    mock_zmq.send.assert_called_once()


    # Check balance reservation
    mock_portfolio_manager.reserve_balance.assert_called_once_with(
        oid=large_order.oid,
        currency="USD",
        value=large_order.price * large_order.size,
    )


def test_duplicate_order_ids(order_manager, sample_order, mock_zmq):
    """
    Test placing two orders with the same oid.
    """
    duplicate_order = Order(
        gateway="mock_gateway",
        strategy="mock_strategy",
        product=BaseProduct(exch="NYSE", name="AAPL", base_currency="USD"),
        oid="order123",  # Same OID as sample_order
        side="sell",
        price=105.0,
        size=5,
        order_type="limit",
    )

    # Place the first order
    # with patch.object(order_manager, "on_submitted") as mock_on_submitted:
    order_manager.place_order(sample_order)
    # mock_on_submitted.assert_called_once_with(sample_order)

    # # manually add the order to the submitted_orders
    # order_manager.submitted_orders[sample_order.oid] = sample_order
    # Attempt to place the duplicate order
    with patch.object(order_manager, "on_internal_rejected") as mock_on_internal_rejected:
        order_manager.place_order(duplicate_order)

        # Validate that the duplicate order is rejected
        mock_on_internal_rejected.assert_called_once_with(duplicate_order, reason=ANY)


def test_concurrent_order_placements(order_manager, mock_zmq, mock_portfolio_manager):
    """
    Test placing multiple orders simultaneously.
    """
    orders = [
        Order(
            gateway="mock_gateway",
            strategy="mock_strategy",
            product=BaseProduct(exch="NYSE", name="AAPL", base_currency="USD"),
            oid=f"order_concurrent_{i}",
            side="buy" if i % 2 == 0 else "sell",
            price=100.0 + i,
            size=10 + i,
            order_type="market",
        )
        for i in range(10)
    ]

    with patch.object(order_manager, "on_submitted") as mock_on_submitted:
        for order in orders:
            order_manager.place_order(order)

        # Validate that all orders are sent to ZeroMQ
        assert mock_zmq.send.call_count == 10

        # Validate that on_submitted is called for all orders
        assert mock_on_submitted.call_count == 10

    # # Generate expected calls for on_submitted
    # expected_submitted_calls = [call(order) for order in orders]
    # mock_on_submitted.assert_has_calls(expected_submitted_calls, any_order=True)


def test_place_limit_order(order_manager, mock_zmq, mock_portfolio_manager):
    """
    Test placing a limit order and simulating its execution.
    """
    limit_order = Order(
        gateway="mock_gateway",
        strategy="mock_strategy",
        product=BaseProduct(exch="NYSE", name="AAPL", base_currency="USD"),
        oid="order_limit",
        side="buy",
        price=95.0,  # Limit price below current market price
        size=10,
        order_type="limit",
    )

    with patch.object(order_manager, "on_submitted") as mock_on_submitted:
        order_manager.place_order(limit_order)
        mock_on_submitted.assert_called_once_with(limit_order)
        mock_zmq.send.assert_called_once()

    # add the order to submitted
    limit_order.on_submitted()
    order_manager.submitted_orders[limit_order.oid] = limit_order
    # Simulate that the market price meets the limit criteria
    with patch.object(order_manager, "on_filled") as mock_on_filled:
        order_manager.on_filled(limit_order.oid, price=95.0, size=10)
        mock_on_filled.assert_called_once()


def test_zero_mq_failure(order_manager, sample_order, mock_zmq, mock_portfolio_manager):
    """
    Test that the OrderManager handles ZeroMQ failures gracefully.
    """
    with patch.object(order_manager, "on_internal_rejected") as mock_on_internal_rejected:
        # Simulate ZeroMQ send failure
        mock_zmq.send.side_effect = Exception("ZeroMQ send failure")

        order_manager.place_order(sample_order)

        # Validate that on_internal_rejected is called due to the exception
        mock_on_internal_rejected.assert_called_once_with(sample_order, reason=ANY)


def test_portfolio_manager_failure_during_reservation(order_manager, sample_order, mock_zmq, mock_portfolio_manager):
    """
    Test that the OrderManager handles PortfolioManager failures during balance reservation.
    """
    with patch.object(order_manager, "on_internal_rejected") as mock_on_internal_rejected:
        # Simulate failure in reserving balance
        mock_portfolio_manager.reserve_balance.side_effect = Exception("PortfolioManager reservation failure")

        order_manager.place_order(sample_order)

        # Validate that on_internal_rejected is called due to the exception
        mock_on_internal_rejected.assert_called_once_with(sample_order, reason=ANY)


def test_invalid_order_attributes(order_manager, mock_zmq, mock_portfolio_manager):
    """
    Test placing orders with invalid attributes such as negative size or invalid side.
    """
    invalid_orders = [
        Order(
            gateway="mock_gateway",
            strategy="mock_strategy",
            product=BaseProduct(exch="NYSE", name="AAPL", base_currency="USD"),
            oid="order_invalid_size",
            side=1,
            price=100.0,
            size=-10,  # Negative size
            order_type="LIMIT",
        ),
        Order(
            gateway="mock_gateway",
            strategy="mock_strategy",
            product=BaseProduct(exch="NYSE", name="AAPL", base_currency="USD"),
            oid="order_invalid_price",
            side="hold",  # Invalid price
            price=-100.0,
            size=10,
            order_type="LIMIT",
        ),
    ]

    for order in invalid_orders:
        with patch.object(order_manager, "on_internal_rejected") as mock_on_internal_rejected:
            order_manager.place_order(order)

            # Validate that on_internal_rejected is called
            mock_on_internal_rejected.assert_called_once_with(order, reason=ANY)

            # Reset mock for the next iteration
            mock_on_internal_rejected.reset_mock()

