import pytest
from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.order import Order
from alchemist.products.base_product import BaseProduct


@pytest.fixture
def portfolio_manager():
    """Fixture for initializing PortfolioManager with mocked dependencies."""
    return PortfolioManager()

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


def test_get_portfolio_value(portfolio_manager, sample_order):
    portfolio_manager.update_balance(currency='USD', value=1000)
    portfolio_manager.create_position(
        product=sample_order.product,
        side=1,
        size=10,
        last_price=100.0,
        avg_price=100.0,
        realized_pnl=90.0,
        unrealized_pnl=-100.0,
    )
    portfolio_value = portfolio_manager.get_portfolio_value(currency='USD')
    expected_portfolio_value = (10 * 100.0 - 100.0) + 1000
    assert portfolio_value == expected_portfolio_value


def test_get_reserved_balance(portfolio_manager, sample_order):
    """
    Test that get_reserved_balance correctly returns the reserved balance.
    """
    # Initialize balance
    portfolio_manager.update_balance('USD', 10000.0)

    # Initially, reserved balance should be zero
    reserved_total = portfolio_manager.get_reserved_balance('USD')
    assert reserved_total == 0.0, "Initial reserved balance should be zero."

    # Reserve balance for sample_order
    reserve_success = portfolio_manager.reserve_balance(
        oid=sample_order.oid,
        currency='USD',
        value=1000.0
    )
    assert reserve_success is True, "Failed to reserve balance for sample_order."

    # Check reserved balance without specifying oid
    reserved_total = portfolio_manager.get_reserved_balance('USD')
    assert reserved_total == 1000.0, f"Expected total reserved balance to be 1000.0, got {reserved_total}."

    # Check reserved balance for specific oid
    reserved_oid = portfolio_manager.get_reserved_balance('USD', oid=sample_order.oid)
    assert reserved_oid == 1000.0, f"Expected reserved balance for {sample_order.oid} to be 1000.0, got {reserved_oid}."

    # Reserve another order
    another_order = Order(
        gateway="mock_gateway",
        strategy="mock_strategy",
        product=sample_order.product,
        oid="order124",
        side="buy",
        price=200.0,
        size=5,
        order_type="market",
    )
    reserve_success = portfolio_manager.reserve_balance(
        oid=another_order.oid,
        currency='USD',
        value=1000.0
    )
    assert reserve_success is True, "Failed to reserve balance for another_order."

    # Now, total reserved should be 2000.0
    reserved_total = portfolio_manager.get_reserved_balance('USD')
    assert reserved_total == 2000.0, f"Expected total reserved balance to be 2000.0, got {reserved_total}."

    # Reserved balance for another_order
    reserved_oid2 = portfolio_manager.get_reserved_balance('USD', oid=another_order.oid)
    assert reserved_oid2 == 1000.0, f"Expected reserved balance for {another_order.oid} to be 1000.0, got {reserved_oid2}."


def test_reserve_balance(portfolio_manager, sample_order):
    """
    Test that reserve_balance successfully reserves available balance and fails when insufficient.
    """
    # Initialize balance
    portfolio_manager.update_balance('USD', 10000.0)

    # Reserve $1,000 for sample_order
    reserve_success = portfolio_manager.reserve_balance(
        oid=sample_order.oid,
        currency='USD',
        value=1000.0
    )
    assert reserve_success is True, "Failed to reserve $1,000 for sample_order."

    # Check that the reserved balance is correctly updated
    reserved = portfolio_manager.get_reserved_balance('USD', oid=sample_order.oid)
    assert reserved == 1000.0, f"Expected reserved balance to be 1000.0, got {reserved}."

    # Attempt to reserve $9,500, which exceeds the available balance ($9,000)
    reserve_success = portfolio_manager.reserve_balance(
        oid="order124",
        currency='USD',
        value=9500.0
    )
    assert reserve_success is False, "Should not allow reserving $9,500 due to insufficient available balance."

    # Ensure that the reserved balance for order124 is not set
    reserved_order124 = portfolio_manager.get_reserved_balance('USD', oid="order124")
    assert reserved_order124 == 0.0, f"Expected reserved balance for order124 to be 0.0, got {reserved_order124}."

    # Available balance should remain $9,000
    available = portfolio_manager.get_available_balance('USD')
    assert available == 9000.0, f"Expected available balance to be 9000.0, got {available}."


def test_release_balance(portfolio_manager, sample_order):
    """
    Test that release_balance correctly releases reserved funds, partially or fully.
    """
    # Initialize balance
    portfolio_manager.update_balance('USD', 10000.0)

    # Reserve $1,000 for sample_order
    reserve_success = portfolio_manager.reserve_balance(
        oid=sample_order.oid,
        currency='USD',
        value=1000.0
    )
    assert reserve_success is True, "Failed to reserve $1,000 for sample_order."

    # Release the reserved balance fully
    released = portfolio_manager.release_balance(
        oid=sample_order.oid,
        currency='USD'
    )
    assert released == 1000.0, f"Expected released balance to be 1000.0, got {released}."

    # Verify that reserved balance is now zero
    reserved = portfolio_manager.get_reserved_balance('USD', oid=sample_order.oid)
    assert reserved == 0.0, f"Expected reserved balance to be 0.0, got {reserved}."

    # Attempt to release balance again, which should have no effect
    released = portfolio_manager.release_balance(
        oid=sample_order.oid,
        currency='USD'
    )
    assert released == 0.0, f"Expected released balance to be 0.0 on second release, got {released}."

    # Reserve another $500 partially
    reserve_success = portfolio_manager.reserve_balance(
        oid="order124",
        currency='USD',
        value=500.0
    )
    assert reserve_success is True, "Failed to reserve $500 for order124."

    # Release $300 of the reserved balance
    released = portfolio_manager.release_balance(
        oid="order124",
        currency='USD',
        value=300.0
    )
    assert released == 300.0, f"Expected released balance to be 300.0, got {released}."

    # Verify that $200 remains reserved
    reserved = portfolio_manager.get_reserved_balance('USD', oid="order124")
    assert reserved == 200.0, f"Expected remaining reserved balance to be 200.0, got {reserved}."


def test_get_available_balance(portfolio_manager, sample_order):
    """
    Test that get_available_balance returns correct available funds after reservations and releases.
    """
    # Initialize balance
    portfolio_manager.update_balance('USD', 10000.0)

    # Check initial available balance
    available = portfolio_manager.get_available_balance('USD')
    assert available == 10000.0, f"Expected initial available balance to be 10000.0, got {available}."

    # Reserve $1,000 for sample_order
    reserve_success = portfolio_manager.reserve_balance(
        oid=sample_order.oid,
        currency='USD',
        value=1000.0
    )
    assert reserve_success is True, "Failed to reserve $1,000 for sample_order."

    # Available balance should now be $9,000
    available = portfolio_manager.get_available_balance('USD')
    assert available == 9000.0, f"Expected available balance to be 9000.0 after reservation, got {available}."

    # Reserve another $2,000 for a different order
    another_order = Order(
        gateway="mock_gateway",
        strategy="mock_strategy",
        product=sample_order.product,
        oid="order124",
        side="buy",
        price=400.0,
        size=5,
        order_type="market",
    )
    reserve_success = portfolio_manager.reserve_balance(
        oid=another_order.oid,
        currency='USD',
        value=2000.0
    )
    assert reserve_success is True, "Failed to reserve $2,000 for order124."

    # Available balance should now be $7,000
    available = portfolio_manager.get_available_balance('USD')
    assert available == 7000.0, f"Expected available balance to be 7000.0 after second reservation, got {available}."

    # Release $1,000 from sample_order
    released = portfolio_manager.release_balance(
        oid=sample_order.oid,
        currency='USD'
    )
    assert released == 1000.0, f"Expected released balance to be 1000.0, got {released}."

    # Available balance should now be $8,000
    available = portfolio_manager.get_available_balance('USD')
    assert available == 8000.0, f"Expected available balance to be 8000.0 after release, got {available}."


def test_get_balance(portfolio_manager):
    """
    Test that get_balance returns correct balance for given currency.
    """
    # Set balance for USD
    portfolio_manager.update_balance('USD', 10000.0)

    # Retrieve balance for USD
    balance_usd = portfolio_manager.get_balance('USD')
    assert balance_usd == 10000.0, f"Expected USD balance to be 10000.0, got {balance_usd}."

    # Retrieve balance for EUR (not set)
    balance_eur = portfolio_manager.get_balance('EUR')
    assert balance_eur == 0.0, f"Expected EUR balance to be 0.0, got {balance_eur}."

    # Set balance for EUR
    portfolio_manager.update_balance('EUR', 5000.0)

    # Retrieve updated balance for EUR
    balance_eur = portfolio_manager.get_balance('EUR')
    assert balance_eur == 5000.0, f"Expected EUR balance to be 5000.0, got {balance_eur}."


def test_update_balance(portfolio_manager):
    """
    Test that update_balance correctly sets the balance for a currency.
    """
    # Update balance for USD to $10,000
    portfolio_manager.update_balance('USD', 10000.0)
    balance_usd = portfolio_manager.get_balance('USD')
    assert balance_usd == 10000.0, f"Expected USD balance to be 10000.0, got {balance_usd}."

    # Update balance for USD to $15,000
    portfolio_manager.update_balance('USD', 15000.0)
    balance_usd = portfolio_manager.get_balance('USD')
    assert balance_usd == 15000.0, f"Expected USD balance to be 15000.0, got {balance_usd}."

    # Update balance for EUR to $5,000
    portfolio_manager.update_balance('EUR', 5000.0)
    balance_eur = portfolio_manager.get_balance('EUR')
    assert balance_eur == 5000.0, f"Expected EUR balance to be 5000.0, got {balance_eur}."


def test_update_position(portfolio_manager, sample_order):
    """
    Test that update_position correctly updates an existing position.
    """
    # Create a new position for sample_order
    portfolio_manager.create_position(
        product=sample_order.product,
        side=1,  # Buy side
        size=sample_order.size,
        last_price=sample_order.price,
        avg_price=sample_order.price,
        realized_pnl=0.0,
        unrealized_pnl=0.0
    )

    # Verify initial position
    position = portfolio_manager.get_position(sample_order.product)
    assert position is not None, "Position should exist after creation."
    assert position.size == 10, f"Expected position size to be 10, got {position.size}."
    assert position.avg_price == 100.0, f"Expected average price to be 100.0, got {position.avg_price}."
    assert position.unrealized_pnl == 0.0, f"Expected unrealized P&L to be 0.0, got {position.unrealized_pnl}."

    # Update position with an additional buy order
    portfolio_manager.update_position(
        product=sample_order.product,
        side=1,  # Buy side
        size=15,
        last_price=110.0,
        avg_price=105.0,  # New average price after additional buys
        realized_pnl=0.0,
        unrealized_pnl=50.0
    )

    # Verify updated position
    position = portfolio_manager.get_position(sample_order.product)
    assert position.size == 15, f"Expected updated position size to be 15, got {position.size}."
    assert position.avg_price == 105.0, f"Expected updated average price to be 105.0, got {position.avg_price}."
    assert position.unrealized_pnl == 50.0, f"Expected unrealized P&L to be 50.0, got {position.unrealized_pnl}."


def test_create_position(portfolio_manager, sample_order):
    """
    Test that create_position correctly initializes a new position.
    """
    # Initially, no positions should exist
    position = portfolio_manager.get_position(sample_order.product)
    assert position is None, "No position should exist before creation."

    # Create a new position
    portfolio_manager.create_position(
        product=sample_order.product,
        side=1,  # Buy side
        size=sample_order.size,
        last_price=sample_order.price,
        avg_price=sample_order.price,
        realized_pnl=0.0,
        unrealized_pnl=0.0
    )

    # Retrieve the newly created position
    position = portfolio_manager.get_position(sample_order.product)
    assert position is not None, "Position should exist after creation."
    assert position.size == 10, f"Expected position size to be 10, got {position.size}."
    assert position.avg_price == 100.0, f"Expected average price to be 100.0, got {position.avg_price}."
    assert position.unrealized_pnl == 0.0, f"Expected unrealized P&L to be 0.0, got {position.unrealized_pnl}."


def test_get_position(portfolio_manager, sample_order):
    """
    Test that get_position retrieves the correct position for a given product.
    """
    # Initially, no position should exist
    position = portfolio_manager.get_position(sample_order.product)
    assert position is None, "Position should not exist before creation."

    # Create a new position
    portfolio_manager.create_position(
        product=sample_order.product,
        side=1,  # Buy side
        size=sample_order.size,
        last_price=sample_order.price,
        avg_price=sample_order.price,
        realized_pnl=0.0,
        unrealized_pnl=0.0
    )

    # Retrieve the created position
    position = portfolio_manager.get_position(sample_order.product)
    assert position is not None, "Position should exist after creation."
    assert position.size == 10, f"Expected position size to be 10, got {position.size}."
    assert position.avg_price == 100.0, f"Expected average price to be 100.0, got {position.avg_price}."

    # Attempt to retrieve a position for a non-existent product
    non_existent_product = BaseProduct(exch="NASDAQ", name="GOOGL", base_currency="USD")
    position = portfolio_manager.get_position(non_existent_product)
    assert position is None, "Position retrieval should return None for non-existent product."
