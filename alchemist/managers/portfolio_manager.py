"""
portfolio_manager.py
====================

This module implements the PortfolioManager class which is responsible for managing account balances,
positions, and reserved funds within the automated trading system. It provides methods to compute
the overall portfolio value, track available versus reserved capital, and update positions based on
trading activity.

Key functionalities include:
    - Retrieving the current cash balance and reserved balance for a specific currency.
    - Reserving funds when an order is placed and releasing them when an order is filled or canceled.
    - Maintaining and updating positions for various products, ensuring that the portfolio reflects
      real-time trading activities.
"""

import typing

from alchemist.position import Position
from alchemist.products.base_product import BaseProduct
from alchemist.logger import get_logger


class PortfolioManager:
    """
    Manages cash balances, positions, and reserved funds for active orders.

    Attributes:
        balances (Dict[str, float]): Mapping of currency codes to available cash balances.
        positions (Dict[str, Dict[str, Position]]): Nested dictionary mapping exchange identifiers
            to product positions.
        reserved_balances (Dict[str, Dict[str, float]]): Mapping of currency codes to dictionaries that
            track reserved amounts for specific order IDs.
        logger: Logger instance for recording portfolio-related events.
    """
    def __init__(self):
        """
        Initializes the PortfolioManager with empty data structures for balances, positions,
        and reserved balances, and sets up logging.
        """
        self.balances: typing.Dict[str, float] = {}
        self.positions: typing.Dict[str, typing.Dict[str, Position]] = {}
        # currency -> oid -> reserved balance 
        self.reserved_balances: typing.Dict[str, typing.Dict[str, float]] = {}
        
        self.logger = get_logger('pm', console_logger_lv='info', file_logger_lv='debug')

    def get_portfolio_value(self, currency):
        """
        Calculates the total portfolio value in the specified currency.

        The portfolio value is computed by summing the cash balance with the value of all positions
        that have a matching base currency. The value of each position is derived from its side, size,
        average price, and unrealized profit and loss.

        Args:
            currency (str): The currency code for which the portfolio value is computed.

        Returns:
            float: The total portfolio value in the specified currency.
        """
        # currency value
        # only count the balance with currency
        cash = self.balances[currency]
        for exch, positions in self.positions.items():
            for pdt, position in positions.items():
                if position.product.base_currency == currency:
                    cash += (position.side * position.size * position.avg_price + position.unrealized_pnl)
        return cash

    def get_reserved_balance(self, currency: float, oid: str=None) -> float:
        """
        Retrieves the reserved (locked) balance for the given currency.

        If an order ID (oid) is provided, returns the reserved amount specific to that order.
        Otherwise, returns the total reserved balance across all orders for the currency.

        Args:
            currency (str): The currency code.
            oid (str, optional): Specific order ID to retrieve the reserved amount for.

        Returns:
            float: The reserved balance for the currency or for the specific order if oid is provided.
        """
        if currency in self.reserved_balances:
            if oid:
                return self.reserved_balances[currency].get(oid, 0.0)
            else:
                return sum(self.reserved_balances[currency].values())
        else:
            return 0.0

    def reserve_balance(self, oid: str, currency: str, value: float):
        """
        Reserves (locks) a specified amount of capital for an order to avoid overcommitting funds.

        Before reserving, the method checks if the available balance is sufficient. If not,
        it logs a warning and returns False. Otherwise, it reserves the amount and logs the reservation.

        Args:
            oid (str): The unique order identifier.
            currency (str): The currency code.
            value (float): The amount to reserve.

        Returns:
            bool: True if the reservation was successful; False otherwise.
        """
        available = self.get_available_balance(currency)
        if value > available:
            self.logger.warning(f"Not enough available balance to reserve for {oid=}. needed={value}, {available=}")
            # Depending on your logic, you might reject the order here or raise an exception
            return False
        if currency not in self.reserved_balances:
            self.reserved_balances[currency] = {}
        self.reserved_balances[currency][oid] = value
        self.logger.debug(f"Reserved {value=} {currency} for order {oid=}. Pending: {self.reserved_balances}")
        return True

    def release_balance(self, oid: str, currency, value: typing.Optional[float]=None) -> float:
        """
        Releases reserved capital for a given order, either partially or fully.

        If a value is provided, the reserved amount for the order is reduced by that value (partial release).
        Otherwise, the entire reserved amount for the order is released.

        Args:
            oid (str): The unique order identifier.
            currency (str): The currency code.
            value (float, optional): The amount to release (for partial releases).

        Returns:
            float: The amount of capital released. If no reservation exists, returns 0.0.
        """
        if currency in self.reserved_balances:
            if oid in self.reserved_balances[currency]:
                if value is not None:
                    # reduce by value for partial filled
                    self.reserved_balances[currency][oid] -= value
                    released = value
                else:
                    released = self.reserved_balances[currency].pop(oid)
                self.logger.debug(f"Released {released} capital from order {oid=}. Pending: {self.reserved_balances}")
                return released
        return 0.0

    def get_available_balance(self, currency: float) -> float:
        """
        Computes the available balance for the specified currency.

        The available balance is determined by subtracting the total reserved balance from the overall
        cash balance.

        Args:
            currency (str): The currency code.

        Returns:
            float: The available balance, ensuring that it is not negative.
        """
        balance = self.get_balance(currency)
        reserved_balance = self.get_reserved_balance(currency)
        return max(0, balance - reserved_balance)

    def get_balance(self, currency) -> float:
        """
        Retrieves the current cash balance for a specified currency.

        Args:
            currency (str): The currency code.

        Returns:
            float: The cash balance for the given currency. Defaults to 0.0 if not set.
        """
        return self.balances.get(currency, 0.0)
    
    def get_position(self, product: BaseProduct):
        """
        Retrieves the position for a given product.

        The method checks if the product's exchange and name exist in the positions mapping.

        Args:
            product (BaseProduct): The product for which to retrieve the position.

        Returns:
            Position or None: The corresponding position if it exists; otherwise, None.
        """
        exch, pdt = product.exch, product.name
        if exch in self.positions:
            if pdt in self.positions[exch]:
                return self.positions[exch][pdt]
        return None

    def update_balance(self, currency, value):
        """
        Updates the cash balance for a given currency.

        This method sets the balance for the specified currency to the new value and logs the update.

        Args:
            currency (str): The currency code.
            value (float): The new cash balance.
        """
        self.balances[currency] = value
        self.logger.debug(f'balances={self.balances}')

    def create_position(self, product: BaseProduct, side: int, size: float, last_price: float, avg_price: float, realized_pnl: None, unrealized_pnl: None):
        """
        Creates a new position for a specified product.

        The position is stored under the product's exchange and name, and includes metrics such as side,
        size, last price, average price, and profit and loss figures. The creation of a new position is
        logged for tracking.

        Args:
            product (BaseProduct): The product for which the position is created.
            side (int): The position side (e.g., 1 for long, -1 for short).
            size (float): The size or quantity of the position.
            last_price (float): The most recent traded price.
            avg_price (float): The average price of the position.
            realized_pnl: The realized profit and loss.
            unrealized_pnl: The unrealized profit and loss.
        """
        exch, pdt = product.exch, product.name
        self.positions[exch] = {pdt: Position(product, side, size, last_price, avg_price, realized_pnl, unrealized_pnl)}
        self.logger.debug(f'positions={self.positions}')
        # return self.positions[exch][pdt]
    
    def update_position(self, product: BaseProduct, side: int, size: float, last_price: float, avg_price: float, realized_pnl: float, unrealized_pnl: float):
        """
        Updates an existing position for a specified product with new metrics.

        The position is updated with the latest side, size, last price, average price, and profit and loss
        values. The update is logged to reflect the changes.

        Args:
            product (BaseProduct): The product for which the position is updated.
            side (int): The updated position side.
            size (float): The updated size or quantity.
            last_price (float): The updated last traded price.
            avg_price (float): The updated average price.
            realized_pnl (float): The updated realized profit and loss.
            unrealized_pnl (float): The updated unrealized profit and loss.
        """
        exch, pdt = product.exch, product.name
        position = self.positions[exch][pdt]
        position.update(side, size, last_price, avg_price, realized_pnl, unrealized_pnl)
        self.logger.debug(f'positions={self.positions}')
