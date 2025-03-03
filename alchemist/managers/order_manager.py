"""
order_manager.py
================

This module defines the OrderManager class, which is responsible for managing the life cycle
of orders within the automated trading system. The OrderManager validates orders, sends them
to the corresponding trading gateway via ZeroMQ, and handles various order status updates (e.g.,
opened, submitted, canceled, filled, or partially filled). It also interfaces with the PortfolioManager
to manage balance reservations and releases based on order events.

Key responsibilities include:
    - Validating orders before submission.
    - Sending order placement messages to the trading gateway.
    - Handling order status updates (open, filled, partial fill, canceled, etc.) and triggering the
      corresponding actions.
    - Coordinating with the PortfolioManager to reserve or release funds based on the order lifecycle.
    
The module ensures that orders adhere to constraints such as sufficient balance, non-duplication,
and valid price and size values.
"""


import time
import typing

from alchemist.logger import get_logger
from alchemist.zeromq import ZeroMQ
from alchemist.order import Order
from alchemist.standardized_messages import create_place_order_message
from alchemist.managers.portfolio_manager import PortfolioManager


class OrderManager:
    """
    Manages the lifecycle of orders within the automated trading system.

    Attributes:
        open_orders (Dict[str, Order]): Orders that are currently open.
        submitted_orders (Dict[str, Order]): Orders that have been submitted but not yet opened.
        zmq (ZeroMQ): Instance for sending messages to the trading gateway.
        portfolio_manager (PortfolioManager): Manages account balances and positions.
        logger: Logger instance for debugging and monitoring.
    """

    def __init__(self, zmq: ZeroMQ, portfolio_manager: PortfolioManager):
        """
        Initializes the OrderManager with ZeroMQ communication and portfolio management.

        Args:
            zmq (ZeroMQ): ZeroMQ instance used for sending messages to the gateway.
            portfolio_manager (PortfolioManager): Instance responsible for managing portfolio data.
        """
        self.open_orders: typing.Dict[str, Order] = {}
        self.submitted_orders: typing.Dict[str, Order] = {}
        
        self.logger = get_logger('om', console_logger_lv='info', file_logger_lv='debug')
        # only send message out
        # TEMP
        self.zmq = zmq
        self.portfolio_manager = portfolio_manager

    def _validate_order(self, order: Order):
        """
        Validates an order against a series of conditions to ensure it meets the system's criteria.

        This includes checks for sufficient balance, duplicate order IDs, and valid price and size values.

        Args:
            order (Order): The order to validate.

        Returns:
            dict: A dictionary with keys 'passed' (bool) indicating if the order passed validation,
                  and 'reason' (str) explaining why validation failed if applicable.
        """
        # TODO: a set of filters for weird orders
        # 1. check if the order will lead to negative cash balance
        reason = ''
        condition_filters = [
            self._check_enough_balance(order),
            self._check_duplicated_oid(order),
            self._check_valid_price(order),
            self._check_valid_size(order)
        ]
        for result in condition_filters:
            if not result['passed']:
                return result
        return {'passed': True, 'reason': ''}
            
    def _check_enough_balance(self, order: Order) -> dict:
        """
        Checks if the order has sufficient available balance in the portfolio.

        The check considers whether the order will increase or decrease the position and ensures
        that the resulting balance will not be negative.

        Args:
            order (Order): The order to check.

        Returns:
            dict: A dictionary with 'passed' as True if enough balance is available, or False with a reason.
        """
        # Here we assume that the balance will be updated immediately if the order get filled. so the balance will descrease since increasing position have already costed
        # 1.1 check the available balance
        balance = self.portfolio_manager.get_available_balance(currency=order.product.base_currency)
        required_balance = order.price * order.size
        # 1.2 check the position to see if the order reduce the position or not
        position = self.portfolio_manager.get_position(product=order.product)
        if ((not position) or (position.side == order.side)) and (balance - order.price * order.size < 0):
            # case 1: if the position is not exist, then the order will increase the position
            # case 2: if the side of postiion and order is the same, then the order will increase the position
            return {'passed': False, 'reason': f'Not enough balance: {balance=} {required_balance=}'}
        else:
            return {'passed': True, 'reason': ''}

    def _check_duplicated_oid(self, order: Order) -> dict:
        """
        Checks if an order with the same order ID (oid) already exists.

        Args:
            order (Order): The order to check.

        Returns:
            dict: A dictionary indicating if the order ID is unique.
        """
        oid = order.oid
        existing_order = self.get_order(oid=oid)
        if existing_order is not None:
            return {'passed': False, 'reason': f'Duplicated order {oid=}'}
        return {'passed': True, 'reason': ''}

    def _check_valid_price(self, order: Order) -> dict:
        """
        Validates that the order price is positive if the order type is LIMIT.

        Args:
            order (Order): The order to validate.

        Returns:
            dict: A dictionary with validation result for the price.
        """
        passed = True
        if order.order_type == 'LIMIT':
            passed = order.price > 0 if order.price else False
        if passed:
            return {"passed": True, 'reason': ''}
        else:
            return {'passed': False, 'reason': f'price must be larger than 0: {order.price=}'}
    
    def _check_valid_size(self, order: Order) -> dict:
        """
        Ensures that the order size is greater than zero.

        Args:
            order (Order): The order to validate.

        Returns:
            dict: A dictionary with validation result for the order size.
        """
        if order.size > 0:
            return {'passed': True, 'reason': ''}
        else:
            return {'passed': False, 'reason': f'size must be larger than 0: {order.size=}'}
        
    def place_order(self, order: Order):
        """
        Validates and places an order by sending it via ZeroMQ to the appropriate gateway.

        If the order passes validation, a place order message is constructed and sent.
        If sending the message fails, the order is internally rejected.

        Args:
            order (Order): The order to be placed.
        """
        validation_result = self._validate_order(order)
        if validation_result['passed']:
            self.logger.debug(f'{order.strategy=} {order=}')
            # send to the corresponding gatewa=y via zeromq 
            zmq_msg = create_place_order_message(
                ts=time.time(),
                gateway=order.gateway,
                strategy=order.strategy,
                product=order.product,
                oid=order.oid,
                side=order.side,
                price=order.price,
                size=order.size,
                order_type=order.order_type,
            )
            try:
                self.zmq.send(*zmq_msg)
                self.on_submitted(order)
            except:
                self.on_internal_rejected(order, reason=f'Error happend when sending messages in zeromq: {zmq_msg=}')        
        else:
            self.on_internal_rejected(order, reason=validation_result['reason'])

    def on_order_status_update(self, gateway: str, order_update: dict):
        """
        Handles updates to order statuses received from the trading gateway.

        Depending on the status provided in the update, the corresponding handler is invoked.
        Supported statuses include: OPENED, REJECTED, CANCELED, AMENDED, FILLED, and PARTIAL_FILLED.

        Args:
            gateway (str): The identifier of the gateway sending the update.
            order_update (dict): A dictionary containing the update details for an order.
        """
        # update order status using order_update
        # for pdt in order_update['data'].items():
        update = order_update['data']
        status_update = update['status']
        oid = update['oid']

        # check if the order exists internally
        if self.get_order(oid=oid) is not None:
            if status_update == 'OPENED':
                return self.on_opened(oid)
            elif status_update == 'REJECTED':
                return self.on_rejected(oid, reason=update.get('reason', ''))
            elif status_update == 'CANCELED':
                return self.on_canceled(oid)
            elif status_update == 'AMENDED':
                return self.on_amended(oid, price=update['amend_price'], size=update['amend_size'])
            elif status_update == 'FILLED':
                return self.on_filled(oid, price=update['last_filled_price'], size=update['last_filled_size'])
            elif status_update == 'PARTIAL_FILLED':
                return self.on_partial_filled(oid, price=update['last_filled_price'], size=update['last_filled_size'])
        else:
            self.logger.error(f'order {oid=} not found: {update=}.')
    
    def get_order(self, oid: str) -> typing.Union[None, Order]:
        """
        Retrieves an order by its order ID from either submitted or open orders.

        Args:
            oid (str): The unique order ID.

        Returns:
            Order or None: The corresponding order if found; otherwise, None.
        """
        return (
            self.submitted_orders.get(oid) or 
            self.open_orders.get(oid)
        )

    def on_opened(self, oid: str) -> typing.List[str]:
        """
        Processes the event when an order transitions to the OPENED state.

        This method moves the order from submitted orders to open orders, updates its state,
        and logs the change.

        Args:
            oid (str): The order ID that has been opened.

        Returns:
            List[str]: A list of status updates triggered by this event.
        """
        order_status_updates = []
        order = self.submitted_orders.get(oid, None)
        if order:
            order.on_opened()
            order_status_updates.append('OPENED')
            self.open_orders[order.oid] = order
            # remove the order from the submitted_orders
            del self.submitted_orders[oid]
            self.logger.debug(f'{order=}')
        return order_status_updates

    def on_submitted(self, order: Order) -> typing.List[str]:
        """
        Handles the event when an order is successfully submitted.

        The order is marked as submitted, stored in the submitted orders dictionary,
        and funds are reserved via the PortfolioManager.

        Args:
            order (Order): The order that has been submitted.

        Returns:
            List[str]: A list containing the 'SUBMITTED' status update.
        """
        order_status_updates = []
        order.on_submitted()
        order_status_updates.append('SUBMITTED')
        self.submitted_orders[order.oid] = order
        self.logger.debug(f'{order=}')
        self.portfolio_manager.reserve_balance(
            oid=order.oid,
            currency=order.product.base_currency,
            value=order.price * order.size, # TODO: not quite correct
        )
        return order_status_updates

    def on_amend_submitted(self, oid: str) -> typing.List[str]:
        """
        Handles the event when an order amendment is submitted.

        Args:
            oid (str): The order ID of the order being amended.

        Returns:
            List[str]: A list containing the 'AMEND_SUBMITTED' status update.
        """
        order_status_updates = []
        order = self.open_orders.get(oid, None)
        if order:
            order.on_amend_submitted()
            order_status_updates.append('AMEND_SUBMITTED')
            self.logger.debug(f'{order=}')
        return order_status_updates

    def on_amended(self, oid: str, price: float, size: float):
        """
        Handles the event when an order has been amended.

        Updates the order's price and size, and logs the change.

        Args:
            oid (str): The order ID of the amended order.
            price (float): The new price.
            size (float): The new size.

        Returns:
            List[str]: A list containing the 'AMENDED' status update.
        """
        order_status_updates = []
        order = self.open_orders.get(oid, None)
        if order:
            order.on_amended(price=price, size=size)
            order_status_updates.append('AMENDED')
            self.logger.debug(f'{order=}')
        return order_status_updates
    
    def on_cancel_submitted(self, oid) -> typing.List[str]:
        """
        Handles the event when an order cancellation is submitted.

        Args:
            oid (str): The order ID for which cancellation is submitted.

        Returns:
            List[str]: A list containing the 'CANCEL_SUBMITTED' status update.
        """
        order_status_updates = []
        order = self.open_orders.get(oid, None)
        if order:
            order.on_cancel_submitted()
            order_status_updates.append('CANCEL_SUBMITTED')
            self.logger.debug(f'{order=}')
        return order_status_updates

    def on_canceled(self, oid) -> typing.List[str]:
        """
        Processes the cancellation of an order.

        The order's state is updated to canceled, it is removed from open orders,
        and reserved funds are released.

        Args:
            oid (str): The order ID that has been canceled.

        Returns:
            List[str]: A list containing the 'CANCELED' status update.
        """
        order_status_updates = []
        order = self.open_orders.get(oid, None)
        if order:
            order.on_canceled()
            order_status_updates.append('CANCELED')
            # remove the order from the open_orders
            del self.open_orders[oid]
            self.logger.debug(f'{order=}')
            self.portfolio_manager.release_balance(
                oid,
                currency=order.product.base_currency,
            )
        return order_status_updates
    
    def on_rejected(self, oid: str, reason: str=''):
        """
        Processes the event when an order is rejected by the gateway.

        Updates the order state to rejected, removes it from internal tracking,
        and releases any reserved funds.

        Args:
            oid (str): The order ID that has been rejected.
            reason (str, optional): Reason for rejection.

        Returns:
            List[str]: A list containing the 'REJECTED' status update.
        """
        order_status_updates = []
        # remove the order from the open_orders
        order = self.open_orders.pop(oid, None)
        if not order:
            # submitted but not opened
            order = self.submitted_orders.pop(oid, None)
        if order:
            order.on_rejected(reason=reason)  # TODO: add reason, should be passed from the gateway
            order_status_updates.append('REJECTED')
            self.logger.debug(f'{order=}')
            self.portfolio_manager.release_balance(
                oid,
                currency=order.product.base_currency,
            )
        return order_status_updates

    def on_internal_rejected(self, order: Order, reason: str=''):
        """
        Handles the rejection of an order due to internal validation or messaging errors.

        Updates the order state to internal rejected and releases any reserved funds.

        Args:
            order (Order): The order that is being rejected.
            reason (str, optional): Explanation for the internal rejection.

        Returns:
            List[str]: A list containing the 'INTERNAL_REJECTED' status update.
        """
        print(f'on_internal_rejected {reason=}')
        order_status_updates = []
        order.on_internal_rejected(reason=reason)
        order_status_updates.append('INTERNAL_REJECTED')
        self.logger.debug(f'{order=}')
        self.portfolio_manager.release_balance(
            order.oid,
            currency=order.product.base_currency,
        )
        return order_status_updates
    
    def on_filled(self, oid: str, price: float, size: float):
        """
        Processes the event when an order is completely filled.

        Updates the order state to filled, removes the order from tracking, and releases reserved funds.

        Args:
            oid (str): The order ID that has been filled.
            price (float): The price at which the order was filled.
            size (float): The quantity filled.

        Returns:
            List[str]: A list containing the 'FILLED' status update.
        """
        order_status_updates = []
        order = self.open_orders.get(oid, None)
        if not order:
            # case 1: the order is opened, but the broker only give you updates of the filled message
            order = self.submitted_orders.get(oid, None)
            if order:
                # complete the life cycle
                order_status_updates.extend(self.on_opened(oid))
                order.on_filled(price, size)
                order_status_updates.append('FILLED')
            else:
                return order_status_updates # REMINDER: just ignore this message if the order is either opened or submitted. This may be due to the duplicate message from the broker
        else:
            # case 2: the order is opened, then we receive a filled message
            order.on_filled(price, size)
            order_status_updates.append('FILLED')
        # remove the order from the open_orders
        del self.open_orders[oid]
        self.logger.debug(f'{order=}')
        self.portfolio_manager.release_balance(
            order.oid,
            currency=order.product.base_currency,
        )
        return order_status_updates

    def on_partial_filled(self, oid: str, price: float, size: float):
        """
        Processes the event when an order is partially filled.

        Updates the order's state accordingly and releases a portion of the reserved funds based on the filled quantity.

        Args:
            oid (str): The order ID that is partially filled.
            price (float): The price at which the partial fill occurred.
            size (float): The quantity that was filled.

        Returns:
            List[str]: A list containing the 'PARTIAL_FILLED' status update.
        """
        order_status_updates = []
        order = self.open_orders.get(oid, None)
        if not order:
            # case 1: the order is opened, but the broker only give you updates of the filled message
            order = self.submitted_orders.get(oid, None)
            if order:
                # complete the life cycle
                order_status_updates.extend(self.on_opened(oid))
                order.on_partial_filled(price, size)
                order_status_updates.append('PARTIAL_FILLED')
            else:
                return  # REMINDER: just ignore this message if the order is either opened or submitted. This may be due to the duplicate message from the broker
        else:
            # case 2: the order is opened, then we receive a filled message
            order.on_partial_filled(price, size)
            order_status_updates.append('PARTIAL_FILLED')
            # # remove the order from the open_orders
            # del self.open_orders[oid]
        self.logger.debug(f'{order=}')
        self.portfolio_manager.release_balance(
            order.oid,
            currency=order.product.base_currency,
            value=price * size,
        )
        return order_status_updates


    