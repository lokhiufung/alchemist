"""
ib_gateway.py
=============

This module implements the IbGateway class, which serves as the integration layer between the
automated trading system and Interactive Brokers (IB) API. It provides functionality for:

- Establishing and managing a persistent connection to IB's Trader Workstation (TWS).
- Subscribing to real-time market data streams, including tick-by-tick data and real-time bars.
- Converting between internal product representations and IB Contract objects.
- Handling order placements, updates, and cancellations.
- Processing account updates and portfolio changes.
- Routing market data and order/account events via ZeroMQ messaging for further processing in the trading system.

The IbGateway is implemented as a Ray remote actor to facilitate distributed processing and to 
enable parallel operations across multiple instruments and data streams.

Key components include:
    - Connection management: Establishes and maintains a connection to IB with reconnection logic.
    - Data subscription: Subscribes to tick-by-tick and bar data, then processes and forwards this data.
    - Order management: Places orders, tracks order statuses, and handles order events.
    - Product conversion: Translates internal product definitions to IB API contracts and vice versa.
    - Messaging: Sends processed messages (ticks, bars, balance updates, order statuses) through ZeroMQ.

Usage Example:
    >>> import ray
    >>> ray.init()
    >>> subscriptions = ["tick_data", "bar_data"]
    >>> zmq_send_port = 5555
    >>> zmq_recv_ports = [5556]
    >>> accounts = [{"host": "127.0.0.1", "port": 7497, "client_id": 123}]
    >>> ib_gateway = IbGateway.remote(subscriptions, zmq_send_port, zmq_recv_ports, accounts=accounts)
    >>> ray.get(ib_gateway._run_connection_server.remote())
    
Dependencies:
    - IB API (ibapi)
    - Ray (for distributed processing)
    - ZeroMQ (for inter-process messaging)
    - Other internal modules: IBClient, IBWrapper, BaseGateway, etc.
"""


import typing
import time
from collections import defaultdict
from threading import Thread
from typing import Callable, List, Literal
import datetime

import numpy as np
import ray

from decimal import Decimal
from ibapi.wrapper import TickerId
from ibapi.contract import Contract
from ibapi.wrapper import *
# from ibapi.ticktype import TickTypeEnum
from ibapi.order import Order
from ibapi.account_summary_tags import *
from alchemist.gateways.ib_gateway.ib_client import IBClient
from alchemist.gateways.ib_gateway.ib_wrapper import IBWrapper
from alchemist.gateways.base_gateway import BaseGateway
from alchemist.products.base_product import BaseProduct
from alchemist.standardized_messages import create_position_update
from alchemist.gateways.ib_gateway.ib_convertor import IbConvertor


@ray.remote
class IbGateway(IBClient, IBWrapper, BaseGateway):
    """
    IbGateway Class

    Serves as the gateway between the automated trading system and the Interactive Brokers API.
    This class handles connections, market data subscriptions, order management, and account/portfolio
    updates. It leverages IBClient and IBWrapper for direct IB API interactions and inherits from BaseGateway
    to conform to our internal gateway interface.

    Attributes:
        NAME (str): Identifier for the gateway.
        CONVERTOR (IbConvertor): Utility to convert between internal order/product representations and IB formats.
        _connection_thread (Thread): Thread managing the IB connection.
        _background_thread (Thread): Thread for executing periodic background tasks.
        _subscribed_market_data_tick_types (defaultdict): Tracks subscribed tick data types for each product.
        _request_id (int): Incremental request identifier for IB API calls.
        _next_order_id (int): Next order identifier provided by IB.
        _request_id_to_product (dict): Maps IB request IDs to internal product objects.
        _pdt_to_request_id (dict): Maps product names to IB request IDs.
    """

    NAME = 'ib_gateway'
    CONVERTOR = IbConvertor()

    def __init__(self, subscriptions: List[str], zmq_send_port: int, zmq_recv_ports: List[int], accounts=None, products=None, reconnect_interval=5):
        """
        Initialize the IbGateway.

        Args:
            subscriptions (List[str]): List of market data subscriptions.
            zmq_send_port (int): Port number for sending messages via ZeroMQ.
            zmq_recv_ports (List[int]): List of port numbers for receiving messages via ZeroMQ.
            accounts (optional): Account configuration for connecting to IB.
            products (optional): List of product objects to subscribe to.
            reconnect_interval (int, optional): Time (in seconds) to wait before attempting reconnection.
        """
        IBClient.__init__(self)
        IBWrapper.__init__(self)
        BaseGateway.__init__(self, subscriptions, zmq_send_port, zmq_recv_ports, products=products, accounts=accounts, reconnect_interval=reconnect_interval)
        
        self._connection_thread = None
        
        self._background_task_freq = 10  # in seconds
        self._background_thread = None
        self._subscribed_market_data_tick_types = defaultdict(list)

        self._request_id = 0
        self._next_order_id = None
        self._request_id_to_product = {}
        self._pdt_to_request_id = {}

    def is_connected(self):
        """
        Check the connection status to IB.

        Returns:
            bool: True if connected, False otherwise.
        """
        return self._is_connected 
    
    def _run_connection_server(self):
        """
        Establishes connection to IB and starts the connection thread.

        This method:
            - Connects to IB's TWS using account credentials.
            - Starts a daemon thread to run the IB API event loop.
            - Waits for a successful connection before subscribing to market data.
            - Invokes the _on_connected callback upon successful connection.
        """
        super().connect(host=self.accounts[0].host, port=self.accounts[0].port, clientId=self.accounts[0].client_id)
        self._connection_thread = Thread(
            name=f'{self.NAME}_api',
            target=self.run, 
            daemon=True
        )
        self._connection_thread.start()
        self._logger.debug(f'{self.NAME} thread started')

        if self._wait(self.is_connected, reason='connection'):
            time.sleep(1)
            self._subscribe()
            self._on_connected()

    def _wait(self, condition_func: Callable, reason: str='', timeout: int=10):
        """
        Wait for a given condition to be met.

        Args:
            condition_func (Callable): Function to check the condition.
            reason (str, optional): Description of the condition.
            timeout (int, optional): Maximum number of seconds to wait.

        Returns:
            bool: True if condition is met within the timeout, False otherwise.
        """
        while timeout:
            if condition_func():
                self._logger.debug(f'{reason} is successful')
                return True
            timeout -= 1
            time.sleep(1)
            self._logger.debug(f'waiting for {reason}')
        else:
            self._logger.error(f'failed waiting for {reason}')
            return False

    def convert_to_ib_contract(self, product: BaseProduct) -> Contract:
        """
        Convert an internal product representation to an IB Contract.

        Args:
            product (BaseProduct): The internal product object.

        Returns:
            Contract: The corresponding IB contract.
        """
        contract = Contract()
        contract.symbol = product.name
        contract.exchange = product.exch
        contract.currency = product.base_currency
        if product.product_type == 'FUTURE':
            contract.secType = 'FUT'
            contract.lastTradeDateOrContractMonth = datetime.datetime.strptime(product.contract_month, "%Y-%m").strftime("%Y%m")
        elif product.product_type == 'STOCK':
            contract.secType = 'STK'
        return contract

    def convert_to_product(self, contract: Contract) -> BaseProduct:
        """
        Convert an IB Contract to an internal product representation.

        Args:
            contract (Contract): The IB contract.

        Returns:
            BaseProduct: The corresponding internal product object.

        Raises:
            ValueError: If the contract type or exchange is not recognized.
        """
        if contract.secType == 'FUT':
            from alchemist.products.future_product import FutureProduct

            # Ensure lastTradeDateOrContractMonth is handled for both YYYYMM and YYYYMMDD
            contract_month_str = contract.lastTradeDateOrContractMonth
            if len(contract_month_str) == 6:  # Handle YYYYMM format
                contract_month = datetime.datetime.strptime(contract_month_str, '%Y%m').strftime('%Y-%m')
            elif len(contract_month_str) == 8:  # Handle YYYYMMDD format
                contract_month = datetime.datetime.strptime(contract_month_str, '%Y%m%d').strftime('%Y-%m')
            else:
                raise ValueError(f"Unexpected contract month format: {contract.lastTradeDateOrContractMonth}")
        
            return FutureProduct(
                name=contract.symbol,
                base_currency=contract.currency,
                exch=contract.exchange or contract.primaryExchange,  # REMINDER: Both exchange and primaryExchange can be empty in some cases
                contract_month=contract_month
            )
        elif contract.secType == 'STK':
            from alchemist.products.stock_product import StockProduct

            if contract.exchange:
                exch = contract.exchange
            else:
                exch = contract.primaryExchange
            
            if not exch:
                raise ValueError(f'Contract is not recognized: {contract=}')
            
            if exch == 'ISLAND':
                exch = 'NASDAQ'

            return StockProduct(
                name=contract.symbol,
                base_currency=contract.currency,
                exch=exch
            )
        else:
            raise ValueError(f'Contract is not recognized: {contract=}')

    def _subscribe_tick(self, product):
        """
        Subscribe to tick-by-tick data for a given product.

        Converts the internal product to an IB contract and requests tick-by-tick data from IB.
        Tracks the request ID to product mapping for later reference.

        Args:
            product (BaseProduct): The product to subscribe to.
        """
        self._logger.info(f'subscribing tick data {self._request_id}')
        contract = self.convert_to_ib_contract(product)
        self._logger.info(f'{contract=}')
        self.reqTickByTickData(
            self._request_id,
            contract,
            'Last',
            # IB will continue sending ticks to you if set to 0
            0,
            False,
        )
        self._logger.debug(f'{self.NAME} requested (req_id={self._request_id}) {contract.symbol} tick by tick data')
        self._request_tick_by_tick_data(
            request_id=self._request_id,
            tick_type='Last',
            product=contract
        )
        pdt = product.name
        # write down the requestId
        self._request_id_to_product[self._request_id] = product
        self._pdt_to_request_id[pdt] = self._request_id
        self._request_id += 1  # each symbol takes 1 unique request id
        self._logger.info(f'next request_id is {self._request_id}')

    def _subscribe_quote(self, product):
        """
        Placeholder for subscribing to quote data for a product.

        Args:
            product (BaseProduct): The product for which to subscribe to quotes.
        """
        pass

    def _subscribe_bar(self, product):
        """
        Subscribe to real-time bar data for a given product.

        Converts the internal product to an IB contract and requests real-time bar data.
        Updates the request mappings accordingly.

        Args:
            product (BaseProduct): The product to subscribe to.
        """
        contract = self.convert_to_ib_contract(product)
        self.reqRealTimeBars(
            reqId=self._request_id,
            contract=contract,
            barSize=5,
            whatToShow='TRADES',
            useRTH=False,
            realTimeBarsOptions=[]
        )
        pdt = product.name
        # write down the requestId
        self._request_id_to_product[self._request_id] = product
        self._pdt_to_request_id[pdt] = self._request_id
        self._request_id += 1  # each symbol takes 1 unique request id

    def _subscribe_account_update(self, account):
        """
        Subscribe to account updates from IB.

        Args:
            account (dict): The account configuration for which to subscribe to updates.
        """
        self.reqAccountUpdates(True, account.acc)
        # pass

    def _subscribe_account_summary(self, account):
        """
        Subscribe to account summary updates from IB.

        Args:
            account (dict): The account configuration for which to subscribe to account summary.
        """
        self.reqAccountSummary(self._request_id, 'ALL', AccountSummaryTags)
        # pass

    def _unsubscribe(self):
        """
        Placeholder for unsubscribing from all market data subscriptions.
        """
        self._sub_num = 0
        self._num_subscribed = 0

    # def _update_orderbook(self, req_id, position: int, operation: int, side: int, px, qty, **kwargs):
    #     def _update(boa: list):
    #         if operation == 0:
    #             boa.insert(position, (Decimal(px), qty))
    #         elif operation == 1:
    #             boa[position] = (Decimal(px), qty)
    #         elif operation == 2:
    #             del boa[position]
    #     try:
    #         product = self._req_id_to_product[req_id]
    #         if side == 0:
    #             asks = self._asks[product.pdt]
    #             _update(asks)
    #         else:
    #             bids = self._bids[product.pdt]
    #             _update(bids)
    #         quote = {'ts': time.time(), 'other_info': kwargs, 'data': {'bids': bids, 'asks': asks}}
    #         zmq_msg = (1, 1, (self.NAME, product.exch, product.pdt, quote))
    #         self._zmq.send(*zmq_msg)
    #     except:
    #         self._logger.exception(f'_update_orderbook exception ({position=} {operation=} {side=} {px=} {qty=} {kwargs=}):')

    # Methods from EWrapper
    #####
    # missing 1 required positional argument: 'advancedOrderRejectJson'
    # this happens sometime but not always, wired. leave a default value for now for safety
    # TODO
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson: str=''):  
        """
        Handles errors received from IB's TWS.

        Logs the error details and invokes the parent error handler.

        Args:
            reqId (TickerId): The request ID associated with the error.
            errorCode (int): The error code.
            errorString (str): The error message.
            advancedOrderRejectJson (str, optional): Additional error details in JSON format.
        """
        self._logger.error(f'Error from TWS {reqId=} {errorCode=} {errorString=}')
        super().error(reqId, errorCode, errorString)

    def nextValidId(self, orderId: int):
        """
        Receives the next valid order ID from IB.

        Sets the internal _next_order_id attribute and logs the value.

        Args:
            orderId (TickerId): The next valid order identifier.
        """
        super().nextValidId(orderId)
        self._next_order_id = orderId
        self._logger.debug(f'{self._next_order_id=}')

    def connectAck(self):
        """
        Called when a connection acknowledgment is received from IB.

        Invokes the _on_connected callback.
        """
        super().connectAck()
        self._on_connected()

    def connectionClosed(self):
        """
        Handles the event when the connection to IB is closed.

        Invokes the _on_disconnected callback.
        """
        super().connectionClosed()
        self._on_disconnected()

    def tickByTickAllLast(self, reqId: int, tickType: int, ts: int, price: float,
                          size: Decimal, tickAttribLast: TickAttribLast, exch: str,
                          specialConditions: str):
        """
        Processes tick-by-tick trade data from IB.

        Forwards the received tick data via ZeroMQ after logging the event.

        Args:
            reqId (int): The request ID associated with the tick data.
            tickType (int): The type of tick.
            ts (int): The timestamp of the tick.
            price (float): The trade price.
            size (Decimal): The trade size.
            tickAttribLast: Additional tick attributes.
            exch (str): The exchange where the trade occurred.
            specialConditions (str): Special conditions associated with the tick.
        """
        product = self._request_id_to_product[reqId]
        # TODO: consider using int for the moment
        # ts = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        # TODO, HALTED
        # if price == 0 and size == 0 and tickAttribLast.pastLimit:
        self._logger.debug(f'reqId: {reqId}, tickType: {tickType}, ts: {ts}, price: {price}, size: {size}, '
                       f'tickAttribLast: {tickAttribLast}, exchange: {exch}, specialConditions: {specialConditions}')
        
        zmq_msg = self.create_tick_message(ts, self.NAME, product.exch, product.name, price=float(price), size=float(size))
        self._zmq.send(*zmq_msg)
        super().tickByTickAllLast(reqId, tickType, ts, price, size, tickAttribLast, exch, specialConditions)

    def realtimeBar(self, reqId: int, ts: int, open_: float, high: float, low: float, close: float, volume: Decimal, wap: Decimal, count: int):
        """
        Processes real-time bar data from IB.

        Logs the bar data and sends it via ZeroMQ for further processing.

        Args:
            reqId (int): The request ID for the bar data.
            ts (int): The timestamp of the bar.
            open_ (float): The opening price.
            high (float): The high price.
            low (float): The low price.
            close (float): The closing price.
            volume (Decimal): The volume during the bar.
            wap (Decimal): The weighted average price.
            count (int): The number of trades in the bar.
        """
        product = self._request_id_to_product[reqId]

        self._logger.debug(f'reqId: {reqId}, ts: {ts}, open_: {open_}, high: {high}, low: {low}, '
                       f'close: {close}, volume: {volume}, wap: {wap}, count: {count}')
        
        zmq_msg = self.create_bar_message(ts, self.NAME, product.exch, product.name, '5s', open_, high, low, close, float(volume))
        self._zmq.send(*zmq_msg)

        super().realtimeBar(reqId, ts, open_, high, low, close, volume, wap, count)
    
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """
        Processes account value updates from IB.

        Sends balance update messages via ZeroMQ when relevant keys (e.g., TotalCashBalance) are received.

        Args:
            key (str): The account value key.
            val (str): The value.
            currency (str): The currency associated with the value.
            accountName (str): The account name.
        """
        ts = time.time()
        self._logger.debug(f'{ts=} {key=} {val=} {currency=} {accountName}')
        if (key == 'TotalCashBalance') and (currency != 'BASE'):
            balance_update = {
                currency: float(val),
            }
            zmq_msg = self.create_balance_update_message(ts, self.NAME, balance_update)
            self._zmq.send(*zmq_msg)
        super().updateAccountValue(key, val, currency, accountName)

    def updatePortfolio(self, contract: Contract, position: Decimal, marketPrice: float, marketValue: float, averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str):
        """
        Processes portfolio updates from IB.

        Converts the IB contract to an internal product, constructs a position update message,
        and sends it via ZeroMQ.

        Args:
            contract (Contract): The IB contract.
            position (Decimal): The position size.
            marketPrice (float): The current market price.
            marketValue (float): The market value of the position.
            averageCost (float): The average cost of the position.
            unrealizedPNL (float): Unrealized profit and loss.
            realizedPNL (float): Realized profit and loss.
            accountName (str): The account name.
        """
        self._logger.debug(f'updatePortfolio {contract=} {position=} {marketPrice=} {marketValue=} {averageCost=} {unrealizedPNL=} {realizedPNL=} {accountName=}')
        product = self.convert_to_product(contract)
        side = np.sign(position)
        ts = time.time()
        position_update = create_position_update(
            product.name,
            side,
            size=abs(float(position)),  # alway positive
            last_price=float(marketPrice),
            average_price=float(averageCost),
            unrealized_pnl=float(unrealizedPNL),
            realized_pnl=float(realizedPNL),
        )
        zmq_msg = self.create_position_update_message(ts, self.NAME, product.exch, position_update)
        self._zmq.send(*zmq_msg)
        super().updatePortfolio(contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName)

    def orderStatus(self, orderId: TickerId, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: TickerId, parentId: TickerId, lastFillPrice: float, clientId: TickerId, whyHeld: str, mktCapPrice: float):
        """
        Processes order status updates from IB.

        Constructs and sends order update messages via ZeroMQ based on the order's status.

        Args:
            orderId (TickerId): The IB order ID.
            status (str): The order status (e.g., 'Submitted', 'Filled', 'Cancelled').
            filled (Decimal): The filled quantity.
            remaining (Decimal): The remaining quantity.
            avgFillPrice (float): The average fill price.
            permId (TickerId): Permanent order identifier.
            parentId (TickerId): Parent order identifier (if applicable).
            lastFillPrice (float): The price of the last fill.
            clientId (TickerId): The client ID.
            whyHeld (str): Reason for holding the order.
            mktCapPrice (float): Market cap price (if applicable).
        """
        self._logger.debug(f'orderStatus {orderId=} {status=} {filled=} {remaining=} {avgFillPrice=} {permId=} {parentId=} {lastFillPrice=} {clientId=} {whyHeld=} {mktCapPrice=}')
        # Create a message for ZeroMQ
        ts = time.time()
        eoid = orderId
        order_dict = self.submitted_orders.get(eoid, None)
        self._logger.debug(f'orderStatus {self.submitted_orders=}')
        if order_dict is not None:
            oid = order_dict['data']['oid']
            if status == 'Submitted':
                if filled > 0.0:
                    # partially filled
                    zmq_msg = self.create_order_update_message(
                        ts,
                        gateway=self.NAME,
                        strategy=order_dict['data']['strategy'],
                        exch=order_dict['data']['product']['exch'],
                        pdt=order_dict['data']['product']['name'],
                        oid=oid,
                        status='PARTIAL_FILLED',
                        average_filled_price=float(avgFillPrice),
                        last_filled_price=float(lastFillPrice),
                        last_filled_size=float(filled),
                        create_ts=order_dict['data']['create_ts'],
                    )
                    self._zmq.send(*zmq_msg)
                else:
                    # (SmaStrategy pid=50201) 2024-09-10 14:48:31,019 - sma_strategy - DEBUG - send - sma_strategy_zmq sent (1725994111.019837, '', 2, 2, ('ib_gateway', 'sma_strategy', {'ts': 1725994111.0198138, 'data': {'oid': 'ac49b6f7-d162-4a79-b4c4-77148d63a2d8', 'strategy': 'sma_strategy', 'product': {'name': 'NVDA', 'base_currency': 'USD', 'exch': 'SMART', 'product_type': 'STOCK'}, 'side': 1, 'price': 90, 'size': 1, 'order_type': 'limit'}}))
                    zmq_msg = self.create_order_update_message(
                        ts,
                        gateway=self.NAME,
                        strategy=order_dict['data']['strategy'],
                        exch=order_dict['data']['product']['exch'],
                        pdt=order_dict['data']['product']['name'],
                        oid=oid,
                        status='OPENED',
                        create_ts=order_dict['data']['create_ts'],
                    )
                    self._zmq.send(*zmq_msg)
                    # # remove from submitted orders
                    # del self.submitted_orders[eoid]
            elif status == 'Cancelled':
                zmq_msg = self.create_order_update_message(
                    ts,
                    gateway=self.NAME,
                    strategy=order_dict['data']['strategy'],
                    exch=order_dict['data']['product']['exch'],
                    pdt=order_dict['data']['product']['name'],
                    oid=oid,
                    status='CANCELED',
                    create_ts=order_dict['data']['create_ts'],
                )
                self._zmq.send(*zmq_msg)
                # remove from submitted orders
                del self.submitted_orders[eoid]
            elif status == 'Filled':
                zmq_msg = self.create_order_update_message(
                    ts,
                    gateway=self.NAME,
                    strategy=order_dict['data']['strategy'],
                    exch=order_dict['data']['product']['exch'],
                    pdt=order_dict['data']['product']['name'],
                    oid=oid,
                    status='FILLED',
                    average_filled_price=float(avgFillPrice),
                    last_filled_price=float(lastFillPrice),
                    last_filled_size=float(filled),
                    create_ts=order_dict['data']['create_ts'],
                    target_price=order_dict['data']['price'],
                )
                self._zmq.send(*zmq_msg)
                # remove from submitted orders
                del self.submitted_orders[eoid]
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
    #####

    # EClient
    ####
    def _request_tick_by_tick_data(self, request_id, tick_type: Literal['Last', 'AllLast', 'BidAsk', 'MidPoint'], product, number_of_ticks=0, ignore_size=False):
        """
        Request tick-by-tick data from IB for a given product.

        Args:
            request_id (int): Unique request identifier.
            tick_type (Literal): The type of tick data requested ('Last', 'AllLast', 'BidAsk', 'MidPoint').
            product (Contract): The IB contract for which tick data is requested.
            number_of_ticks (int, optional): Number of ticks to request (0 for continuous streaming).
            ignore_size (bool, optional): Whether to ignore size data.
        """
        self.reqTickByTickData(
            request_id,
            product,
            tick_type,
            # IB will continue sending ticks to you if set to 0
            number_of_ticks,
            ignore_size,
        )
        self._logger.debug(f'{self.NAME} requested (req_id={self._request_id}) {product.symbol} tick by tick data ({tick_type=})')
    
    def place_order(self, order_dict):
        """
        Place an order through IB's API.

        Depending on the product type (FUTURE or STOCK), converts the internal order representation to an IB order,
        submits it, and updates internal order tracking.

        Args:
            order_dict (dict): Dictionary containing order details including product information, order side,
                               order type, size, price, and time in force.

        Note:
            This method requires a valid _next_order_id; if not set, the order will be rejected.
        """
        if self._next_order_id is None:
            self._logger.warning('next_order_id is not set, please wait for the connection to be established')
            # internal reject from gateway: reason = gateway not ready
            return
        
        self._logger.debug(f'ib_gateway place_order {order_dict=}')
        if order_dict['data']['product']['product_type'] == 'FUTURE':
            from alchemist.products.future_product import FutureProduct
            product_dict = order_dict['data']['product']
            product = FutureProduct(
                name=product_dict['name'],
                base_currency=product_dict['base_currency'],
                exch=product_dict['exch'],
                contract_month=product_dict['contract_month']
            )
            # convert to ib contract
            contract = self.convert_to_ib_contract(product)
            order = Order()
            order.action = self.convertor.convert_order_side_to_external(order_dict['data']['side'])
            order.orderType = self.convertor.convert_order_type_to_external(order_dict['data']['order_type'])
            order.totalQuantity = order_dict['data']['size']
            order.lmtPrice = order_dict['data'].get('price', 0.0)
            order.tif = self.convertor.convert_time_in_force_to_external(order_dict['data']['time_in_force'])  # 'DAY', 'GTC', etc.
            order.account = order_dict['data'].get('acc', '')
            # REMINDER: Legacy attributes and must set them to False for API orders
            order.eTradeOnly = False  # Set eTradeOnly to False
            order.firmQuoteOnly = False  # Set firmQuoteOnly to False

            # Place the order through IB's API
            self.placeOrder(self._next_order_id, contract, order)
            order_dict['data']['eoid'] = self._next_order_id  # add the eoid to the order_dict
            [self._next_order_id] = order_dict
            # self._request_id += 1  # each symbol takes 1 unique request id
            self._next_order_id += 1  # REMINDER: to make sure the next order id is unique
            ####
        elif order_dict['data']['product']['product_type'] == 'STOCK':
            from alchemist.products.stock_product import StockProduct
            product_dict = order_dict['data']['product']
            product = StockProduct(
                name=product_dict['name'],
                base_currency=product_dict['base_currency'],
                # exch=product_dict['exch'],  # TODO: may need to pass the exchange from the order in the future. I configured the exchange wrongly (a NYSE stock with NASDAQ exchange). I cannot update my positions due to the wrong exchange.
                exch='SMART',  # TEMP: only use SMART routing  
            )
            # convert to ib contract
            contract = self.convert_to_ib_contract(product)
            order = Order()
            order.action = self.convertor.convert_order_side_to_external(order_dict['data']['side'])
            order.orderType = self.convertor.convert_order_type_to_external(order_dict['data']['order_type'])
            order.totalQuantity = order_dict['data']['size']
            order.lmtPrice = order_dict['data'].get('price', 0.0)
            order.tif = self.convertor.convert_time_in_force_to_external(order_dict['data']['time_in_force'])  # 'DAY', 'GTC', etc.
            order.account = order_dict['data'].get('acc', '')
            # Place the order through IB's API
            self.placeOrder(self._next_order_id, contract, order)
            order_dict['data']['eoid'] = self._next_order_id  # add the eoid to the order_dict
            self.submitted_orders[self._next_order_id] = order_dict
            self._next_order_id += 1 # REMINDER: to make sure the next order id is unique
            # self._request_id += 1  # each symbol takes 1 unique request id
            ####
        self._logger.info(f'place_order {self.submitted_orders=}')
        
    def cancel_o(self, orderId):
        """
        Placeholder method for order cancellation.

        Args:
            orderId (int): The order identifier to cancel.
        """
        pass

    def amend_o(self, orderId):
        """
        Placeholder method for order amendment.

        Args:
            orderId (int): The order identifier to amend.
        """
        pass
