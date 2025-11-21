import traceback
import typing
from abc import ABC, abstractmethod
import time
from datetime import datetime

from alchemist.zeromq import ZeroMQ
from alchemist import standardized_messages
from alchemist.products.base_product import BaseProduct
from alchemist.account import Account
from alchemist.logger import get_logger
from alchemist.enums import DataSubscriptionEnum
from alchemist.convertor import Convertor
from alchemist.errors import *


class BaseGateway(ABC):
    NAME = None
    CONVERTOR = None

    def __init__(self, subscriptions: typing.List[str], zmq_send_port: int, zmq_recv_ports: typing.List[int], products: typing.List[BaseProduct]=None, accounts: typing.List[Account]=None, reconnect_interval: int=5):
        """_summary_

        Args:
            subscriptions (typing.List[str]): List of streaming data subscription. `tick`, `quote`, `bar`, `account_update`, `account_summary`
            zmq_send_port (int): The port number that the gateway
            zmq_recv_ports (typing.List[int]): _description_
            products (typing.List[BaseProduct], optional): List of Prodcuts. Defaults to None.
            accounts (typing.List[Account], optional): List of Accounts. Defaults to None.
            reconnect_interval (int, optional): _description_. Defaults to 5.
        """
        self.name = self.NAME
        self.convertor: Convertor = self.CONVERTOR
        # self.subscriptions = subscriptions
        self.subscriptions = [subscription.upper() for subscription in subscriptions]  # for later
        # validate the DataSubscriptionEnum
        for subscription in self.subscriptions:
            DataSubscriptionEnum.validate(subscription)

        self.products = products
        self.accounts = accounts
        # for zmq
        self._zmq = ZeroMQ(name=self.NAME)
        self._zmq_send_port = zmq_send_port
        self._zmq_recv_ports = zmq_recv_ports
        # for external connection
        self._is_running = False
        self._is_connected = False
        self._connection_thread = None
        self._reconnect_interval = reconnect_interval

        # use eoid as keys
        self.submitted_orders: typing.Dict[str, dict] = {}

        # for logging
        self._logger = get_logger(self.NAME.lower(), console_logger_lv='info', file_logger_lv='debug')

    @abstractmethod
    def _run_connection_server(self):
        """Abstract method to handle connection to the data source."""
        raise NotImplementedError

    def start(self):
        self._is_running = True
        self._zmq.start(
            logger=self._logger,
            send_port=self._zmq_send_port,
            recv_ports=self._zmq_recv_ports
        )

        """
        main thread of this actor
        """
        # 1. initialize connection to the data streaming server
        self._run_connection_server()
        
        # 2. initialize listener to the output actions
        while self._is_running:
            try:
                if msg := self._zmq.recv():
                    self._logger.info(f'gateway {msg=} {self.NAME=}')
                    channel, topic, info = msg
                    if channel == 0 and topic == 0:
                        self.pong()
                    elif channel == 2 and topic == 2:
                        gateway, strategy, order_dict = info
                    if gateway == self.NAME:
                        try:
                            self.place_order(order_dict)
                        except Exception as err:
                            zmq_msg = self.create_order_update_message(
                                ts=time.time(),
                                gateway=self.NAME,
                                strategy=order_dict['data']['strategy'],
                                exch=order_dict['data']['product']['exch'],
                                pdt=order_dict['data']['product']['name'],
                                oid=order_dict['data']['oid'],
                                status='INTERNAL_REJECTED',
                            )
                            self._zmq.send(*zmq_msg)
                            self._logger.error(f'Order placement failed {order_dict=} {err=}')
                    else:
                        self._logger.debug(f'Order from other gateway {gateway=}. Current gateway {self.NAME=}. Just ignore the it.')
            except Exception as e:
                self._logger.error(f"Error in gateway {self.NAME}: {e}")
                self._logger.error(traceback.format_exc())  # Log the full traceback
                # self._logger.debug(f"Reconnecting in {self._reconnect_interval} seconds...")
                # time.sleep(self._reconnect_interval)

    def place_order(self, order_dict):
        raise NotImplementedError('Please implement the place_order method in the subclass.')

    def shutdown(self):
        self._is_running = False
        self.disconnect()
        self._logger.debug(f'{self.name} shutdowned.')

    def disconnect(self):
        if self._connection_thread:
            self._connection_thread.join()
        self._zmq.stop()
        self._on_disconnected()

    def reconnect(self):
        self.disconnect()
        self.connect(self._config['zmq_ports'])

    def _on_connected(self):
        if not self._is_connected:
            self._is_connected = True
            # self._management_service_handler.update_gateway_status.remote(self.NAME, 'connected')
            self._logger.debug(f'{self.NAME} is connected')

    def _on_disconnected(self):
        if self._is_connected:
            self._is_connected = False
            # self._management_service_handler.update_gateway_status.remote(self.NAME, 'disconnected')
            self._logger.debug(f'{self.NAME} is disconnected')

    def _subscribe(self):
        # subscribe data for products
        for product in self.products:
            # subscribe to data
            if 'TICK' in self.subscriptions:
                # subscribe to tick data
                self._subscribe_tick(product)
            elif 'QUOTE' in self.subscriptions:
                self._subscribe_quote(product)
            elif 'BAR' in self.subscriptions:
                self._subscribe_bar(product)
            else:
                raise ValueError(f'No Valid subscription found! {self.subscriptions=} {product=}')

        # subscribe data for acc
        for account in self.accounts:
            if 'ACCOUNT_UPDATE' in self.subscriptions:
                self._subscribe_account_update(account)
            elif 'ACCOUNT_SUMMARY' in self.subscriptions:
                self._subscribe_account_summary(account)
            else:
                raise ValueError(f'No Valid subscription found! {self.subscriptions=} {account=}')

    def _subscribe_tick(self, product: BaseProduct):
        pass

    def _subscribe_quote(self, product: BaseProduct):
        pass

    def _subscribe_bar(self, product: BaseProduct):
        pass

    def _subscribe_account_update(self, account: Account):
        pass

    def _subscribe_account_summary(self, account: Account):
        pass

    @staticmethod
    def create_quote_message(ts, gateway, pdt, exch, bids, asks):
        return standardized_messages.create_quote_message(ts, gateway, pdt, exch, bids, asks)
    
    @staticmethod
    def create_tick_message(ts, gateway, exch, pdt, price, size):
        return standardized_messages.create_tick_message(ts, gateway, exch, pdt, price, size)
    
    @staticmethod
    def create_bar_message(ts, gateway, exch, pdt, resolution, open_, high, low, close, volume):
        return standardized_messages.create_bar_message(ts, gateway, exch, pdt, resolution, open_, high, low, close, volume)
    
    @staticmethod
    def create_order_update_message(ts, gateway, strategy, exch, pdt, oid, status, average_filled_price=None, last_filled_price=None, last_filled_size=None, amend_price=None, amend_size=None, create_ts=None, target_price=None):
        return standardized_messages.create_order_update_message(ts, gateway, strategy, exch, pdt, oid, status, average_filled_price, last_filled_price, last_filled_size, amend_price, amend_size, create_ts, target_price)
    
    @staticmethod
    def create_place_order_message(ts, gateway, exch, product, oid, side, price, size, order_type, time_in_force):
        return standardized_messages.create_place_order_message(ts, gateway, exch, product, oid, side, price, size, order_type, time_in_force)

    @staticmethod
    def create_balance_update_message(ts, gateway, balance_dict):
        return standardized_messages.create_balance_update_message(ts, gateway, balance_dict)
    
    @staticmethod
    def create_position_update_message(ts, gateway, exch, position_dict):
        return standardized_messages.create_position_update_message(ts, gateway, exch, position_dict)
    


    
