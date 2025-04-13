from abc import ABC, abstractmethod
import time
from datetime import datetime
import typing
import traceback
import re
from importlib import import_module

# from alchemist.orderbook import Orderbook
from alchemist.managers.order_manager import OrderManager
from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.managers.data_manager import DataManager
from alchemist.zeromq import ZeroMQ
from alchemist.order import Order
from alchemist.products.base_product import BaseProduct
from alchemist.data_pipelines.base_data_pipeline import BaseDataPipeline
from alchemist.monitors.base_monitor import BaseMonitor
from alchemist.position import Position
from alchemist.logger import get_logger
from alchemist.enums import OrderTypeEnum, TimeInForceEnum
from alchemist.data_card import DataCard


class BaseStrategy(ABC):
    STRAT = None  # REMINDER: now every strategy should have its own unique name
    PARAMS = {}

    def __init__(
            self, 
            name, 
            zmq_send_port, 
            zmq_recv_ports, 
            products: typing.List[BaseProduct], 
            data_cards: typing.List[DataCard], 
            data_pipeline: typing.Union[BaseDataPipeline, callable]=None, 
            data_pipeline_kwargs: dict=None, 
            max_len=1000,
            params=None,
            monitor_actor: BaseMonitor=None,
        ):
        # assert self.NAME is not None, f'Please initialize the NAME like `xxx_strategy`: {self.NAME}'
        self.name = name
        self.products = products
        self.data_cards = data_cards
        self.max_len = max_len  # the maximum length of the datas
        self.params = self.PARAMS
        if params is not None and isinstance(params, dict):
            self.params = {**self.params, **params}
        # for zmq
        self._zmq = ZeroMQ(name=self.name)
        self._zmq_send_port = zmq_send_port  # send msg to order manager
        self._zmq_recv_ports = zmq_recv_ports
        # running thread
        self._is_running = False

        self.monitor_actor = monitor_actor

        self.portfolio_manager = self.pm = PortfolioManager()  # REMINDER: DO NOT share accounts between strategies
        self.order_manager = self.om = OrderManager(zmq=self._zmq, portfolio_manager=self.pm)  # TEMP
        # self.orderbook = Orderbook()

        # just give indexes for the gateway
        self.gateways = [i for i in range(len(self._zmq_recv_ports))]
        self.data_manager = self.dm = DataManager(data_cards=data_cards)
        self.datas = self.data_manager.datas
        # TEMP: automatically create a list of indexes that require synchronization 
        self.sync_data_indexes = self.auto_get_sync_indexes()

        if data_pipeline_kwargs is not None and isinstance(data_pipeline, str):
            self.data_pipeline = getattr(import_module('alchemist.data_pipelines'), data_pipeline)(**data_pipeline_kwargs)

        self._logger = get_logger(self.name.lower(), console_logger_lv='debug', file_logger_lv='debug')

        self._logger.debug(f'{self.params=}')
    
    def get_params(self):
        return self.params

    def auto_get_sync_indexes(self):
        highest_resolution = min([data_card.frequency for data_card in self.data_cards])
        sync_data_cards = [data_card for data_card in self.data_cards if data_card.frequency == highest_resolution]
        return [
            self.create_data_index(data_card.product.exch, data_card.product.name, data_card.freq, data_card.aggregation)
            for data_card in sync_data_cards
        ]

    def create_data_index(self, exch, pdt, freq, aggregation):
        return self.dm.create_data_index(exch, pdt, freq, aggregation)
    
    def get_product(self, exch, pdt):
        for product in self.products:
            if  product.exch == exch and product.name == pdt:
                return product
        return None

    def start_backfilling(self, backfilling_start_date: str):
        start_time = time.time()
        if self.data_pipeline is None:
            raise ValueError('data_pipeline is None!')
        
        self.data_pipeline.start()
        self._logger.info(f'Start backfilling strategy={self.name} {self.data_pipeline=}...')
        # 1. cache the updates to dict first
        updates_dict = {}
        for data_card in self.data_cards:
            index = self.dm.create_data_index(data_card.product.exch, data_card.product.name, data_card.freq, data_card.aggregation)
            if 't' in data_card.freq:
                updates = self.data_pipeline.historical_ticks(data_card.product, start=backfilling_start_date, end=None)
                updates_dict[index] = updates
                # for update in updates:
                #     self.datas[index].update_from_tick(update)
            elif 's' in data_card.freq or 'm' in data_card.freq or 'h' in data_card.freq:
                updates = self.data_pipeline.historical_bars(data_card.product, freq=data_card.freq, start=backfilling_start_date)
                updates_dict[index] = updates
                # for update in updates:
                #     self.datas[index].update_from_bar(update['ts'], update['data']['open'], update['data']['high'], update['data']['low'], update['data']['close'], update['data']['volume'])
            else:
                raise ValueError(f'Invalid data type for backfilling: {data_card.freq=}')
        # 2. pad the updates to the same length
        max_updates_len = max([len(updates) for updates in updates_dict.values()])
        for index, updates in updates_dict.items():
            updates_dict[index] = updates + [None] * (max_updates_len - len(updates))
        # 3. push the updates to the data
        for i in range(max_updates_len):
            for index in updates_dict:
                data = self.datas[index]
                update = updates_dict[index][i]
                if 't' in data.freq:
                    data.update_from_tick(update['ts'], update['data']['price'], update['data']['size'])
                elif 's' in data.freq or 'm' in data.freq or 'h' in data.freq:
                    data.update_from_bar(update['ts'], update['data']['open'], update['data']['high'], update['data']['low'], update['data']['close'], update['data']['volume'])
                else:
                    raise ValueError(f'Invalid data type for backfilling: {data.freq=}')

        self._logger.info(f'Finished backfilling strategy={self.name} {self.data_pipeline=}...')
        self.data_pipeline.stop()
        end_time = time.time()
        self._logger.debug(f'Backfilling time: {(end_time - start_time) / 60:.2f} minutes')

    def start(self):
        self._is_running = True

        self._zmq.start(
            logger=self._logger,
            send_port=self._zmq_send_port,
            recv_ports=self._zmq_recv_ports
        )
        self._logger.debug(f'Strategy {self.name} started!')
        # 2. initialize listener to the output actions
        while self._is_running:
            try:
                if msg := self._zmq.recv():
                    channel, topic, info = msg
                    self._process_info(channel, topic, info)
                # if self.pm.get_balance(currency='USD') > 0:
                #     self.next()
            except Exception as e:
                self._logger.error(f"Error in gateway {self.name}: {e}")
                self._logger.error(traceback.format_exc())  # Log the full traceback
                # self._logger.debug(f"Reconnecting in {self._reconnect_interval} seconds...")
                # time.sleep(self._reconnect_interval)

    def _log_trade(self, ts: datetime, order: Order, order_update: dict):
        """
        Log a trade (filled or partially filled order) to the monitor actor.

        :param ts: Timestamp of the trade.
        :param order: The associated order object.
        :param order_update: TODO
        """
        # Send trade details to the monitor actor
        self.monitor_actor.log_trade.remote(
            strategy_name=self.name,
            ts=ts,
            oid=order.oid,
            pdt=order.product.name,
            filled_price=order_update['data']['last_filled_price'],
            filled_size=order_update['data']['last_filled_size'],
            side='LONG' if order.side == 1 else 'SHORT',
        )

    def _log_signal(self):
        signals = self.get_signals()
        for signal_name, value in signals.items():
            self.monitor_actor.log_signal.remote(
                strategy_name=self.name,
                ts=datetime.now(),
                signal_name=signal_name,
                value=value,
            )

    def _log_order(self, ts, order: Order, order_statuses: typing.List[str], order_update: dict):
        for order_status in order_statuses:
            self.monitor_actor.log_order.remote(
                strategy_name=self.name,
                ts=ts,
                oid=order.oid,
                order_type=order.order_type,
                time_in_force=order.time_in_force,  # temp: hard-coded
                pdt=order.product.name,
                status=order_status,
                size=order.size,
                price=order.price,
                side='LONG' if order.side == 1 else 'SHORT',
            )

    def _log_position(self, ts: datetime, position: Position):
        self.monitor_actor.log_position.remote(
            strategy_name=self.name,
            ts=ts,
            pdt=position.product.name,
            size=position.size,
            side='LONG' if position.side > 0 else 'SHORT',
            avg_price=position.avg_price,
            realized_pnl=position.realized_pnl,
            unrealized_pnl=position.unrealized_pnl,
        )

    def _log_portfolio(self, ts: datetime, balance: float, margin_used: float, total_value: float):
        self.monitor_actor.log_portfolio.remote(
            strategy_name=self.name,
            ts=ts,
            balance=balance,  # TEMP: hard-coded for base currency
            margin_used=margin_used,  
            total_value=total_value,
        )
    
    def _process_info(self, channel, topic, info):
        # parse standardized message here
        if channel == 1:
            if topic == 1:
                # process order book updates
                # REMINDER: users should override the on_quote_update instead of next() to handle strategies on quote update
                self._on_quote()
            elif topic == 2:
                gateway, exch, pdt, tick = info
                freq = bar['resolution']  # TODO: need to change the standardized messages
                tick['ts'] = datetime.fromtimestamp(tick['ts'])
                # process trade updates
                self._on_tick(gateway, exch, pdt, freq, ts=tick['ts'], price=tick['data']['price'], size=tick['data']['size'])
                if self.monitor_actor is not None:
                    self._log_signal()
            elif topic == 3:
                # bar updates
                gateway, exch, pdt, bar = info
                bar['ts'] = datetime.fromtimestamp(bar['ts'])
                freq = bar['resolution']  # TODO: need to change the standardized messages
                self._on_bar(gateway, exch, pdt, freq, ts=bar['ts'], open_=bar['data']['open'], high=bar['data']['high'], low=bar['data']['low'], close=bar['data']['close'], volume=bar['data']['volume'])
                if self.monitor_actor is not None:
                    self._log_signal()
        elif channel == 2:
            if topic == 1:
                # process order updates
                gateway, strategy, order_update = info
                if strategy == self.name:
                    oid = order_update['data']['oid']
                    order_update['ts'] = datetime.fromtimestamp(order_update['ts'])
                    order = self.om.open_orders.get(oid) or self.om.submitted_orders.get(oid)
                    order_statuses = self.om.on_order_status_update(gateway, order_update)
                    self.on_order_status_update(gateway, order_update)
                    if self.monitor_actor is not None:
                        if order:
                            self._log_order(
                                ts=datetime.now(),
                                order=order,
                                order_statuses=order_statuses,
                                order_update=order_update,
                            )
                            if 'FILLED' in order_statuses or 'PARTIAL_FILLED' in order_statuses:
                                self._log_trade(
                                    ts=datetime.now(),
                                    order=order,
                                    order_update=order_update,
                                )
                        else:
                            self._logger.warning(f"Order {oid} not found during logging for statuses: {order_statuses}")

        elif channel == 3:
            # process portfolio updates
            if topic == 1:
                # process balance updates
                gateway, balance_update = info
                for currency, value in balance_update['data'].items():
                    self.update_balance(currency, value)
                self._logger.debug(f'{self.pm.balances=}')  # log the balance
            elif topic == 2:
                # process position updates
                gateway, exch, position_update = info
                # 1. update the existing position
                for pdt, position in position_update['data'].items():
                    # create the product object
                    product = self.get_product(exch, pdt)
                    if product is not None:
                        pdt, exch = product.name, product.exch
                        if self.pm.get_position(product):
                            for side in position:
                                self.pm.update_position(product, side, position[side]['size'], position[side]['last_price'], position[side]['average_price'], position[side]['realized_pnl'], position[side]['unrealized_pnl'])
                        else:
                            # 2. create a new position
                            for side in position:
                                self.pm.create_position(product, side, position[side]['size'], position[side]['last_price'], position[side]['average_price'], position[side]['realized_pnl'], position[side]['unrealized_pnl'])
                        self._logger.info(f'{self.pm.positions=}')

                        # monitor
                        if self.monitor_actor is not None:
                            # update position
                            self._log_position(
                                ts=datetime.now(),
                                position=self.pm.get_position(product),
                            )
                            # update portfolio 
                            self._log_portfolio(
                                ts=datetime.now(),
                                balance=self.pm.balances['USD'],
                                margin_used=0.0, # TEMP: do not consider margin at the moment
                                total_value=self.pm.get_portfolio_value(currency='USD'),  # TEMP: hard-coded for base currency
                            )
                            
                    else:
                        self._logger.error(f'Product {pdt} not found!')

    def create_position(self, product: BaseProduct, side: int, size: float, last_price: float, avg_price: float, realized_pnl: float, unrealized_pnl: float):
        return self.pm.create_position(product, side, size, last_price, avg_price, realized_pnl, unrealized_pnl)

    def get_position(self, product: BaseProduct):
        return self.pm.get_position(product)
    
    def place_order(self, order: Order):
        return self.order_manager.place_order(order)

    def get_submitted_orders(self):
        return self.om.submitted_orders
    
    def buy(self, gateway, product, price, size, order_type='MARKET', time_in_force='GTC'):
        # validation
        OrderTypeEnum.validate(order_type)
        TimeInForceEnum.validate(time_in_force)

        order = Order(
            gateway=gateway,
            strategy=self.name,
            side=1,
            price=price,
            size=size,
            order_type=order_type,
            product=product,
            time_in_force=time_in_force
        )
        return self.place_order(order)
    
    def sell(self, gateway, product, price, size, order_type='MARKET', time_in_force='GTC'):
        # validation
        OrderTypeEnum.validate(order_type)
        TimeInForceEnum.validate(time_in_force)

        order = Order(
            gateway=gateway,
            strategy=self.name,
            side=-1,
            price=price,
            size=size,
            order_type=order_type,
            product=product,
            time_in_force=time_in_force)
        return self.place_order(order)
    
    def close_all(self, gateway, product):
        for exch, positions in self.pm.positions.items():
            for pdt, position in positions.items():
                if pdt == product.name and exch == product.exch:
                    if position.side > 0:
                        self.sell(gateway, product, position.last_price, position.size)
                    else:
                        self.buy(gateway, product, position.last_price, position.size)

    def close(self, gateway: int, product: BaseProduct):
        """
        Close all positions for a single product.

        :param gateway: The gateway through which to place orders.
        :param product: The product to close positions for.
        """
        # Retrieve the current position for the specified product
        position: Position = self.pm.get_position(product)
        
        if position:
            # Determine the side of the position and place the corresponding order to close it
            if position.side > 0:  # Long position
                self.sell(
                    gateway=gateway,
                    product=product,
                    price=position.last_price,
                    size=position.size,
                    order_type='MARKET',  # Assuming OrderTypeEnum is properly defined
                    time_in_force='GTC'   # Assuming TimeInForceEnum is properly defined
                )
                self._logger.info(f"Closed long position for {product=}")
            
            elif position.side < 0:  # Short position
                self.buy(
                    gateway=gateway,
                    product=product,
                    price=position.last_price,
                    size=position.size,
                    order_type='MARKET',
                    time_in_force='GTC'
                )
                self._logger.info(f"Closed short position for {product=}")
            
            else:
                self._logger.warning(f"No actionable position found for {product=}")
        else:
            self._logger.info(f"No open positions to close for product {product=}")

    ####
    # hooks for developers to implement their trading logic on updates
    def on_quote_update(self):
        pass

    def on_tick_update(self, gateway, exch, ts, pdt, price, size):
        pass

    def on_trade_update(self):
        pass

    def on_bar_update(self, gateway, exch, pdt, freq, ts, open_, high, low, close, volume):
        pass

    def on_order_status_update(self, gateway, order_update):
        pass

    def next(self):
        # here is your trading logic
        # you can access all indicators, datas with different timeframes (resolution lower than the base timeframe)
        pass
    ####

    ####
    # for internal update, i.e order book, portfolio, ...etc
    def _on_order_status_update(self):
        # update om accordingly
        pass

    def update_balance(self, currency, value):
        # process balance updates
        self.pm.update_balance(currency, value)

    def _on_tick(self, gateway, exch, pdt, freq, ts, price, size):
        # 1. internal updates
        # update data and indicators accordingly
        self.dm.on_tick_update(gateway, exch, pdt, freq, ts, price, size)
        # 2. user defined updates
        # REMINDER: users should override the on_tick_update instead of next() to handle strategies on tick update
        self.on_tick_update(gateway, exch, pdt, ts, price, size)

    def _on_quote(self):
        self.on_quote_update()

    def _on_bar(self, gateway, exch, pdt, freq, ts, open_, high, low, close, volume):
        self.dm.on_bar_update(gateway, exch, pdt, freq, ts, open_, high, low, close, volume)
        self.on_bar_update(gateway, exch, freq, pdt, ts=ts, open_=open_, high=high, low=low, close=close, volume=volume)
        if self.dm.check_sync(indexes=self.sync_data_indexes):  # TODO
            # check if the corresponding data cards are synced i.e having the same ts
            self.next()

    def shutdown(self):
        self._is_running = False

    def get_open_orders(self) -> dict:
        return self.om.open_orders

    def get_signals(self) -> dict:
        return {}


