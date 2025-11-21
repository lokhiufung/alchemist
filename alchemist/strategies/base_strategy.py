from abc import ABC, abstractmethod
import time
from datetime import datetime
import typing
import traceback
from importlib import import_module

import pandas as pd

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
            is_backtesting=False,
            trading_periods: typing.List[typing.Tuple[str, str]]=None,
        ):
        # assert self.NAME is not None, f'Please initialize the NAME like `xxx_strategy`: {self.NAME}'
        self.name = name
        self.products = products
        self.data_cards = data_cards
        self.max_len = max_len  # the maximum length of the datas
        self.params = self.PARAMS
        self.trading_periods = trading_periods
        if trading_periods is not None:
            self.trading_periods = self._load_trading_periods_from_str(trading_periods=trading_periods)
        if params is not None and isinstance(params, dict):
            self.params = {**self.params, **params}
        # for zmq
        self._zmq = ZeroMQ(name=self.name)
        self._zmq_send_port = zmq_send_port  # send msg to order manager
        self._zmq_recv_ports = zmq_recv_ports
        # running thread
        self._is_running = False
        # backtesting flag
        self._is_backtesting = is_backtesting
        self.backtest_manager = None

        self.monitor_actor = monitor_actor

        self.portfolio_manager = self.pm = PortfolioManager()  # REMINDER: DO NOT share accounts between strategies
        self.order_manager = self.om = OrderManager(zmq=self._zmq, portfolio_manager=self.pm)  # TEMP
        # self.orderbook = Orderbook()

        # just give indexes for the gateway
        self.gateways = [i for i in range(len(self._zmq_recv_ports))]
        self.data_manager = self.dm = DataManager(data_cards=data_cards)
        self.datas = self.data_manager.datas
        # TEMP: automatically create a list of indexes that require synchronization 
        self.highest_resolution_data_indexes = self.auto_get_highest_resolution_indexes()

        if data_pipeline_kwargs is not None and isinstance(data_pipeline, str):
            self.data_pipeline = getattr(import_module('alchemist.data_pipelines'), data_pipeline)(**data_pipeline_kwargs)

        self._logger = get_logger(self.name.lower(), console_logger_lv='debug', file_logger_lv='debug')

        self._logger.debug(f'{self.params=}')

        # useful flags
        self.num_orders = 0
        
        # Latency tracking
        self.last_data_recv_ts = None

        # handy references
        self.product = self.products[0] 
        data_card = self.data_cards[0]
        data_card_index = self.create_data_index(data_card.product.exch, data_card.product.name, data_card.freq, aggregation=data_card.aggregation)
        self.data = self.datas[data_card_index]
    
    def get_params(self):
        return self.params

    def _load_trading_periods_from_str(self, trading_periods: typing.List[typing.Tuple[str, str]]) -> typing.List[typing.Tuple[datetime.time, datetime.time]]:
        trading_periods_dt = []
        for trading_period in trading_periods:
            start_str, end_str = trading_period
            start_time = datetime.strptime(start_str, '%H:%M').time()
            end_time = datetime.strptime(end_str, '%H:%M').time()
            trading_periods_dt.append((start_time, end_time))
        return trading_periods_dt
        
    # filtering methods for time dependent trading logic
    def is_trading_period(self, ts: datetime) -> bool:
        """
        Check if the given timestamp is within the trading period.

        :param ts: The timestamp to check.
        :return: True if within trading period, False otherwise.
        """
        if self.trading_periods is not None:
            for period in self.trading_periods:
                start_time, end_time = period
                if start_time <= ts.time() <= end_time:
                    return True
            # not in any trading period
            return False
        return True

    def auto_get_highest_resolution_indexes(self):
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
                if update is None:
                    continue
                if 't' in data.freq:
                    data.on_tick_update(update['ts'], update['data']['price'], update['data']['size'])
                elif 's' in data.freq or 'm' in data.freq or 'h' in data.freq:
                    data.on_bar_update(update['ts'], update['data']['open'], update['data']['high'], update['data']['low'], update['data']['close'], update['data']['volume'])
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
            self.last_data_recv_ts = datetime.now().timestamp()
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
                    
                    if self.monitor_actor:
                        if order_update['data']['status'] in ("OPENED", "CANCELLED"):
                            # Latency Metrics
                            now = datetime.now().timestamp()
                            latency = now - order_update['data']['create_ts']
                            self.monitor_actor.log_latency.remote('strategy_order_roundtrip', latency, now)
                        # Metric 5: Strategy placing order -> receiving order (Fill)
                        if 'FILLED' in order_statuses:
                            # Metric 4: Strategy placing order -> receiving order update (Ack/First update)
                            # We might want to log this only once per order? 
                            # For now, logging every update latency is fine, or we can check if it's the first update.
                            # Actually, 'strategy_order_roundtrip' usually refers to the first Ack.
                            # But let's log it as 'strategy_order_update_latency'.
                            now = datetime.now().timestamp()
                            latency = now - order_update['data']['create_ts']
                            self.monitor_actor.log_latency.remote('strategy_order_fill_latency', latency, now)

                            target_price = order_update['data'].get('target_price')
                            filled_price = order_update['data'].get('average_filled_price')
                            slippage = filled_price - target_price
                            # For SELL orders, slippage is target - filled? Or just deviation?
                            # User said: "how does it deviate from the target entry price".
                            # Usually slippage is unfavorable deviation.
                            # But let's just log the difference or absolute difference.
                            # Or (filled - target) for BUY, (target - filled) for SELL.
                            # I need to know the side.
                            self.monitor_actor.log_slippage.remote(slippage, now)

                    self._on_order_status_update(gateway, order_update)
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
    
    def place_order(self, order: Order, action: str='BET') -> Order:
        # action: 'BET', 'PARTIALLY_BET', 'CLOSE', 'PARTIALLY_CLOSE'
        
        # Metric 2: Strategy receiving update to placing order to zmq
        now = datetime.now().timestamp()
        if self.last_data_recv_ts:
            latency = now - self.last_data_recv_ts
            if self.monitor_actor:
                self.monitor_actor.log_latency.remote('strategy_update_to_order', latency, now)

        position = self.pm.get_position(order.product)
        if position is None:
            self.pm.create_position(
                order.product,
                side=order.side,
                size=0,
                last_price=0.0,
                avg_price=0.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
            )
            self.pm.update_position_status(order.product, 'IDLE')  # this line is only for security. May not need it
        if self._is_backtesting:
            order = self.backtest_manager.place_order(order)
        else:
            order = self.order_manager.place_order(order)
        return order

    def get_submitted_orders(self):
        return self.om.submitted_orders
    
    def buy(self, gateway, product, price, size, order_type='MARKET', time_in_force='GTC', reason='') -> Order:
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
            time_in_force=time_in_force,
            reason=reason,
        )
        return self.place_order(order)
    
    def sell(self, gateway, product, price, size, order_type='MARKET', time_in_force='GTC', reason='') -> Order:
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
            time_in_force=time_in_force,
            reason=reason,
        )
        return self.place_order(order)
    
    def close_all(self, gateway, product):
        for exch, positions in self.pm.positions.items():
            for pdt, position in positions.items():
                if pdt == product.name and exch == product.exch:
                    if position.side > 0:
                        self.sell(gateway, product, position.last_price, position.size)
                    else:
                        self.buy(gateway, product, position.last_price, position.size)

    def close(self, gateway: int, product: BaseProduct, price=None, reason=''):
        """
        Close all positions for a single product.

        :param gateway: The gateway through which to place orders.
        :param product: The product to close positions for.
        """
        # Retrieve the current position for the specified product
        position: Position = self.pm.get_position(product)
        if price is None:
            if position.last_price is not None:
                price = position.last_price
            else:
                raise ValueError(f"Price is None and position.last_price is None for {product=}")
        
        if position:
            # Determine the side of the position and place the corresponding order to close it
            if position.side > 0:  # Long position
                self.sell(
                    gateway=gateway,
                    product=product,
                    price=price,
                    size=position.size,
                    order_type='MARKET',  # Assuming OrderTypeEnum is properly defined
                    time_in_force='GTC',   # Assuming TimeInForceEnum is properly defined
                    reason=reason,
                )
                self._logger.info(f"Closed long position for {product=}")
            
            elif position.side < 0:  # Short position
                self.buy(
                    gateway=gateway,
                    product=product,
                    price=price,
                    size=position.size,
                    order_type='MARKET',
                    time_in_force='GTC',
                    reason=reason
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
        signals = self.compute_signals()

        trade_decision = self.should_enter(signals)
        trade_size = self.compute_size(signals, side=trade_decision)
        position = self.get_position(self.product)

        if position is None or position.size == 0 and trade_decision != 0 and trade_size > 0:
            if trade_decision > 0:
                # TODO: should use an execution algo to handle order placement
                self.buy(
                    gateway='ib_gateway',  # TODO: hardcoded for now
                    product=self.product,
                    price=self.data[-1].close,  # TODO: just put a placeholder for now
                    size=trade_size,
                    order_type='MARKET',
                    time_in_force='GTC',
                    reason='enter position ',
                )
            elif trade_decision < 0: 
                self.sell(
                    gateway='ib_gateway',  # TODO: hardcoded for now
                    product=self.product,
                    price=self.data[-1].close,  # TODO: just put a placeholder for now
                    size=trade_size,
                    order_type='MARKET',
                    time_in_force='GTC',
                    reason='enter position ',
                )
        elif position.size > 0:
            exit_decision = self.should_exit(signals, current_side=position.side)
            if exit_decision:
                self.close(
                    gateway='ib_gateway',  # TODO: hardcoded for now
                    product=self.product,
                    price=self.data[-1].close,  # TODO: just put a placeholder for now
                    reason='exit position ',
                )

    ####
    #### high level hooks for strategy logic
    def compute_signals(self) -> dict[str, float]:
        return {}
    
    def compute_size(self, signals: dict[str, float], side: int) -> float:
        return 0.0
    
    def should_enter(self, signals: dict[str, float]) -> bool:
        return False
    
    def should_exit(self, signals: dict[str, float], current_side: int) -> bool:
        return True
    ####
    ####
    # for internal update, i.e order book, portfolio, ...etc
    def _on_order_status_update(self, gateway, order_update):
        # updat the position status here
        product = self.get_product(order_update['data']['exch'], order_update['data']['pdt'])
        position = self.pm.get_position(product)
        if position is not None:
            if order_update['data']['status'] in ['CLOSED', 'CANCELED', 'REJECTED']:
                position.update_status('IDLE')
            elif order_update['data']['status'] in ['FILLED']:
                if position.status == 'PENDING_BETTING':
                    position.update_status('BETTED')
                elif position.status == 'PENDING_CLOSING':
                    position.update_status('IDLE')

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

    def _on_bar(self, gateway, exch, pdt, freq, ts, open_, high, low, close, volume, is_warmup=False):
        self.dm.on_bar_update(gateway, exch, pdt, freq, ts, open_, high, low, close, volume)
        self.on_bar_update(gateway, exch, freq, pdt, ts=ts, open_=open_, high=high, low=low, close=close, volume=volume)
        if (
            not is_warmup and
            self.dm.check_sync(indexes=self.highest_resolution_data_indexes) and
            self.dm.check_highest_resolution(ts=ts, freq=freq, indexes=self.highest_resolution_data_indexes)
        ):
            # check if the corresponding data cards are synced i.e having the same ts
            if self.is_trading_period(ts):
                self.next()
            else:
                # outside your target trading periods
                # TODO: for now always automatically close all positions when outside trading periods
                for product in self.products:
                    position = self.pm.get_position(product=product)
                    # 1. exit existing position if any
                    if position is not None and position.status != 'PENDING_CLOSING' and position.size > 0:
                        current_close = self.data[-1].close
                        self.close(
                            gateway='ib_gateway',
                            product=self.product,
                            price=current_close,
                            reason='close position outside the trading periods'
                        )

    def shutdown(self):
        self._is_running = False

    def get_open_orders(self) -> dict:
        return self.om.open_orders

    def get_signals(self):
        return self.compute_signals()
    
    def start_backtesting(self, data_pipeline: BaseDataPipeline, start_date: str, end_date: str, initial_cash: float, commission=None, n_warmup=0, export_data=False, path_prefix=''):
        from alchemist.managers.backtest_manager import BacktestManager
        
        if len(self.products) > 1:
            raise ValueError('Currently backtesting does not support multiple products!')

        signals = []

        # turn on the backtesting flag
        self.backtest_manager = BacktestManager(strategy=self, pm=self.pm, commission=commission)

        start_time = time.time()

        # set the initial capital of your account
        self.update_balance('USD', initial_cash)

        data_pipeline.start()
        self._logger.info(f'Start backtesting strategy={self.name} {data_pipeline=}...')
        # 1. cache the updates to dict first
        updates_dict = {}
        for data_card in self.data_cards:
            index = self.dm.create_data_index(data_card.product.exch, data_card.product.name, data_card.freq, data_card.aggregation)
            if 's' in data_card.freq or 'm' in data_card.freq or 'h' in data_card.freq:
                updates = data_pipeline.historical_bars(data_card.product, freq=data_card.freq, start=start_date, end=end_date)
                updates_dict[index] = updates
                # for update in updates:
                #     self.datas[index].update_from_bar(update['ts'], update['data']['open'], update['data']['high'], update['data']['low'], update['data']['close'], update['data']['volume'])
            else:
                raise ValueError(f'Backtesting does not support {data_card.freq=}')
        # 2. pad the updates to the same length
        max_updates_len = max([len(updates) for updates in updates_dict.values()])
        for index, updates in updates_dict.items():
            updates_dict[index] = updates + [None] * (max_updates_len - len(updates))
        # 3. push the updates to the data
        for i in range(max_updates_len):
            for index in updates_dict:
                exch, pdt, _, _ = self.dm.factor_data_index(index)
                data = self.datas[index]
                update = updates_dict[index][i]
                if update is None:
                    continue
                if 's' in data.freq or 'm' in data.freq or 'h' in data.freq:
                    # gateway, exch, pdt, bar = info
                    update['ts'] = datetime.fromtimestamp(update['ts'])
                    self._logger.debug('current_ts={}'.format(update['ts']))
                    freq = update['resolution']  # TODO: need to change the standardized messages
                    gateway = 'ib_gateway' # TODO
                    exch = self.products[0].exch # TODO
                    
                    is_warmup = i < n_warmup

                    self._on_bar(
                        gateway,
                        exch,
                        pdt,
                        freq, 
                        ts=update['ts'],
                        open_=update['data']['open'],
                        high=update['data']['high'],
                        low=update['data']['low'],
                        close=update['data']['close'],
                        volume=update['data']['volume'],
                        is_warmup=is_warmup,  # fill the indicators with warmup
                    )
                    if not is_warmup:
                        signals.append(self.get_signals())
                    # 4. simulate the order filling, balance updates, and position updates
                    # always lag behind the strategy's `_on_bar` by 1 bar
                    self.backtest_manager.on_bar(gateway, exch, pdt, freq, ts=update['ts'], open_=update['data']['open'], high=update['data']['high'], low=update['data']['low'], close=update['data']['close'], volume=update['data']['volume'], on_order_status_update=self.on_order_status_update)
                else:
                    raise ValueError(f'Invalid data type for backfilling: {data.freq=}')

        self._logger.info(f'Finished backtesting strategy={self.name} {data_pipeline=}...')
        data_pipeline.stop()
        end_time = time.time()
        self._logger.debug(f'Backtesting time: {(end_time - start_time) / 60:.2f} minutes')

        signals = pd.DataFrame(signals)

        # write backtesting data
        if export_data:
            self.backtest_manager.export_data(path_prefix=path_prefix)
            signals.to_csv(f'{path_prefix}_signals.csv')

        return {
            'portfolio_value': pd.DataFrame(self.backtest_manager.portfolio_history),
            'transaction': pd.DataFrame(self.backtest_manager.transaction_log),
            'signal': signals
        }