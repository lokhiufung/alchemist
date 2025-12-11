from datetime import datetime

import pandas as pd

from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.managers.order_manager import OrderManager
from alchemist import standardized_messages
from alchemist.datas.common import Bar
from alchemist.order import Order


class BacktestManager(OrderManager):
    def __init__(self, strategy, pm: PortfolioManager, commission):
        self.strategy = strategy
        self.pm = pm
        self.commission = commission
        self.portfolio_history = []  # list of (ts, portfolio_value)
        self.transaction_log = []    # list of dicts with transaction details
        # Track how much margin has been reserved per product (for futures)
        self.position_margin = {}    # key: product name, value: reserved margin
        super().__init__(zmq=None, portfolio_manager=pm)

    def place_order(self, order):
        """
        Overwrite the place order of OrderManager, which will send order to an external gateway through zmq 
        """
        validation_result = self._validate_order(order)
        if validation_result['passed']:
            self.on_submitted(order)
            self.on_opened(oid=order.oid)  # immediately opened
        else:
            self.on_internal_rejected(order, reason=validation_result['reason'])
    
    def execute_order(self, gateway, exch, pdt, order: Order, bar: Bar, on_order_status_update):
        # 1. match orders with simple logic
        if order.order_type != 'MARKET':
            self.logger.warning('Only MARKET order is supported for backtesting. All other orders will be converted to MARKET order.')

        # REMINDER: for MARKET order, we assume near immediately filling the order at the triggered price
        filled_price = order.price
        # add 1 more minute to the bar time
        filling_time = bar.ts + pd.Timedelta(minutes=1)
        
        # if `MARKET` order, fill with the open price
        _, _, (_, _, order_update) = standardized_messages.create_order_update_message(
            ts=filling_time,
            gateway=gateway,
            strategy=self.strategy.name,
            exch=exch,
            pdt=pdt,
            oid=order.oid,
            status='FILLED',
            average_filled_price=filled_price,
            last_filled_price=filled_price,
            last_filled_size=order.size,
            amend_price=None,
            amend_size=None,
            create_ts=filling_time,
            target_price=order.price,
        )

        on_order_status_update(gateway, order_update)  # TODO: just a quick fix

        # order_update['ts'] = datetime.fromtimestamp(order_update['ts'])
        order_statuses = self.on_order_status_update(
            gateway=gateway,
            order_update=order_update
        )
        
        # 2. update balance accordingly
        # 2. update balance and position accordingly
        product_type = order.product.product_type
        last_balance = self.pm.get_balance(currency='USD')
        
        if product_type == 'FUTURE':
            # if hasattr(self.commission, 'tick_value') and hasattr(self.commission, 'tick_size') and self.commission.tick_size != 0:
            #     multiplier = self.commission.tick_value / self.commission.tick_size
            # else:
            #     multiplier = self.commission.multiplier
            multiplier = self.commission.multiplier

        existing_pos = self.pm.get_position(order.product)
        current_price = bar.close

        if existing_pos and existing_pos.size > 0:
            is_same_direction = (existing_pos.side == order.side)
            
            if is_same_direction:
                # Increase position
                new_size = existing_pos.size + order.size
                new_avg_price = (existing_pos.avg_price * existing_pos.size + filled_price * order.size) / new_size
                
                # Balance update
                # the margin / initial cash invested is deducted from the cash account
                if product_type == 'FUTURE':
                    margin_required = self.commission.initial_margin * order.size
                    # Reserve additional margin for the added size
                    prev_margin = self.position_margin.get(order.product.name, 0.0)
                    new_margin = prev_margin + margin_required
                    self.position_margin[order.product.name] = new_margin
                    self.pm.update_balance(currency='USD', value=last_balance - margin_required)
                    unrealized_pnl = (current_price - new_avg_price) * new_size * order.side * multiplier 
                else: # Stock
                    self.pm.update_balance(currency='USD', value=last_balance - order.size * filled_price)
                    unrealized_pnl = (current_price - new_avg_price) * new_size * order.side 
                
                self.pm.update_position(
                    product=order.product,
                    side=order.side,
                    size=new_size,
                    last_price=filled_price,
                    avg_price=new_avg_price,
                    realized_pnl=existing_pos.realized_pnl - self.commission.commission,
                    unrealized_pnl=unrealized_pnl,
                )
            else:
                # Reducing, Closing or Reversing
                if order.size <= existing_pos.size:
                    # Reducing or Closing
                    remaining_size = existing_pos.size - order.size
                    closed_size = order.size
                    
                    # Balance update
                    if product_type == 'FUTURE':
                        trade_pnl = (filled_price - existing_pos.avg_price) * closed_size * existing_pos.side * multiplier
                        margin_released = self.commission.initial_margin * closed_size
                        # Do not release more than was reserved
                        prev_margin = self.position_margin.get(order.product.name, 0.0)
                        margin_released = min(margin_released, prev_margin)
                        self.position_margin[order.product.name] = prev_margin - margin_released

                        self.pm.update_balance(currency='USD', value=last_balance + margin_released + trade_pnl)
                        unrealized_pnl = (current_price - existing_pos.avg_price) * remaining_size * existing_pos.side * multiplier
                    else: # Stock
                        # For stock, we get back the cash value of sold shares (revenue)
                        trade_pnl = (filled_price - existing_pos.avg_price) * closed_size * existing_pos.side
                        revenue = closed_size * filled_price
                        self.pm.update_balance(currency='USD', value=last_balance + revenue)
                        unrealized_pnl = (current_price - existing_pos.avg_price) * remaining_size * existing_pos.side

                    self.pm.update_position(
                        product=order.product,
                        side=existing_pos.side,
                        size=remaining_size,
                        last_price=filled_price,
                        avg_price=existing_pos.avg_price,
                        realized_pnl=existing_pos.realized_pnl + trade_pnl - self.commission.commission,
                        unrealized_pnl=unrealized_pnl,
                    )
                else:
                    # Reversing (Close old pos + Open new pos)
                    closed_size = existing_pos.size
                    new_open_size = order.size - existing_pos.size
                    
                    # Balance update
                    if product_type == 'FUTURE':
                        # Release old margin
                        trade_pnl = (filled_price - existing_pos.avg_price) * closed_size * existing_pos.side * multiplier
                        margin_released = self.commission.initial_margin * closed_size
                        prev_margin = self.position_margin.get(order.product.name, 0.0)
                        margin_released = min(margin_released, prev_margin)
                        self.position_margin[order.product.name] = prev_margin - margin_released

                        # release the old margin and lock the new margin
                        # Deduct new margin
                        margin_required = self.commission.initial_margin * new_open_size
                        # Track new margin for the reversed position
                        self.position_margin[order.product.name] = self.position_margin.get(order.product.name, 0.0) + margin_required
                        
                        self.pm.update_balance(currency='USD', value=last_balance + margin_released + trade_pnl - margin_required)
                        unrealized_pnl = (current_price - filled_price) * new_open_size * order.side * multiplier
                    else: # Stock
                        revenue = closed_size * filled_price
                        cost = new_open_size * filled_price
                        self.pm.update_balance(currency='USD', value=last_balance + revenue - cost)
                        unrealized_pnl = (current_price - filled_price) * new_open_size * order.side

                    self.pm.update_position(
                        product=order.product,
                        side=order.side,
                        size=new_open_size,
                        last_price=filled_price,
                        avg_price=filled_price,
                        realized_pnl=existing_pos.realized_pnl + trade_pnl - self.commission.commission,
                        unrealized_pnl=unrealized_pnl,
                    )
        else:
            # New Position
            if product_type == 'FUTURE':
                margin_required = self.commission.initial_margin * order.size
                self.position_margin[order.product.name] = margin_required
                self.pm.update_balance(currency='USD', value=last_balance - margin_required)
                unrealized_pnl = (current_price - filled_price) * order.size * order.side * multiplier
            else: # Stock
                self.pm.update_balance(currency='USD', value=last_balance - order.size * filled_price)
                unrealized_pnl = (current_price - filled_price) * order.size * order.side

            self.pm.create_position(
                product=order.product,
                side=order.side,
                size=order.size,
                last_price=filled_price,
                avg_price=filled_price,
                realized_pnl=0.0 - self.commission.commission,
                unrealized_pnl=unrealized_pnl
            )

        # Apply commission per transaction
        self.pm.update_balance(currency='USD', value=self.pm.get_balance('USD') - self.commission.commission)
        self.transaction_log.append({
            'ts': filling_time,
            'oid': order.oid,
            'product': order.product.name,
            'side': order.side,
            'size': order.size,
            'filled_price': filled_price,
            'order_type': order.order_type,
            'status': 'FILLED',
        })

    def on_bar(self, gateway, exch, pdt, freq, ts, open_, high, low, close, volume, on_order_status_update):
        """
        Take care of order filling and position updates on historical bar update
        """
        # make a copy of the open_orders
        ###
        open_orders = self.open_orders.copy()
        ###

        for _, order in open_orders.items():
            self.execute_order(
                gateway=gateway,
                exch=exch,
                pdt=pdt,
                order=order,
                bar=Bar(ts=ts, open_=open_, high=high, low=low, close=close, volume=volume),
                on_order_status_update=on_order_status_update
            )

        # update position unrealized PnL
        product = self.strategy.get_product(exch, pdt)
        if self.pm.get_position(product) is not None:
            position = self.pm.get_position(product)
            
            # Determine multiplier
            multiplier = 1.0
            if product.product_type == 'FUTURE':
                # if hasattr(self.commission, 'tick_value') and hasattr(self.commission, 'tick_size') and self.commission.tick_size != 0:
                #     multiplier = self.commission.tick_value / self.commission.tick_size
                # else:
                #     multiplier = self.commission.multiplier
                multiplier = self.commission.multiplier

            self.pm.update_position(
                product=product,
                side=position.side,
                size=position.size,
                last_price=close,
                avg_price=position.avg_price,
                realized_pnl=position.realized_pnl,
                unrealized_pnl=(close - position.avg_price) * position.size * position.side * multiplier
            )

        portfolio_value = self.pm.get_portfolio_value(currency='USD')
        self.portfolio_history.append({'ts': ts, 'portfolio_value': portfolio_value})
            
    def export_data(self, path_prefix='results'):
        df_portfolio = pd.DataFrame(self.portfolio_history)
        df_portfolio.to_csv(f'{path_prefix}_portfolio.csv', index=False)

        df_transactions = pd.DataFrame(self.transaction_log)
        df_transactions.to_csv(f'{path_prefix}_transactions.csv', index=False)
