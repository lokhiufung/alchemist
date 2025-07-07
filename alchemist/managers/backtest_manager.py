from datetime import datetime

import pandas as pd

from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.managers.order_manager import OrderManager
from alchemist import standardized_messages
from alchemist.datas.common import Bar
from alchemist.order import Order


class BacktestManager(OrderManager):
    def __init__(self, strategy, pm: PortfolioManager):
        self.strategy = strategy
        self.pm = pm
        self.portfolio_history = []  # list of (ts, portfolio_value)
        self.transaction_log = []    # list of dicts with transaction details
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
        if order.order_type == 'MARKET':
            filled_price = bar.open
        else:
            raise NotImplementedError
        
        # if `MARKET` order, fill with the open price
        _, _, (_, _, order_update) = standardized_messages.create_order_update_message(
            ts=bar.ts,
            gateway=gateway,
            strategy=self.strategy,
            exch=exch,
            pdt=pdt,
            oid=order.oid,
            status='FILLED',
            average_filled_price=filled_price,
            last_filled_price=filled_price,
            last_filled_size=order.size,
            amend_price=None,
            amend_size=None,
        )

        on_order_status_update(gateway, order_update)  # TODO: just a quick fix

        # order_update['ts'] = datetime.fromtimestamp(order_update['ts'])
        order_statuses = self.on_order_status_update(
            gateway=gateway,
            order_update=order_update
        )
        
        # 2. update balance accordingly
        product_type = order.product.product_type
        last_balance = self.pm.get_balance(currency='USD')
        pos = self.pm.get_position(order.product)
        is_reducing = (
            pos is not None and pos.size != 0 and
            ((pos.side == 1 and order.side == -1) or (pos.side == -1 and order.side == 1))
        )

        if product_type == 'futures':
            margin_required = 0.1 * abs(order.size * filled_price)  # assume 10% initial margin
            if not is_reducing:
                self.pm.update_balance(currency='USD', value=last_balance - margin_required)
            else:
                self.pm.update_balance(currency='USD', value=last_balance + margin_required)
        else:  # stock
            if not is_reducing:
                self.pm.update_balance(currency='USD', value=last_balance - order.size * filled_price)
            else:
                self.pm.update_balance(currency='USD', value=last_balance + order.size * filled_price)
        # 3. update position accordingly
        current_price = bar.close  # use close price for unrealized PnL

        if self.pm.get_position(order.product):
            existing_pos = self.pm.get_position(order.product)
            is_same_direction = existing_pos.size < 1e-6 or (existing_pos.side == order.side)

            if is_same_direction:
                # Increase position size and recalculate avg_price
                new_size = existing_pos.size + order.size
                new_avg_price = (
                    (existing_pos.avg_price * existing_pos.size + filled_price * order.size) / new_size
                )
                self.pm.update_position(
                    product=order.product,
                    side=order.side,
                    size=new_size,
                    last_price=filled_price,
                    avg_price=new_avg_price,
                    realized_pnl=existing_pos.realized_pnl,
                    unrealized_pnl=(current_price - new_avg_price) * new_size * order.side
                )
            else:
                # Reducing or closing position
                if order.size < existing_pos.size:
                    remaining_size = existing_pos.size - order.size
                    realized_pnl = (filled_price - existing_pos.avg_price) * order.size * existing_pos.side
                    self.pm.update_position(
                        product=order.product,
                        side=existing_pos.side,
                        size=remaining_size,
                        last_price=filled_price,
                        avg_price=existing_pos.avg_price,
                        realized_pnl=existing_pos.realized_pnl + realized_pnl,
                        unrealized_pnl=(current_price - existing_pos.avg_price) * remaining_size * existing_pos.side
                    )
                else:
                    # Fully closed or reversed
                    realized_pnl = (filled_price - existing_pos.avg_price) * existing_pos.size * existing_pos.side
                    new_size = order.size - existing_pos.size
                    self.pm.update_position(
                        product=order.product,
                        side=order.side,
                        size=new_size,
                        last_price=filled_price,
                        avg_price=filled_price,
                        realized_pnl=realized_pnl,
                        unrealized_pnl=(current_price - filled_price) * new_size * order.side
                    )
        else:
            self.pm.create_position(
                product=order.product,
                side=order.side,
                size=order.size,
                last_price=filled_price,
                avg_price=filled_price,
                realized_pnl=0.0,
                unrealized_pnl=(current_price - filled_price) * order.size * order.side
            )
        self.transaction_log.append({
            'ts': bar.ts,
            'oid': order.oid,
            'product': order.product.name,
            'side': order.side,
            'size': order.size,
            'filled_price': filled_price,
            'order_type': order.order_type,
            'status': 'FILLED'
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
        portfolio_value = self.pm.get_portfolio_value(currency='USD')
        self.portfolio_history.append({'ts': ts, 'portfolio_value': portfolio_value})
            
    def export_data(self, path_prefix='results'):
        df_portfolio = pd.DataFrame(self.portfolio_history)
        df_portfolio.to_csv(f'{path_prefix}_portfolio.csv', index=False)

        df_transactions = pd.DataFrame(self.transaction_log)
        df_transactions.to_csv(f'{path_prefix}_transactions.csv', index=False)
