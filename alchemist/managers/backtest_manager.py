from datetime import datetime

from alchemist.managers.portfolio_manager import PortfolioManager
from alchemist.managers.order_manager import OrderManager
from alchemist import standardized_messages


class BacktestManager(OrderManager):
    def __init__(self, pm: PortfolioManager):
        self.pm = pm
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

    def on_bar(self, gateway, exch, pdt, freq, ts, open_, high, low, close, volume):
        """
        Take care of order filling and position updates on historical bar update
        """
        # make a copy of the open_orders
        ###
        open_orders = self.open_orders.copy()
        ###

        for oid, order in open_orders.items():
            # 1. match orders with simple logic
            if order.order_type == 'MARKET':
                filled_price = open_
            else:
                raise NotImplementedError
            
            # if `MARKET` order, fill with the open price
            _, _, (_, _, order_update) = standardized_messages.create_order_update_message(
                ts=ts,
                gateway=gateway,
                strategy='',  # TODO
                exch=exch,
                pdt=pdt,
                oid=oid,
                status='FILLED',
                average_filled_price=open_,
                last_filled_price=open_,
                last_filled_size=order.size,
                amend_price=None,
                amend_size=None,
            )
            # order_update['ts'] = datetime.fromtimestamp(order_update['ts'])
            order_statuses = self.on_order_status_update(
                gateway=gateway,
                order_update=order_update
            )
            # 2. update balance accordingly
            last_balance = self.pm.get_balance(currency='USD')
            self.pm.update_balance(currency='USD', value=last_balance - order.size * filled_price)
            # 3. update position accordingly
            if self.pm.get_position(order.product):
                self.pm.update_position(
                    product=order.product,
                    side=order.side,
                    size=order.size,
                    last_price=filled_price,
                    avg_price=None,  # TODO: should be the weighted average price of the filled price and the avg_price
                    realized_pnl=0.0,
                    unrealized_pnl=0.0  # TODO: should be updated here
                )
            else:
                self.pm.create_position(
                    product=order.product,
                    side=order.side,
                    size=order.size,
                    last_price=filled_price,
                    avg_price=filled_price,
                    realized_pnl=0.0,
                    unrealized_pnl=0.0
                )

            




        

