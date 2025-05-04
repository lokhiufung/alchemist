import uuid

from alchemist.products.base_product import BaseProduct

class Order:
    def __init__(self, gateway: str, strategy: str, side: int, price: float, size: float, order_type: str, product: BaseProduct, info: str=None, oid: str=None, time_in_force: str='GTC', reason=''):
        self.oid = oid if oid is not None else str(uuid.uuid4())
        self.gateway = gateway
        self.strategy = strategy  # TODO: this is only the temperaroy solution for the
        self.side = side
        self.price = price
        self.size = size
        self.order_type = order_type  # a standardized order type
        self.product = product
        self.info = info if info is None else {}
        self.status = 'CREATED'
        self.time_in_force = time_in_force
        self.trigger_price = None

        self.filled_size: float = 0.0
        self.last_traded_price: float = None
        self.last_traded_size: float = None
        self.amend_price: float = None
        self.amend_size: float = None
        self.average_filled_price: float = None

        self.eoid: str = None
        self.is_reduce_only = False
        self.reason = reason

    @property
    def remaining_size(self):
        return self.size - self.filled_size
    
    def set_eoid(self, eoid):
        self.eoid = eoid  # external order id

    def on_opened(self):
        self.status = 'OPENED'

    def on_submitted(self):
        self.status = 'SUBMITTED'

    def on_canceled(self):
        self.status = 'CANCELED'

    def on_filled(self, price, size):
        self.status = 'FILLED'
        self.last_traded_price = price
        self.last_traded_size = size
        self.filled_size = size
    
    def on_partial_filled(self, price, size):
        self.status = 'PARTIALLY_FILLED'
        self.last_traded_price = price
        self.last_traded_size = size
        self.filled_size = size

    def on_rejected(self, reason=''):
        self.status = 'REJECTED'
        if len(reason) > 0:
            self.reason = ','.join([self.reason, reason])
    
    def on_internal_rejected(self, reason=''):
        self.status = 'INTERNALLY_REJECTED'
        if len(reason) > 0:
            self.reason = ','.join([self.reason, reason])

    def on_closed(self):
        self.status = 'CLOSED'

    def on_amended(self, price, size):
        self.price = price
        self.size = size
        self.status = 'AMENDED'
    
    def on_amend_submitted(self):
        self.status = 'AMEND_SUBMITTED'
        
    def __repr__(self):
        sided_size = self.size * self.side
        return f'{self.product.name}|{self.oid}|{self.eoid}|' \
               f'{self.order_type}|{self.time_in_force}|{self.status}|{sided_size}@{self.price}|' \
               f'filled={self.filled_size}@{self.average_filled_price}|' \
               f'last={self.last_traded_size}@{self.last_traded_price}|' \
               f'amend={self.amend_size}@{self.amend_price}|' \
               f'trigger={self.trigger_price}|target={self.trigger_price}|' \
               f'is_reduce_only={self.is_reduce_only}|' \
               f'reason={self.reason}'