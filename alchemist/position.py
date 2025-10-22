from alchemist.products.base_product import BaseProduct


class Position:
    def __init__(self, product: BaseProduct, side, size, last_price, avg_price, realized_pnl, unrealized_pnl):
        self.product = product
        self.size = size
        self.side = side
        self.last_price = last_price
        self.avg_price = avg_price
        self.realized_pnl = realized_pnl
        self.unrealized_pnl = unrealized_pnl

        # status flags for risk management
        self.status = 'IDLE' # "IDLE", "PENDING_BETTING", "PARTIALLY_BETTED", "BETTED", "PENDING_CLOSING", "CLOSED"

    def update(self, side, size, last_price, avg_price=None, realized_pnl=None, unrealized_pnl=None):
        self.side = side
        self.size = size
        self.last_price = last_price
        self.avg_price = avg_price
        self.realized_pnl = realized_pnl 
        self.unrealized_pnl = unrealized_pnl
    
    def update_status(self, status):
        self.status = status

    def __str__(self):
        return f'Product(name={self.product.name},side={self.side},size={self.size},last_price={self.last_price},avg_price={self.avg_price},realized_pnl={self.realized_pnl},unrealized_pnl={self.unrealized_pnl})'
    
    def __repr__(self):
        return f'Product(name={self.product.name},side={self.side},size={self.size},last_price={self.last_price},avg_price={self.avg_price},realized_pnl={self.realized_pnl},unrealized_pnl={self.unrealized_pnl})'
