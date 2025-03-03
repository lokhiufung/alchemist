

class BaseProduct:
    product_type = None
    
    def __init__(self, name, base_currency, exch):
        self.name = name
        self.base_currency = base_currency
        self.exch = exch

    def to_dict(self):
        return {**vars(self), 'product_type': self.product_type}
    