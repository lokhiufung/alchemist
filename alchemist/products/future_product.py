from alchemist.products.base_product import BaseProduct

class FutureProduct(BaseProduct):
   product_type = 'FUTURE'
   
   def __init__(self, name, base_currency, exch, contract_month):
        super().__init__(name, base_currency, exch)
        self.contract_month = contract_month





    
      
