from alchemist.products.base_product import BaseProduct

class FutureProduct(BaseProduct):
   product_type = 'FUTURE'
   
   def __init__(self, name: str, base_currency: str, exch: str, contract_month: str):
      """Product class for futures

      Args:
         name (str): _description_
         base_currency (str): _description_
         exch (str): _description_
         contract_month (str): e.g `2025-06` for June 2025 contract 
      """
      super().__init__(name, base_currency, exch)
      self.contract_month = contract_month





    
      
