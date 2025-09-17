
from dataclasses import dataclass

@dataclass
class Commission:
    commission: float
    is_percentage: bool = False
    


@dataclass
class FutureContractCommission(Commission):
    # e.g for MES, initial_margin can be 2320, maintance_margin can be 2109, multipler can be 5 ($5 per point)
    initial_margin: float = 0.0
    maintainance_margin: float = 0.0
    multiplier: float = 1.0
    is_margin_percentage: bool = False
    