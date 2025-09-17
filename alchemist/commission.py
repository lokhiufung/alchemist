
from dataclasses import dataclass

@dataclass
class Commission:
    commssion: float
    is_percentage: bool = False
    


@dataclass
class FutureContractCommission(Commission):
    initial_margin: float = 0.0
    maintainance_margin: float = 0.0
    multiplier: float = 1.0
    