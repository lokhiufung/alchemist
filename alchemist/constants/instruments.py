from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class AssetClass(str, Enum):
    FUTURE = "future"
    STOCK = "stock"
    OPTION = "option"


@dataclass(frozen=True)
class Instrument:
    """System map decisions 37, 41."""
    asset_class: AssetClass
    symbol: str
    exchange: str
    currency: str
    multiplier: float
    tick_size: float


ES = Instrument(AssetClass.FUTURE, "ES", "CME", "USD", multiplier=50.0, tick_size=0.25)
NQ = Instrument(AssetClass.FUTURE, "NQ", "CME", "USD", multiplier=20.0, tick_size=0.25)
MES = Instrument(AssetClass.FUTURE, "MES", "CME", "USD", multiplier=5.0, tick_size=0.25)
MNQ = Instrument(AssetClass.FUTURE, "MNQ", "CME", "USD", multiplier=2.0, tick_size=0.25)
