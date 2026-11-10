from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from alchemist.exchanges import CME_NAME, EUREX_NAME


class AssetClass(str, Enum):
    FUTURE = "future"
    STOCK = "stock"
    OPTION = "option"


@dataclass(frozen=True)
class Instrument:
    asset_class: AssetClass
    symbol: str
    exchange: str
    currency: str
    multiplier: float
    tick_size: float

    def __str__(self) -> str:
        return self.symbol


ES = Instrument(AssetClass.FUTURE, "ES", CME_NAME, "USD", multiplier=50.0, tick_size=0.25)
NQ = Instrument(AssetClass.FUTURE, "NQ", CME_NAME, "USD", multiplier=20.0, tick_size=0.25)
MES = Instrument(AssetClass.FUTURE, "MES", CME_NAME, "USD", multiplier=5.0, tick_size=0.25)
MNQ = Instrument(AssetClass.FUTURE, "MNQ", CME_NAME, "USD", multiplier=2.0, tick_size=0.25)
FDAX = Instrument(AssetClass.FUTURE, "FDAX", EUREX_NAME, "EUR", multiplier=25.0, tick_size=1.0)
FDXM = Instrument(AssetClass.FUTURE, "FDXM", EUREX_NAME, "EUR", multiplier=5.0, tick_size=1.0)

INSTRUMENTS: dict[str, Instrument] = {i.symbol: i for i in (ES, NQ, MES, MNQ, FDAX, FDXM)}


def register_instrument(item: Instrument) -> None:
    INSTRUMENTS[item.symbol] = item


def instrument(symbol: str) -> Instrument:
    if symbol not in INSTRUMENTS:
        raise ValueError(f"unknown instrument {symbol!r}; known: {sorted(INSTRUMENTS)}")
    return INSTRUMENTS[symbol]
