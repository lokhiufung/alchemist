from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from alchemist.clock import Clock
from alchemist.config import GatewayConfig
from alchemist.instruments import Instrument
from alchemist.registry import get_gateway, register_gateway

__all__ = ["Gateway", "get_gateway", "register_gateway"]


class Gateway(ABC):
    """Gateway map: base of every broker gateway and of the simulated broker."""

    def __init__(self, event_buffer, config: GatewayConfig, clock: Clock) -> None:
        self.event_buffer = event_buffer
        self.config = config
        self.clock = clock

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def reconnect(self) -> None: ...

    @abstractmethod
    async def subscribe_order_update(self) -> None: ...

    @abstractmethod
    async def resolve_contract(self, instrument: Instrument) -> Any: ...

    @abstractmethod
    async def subscribe_tick(self, contract: Any) -> None: ...

    @abstractmethod
    def next_order_id(self) -> int: ...

    @abstractmethod
    async def place_order(self, order: dict[str, Any]) -> None: ...

    @abstractmethod
    async def cancel_order(self, order_id: int) -> None: ...

    @abstractmethod
    async def get_executions(self, since: float | None = None) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def get_open_orders(self, timeout: float = 5.0) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def heartbeat(self, timeout: float = 3.0) -> bool: ...

    @abstractmethod
    async def get_daily_history(self, instrument: Instrument, start: date,
                                end: date, session: str) -> Any:
        """Behaviours 7, 11: daily frame in broker-neutral units, no look-ahead."""

    @abstractmethod
    async def get_intraday_seed_bars(self, instrument: Instrument, session: str) -> list: ...

    @abstractmethod
    async def subscribe_account_update(self) -> None: ...

    @abstractmethod
    async def get_positions(self, timeout: float = 5.0) -> list[dict]: ...

    @property
    def last_tick_ts(self) -> float | None:
        return None

    def set_instrument_sessions(self, sessions: dict[Instrument, str]) -> None: ...

    def market_data_type(self, instrument: Instrument) -> tuple[str, float | None]:
        return ("tick_update", None)

    def market_data_ready(self) -> bool:
        return True

    def consume_resubscribe_request(self) -> bool:
        return False

    def roll_mismatch_dates(self) -> frozenset:
        return frozenset()

    async def get_commissions(self) -> dict[str, dict]:
        return {}

    def on_bar(self, instrument: Instrument, bar: dict[str, Any]) -> None: ...

    def intraday_seed_failure_is_fatal(self) -> bool:
        return False

    async def unsubscribe_tick(self, contract: Any) -> None: ...

    async def start_feed(self) -> None:
        return None
