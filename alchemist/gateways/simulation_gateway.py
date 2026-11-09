from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Any

from alchemist.gateways.gateway import Gateway, register_gateway
from alchemist.instruments import Instrument


@dataclass(frozen=True)
class ReplayConfig:
    """The config's replay: section (System map decision 47)."""
    tape: str
    date: date
    session: str
    pad_before_s: int = 0
    pad_after_s: int = 0
    fill_reference: str = "close"
    slippage_ticks: float = 0.0


class TapeSource(ABC):
    """Gateway behaviour 15: 1-second bars, read lazily."""

    @abstractmethod
    def scan(self, instrument: Instrument, start_ts: int, end_ts: int) -> Any: ...


class ParquetTape(TapeSource):
    def __init__(self, path: str) -> None: ...
    def scan(self, instrument: Instrument, start_ts: int, end_ts: int) -> Any: ...


class SyntheticTape(TapeSource):
    def __init__(self, generator: str, seed: int) -> None: ...
    def scan(self, instrument: Instrument, start_ts: int, end_ts: int) -> Any: ...


def check_tape_in_session(tape: TapeSource, instrument: Instrument, session: str,
                          start_ts: int, end_ts: int) -> None:
    """Gateway behaviour 15: one generic check from the exchange profile; raises on stray rows."""


class OrderFillingEngine:
    """Gateway decision 2: prices simulated fills off the current bar."""

    def __init__(self, fill_reference: str = "close", slippage_ticks: float = 0.0) -> None: ...
    def try_fill(self, order: dict, bar: dict, instrument: Instrument) -> dict | None:
        """The fill for a working order on this bar, or None."""
        ...


@register_gateway("simulation")
class SimulationGateway(Gateway):
    """Glossary: simulated broker. Replays a tape and fills orders; prepares replay data on request."""

    def __init__(self, event_buffer, drain_wakeup, config: ReplayConfig, tape: TapeSource) -> None: ...

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def reconnect(self) -> None: ...
    async def subscribe_order_update(self) -> None: ...
    async def resolve_contract(self, instrument: Instrument) -> Any: ...
    async def subscribe_tick(self, contract: Any) -> None:
        """Preload the padded replay window into the event buffer, then end_of_data."""

    def next_order_id(self) -> int: ...
    async def place_order(self, order: dict[str, Any]) -> None: ...
    async def cancel_order(self, order_id: int) -> None: ...
    async def get_executions(self, since: float | None = None) -> list[dict[str, Any]]: ...
    async def get_open_orders(self, timeout: float = 5.0) -> list[dict[str, Any]]: ...
    async def heartbeat(self, timeout: float = 3.0) -> bool:
        return True

    async def get_daily_history(self, instrument: Instrument, start: date,
                                end: date, session: str) -> Any:
        """Gateway behaviours 7, 15: daily frame from the tape, broker definitions."""

    async def get_intraday_seed_bars(self, instrument: Instrument, session: str) -> list:
        """Gateway behaviour 15: [] when the tape has no prior session."""
        ...

    async def subscribe_account_update(self) -> None: ...
    async def get_positions(self, timeout: float = 5.0) -> list[dict]: ...
    def on_bar(self, instrument: Instrument, bar: dict[str, Any]) -> None:
        """Gateway behaviour 3: OrderFillingEngine prices fills off this bar."""
