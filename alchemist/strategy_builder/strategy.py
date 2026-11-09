from __future__ import annotations

from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncIterator

from alchemist.event_loop.seeder import SeedState
from alchemist.constants.instruments import Instrument
from alchemist.managers.order_manager import OrderManager
from alchemist.stores import CalcStateStore, RoundTripStore
from alchemist.strategy_builder.strategy_executions import StrategyExecution
from alchemist.strategy_builder.strategy_signal import StrategySignal, WarmupRequirements


class Strategy:
    """Strategy map: the one per-slot object the Event Loop talks to."""

    signal_name: str

    name: str
    config: dict
    signal_instrument: Instrument
    execution_instrument: Instrument
    session: str
    position: dict
    balance: dict
    signal: StrategySignal
    om: OrderManager
    execution: StrategyExecution
    deferred_bars: list | None
    seed_state: SeedState
    snapshot_interval: int

    def __init__(self, name: str, config: dict, gateway,
                 calc_state_store: CalcStateStore | None,
                 roundtrip_store: RoundTripStore | None) -> None: ...

    def on_bar(self, bars: deque, bar: dict) -> None:
        """Behaviour 1: execution.on_bar(bar, step_signal); step_signal steps the signal once."""

    def strategy_data(self) -> dict:
        """{'entry_price': ..., 'secs_since_entry': ...}; both None while flat."""
        ...

    def on_order_update(self, update: dict) -> None:
        """Behaviour 3: book only from Order Manager's change, then tell StrategyExecution."""

    def on_position_update(self, update: dict) -> None: ...
    def on_balance_update(self, update: dict) -> None: ...

    def warmup_requirements(self) -> WarmupRequirements: ...
    def warmup(self, history: dict) -> None: ...
    async def rebuild_intraday_state(self, seed_bars: list) -> None:
        """Behaviour 4: replay under deferred bars; yields within a time budget."""

    @asynccontextmanager
    async def deferring_bars(self) -> AsyncIterator[None]:
        """Behaviour 4: hold live bars aside during a replay, then step them in order."""
        yield

    def reset_signal(self) -> None:
        """self.signal = get_signal_dag(self.signal_name).from_config(self.config)."""

    def is_flat_idle(self) -> bool:
        """No position; no submitted, open or cancelling order."""
        ...

    def on_aux_ready(self, ready: bool, frames: dict) -> None:
        """Aux Data decision 2: seed the signal, then tell StrategyExecution."""

    def request_flatten(self, reason: str, bar_ts: float) -> None: ...

    def restore_signal_state(self) -> None: ...
    def signal_snapshot(self) -> dict: ...

    def is_processing_order(self) -> bool: ...
    def quarantine(self, reason: str) -> None: ...
    def release_quarantine(self) -> None: ...
    def is_quarantined(self) -> bool: ...
    def consume_trade_closed(self) -> bool: ...
    def get_submitted_orders(self) -> list: ...
    def get_open_orders(self) -> list: ...
    def get_cancelling_orders(self) -> list: ...
