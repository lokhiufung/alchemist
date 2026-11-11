from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Callable

from alchemist.event_loop.resampler import Resampler
from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument
from alchemist.constants.event_loop import BAR_HISTORY


class BookError(RuntimeError): ...


class RuntimeBook:
    """Event Loop behaviours 15, 17: the one registry of strategies and per-instrument resources."""

    strategies: list
    resamplers: dict[Instrument, Resampler]
    bars: dict[Instrument, deque]
    contracts: dict[Instrument, Any]
    fed_instruments: set[Instrument]
    sessions_by_instrument: dict[Instrument, str]
    sessions_by_strategy: dict[str, str]
    sessions_by_execution_instrument: dict[Instrument, str]
    retry_tasks: dict[str, list[asyncio.Task]]

    def __init__(self) -> None:
        self.strategies = []
        self.resamplers = {}
        self.bars = {}
        self.contracts = {}
        self.fed_instruments = set()
        self.sessions_by_instrument = {}
        self.sessions_by_strategy = {}
        self.sessions_by_execution_instrument = {}
        self.retry_tasks = {}
        self.subscribed: set[Instrument] = set()
        self.requests = None
        self.seeder = None
        self.member_listeners: list = []

    def attach(self, requests, seeder) -> None:
        self.requests = requests
        self.seeder = seeder

    def reserve_instrument(self, instrument: Instrument, contract: Any, fed: bool) -> bool:
        created = instrument not in self.contracts
        if created:
            self.contracts[instrument] = contract
        self.bars.setdefault(instrument, deque(maxlen=BAR_HISTORY))
        if fed:
            self.fed_instruments.add(instrument)
        return created

    def release_instrument(self, instrument: Instrument) -> None:
        if any(instrument in (s.signal_instrument, s.execution_instrument) for s in self.strategies):
            return
        for table in (self.contracts, self.resamplers, self.bars, self.sessions_by_instrument):
            table.pop(instrument, None)
        self.fed_instruments.discard(instrument)

    def commit(self, strategy, retry_tasks: list[asyncio.Task]) -> None:
        if any(s.name == strategy.name for s in self.strategies):
            raise BookError(f"strategy key {strategy.name!r} is already in the book")
        self.strategies.append(strategy)
        self.sessions_by_strategy[strategy.name] = strategy.session
        self.sessions_by_execution_instrument[strategy.execution_instrument] = strategy.session
        self.sessions_by_instrument[strategy.signal_instrument] = (
            exchange_profile(strategy.signal_instrument).data_session)
        self.retry_tasks[strategy.name] = list(retry_tasks)
        for listener in self.member_listeners:
            listener.member_joined(strategy)

    async def admit(self, strategy, guard: Callable[["RuntimeBook"], None] | None = None) -> None:
        created: list[Instrument] = []
        try:
            await self.stage(strategy, created)
            await self.prepare(strategy)
            if guard is not None:
                guard(self)
            self.commit(strategy, self.pending_retries(strategy))
        except BaseException:
            for instrument in created:
                if instrument in self.subscribed:
                    self.requests.command("unsubscribe", contract=self.contracts[instrument])
                    self.subscribed.discard(instrument)
                    self.resamplers.pop(instrument, None)
            for instrument in created:
                self.release_instrument(instrument)
            raise

    def pending_retries(self, strategy) -> list[asyncio.Task]:
        task = self.seeder.start_retry(strategy)
        return [task] if task is not None else []

    async def stage(self, strategy, created: list[Instrument]) -> None:
        signal_instrument = strategy.signal_instrument
        message_type, resolution = await self.requests.request("market_data_type", instrument=signal_instrument)
        if message_type == "bar_update" and strategy.resolution_s < resolution:
            raise BookError(f"slot {strategy.name}: needs {strategy.resolution_s}s bars but the feed of "
                            f"{signal_instrument.symbol} gives {resolution}s")
        for instrument in dict.fromkeys((signal_instrument, strategy.execution_instrument)):
            fed = instrument == signal_instrument
            contract = await self.requests.request("resolve_contract", instrument=instrument)
            if self.reserve_instrument(instrument, contract, fed):
                created.append(instrument)
            if fed and instrument not in self.subscribed:
                await self.requests.request("subscribe", contract=contract)
                self.subscribed.add(instrument)
                if message_type == "tick_update":
                    self.resamplers.setdefault(instrument, Resampler())
                self.sessions_by_instrument[instrument] = exchange_profile(instrument).data_session
                self.requests.command("set_instrument_sessions", sessions=dict(self.sessions_by_instrument))

    async def prepare(self, strategy) -> None:
        await self.seeder.attempt(strategy)

    def strategies_for(self, instrument: Instrument) -> list:
        return [s for s in self.strategies if s.signal_instrument == instrument]
