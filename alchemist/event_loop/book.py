from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Awaitable, Callable

from alchemist.event_loop.resampler import Resampler
from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument


BAR_HISTORY = 5000


class BookClosed(RuntimeError): ...


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
        self.gateway = None
        self.seeder = None
        self.run_op = self.run_directly

    def attach(self, gateway, seeder, run_op: Callable[[Callable[["RuntimeBook"], Any]], Awaitable[Any]] | None = None) -> None:
        self.gateway = gateway
        self.seeder = seeder
        self.run_op = run_op or self.run_directly

    async def run_directly(self, fn: Callable[["RuntimeBook"], Any]) -> Any:
        return fn(self)

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

    async def admit(self, strategy) -> None:
        created: list[Instrument] = []
        try:
            await self.stage(strategy, created)
            await self.prepare(strategy)
            await self.run_op(lambda book: book.commit(strategy, self.pending_retries(strategy)))
        except BaseException:
            await self.run_op(lambda book: [book.release_instrument(i) for i in created])
            raise

    def pending_retries(self, strategy) -> list[asyncio.Task]:
        task = self.seeder.start_retry(strategy)
        return [task] if task is not None else []

    async def stage(self, strategy, created: list[Instrument]) -> None:
        signal_instrument = strategy.signal_instrument
        message_type, resolution = self.gateway.market_data_type(signal_instrument)
        if message_type == "bar_update" and strategy.resolution_s < resolution:
            raise BookError(f"slot {strategy.name}: needs {strategy.resolution_s}s bars but the feed of "
                            f"{signal_instrument.symbol} gives {resolution}s")
        for instrument in dict.fromkeys((signal_instrument, strategy.execution_instrument)):
            fed = instrument == signal_instrument
            contract = await self.gateway.resolve_contract(instrument)
            newly_reserved = await self.run_op(lambda book: book.reserve_instrument(instrument, contract, fed))
            if newly_reserved:
                created.append(instrument)
            if fed and instrument not in self.subscribed:
                await self.gateway.subscribe_tick(contract)
                self.subscribed.add(instrument)
                if message_type == "tick_update":
                    self.resamplers.setdefault(instrument, Resampler())
                self.sessions_by_instrument[instrument] = exchange_profile(instrument).data_session
                self.gateway.set_instrument_sessions(dict(self.sessions_by_instrument))

    async def prepare(self, strategy) -> None:
        await self.seeder.attempt(strategy)

    def strategies_for(self, instrument: Instrument) -> list:
        return [s for s in self.strategies if s.signal_instrument == instrument]



async def enqueue_book_op(event_buffer: deque, drain_wakeup: Callable[[], None],
                          fn: Callable[[RuntimeBook], Any]) -> Any:
    """Behaviour 15: run fn(book) on the drain loop between events; await its result."""
