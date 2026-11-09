from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Callable

from alchemist.event_loop.resampler import Resampler
from alchemist.instruments import Instrument


class BookClosed(RuntimeError): ...


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

    def __init__(self) -> None: ...
    def reserve_instrument(self, instrument: Instrument, contract: Any, fed: bool) -> None: ...
    def release_instrument(self, instrument: Instrument) -> None: ...
    def commit(self, strategy, retry_tasks: list[asyncio.Task]) -> None:
        """Synchronous; raises on a duplicate strategy key and leaves the book unchanged."""

    def strategies_for(self, instrument: Instrument) -> list: ...


async def enqueue_book_op(event_buffer: deque, drain_wakeup: Callable[[], None],
                          fn: Callable[[RuntimeBook], Any]) -> Any:
    """Behaviour 15: run fn(book) on the drain loop between events; await its result."""
