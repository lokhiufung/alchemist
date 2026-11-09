from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from alchemist.clock import Clock
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.recorders import BarRecorder, TradeRecorder
from alchemist.event_loop.shutdown import ShutdownCoordinator
from alchemist.instruments import Instrument

ON_BAR_ERROR_QUARANTINE = 3
BAR_FLUSH_GRACE_S = 1.0
ORDER_CHECK_INTERVAL_S = 60.0
DRAIN_TIMEOUT_S = 0.020
BACKLOG_ALERT = 5000

Handler = Callable[["EventLoop", dict], Awaitable[None]]
HANDLERS: dict[str, Handler] = {}


def handles(event_type: str) -> Callable[[Handler], Handler]:
    def bind(fn: Handler) -> Handler:
        HANDLERS[event_type] = fn
        return fn
    return bind


@dataclass
class EventLoop:
    """Event Loop map: drain, resample, dispatch. Reads no run mode (behaviour 19)."""

    event_buffer: deque
    book: RuntimeBook
    gateway: Any
    aux: Any
    safety: Any
    bar_recorder: BarRecorder
    trade_recorder: TradeRecorder
    shutdown: ShutdownCoordinator
    clock: Clock

    async def run(self) -> None:
        """Behaviours 1-7, 11, 12: drain on wake-up until end of data or exit."""

    async def dispatch(self, event: dict) -> None:
        """Behaviour 13: the main drain and the final drain both call this."""

    async def drain_remaining(self) -> None:
        """Behaviour 7: dispatch what is left after the loop stops."""

    async def emit_bar(self, instrument: Instrument, bar: dict, source: str) -> None:
        """Behaviours 1, 2, 13: the one place a completed bar enters dispatch."""

    async def dispatch_bar(self, instrument: Instrument, bar: dict) -> None:
        """Behaviour 3: Aux Data annotates, the gateway sees it, then strategies."""

    async def fan_out(self, strategies: list, method: str, *args: Any) -> None:
        """Behaviours 4, 5, 13: call method on each strategy; log and count exceptions."""

    async def fire_due_bars(self) -> None:
        """Behaviour 2: emit quiet buckets older than the grace."""

    async def reconcile_stuck(self) -> None:
        """Behaviour 6: broker truth, then Safety's repair, off the bar path."""


@handles("tick")
async def on_tick(loop: EventLoop, event: dict) -> None: ...


@handles("bar")
async def on_bar(loop: EventLoop, event: dict) -> None: ...


@handles("order_update")
async def on_order_update(loop: EventLoop, event: dict) -> None: ...


@handles("balance_update")
async def on_balance_update(loop: EventLoop, event: dict) -> None: ...


@handles("position_update")
async def on_position_update(loop: EventLoop, event: dict) -> None: ...


@handles("repaired_fill")
async def on_repaired_fill(loop: EventLoop, event: dict) -> None: ...


@handles("book_op")
async def on_book_op(loop: EventLoop, event: dict) -> None: ...


@handles("end_of_data")
async def on_end_of_data(loop: EventLoop, event: dict) -> None: ...
