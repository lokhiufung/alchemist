from __future__ import annotations

import asyncio
import inspect
import logging
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from alchemist.clock import Clock
from alchemist.config import EventLoopConfig
from alchemist.event_loop.book import BookClosed, RuntimeBook
from alchemist.event_loop.buffer import EventBuffer
from alchemist.instruments import Instrument
from alchemist.messages import make_event

logger = logging.getLogger(__name__)

BAR_HISTORY = 5000

Handler = Callable[["EventLoop", dict], Awaitable[None]]
HANDLERS: dict[str, Handler] = {}


def handles(*event_types: str) -> Callable[[Handler], Handler]:
    def bind(fn: Handler) -> Handler:
        for event_type in event_types:
            HANDLERS[event_type] = fn
        return fn
    return bind


class EventLoop:
    def __init__(self, event_buffer: EventBuffer, book: RuntimeBook, gateway: Any, aux: Any, safety: Any,
                 bar_recorder: Any, trade_recorder: Any, shutdown: Any, clock: Clock,
                 config: EventLoopConfig | None = None,
                 clock_advance: Callable[[datetime], None] | None = None) -> None:
        self.event_buffer = event_buffer
        self.book = book
        self.gateway = gateway
        self.aux = aux
        self.safety = safety
        self.bar_recorder = bar_recorder
        self.trade_recorder = trade_recorder
        self.shutdown = shutdown
        self.clock = clock
        self.config = config or EventLoopConfig()
        self.clock_advance = clock_advance or (lambda moment: None)
        self.last_bar_ts: dict[Instrument, float] = {}
        self.errors: dict[str, int] = {}
        self.end_of_data_seen = False
        self.stopping = False
        self.backlog_alerted = False
        self.book_closed = False
        self.feed_types: dict[Instrument, str] = {}

    def finished(self) -> bool:
        return self.stopping or (self.end_of_data_seen and not len(self.event_buffer))

    async def run(self) -> None:
        sys.setswitchinterval(self.config.gil_switch_interval_s)
        self.event_buffer.bind_loop(asyncio.get_running_loop())
        while not self.finished():
            self.safety.check_in()
            await self.drain_pass()
            await self.fire_due_bars()
            await self.shutdown.process_due()
            if self.finished():
                break
            if len(self.event_buffer):
                await asyncio.sleep(0)
            else:
                await self.event_buffer.wait(self.clock)
        self.book_closed = True
        await self.shutdown.finalize(self.drain_remaining)

    async def drain_pass(self) -> None:
        self.event_buffer.begin_pass()
        self.alert_on_backlog()
        deadline = time.perf_counter() + self.config.drain_batch_budget_s
        while (event := self.event_buffer.pop()) is not None:
            await self.safe_dispatch(event)
            if time.perf_counter() >= deadline:
                break

    def alert_on_backlog(self) -> None:
        exceeded = self.event_buffer.control_backlog_exceeded()
        if exceeded and not self.backlog_alerted:
            logger.error("control lane backlog %d is over the threshold %d", len(self.event_buffer.control),
                         self.config.backlog_alert)
        self.backlog_alerted = exceeded

    async def drain_remaining(self) -> None:
        self.event_buffer.begin_pass()
        while (event := self.event_buffer.pop()) is not None:
            await self.safe_dispatch(event)

    async def safe_dispatch(self, event: dict) -> None:
        try:
            await self.dispatch(event)
        except Exception:
            logger.exception("dispatch of %s failed", event.get("event_type"))

    async def dispatch(self, event: dict) -> None:
        handler = HANDLERS.get(event["event_type"])
        if handler is None:
            logger.error("no handler for event type %r", event["event_type"])
            return
        await handler(self, event["data"])

    async def enqueue_book_op(self, fn: Callable[[RuntimeBook], Any]) -> Any:
        if self.book_closed:
            raise BookClosed("book closed")
        future = asyncio.get_running_loop().create_future()
        self.event_buffer.push(make_event("book_op", {"fn": fn, "future": future}, self.clock.timestamp()))
        return await future

    async def emit_bar(self, instrument: Instrument, bar: dict, source: str) -> None:
        bar["instrument"] = instrument
        self.event_buffer.push(make_event("bar", {"instrument": instrument, "bar": bar, "source": source},
                                          self.clock.timestamp()), wake=False)

    async def dispatch_bar(self, instrument: Instrument, bar: dict) -> None:
        self.clock_advance(datetime.fromtimestamp(bar["end_ts"], timezone.utc))
        self.aux.annotate(instrument, bar)
        self.gateway.on_bar(instrument, bar)
        history = self.book.bars.setdefault(instrument, deque(maxlen=BAR_HISTORY))
        history.append(bar)
        self.bar_recorder.record(instrument, bar)
        await self.fan_out(self.book.strategies_for(instrument), "on_bar", history, bar)

    async def fan_out(self, strategies: list, method: str, *args: Any) -> None:
        for strategy in list(strategies):
            is_bar = method == "on_bar"
            if is_bar and strategy.is_quarantined():
                continue
            try:
                result = getattr(strategy, method)(*args)
                if inspect.isawaitable(result):
                    await result
                if is_bar:
                    self.errors[strategy.name] = 0
            except Exception:
                logger.exception("[%s] %s failed", strategy.name, method)
                if is_bar:
                    self.count_error(strategy)

    def count_error(self, strategy: Any) -> None:
        count = self.errors.get(strategy.name, 0) + 1
        self.errors[strategy.name] = count
        if count >= self.config.on_bar_error_quarantine:
            logger.critical("[%s] quarantined after %d consecutive on_bar errors", strategy.name, count)
            strategy.quarantine(f"{count} consecutive on_bar errors")

    def ticked_resamplers(self) -> list[tuple[Instrument, Any]]:
        ticked = []
        for instrument, resampler in list(self.book.resamplers.items()):
            if instrument not in self.feed_types:
                self.feed_types[instrument] = self.gateway.market_data_type(instrument)[0]
            if self.feed_types[instrument] == "tick_update":
                ticked.append((instrument, resampler))
        return ticked

    async def fire_due_bars(self) -> None:
        now = self.clock.timestamp()
        for instrument, resampler in self.ticked_resamplers():
            bar = resampler.flush_due(now, self.config.bar_flush_grace_s)
            if bar is not None:
                await self.emit_bar(instrument, bar, "flush")


@handles("tick_update")
async def on_tick_update(loop: EventLoop, data: dict) -> None:
    instrument = data["instrument"]
    resampler = loop.book.resamplers.get(instrument)
    if resampler is None:
        return
    bar = resampler.update_from_tick(data["broker_ts"], data["price"], data["size"])
    if bar is not None:
        await loop.emit_bar(instrument, bar, "tick_update")


@handles("bar_update")
async def on_bar_update(loop: EventLoop, data: dict) -> None:
    instrument = data["instrument"]
    last = loop.last_bar_ts.get(instrument)
    if last is not None and data["start_ts"] <= last:
        logger.debug("rejected %s bar %s not later than %s", instrument.symbol, data["start_ts"], last)
        return
    loop.last_bar_ts[instrument] = data["start_ts"]
    await loop.emit_bar(instrument, dict(data), "bar_update")


@handles("bar")
async def on_bar_event(loop: EventLoop, data: dict) -> None:
    await loop.dispatch_bar(data["instrument"], data["bar"])


@handles("order_update", "repaired_fill")
async def on_order_update(loop: EventLoop, data: dict) -> None:
    strategies = loop.book.strategies
    await loop.fan_out(strategies, "on_order_update", data)
    loop.trade_recorder.record_update(data)
    for strategy in list(strategies):
        if strategy.consume_trade_closed():
            await loop.trade_recorder.flush()


@handles("balance_update")
async def on_balance_update(loop: EventLoop, data: dict) -> None:
    await loop.fan_out(loop.book.strategies, "on_balance_update", data)


@handles("position_update")
async def on_position_update(loop: EventLoop, data: dict) -> None:
    await loop.fan_out(loop.book.strategies, "on_position_update", data)


@handles("book_op")
async def on_book_op(loop: EventLoop, data: dict) -> None:
    future = data["future"]
    if loop.book_closed:
        future.set_exception(BookClosed("book closed"))
        return
    try:
        result = data["fn"](loop.book)
        if inspect.isawaitable(result):
            result = await result
        future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


@handles("end_of_data")
async def on_end_of_data(loop: EventLoop, data: dict) -> None:
    for instrument, resampler in loop.ticked_resamplers():
        bar = resampler.flush()
        if bar is not None:
            await loop.emit_bar(instrument, bar, "end_of_data")
    loop.end_of_data_seen = True
