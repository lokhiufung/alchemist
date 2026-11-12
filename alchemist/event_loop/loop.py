from __future__ import annotations

import asyncio
import inspect
import logging
import sys
import time
from collections import deque
from typing import Any, Awaitable, Callable

from alchemist.config import EventLoopConfig
from alchemist.constants.event_loop import BAR_HISTORY
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.buffer import EventBuffer
from alchemist.event_loop.metrics import LatencyMetrics
from alchemist.event_loop.recorders import Recorder
from alchemist.event_loop.watch_time import WatchTime
from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument
from alchemist.messages import make_event

logger = logging.getLogger("event_loop")


Handler = Callable[["EventLoop", dict], Awaitable[None]]
HANDLERS: dict[str, Handler] = {}


def handles(*event_types: str) -> Callable[[Handler], Handler]:
    def bind(fn: Handler) -> Handler:
        for event_type in event_types:
            HANDLERS[event_type] = fn
        return fn
    return bind


class EventLoop:
    def __init__(self, event_buffer: EventBuffer, requests: Any, book: RuntimeBook, aux: Any,
                 recorders: list[Recorder], shutdown: Any, watch_time: WatchTime, deploy: Any,
                 check_in: Callable[[], None], config: EventLoopConfig | None = None,
                 metrics: LatencyMetrics | None = None, wall: Callable[[], float] = time.time) -> None:
        self.event_buffer = event_buffer
        self.requests = requests
        self.book = book
        self.aux = aux
        self.recorders = recorders
        self.shutdown = shutdown
        self.watch_time = watch_time
        self.deploy = deploy
        self.check_in = check_in
        self.config = config or EventLoopConfig()
        self.metrics = metrics or LatencyMetrics(wall)
        self.wall = wall
        self.slot_cost = 0.0
        self.current_arrival = 0.0
        self.last_bar_ts: dict[Instrument, float] = {}
        self.errors: dict[str, int] = {}
        self.end_of_data_seen = False
        self.stopping = False
        self.backlog_alerted = False
        self.book_closed = False

    def finished(self) -> bool:
        return self.should_stop() or (self.end_of_data_seen and not len(self.event_buffer))

    def should_stop(self) -> bool:
        return self.stopping or self.shutdown.shutdown_requested

    async def run(self) -> None:
        sys.setswitchinterval(self.config.gil_switch_interval_s)
        self.event_buffer.bind_loop(asyncio.get_running_loop())
        while not self.finished():
            self.check_in()
            self.metrics.maybe_flush(self.wall())
            await self.drain_pass()
            await self.fire_due_bars()
            await self.shutdown.process_due()
            if self.finished():
                break
            if len(self.event_buffer):
                await asyncio.sleep(0)
            else:
                await self.event_buffer.wait()
        self.book_closed = True
        drainer = asyncio.get_running_loop().create_task(self.drain_forever())
        try:
            await self.shutdown.finalize(self.settle)
        finally:
            drainer.cancel()
            await asyncio.gather(drainer, return_exceptions=True)
        for recorder in self.recorders:
            recorder.finalize()

    async def drain_forever(self) -> None:
        while True:
            self.check_in()
            await self.drain_remaining()
            await self.event_buffer.wait()

    async def settle(self) -> None:
        await asyncio.sleep(0)
        await self.event_buffer.wait_idle()

    async def drain_pass(self) -> None:
        self.event_buffer.begin_pass()
        self.alert_on_backlog()
        deadline = time.perf_counter() + self.config.drain_batch_budget_s
        while (event := self.event_buffer.pop()) is not None:
            await self.safe_dispatch(event)
            if time.perf_counter() >= deadline or self.should_stop():
                break

    def alert_on_backlog(self) -> None:
        exceeded = self.event_buffer.control_backlog_exceeded()
        if exceeded and not self.backlog_alerted:
            logger.error("control lane backlog is over the threshold", extra={
                "event": "control_backlog", "backlog": len(self.event_buffer.control),
                "threshold": self.config.backlog_alert})
        self.backlog_alerted = exceeded

    async def drain_remaining(self) -> None:
        self.event_buffer.begin_pass()
        while (event := self.event_buffer.pop()) is not None:
            await self.safe_dispatch(event)

    async def safe_dispatch(self, event: dict) -> None:
        started = self.wall()
        self.current_arrival = event.get("pushed_at", started)
        self.watch_time.observe(event["event_time"], self.watch_time.system_time())
        self.slot_cost = 0.0
        try:
            await self.dispatch(event)
        except Exception:
            logger.exception("dispatch failed", extra={"event": "dispatch_failed", "event_type": event.get("event_type")})
        self.record_latency(event, started)

    def record_latency(self, event: dict, started: float) -> None:
        kind = event["event_type"]
        if kind not in ("bar", "order_update", "repaired_fill"):
            return
        done = self.wall()
        arrival = event.get("pushed_at", started)
        data = event["data"]
        origin = data.get("origin") if kind == "bar" else None
        recv = origin["recv_ts"] if origin else arrival
        instrument = data["instrument"]
        feed = precision = None
        if origin:
            feed = origin["recv_ts"] - origin["broker_ts"]
            precision = self.metrics.precision(instrument)
        self.metrics.record_processing(instrument, recv, done, max(0.0, started - arrival), done - started,
                                       self.slot_cost, kind, feed, precision)

    async def dispatch(self, event: dict) -> None:
        handler = HANDLERS.get(event["event_type"])
        if handler is None:
            logger.error("no handler for event type", extra={"event": "no_handler", "event_type": event["event_type"]})
            return
        await handler(self, event["data"])

    async def emit_bar(self, instrument: Instrument, bar: dict, source: str, origin: dict | None = None) -> None:
        bar["instrument"] = instrument
        self.event_buffer.push(make_event("bar", {"instrument": instrument, "bar": bar, "source": source,
                                                  "origin": origin}, bar["end_ts"]), wake=False)

    async def dispatch_bar(self, instrument: Instrument, bar: dict) -> None:
        await self.shutdown.process_due()
        self.aux.annotate(instrument, bar)
        history = self.book.bars.setdefault(instrument, deque(maxlen=BAR_HISTORY))
        history.append(bar)
        session = self.book.sessions_by_instrument.get(instrument, exchange_profile(instrument).data_session)
        for recorder in self.recorders:
            recorder.record_bar(instrument, bar, session)
        flattening = self.shutdown.flattening_names()
        members = [s for s in self.book.strategies_for(instrument) if s.name not in flattening]
        await self.fan_out(members, "on_bar", history, bar)
        await self.shutdown.flatten_on_bar(instrument, bar)

    async def fan_out(self, strategies: list, method: str, *args: Any) -> None:
        for strategy in list(strategies):
            is_bar = method == "on_bar"
            if is_bar and strategy.is_quarantined():
                continue
            began = time.perf_counter()
            try:
                result = getattr(strategy, method)(*args)
                if inspect.isawaitable(result):
                    await result
                await strategy.settle()
                if is_bar:
                    self.errors[strategy.name] = 0
            except Exception:
                logger.exception("strategy handler failed", extra={
                    "event": "strategy_handler_failed", "strategy": strategy.name, "handler": method})
                if is_bar:
                    self.count_error(strategy)
            self.slot_cost += time.perf_counter() - began

    def count_error(self, strategy: Any) -> None:
        count = self.errors.get(strategy.name, 0) + 1
        self.errors[strategy.name] = count
        if count >= self.config.on_bar_error_quarantine:
            logger.critical("strategy quarantined after consecutive on_bar errors", extra={
                "event": "strategy_quarantined", "strategy": strategy.name, "errors": count})
            strategy.quarantine(f"{count} consecutive on_bar errors")

    def ticked_resamplers(self) -> list[tuple[Instrument, Any]]:
        return list(self.book.resamplers.items())

    def order_owner(self, order_id: int) -> tuple[Any, dict] | None:
        for strategy in self.book.strategies:
            order = strategy.om.get_order(order_id)
            if order is not None:
                return strategy, order
        return None

    async def fire_due_bars(self) -> None:
        now = self.watch_time.now()
        if now is None:
            return
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
    loop.metrics.record_feed(instrument, data["broker_ts"], data["recv_ts"])
    if "decode_s" in data:
        loop.metrics.record_decode(instrument, data["decode_s"])
    bar = resampler.update_from_tick(data["broker_ts"], data["price"], data["size"])
    if bar is not None:
        await loop.emit_bar(instrument, bar, "tick_update",
                            {"recv_ts": data["recv_ts"], "broker_ts": data["broker_ts"]})


@handles("bar_update")
async def on_bar_update(loop: EventLoop, data: dict) -> None:
    instrument = data["instrument"]
    last = loop.last_bar_ts.get(instrument)
    if last is not None and data["start_ts"] <= last:
        logger.debug("bar rejected, not later than the last bar", extra={
            "event": "bar_rejected", "instrument": instrument.symbol, "start_ts": data["start_ts"], "last_ts": last})
        return
    loop.last_bar_ts[instrument] = data["start_ts"]
    origin = {"recv_ts": loop.current_arrival, "broker_ts": data["broker_ts"]}
    loop.metrics.record_feed(instrument, data["broker_ts"], loop.current_arrival)
    await loop.emit_bar(instrument, dict(data), "bar_update", origin)


@handles("bar")
async def on_bar_event(loop: EventLoop, data: dict) -> None:
    await loop.dispatch_bar(data["instrument"], data["bar"])


@handles("order_update", "repaired_fill")
async def on_order_update(loop: EventLoop, data: dict) -> None:
    strategies = loop.book.strategies
    key = data.get("strategy")
    targets = strategies if key is None else [s for s in strategies if s.name == key]
    await loop.fan_out(targets, "on_order_update", data)
    owner = loop.order_owner(data["order_id"])
    if owner is not None:
        for recorder in loop.recorders:
            recorder.record_order_update(data, owner[0].name, owner[1])
    for strategy in list(strategies):
        if strategy.consume_trade_closed():
            for recorder in loop.recorders:
                recorder.flush()


@handles("balance_update")
async def on_balance_update(loop: EventLoop, data: dict) -> None:
    await loop.fan_out(loop.book.strategies, "on_balance_update", data)


@handles("position_update")
async def on_position_update(loop: EventLoop, data: dict) -> None:
    await loop.fan_out(loop.book.strategies, "on_position_update", data)


@handles("reply", "executions", "open_orders", "positions")
async def on_reply(loop: EventLoop, data: dict) -> None:
    loop.requests.resolve(data)
    if data["request"] == "poll_positions":
        logger.info("broker positions", extra={"event": "broker_positions", "positions": data["result"],
                                               "error": data["error"]})


@handles("heartbeat")
async def on_heartbeat(loop: EventLoop, data: dict) -> None:
    return None


@handles("connection_lost")
async def on_connection_lost(loop: EventLoop, data: dict) -> None:
    logger.warning("broker connection lost", extra={"event": "connection_lost"})


@handles("connection_restored")
async def on_connection_restored(loop: EventLoop, data: dict) -> None:
    loop.requests.raise_order_id_floor(data["next_order_id"])


@handles("deploy_request")
async def on_deploy_request(loop: EventLoop, data: dict) -> None:
    loop.deploy.handle(data)


@handles("os_signal")
async def on_os_signal(loop: EventLoop, data: dict) -> None:
    loop.shutdown.request_shutdown("signal")


@handles("end_of_data")
async def on_end_of_data(loop: EventLoop, data: dict) -> None:
    for instrument, resampler in loop.ticked_resamplers():
        bar = resampler.flush()
        if bar is not None:
            await loop.emit_bar(instrument, bar, "end_of_data")
    loop.end_of_data_seen = True
