from __future__ import annotations

import asyncio
import logging
import signal
import time
from functools import partial

from alchemist.clock import Clock
from alchemist.config import EventLoopConfig
from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument

logger = logging.getLogger("shutdown")

POLL_S = 0.005
RECHECK_OUTSIDE_SESSION_S = 30.0


class ShutdownCoordinator:
    def __init__(self, book, gateway, clock: Clock, config: EventLoopConfig | None = None) -> None:
        self.book = book
        self.gateway = gateway
        self.clock = clock
        self.config = config or EventLoopConfig()
        self.shutdown_requested = False
        self.not_flat_at_shutdown = False
        self.first_request: dict[str, float] = {}
        self.last_request: dict[str, float] = {}
        self.window_end: dict[str, float] = {}
        self.settled_until: dict[str, float] = {}
        self.cutoffs: dict[str, tuple[float, float, float]] = {}

    def request_shutdown(self, reason: str) -> None:
        logger.warning("shutdown requested", extra={"event": "shutdown_requested", "reason": reason})
        self.shutdown_requested = True

    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, partial(self.request_shutdown, "signal"))

    def active_strategies(self) -> list:
        return list(self.book.strategies)

    def configured_instruments(self) -> set[Instrument]:
        return {s.execution_instrument for s in self.book.strategies}

    def flattening_names(self) -> set[str]:
        return set(self.first_request)

    def cutoff_for(self, strategy, now: float) -> tuple[float, float, float]:
        known = self.cutoffs.get(strategy.name)
        if known is not None and now < known[2]:
            return known
        profile = exchange_profile(strategy.execution_instrument)
        window = profile.session_window_at(self.book.sessions_by_strategy[strategy.name], self.clock.now())
        if window is None:
            known = (float("inf"), float("inf"), now + RECHECK_OUTSIDE_SESSION_S)
        else:
            end_ts = window[1].timestamp()
            known = (end_ts - self.config.cutoff_offset_s, end_ts, end_ts)
        self.cutoffs[strategy.name] = known
        return known

    async def process_due(self) -> None:
        now = self.clock.timestamp()
        for strategy in self.active_strategies():
            name = strategy.name
            if name in self.first_request:
                continue
            cutoff, end_ts, _ = self.cutoff_for(strategy, now)
            if now < cutoff or self.settled_until.get(name, 0.0) >= end_ts:
                continue
            self.window_end[name] = end_ts
            await self.request_flatten(strategy, now, "session_end")
            if strategy.is_flat_idle():
                self.settled_until[name] = end_ts
            else:
                self.first_request[name] = now

    async def request_flatten(self, strategy, at_ts: float, reason: str) -> None:
        self.last_request[strategy.name] = at_ts
        try:
            strategy.request_flatten(reason, at_ts)
            await strategy.settle()
        except Exception:
            logger.exception("flatten request failed", extra={"event": "flatten_failed", "strategy": strategy.name})

    async def flatten_on_bar(self, instrument: Instrument, bar: dict) -> None:
        now = self.clock.timestamp()
        for strategy in self.active_strategies():
            name = strategy.name
            if name not in self.first_request or strategy.signal_instrument != instrument:
                continue
            if strategy.is_flat_idle() or now - self.first_request[name] >= self.config.flatten_timeout_s:
                self.settled_until[name] = self.window_end[name]
                del self.first_request[name]
                continue
            if self.last_request.get(name, -1.0) < now:
                await self.request_flatten(strategy, now, "session_end")

    def ready_to_exit(self) -> bool:
        return self.shutdown_requested

    async def is_flat(self) -> bool:
        try:
            positions = await self.gateway.get_positions()
        except Exception:
            return False
        configured = self.configured_instruments()
        return all(p["size"] == 0 for p in positions if p["instrument"] in configured)

    async def finalize(self, drain_remaining) -> None:
        await drain_remaining()
        if self.shutdown_requested:
            await self.flatten_strategies(drain_remaining)
        await self.exit_sweep(drain_remaining)

    async def flatten_strategies(self, drain_remaining) -> None:
        deadline = time.monotonic() + self.config.flatten_timeout_s
        while time.monotonic() < deadline:
            busy = [s for s in self.active_strategies() if not s.is_flat_idle()]
            if not busy:
                return
            for strategy in busy:
                await self.request_flatten(strategy, self.clock.timestamp(), "shutdown")
            await asyncio.sleep(POLL_S)
            await drain_remaining()

    async def read_open_orders(self) -> list[dict] | None:
        try:
            return list(await self.gateway.get_open_orders())
        except Exception as exc:
            logger.critical("open orders unreadable at shutdown; every configured instrument is blocked",
                            extra={"event": "open_orders_unreadable", "reason": repr(exc)})
            return None

    async def exit_sweep(self, drain_remaining) -> None:
        configured = self.configured_instruments()
        orders = await self.read_open_orders()
        blocked_all = orders is None
        if not blocked_all:
            orders = await self.cancel_working(configured, orders, drain_remaining)
        working = set() if blocked_all else {o["instrument"] for o in orders if o["instrument"] in configured}
        try:
            positions = [p for p in await self.gateway.get_positions() if p["instrument"] in configured and p["size"]]
        except Exception as exc:
            logger.critical("positions unreadable at shutdown", extra={"event": "positions_unreadable",
                                                                       "reason": repr(exc)})
            self.not_flat_at_shutdown = True
            return
        for position in positions:
            if blocked_all or position["instrument"] in working:
                continue
            await self.force_flatten(position)
        await drain_remaining()
        if not await self.is_flat() or blocked_all or working:
            self.not_flat_at_shutdown = True
            logger.critical("book is not flat at shutdown", extra={
                "event": "not_flat_at_shutdown", "blocked_all": blocked_all,
                "working": sorted(i.symbol for i in working)})

    async def cancel_working(self, configured: set[Instrument], orders: list[dict], drain_remaining) -> list[dict]:
        mine = [o for o in orders if o["instrument"] in configured]
        for order in mine:
            await self.gateway.cancel_order(order["order_id"])
        deadline = time.monotonic() + self.config.flatten_timeout_s
        while mine and time.monotonic() < deadline:
            await drain_remaining()
            current = await self.read_open_orders()
            if current is None:
                return orders
            mine = [o for o in current if o["instrument"] in configured]
            if mine:
                await asyncio.sleep(POLL_S)
        return [o for o in orders if o["instrument"] not in configured] + mine

    async def force_flatten(self, position: dict) -> None:
        instrument = position["instrument"]
        side = "SELL" if position["size"] > 0 else "BUY"
        logger.warning("force-flattening at shutdown", extra={
            "event": "force_flatten", "instrument": instrument.symbol, "qty": abs(position["size"])})
        await self.gateway.place_order({
            "order_id": self.gateway.next_order_id(), "account_id": position["account_id"], "instrument": instrument,
            "side": side, "qty": abs(position["size"]), "order_type": "MKT", "stop_price": None,
            "limit_price": None, "oca_group": None, "oca_type": None, "tif": "DAY", "outside_rth": True,
            "slot": "shutdown"})
