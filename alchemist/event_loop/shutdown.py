from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timedelta
from datetime import time as time_of_day
from zoneinfo import ZoneInfo

from alchemist.config import EventLoopConfig
from alchemist.constants.event_loop import POLL_S
from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument

logger = logging.getLogger("shutdown")


def run_exit_after(at: datetime, config: EventLoopConfig) -> datetime:
    zone = ZoneInfo(config.run_tz)
    local = at.astimezone(zone)
    exit_at = datetime.combine(local.date(), time_of_day.fromisoformat(config.run_end), zone)
    return exit_at if exit_at > local else exit_at + timedelta(days=1)


class ShutdownCoordinator:
    def __init__(self, book, requests, watch_time, config: EventLoopConfig | None = None) -> None:
        self.book = book
        self.requests = requests
        self.watch_time = watch_time
        self.config = config or EventLoopConfig()
        self.shutdown_requested = False
        self.not_flat_at_shutdown = False
        self.first_request: dict[str, float] = {}
        self.last_request: dict[str, float] = {}
        self.window_end: dict[str, float] = {}
        self.settled_until: dict[str, float] = {}
        self.exit_at: datetime | None = None
        self.cutoffs: dict[str, tuple[float, float]] = {}

    @property
    def run_exit(self) -> datetime | None:
        if self.exit_at is None and self.watch_time.now() is not None:
            self.exit_at = run_exit_after(self.watch_time.moment(), self.config)
        return self.exit_at

    @property
    def run_date(self) -> date:
        return self.run_exit.astimezone(ZoneInfo(self.config.run_tz)).date()

    def request_shutdown(self, reason: str) -> None:
        logger.warning("shutdown requested", extra={"event": "shutdown_requested", "reason": reason})
        self.shutdown_requested = True

    def active_strategies(self) -> list:
        return list(self.book.strategies)

    def configured_instruments(self) -> set[Instrument]:
        return {s.execution_instrument for s in self.book.strategies}

    def flattening_names(self) -> set[str]:
        return set(self.first_request)

    def cutoff_for(self, strategy) -> tuple[float, float]:
        known = self.cutoffs.get(strategy.name)
        if known is None:
            known = self.cutoffs[strategy.name] = self.run_cutoff(strategy)
        return known

    def run_cutoff(self, strategy) -> tuple[float, float]:
        profile = exchange_profile(strategy.execution_instrument)
        if not profile.is_trading_day(self.run_date):
            return float("inf"), float("inf")
        _, end = profile.session_window(self.book.sessions_by_strategy[strategy.name], self.run_date)
        return end.timestamp() - self.config.cutoff_offset_s, end.timestamp()

    async def process_due(self) -> None:
        now = self.watch_time.now()
        if now is None:
            return
        if not self.shutdown_requested and now >= self.run_exit.timestamp():
            self.request_shutdown("run_end")
        for strategy in self.active_strategies():
            name = strategy.name
            if name in self.first_request:
                continue
            cutoff, end_ts = self.cutoff_for(strategy)
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
        now = self.watch_time.now()
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
            positions = await self.requests.request("get_positions")
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
                await self.request_flatten(strategy, self.watch_time.now(), "shutdown")
            await asyncio.sleep(POLL_S)
            await drain_remaining()

    async def read_open_orders(self) -> list[dict] | None:
        try:
            return list(await self.requests.request("get_open_orders"))
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
            positions = [p for p in await self.requests.request("get_positions")
                         if p["instrument"] in configured and p["size"]]
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
            self.requests.command("cancel_order", order_id=order["order_id"])
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
        self.requests.command("place_order", order={
            "order_id": self.requests.next_order_id(), "account_id": position["account_id"], "instrument": instrument,
            "side": side, "qty": abs(position["size"]), "order_type": "MKT", "stop_price": None,
            "limit_price": None, "oca_group": None, "oca_type": None, "tif": "DAY", "outside_rth": True,
            "slot": "shutdown"})
