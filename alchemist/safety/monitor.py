from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time
from typing import Awaitable, Callable

from alchemist.clock import Clock
from alchemist.config import SafetyConfig
from alchemist.messages import make_event
from alchemist.safety.fsm_repair import repair_stuck_strategies
from alchemist.safety.freeze import FreezeDetector

logger = logging.getLogger("safety")

__all__ = ["SafetyConfig", "SafetyMonitor", "backoff_delay"]


def backoff_delay(attempt: int, base_s: float, cap_s: float) -> float:
    return min(cap_s, base_s * 2 ** (attempt - 1))


def parse_clock_time(text: str) -> time:
    hour, minute = text.split(":")
    return time(int(hour), int(minute))


async def no_restart() -> None:
    logger.critical("broker gateway restart requested, but no restart action is configured",
                    extra={"event": "gateway_restart_unconfigured"})


class SafetyMonitor:
    def __init__(self, gateway, book, event_buffer, exchange_profiles: dict, clock: Clock, config: SafetyConfig,
                 restart_gateway: Callable[[], Awaitable[None]] = no_restart,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 freeze: FreezeDetector | None = None) -> None:
        self.gateway = gateway
        self.book = book
        self.event_buffer = event_buffer
        self.exchange_profiles = exchange_profiles
        self.clock = clock
        self.config = config
        self.restart_gateway = restart_gateway
        self.sleep = sleep
        self.failures = 0
        self.blind_resubscribes = 0
        self.last_seen_tick: float | None = None
        self.started_at = clock.timestamp()
        self.task: asyncio.Task | None = None
        self.freeze = freeze or FreezeDetector(config)
        self.last_check: float | None = None
        self.last_poll: float | None = None

    async def start(self) -> None:
        self.started_at = self.clock.timestamp()
        self.freeze.start()
        self.task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        self.freeze.stop()

    def check_in(self) -> None:
        self.freeze.check_in()

    async def repair(self, strategies: list) -> int:
        return await repair_stuck_strategies(strategies, self.gateway, self.event_buffer, self.clock.timestamp(),
                                             self.config)

    async def stuck_check(self) -> None:
        picked = [s for s in list(self.book.strategies) if s.is_processing_order()]
        if picked:
            await self.repair(picked)

    async def poll_positions(self) -> None:
        try:
            positions = await self.gateway.get_positions()
        except Exception as exc:
            logger.warning("positions poll failed", extra={"event": "positions_poll_failed", "reason": repr(exc)})
            return
        for row in positions:
            self.event_buffer.push(make_event("position_update", dict(row), self.clock.timestamp()))


    async def run(self) -> None:
        while True:
            try:
                await self.step()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("safety step failed", extra={"event": "safety_step_failed"})
            await self.sleep(self.config.heartbeat_interval_s)

    async def step(self) -> None:
        alive = await self.gateway.heartbeat(self.config.heartbeat_timeout_s)
        if not alive:
            self.failures += 1
            await self.reconnect_forever()
            return
        self.failures = 0
        now = self.clock.timestamp()
        self.note_ticks()
        if self.gateway.consume_resubscribe_request():
            await self.on_connectivity_restored()
        elif self.feed_is_stale(now):
            await self.climb_ladder()
        if self.config.positions_poll_s:
            if self.last_poll is None:
                self.last_poll = now
            elif now - self.last_poll >= self.config.positions_poll_s:
                self.last_poll = now
                await self.poll_positions()
        if self.last_check is None:
            self.last_check = now
        elif now - self.last_check >= self.config.check_interval_s:
            self.last_check = now
            await self.stuck_check()

    def note_ticks(self) -> None:
        tick = self.gateway.last_tick_ts
        if tick is not None and tick != self.last_seen_tick:
            self.last_seen_tick = tick
            self.blind_resubscribes = 0

    def fed_profiles(self) -> list:
        return [self.exchange_profiles[i.exchange] for i in self.book.fed_instruments]

    def open_data_profiles(self) -> list:
        moment = self.clock.now()
        return [p for p in self.fed_profiles() if p.is_open(p.data_session, moment)]

    def feed_expected(self, now: float) -> bool:
        return bool(self.open_data_profiles())

    def stale_threshold(self) -> float:
        thresholds = [self.config.stale_overrides.get(p.name, self.config.stale_tick_s)
                      for p in self.open_data_profiles()]
        return min(thresholds, default=self.config.stale_tick_s)

    def feed_is_stale(self, now: float) -> bool:
        if not self.feed_expected(now):
            return False
        last = self.gateway.last_tick_ts
        return now - (self.started_at if last is None else last) > self.stale_threshold()

    async def on_connectivity_restored(self) -> None:
        if self.blind_resubscribes >= self.config.resubscribe_without_tick_cap:
            await self.reconnect_forever(restore_first=False)
            return
        self.blind_resubscribes += 1
        await self.resubscribe_feeds()

    async def resubscribe_feeds(self) -> None:
        for instrument in list(self.book.fed_instruments):
            await self.gateway.subscribe_tick(self.book.contracts[instrument])

    async def climb_ladder(self) -> None:
        attempts = min(self.config.resubscribe_max_attempts, self.config.resubscribe_without_tick_cap)
        baseline = self.gateway.last_tick_ts
        for attempt in range(attempts):
            await self.resubscribe_feeds()
            self.blind_resubscribes += 1
            await self.sleep(self.config.resubscribe_backoff_base_s * 2 ** attempt)
            if self.gateway.last_tick_ts != baseline:
                return
        await self.reconnect_forever()

    def in_maintenance_window(self) -> bool:
        now = self.clock.now().time()
        for start, end in self.config.maintenance_windows:
            begin, finish = parse_clock_time(start), parse_clock_time(end)
            inside = begin <= now < finish if begin <= finish else now >= begin or now < finish
            if inside:
                return True
        return False

    async def reconnect_forever(self, restore_first: bool = False) -> None:
        attempt = 0
        restarted = False
        while True:
            attempt += 1
            try:
                await self.gateway.reconnect()
                self.blind_resubscribes = 0
                break
            except Exception as exc:
                if attempt > 1:
                    logger.critical("reconnect failed again", extra={
                        "event": "reconnect_failed", "attempt": attempt, "reason": repr(exc)})
                else:
                    logger.error("reconnect failed", extra={
                        "event": "reconnect_failed", "attempt": attempt, "reason": repr(exc)})
                if not restarted and self.in_maintenance_window():
                    restarted = True
                    await self.restart_gateway()
                await self.sleep(backoff_delay(attempt, self.config.reconnect_base_s, self.config.reconnect_cap_s))
        await self.restore_subscriptions()
        await self.repair(list(self.book.strategies))

    async def restore_subscriptions(self) -> None:
        await self.resubscribe_feeds()
        await self.gateway.subscribe_order_update()
        await self.gateway.subscribe_account_update()
