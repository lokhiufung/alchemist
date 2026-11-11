from __future__ import annotations

import asyncio
import enum
import logging
import random
from datetime import timedelta
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from alchemist.config import EventLoopConfig
from alchemist.constants.event_loop import DAYS_PER_MONTH
from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument
from alchemist.requests import TerminalRequestError

logger = logging.getLogger("event_loop")


class TerminalSeedError(Exception):
    pass


class SeedState(enum.Enum):
    PENDING = "pending"
    READY = "ready"
    QUARANTINED_RECOVERABLE = "quarantined_recoverable"
    QUARANTINED_TERMINAL = "quarantined_terminal"


def no_recorded_bars(instrument: Instrument, session: str, at_ts: float) -> list:
    return []


class Backoff:
    def __init__(self, base_s: float, max_s: float, jitter: float, rng: Callable[[], float] = random.random) -> None:
        self.base_s, self.max_s, self.jitter, self.rng = base_s, max_s, jitter, rng
        self.attempts = 0

    def next_delay(self) -> float:
        raw = min(self.max_s, self.base_s * 2 ** self.attempts)
        self.attempts += 1
        return min(self.max_s, raw * (1 + self.jitter * self.rng()))


class SharedFetch:
    def __init__(self, ttl_s: float, now: Callable[[], float]) -> None:
        self.ttl_s = ttl_s
        self.now = now
        self.cache: dict[tuple, tuple[float, Any]] = {}
        self.inflight: dict[tuple, asyncio.Task] = {}

    async def get(self, key: tuple, fetch: Callable[[], Awaitable[Any]]) -> Any:
        cached = self.cache.get(key)
        if cached is not None and self.now() - cached[0] < self.ttl_s:
            return cached[1]
        task = self.inflight.get(key)
        if task is None:
            task = asyncio.ensure_future(self.run(key, fetch))
            self.inflight[key] = task
        return await asyncio.shield(task)

    async def run(self, key: tuple, fetch: Callable[[], Awaitable[Any]]) -> Any:
        try:
            result = await fetch()
            if result:
                self.cache[key] = (self.now(), result)
            return result
        finally:
            self.inflight.pop(key, None)


class Seeder:
    def __init__(self, requests, watch_time, config: EventLoopConfig,
                 recorded_bars: Callable[[Instrument, str, float], list] = no_recorded_bars,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 rng: Callable[[], float] = random.random) -> None:
        self.requests = requests
        self.watch_time = watch_time
        self.config = config
        self.recorded_bars = recorded_bars
        self.sleep = sleep
        self.rng = rng
        self.shared = SharedFetch(config.seed_fetch_ttl_s, watch_time.now)
        self.states: dict[str, SeedState] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self.retry_tasks: dict[str, asyncio.Task] = {}

    def state_of(self, name: str) -> SeedState:
        return self.states.get(name, SeedState.PENDING)

    def set_state(self, name: str, state: SeedState) -> None:
        if self.states.get(name) is SeedState.QUARANTINED_TERMINAL:
            return
        self.states[name] = state

    def history_window(self, strategy, months: int):
        profile = exchange_profile(strategy.signal_instrument)
        end = self.watch_time.moment().astimezone(ZoneInfo(profile.tz)).date()
        days = min(self.config.warmup_max_calendar_days,
                   max(months * DAYS_PER_MONTH, self.config.warmup_min_trading_days * 2))
        return end - timedelta(days=days), end

    async def attempt(self, strategy) -> SeedState:
        name = strategy.name
        lock = self.locks.setdefault(name, asyncio.Lock())
        async with lock:
            if self.state_of(name) in (SeedState.READY, SeedState.QUARANTINED_TERMINAL):
                return self.state_of(name)
            try:
                await self.seed(strategy)
            except TerminalSeedError as exc:
                logger.error("seeding failed for good", extra={
                    "event": "seed_failed_terminal", "strategy": name, "reason": str(exc)})
                self.set_state(name, SeedState.QUARANTINED_TERMINAL)
            except Exception as exc:
                logger.warning("seeding failed, will retry", extra={
                    "event": "seed_failed_recoverable", "strategy": name, "reason": repr(exc)})
                self.set_state(name, SeedState.QUARANTINED_RECOVERABLE)
            return self.state_of(name)

    async def seed(self, strategy) -> None:
        name, instrument, session = strategy.name, strategy.signal_instrument, strategy.session
        needs = strategy.warmup_requirements()
        daily: list = []
        if needs.daily_months > 0 or needs.daily:
            start, end = self.history_window(strategy, needs.daily_months)
            try:
                daily = await self.shared.get(("daily", instrument, session, start, end),
                                              lambda: self.requests.request("get_daily_history",
                                                                            instrument=instrument, start=start,
                                                                            end=end, session=session))
            except TerminalRequestError as exc:
                raise TerminalSeedError(f"daily history for {instrument.symbol} failed: {exc!r}") from exc
            if not daily:
                raise RuntimeError(f"no daily history for {instrument.symbol}")
        try:
            seed_bars = await self.shared.get(("seed", instrument, session),
                                              lambda: self.requests.request("get_intraday_seed_bars",
                                                                            instrument=instrument, session=session))
        except TerminalRequestError as exc:
            raise TerminalSeedError(f"intraday seed for {instrument.symbol} failed: {exc!r}") from exc
        if needs.intraday and not seed_bars:
            raise RuntimeError(f"no prior-session bars for {instrument.symbol}")
        strategy.warmup({"daily": daily, "intraday": seed_bars})
        await strategy.rebuild_intraday_state(seed_bars)
        today = self.recorded_bars(instrument, session, self.watch_time.now())
        if today:
            await strategy.rebuild_intraday_state(today)
        if self.state_of(name) is SeedState.QUARANTINED_TERMINAL:
            return
        strategy.on_seed_state(True)
        self.set_state(name, SeedState.READY)

    async def retry(self, strategy) -> None:
        backoff = Backoff(self.config.seed_backoff_base_s, self.config.seed_backoff_max_s,
                          self.config.seed_backoff_jitter, self.rng)
        while self.state_of(strategy.name) in (SeedState.PENDING, SeedState.QUARANTINED_RECOVERABLE):
            await self.sleep(backoff.next_delay())
            await self.attempt(strategy)

    def start_retry(self, strategy) -> asyncio.Task | None:
        if self.state_of(strategy.name) not in (SeedState.PENDING, SeedState.QUARANTINED_RECOVERABLE):
            return None
        existing = self.retry_tasks.get(strategy.name)
        if existing is not None and not existing.done():
            return existing
        task = asyncio.get_running_loop().create_task(self.retry(strategy))
        self.retry_tasks[strategy.name] = task
        return task

    async def seed_all(self, strategies: list) -> None:
        await asyncio.gather(*(self.attempt(s) for s in strategies))
        for strategy in strategies:
            self.start_retry(strategy)

    def cancel_retries(self) -> None:
        for task in self.retry_tasks.values():
            task.cancel()
