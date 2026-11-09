from __future__ import annotations

import enum
from pathlib import Path
from typing import Any, Awaitable, Callable

MIN_WARMUP_MONTHS = 6
MAX_WARMUP_MONTHS = 12
SEED_FETCH_TTL_S = 60.0
RETRY_BASE_S = 60.0
RETRY_MAX_S = 900.0
RETRY_JITTER = 0.1


class SeedState(enum.Enum):
    """Event Loop behaviour 18."""
    PENDING = "pending"
    READY = "ready"
    QUARANTINED_RECOVERABLE = "quarantined_recoverable"
    QUARANTINED_TERMINAL = "quarantined_terminal"


class Backoff:
    """Jittered, capped delay; never shorter than base."""

    def __init__(self, base_s: float = RETRY_BASE_S, max_s: float = RETRY_MAX_S,
                 jitter: float = RETRY_JITTER) -> None: ...

    def next_delay(self) -> float: ...


class SharedFetch:
    """One in-flight fetch per key, kept for a time-to-live; only non-empty results are kept."""

    def __init__(self, ttl_s: float = SEED_FETCH_TTL_S) -> None: ...

    async def get(self, key: tuple, fetch: Callable[[], Awaitable[Any]]) -> Any: ...


class Seeder:
    """Event Loop behaviours 8, 18: one attempt, one retry, one shared fetch."""

    def __init__(self, gateway, book, clock, bars_dir: Path | None) -> None: ...

    async def attempt(self, strategy) -> SeedState:
        """Fetch daily and prior-session history, hand it to the strategy in order, replay today's bars."""
        ...

    async def retry(self, strategy) -> None:
        """attempt() under Backoff until READY or terminally quarantined."""

    async def seed_all(self, strategies: list) -> None:
        """Cold start: one attempt per slot; one retry task per slot that is not READY."""


def load_recorded_bars(bars_dir: Path, instrument, session: str, clock) -> list:
    """Event Loop behaviour 8: today's recorded bars for a mid-session start."""
    ...
