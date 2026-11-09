from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timezone


class Clock(ABC):
    @abstractmethod
    def now(self) -> datetime: ...

    @abstractmethod
    async def wait(self, timeout: float) -> None: ...

    def timestamp(self) -> float:
        return self.now().timestamp()


class WallClock(Clock):
    def __init__(self, wakeup: asyncio.Event | None = None) -> None:
        self.wakeup = wakeup

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def wait(self, timeout: float) -> None:
        if self.wakeup is None:
            await asyncio.sleep(timeout)
            return
        try:
            await asyncio.wait_for(self.wakeup.wait(), timeout)
        except asyncio.TimeoutError:
            pass


class ReplayClock(Clock):
    def __init__(self, start: datetime) -> None:
        self.current = start

    def now(self) -> datetime:
        return self.current

    async def wait(self, timeout: float) -> None:
        await asyncio.sleep(0)

    def advance(self, to: datetime) -> None:
        if to < self.current:
            raise ValueError(f"replay clock cannot go back from {self.current} to {to}")
        self.current = to
