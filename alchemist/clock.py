from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime


class Clock(ABC):
    """System map decision 46."""

    @abstractmethod
    def now(self) -> datetime: ...

    @abstractmethod
    async def wait(self, timeout: float) -> None: ...


class WallClock(Clock):
    def __init__(self, drain_wakeup=None) -> None: ...
    def now(self) -> datetime: ...
    async def wait(self, timeout: float) -> None:
        """Return on the drain wake-up or after timeout (Event Loop behaviour 11)."""
        ...


class ReplayClock(Clock):
    def __init__(self) -> None: ...
    def now(self) -> datetime: ...
    async def wait(self, timeout: float) -> None: ...
    def advance(self, bar_close: datetime) -> None:
        """Moves forward on each bar close."""
        ...
