from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Callable


class WatchTime:
    def __init__(self, system_time: Callable[[], float] = time.time) -> None:
        self.system_time = system_time
        self.event_time: float | None = None
        self.arrived_at = 0.0
        self.first_event = asyncio.Event()

    def observe(self, event_time: float, arrived_at: float) -> None:
        if self.event_time is not None and event_time < self.now(arrived_at):
            return
        self.event_time = event_time
        self.arrived_at = arrived_at
        self.first_event.set()

    def now(self, system_now: float | None = None) -> float | None:
        if self.event_time is None:
            return None
        system_now = self.system_time() if system_now is None else system_now
        return self.event_time + (system_now - self.arrived_at)

    def moment(self) -> datetime:
        return datetime.fromtimestamp(self.now(), timezone.utc)

    async def ready(self) -> None:
        await self.first_event.wait()
