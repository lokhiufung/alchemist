from __future__ import annotations

import asyncio
import threading
import time
from collections import deque

from alchemist.config import EventLoopConfig
from alchemist.constants.event_loop import CONTROL_EVENTS


class EventBuffer:
    def __init__(self, config: EventLoopConfig | None = None) -> None:
        self.config = config or EventLoopConfig()
        self.control: deque[dict] = deque()
        self.data: deque[dict] = deque()
        self.wake_pending = False
        self.drain_wakeup = asyncio.Event()
        self.idle = asyncio.Event()
        self.idle.set()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.loop_thread: int | None = None
        self.wakeups = 0
        self.threadsafe_wakeups = 0

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.loop_thread = threading.get_ident()

    def lane_for(self, event: dict) -> deque:
        return self.control if event["event_type"] in CONTROL_EVENTS else self.data

    def push(self, event: dict, wake: bool = True) -> None:
        event.setdefault("pushed_at", time.time())
        self.idle.clear()
        self.lane_for(event).append(event)
        if wake:
            self.raise_wakeup()

    def push_front(self, event: dict) -> None:
        event.setdefault("pushed_at", time.time())
        self.idle.clear()
        self.control.appendleft(event)
        self.raise_wakeup()

    def raise_wakeup(self) -> None:
        if self.wake_pending:
            return
        self.wake_pending = True
        self.wakeups += 1
        if self.loop is not None and threading.get_ident() != self.loop_thread:
            self.threadsafe_wakeups += 1
            self.loop.call_soon_threadsafe(self.drain_wakeup.set)
        else:
            self.drain_wakeup.set()

    def begin_pass(self) -> None:
        self.drain_wakeup.clear()
        self.wake_pending = False

    def pop(self) -> dict | None:
        control = self.control
        if control:
            if control[0]["event_type"] != "end_of_data":
                return control.popleft()
            for index in range(1, len(control)):
                if control[index]["event_type"] != "end_of_data":
                    event = control[index]
                    del control[index]
                    return event
            if not self.data:
                return control.popleft()
        if self.data:
            return self.data.popleft()
        self.idle.set()
        return None

    async def wait_idle(self) -> None:
        await self.idle.wait()

    async def wait(self, clock) -> None:
        await clock.wait(self.config.drain_timeout_s)

    def control_backlog_exceeded(self) -> bool:
        return len(self.control) > self.config.backlog_alert

    def __len__(self) -> int:
        return len(self.control) + len(self.data)
