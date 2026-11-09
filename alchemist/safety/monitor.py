from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from alchemist.config import SafetyConfig

HEARTBEAT_INTERVAL_S = 5.0
STALE_TICK_S = 45.0
RESUBSCRIBE_MAX_ATTEMPTS = 4
RESUBSCRIBE_WITHOUT_TICK_CAP = 3
REJECT_SYNTHESIS_MIN_AGE_S = 120.0


class SafetyMonitor:
    """Safety map: heartbeat, reconnect, restore subscriptions."""

    def __init__(self, gateway, book, event_buffer: deque, config: SafetyConfig) -> None: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def run(self) -> None:
        """Behaviours 1-3, 9: heartbeat loop."""

    async def reconnect_forever(self) -> None:
        """Behaviour 4: min(cap, base * 2**(n-1)), never gives up."""

    async def restore_subscriptions(self) -> None:
        """Behaviour 5."""

    def feed_expected(self, now: float) -> bool:
        """Behaviour 2: any configured exchange's data session open."""
        ...


class FreezeDetector:
    """Watches the event loop from a thread; dumps stacks on a stall."""

    def __init__(self, stackdump_path: Path) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def check_in(self) -> None: ...
