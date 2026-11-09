from __future__ import annotations

import asyncio

FLATTEN_WAIT_MAX_S = 10.0


class ShutdownCoordinator:
    """Event Loop behaviours 7, 9: per-session flatten, shutdown requests, final flat check."""

    shutdown_requested: bool
    not_flat_at_shutdown: bool

    def __init__(self, book, gateway, clock) -> None: ...
    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None: ...
    def active_strategies(self) -> list: ...
    async def process_due(self) -> None:
        """Flatten each session's strategies at its cutoff on clock.now()."""

    def ready_to_exit(self) -> bool: ...
    async def finalize(self, drain_remaining) -> None:
        """Cancel working orders, force-flatten leftovers, verify flat."""
