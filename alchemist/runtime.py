from __future__ import annotations

from typing import Protocol, runtime_checkable

from alchemist.config import Configuration
from alchemist.constants.mode import Mode


@runtime_checkable
class Service(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


def log_deploy_fingerprint(config: Configuration) -> None:
    """System map decision 35: the config file's sha256 and the run's identity, at startup."""


class Runtime:
    """System map decision 47: builds every component once, starts in order, stops in reverse."""

    def __init__(self, config: Configuration, mode: Mode, services: list[Service], event_loop) -> None: ...

    @classmethod
    def build(cls, config: Configuration, mode: Mode) -> Runtime:
        """Pick the gateway by registry name, the clock, the stand-ins; load strategies; wire the book."""
        ...

    async def __aenter__(self) -> Runtime:
        """Start services in order; on failure stop those already started."""
        ...

    async def __aexit__(self, *exc) -> None:
        """Stop services in reverse."""

    async def run(self) -> None:
        """Seed, then run the Event Loop until exit; raise SystemExit when not flat at shutdown."""
