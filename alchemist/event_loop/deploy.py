from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class DeployStatus(str, Enum):
    PREPARING = "preparing"
    REGISTERED = "registered"
    NOOP = "noop"
    REJECTED = "rejected"
    FAILED = "failed"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"


@dataclass
class Candidate:
    """A strategy staged by a deploy request but not yet committed to the runtime book."""
    strategy_key: str
    strategy: object
    new_instruments: list = field(default_factory=list)
    retry_tasks: list = field(default_factory=list)


class DeployService:
    """Event Loop behaviour 16, System map decisions 35-36: add one strategy while running."""

    def __init__(self, socket_path: Path, config_path: Path, book, gateway, seeder) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def handle(self, strategy_key: str, config_sha256: str) -> DeployStatus: ...

    async def stage_strategy(self, strategy_key: str) -> Candidate:
        """Pre-flight, reserve and subscribe, prepare; nothing is committed yet."""
        ...

    async def cleanup(self, candidate: Candidate) -> None:
        """Undo only what the candidate created."""


async def send_deploy_request(socket_path: Path, strategy_key: str, config_sha256: str) -> int:
    """The deploy_strategy.py client; prints status lines, returns the exit code."""
    ...
