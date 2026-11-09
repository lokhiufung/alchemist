from __future__ import annotations

from datetime import date

from alchemist.external_data.sources import AuxSource
from alchemist.clock import Clock
from alchemist.constants.instruments import Instrument

AUX_STEP_INTERVAL_S = 60.0
AUX_RECHECK_INTERVAL_S = 3600.0


class AuxArmGroup:
    """Aux Data behaviours 2-5: arming state for one arm session."""

    arm_session: str
    columns: tuple[tuple[Instrument, str], ...]
    ready: bool
    target: date | None

    def __init__(self, arm_session: str, members: list) -> None: ...
    def covers(self, strategy) -> bool: ...
    async def step(self, aux: AuxProvider) -> None: ...


class AuxProvider:
    """The Aux Data component: sources, arming, aux columns on bars, frames for a rebuild."""

    def __init__(self, sources: dict[str, AuxSource], book, clock: Clock,
                 replay_date: date | None = None) -> None: ...

    async def start(self) -> None:
        """Replay: resolve or raise (behaviour 6). Live: start the resident task."""

    async def stop(self) -> None: ...

    def annotate(self, instrument: Instrument, bar: dict) -> None:
        """Behaviour 7."""

    def frames_for(self, instrument: Instrument) -> dict:
        """Behaviour 8."""
        ...
