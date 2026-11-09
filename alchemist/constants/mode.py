from __future__ import annotations

from enum import Enum


class Mode(str, Enum):
    """System map decision 46: read only by the runner."""
    REPLAY = "replay"
    PAPER = "paper"
    LIVE = "live"

    @property
    def is_replay(self) -> bool:
        return self is Mode.REPLAY


MODES = [mode.value for mode in Mode]