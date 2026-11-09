from __future__ import annotations

from enum import Enum


class Mode(str, Enum):
    REPLAY = "replay"
    PAPER = "paper"
    LIVE = "live"

    @property
    def is_replay(self) -> bool:
        return self is Mode.REPLAY


RunMode = Mode
MODES = [mode.value for mode in Mode]
