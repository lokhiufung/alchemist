from __future__ import annotations

from pathlib import Path


class CalcStateStore:
    """Strategy behaviours 6-7: session-fixed signal values per strategy key, kept across restarts."""

    def __init__(self, root: Path, bot_id: str) -> None: ...
    def save(self, strategy_key: str, trading_date: str, state: dict) -> None: ...
    def load(self, strategy_key: str, trading_date: str) -> dict | None:
        """None when missing, stale or corrupt."""
        ...


class RoundTripStore:
    """Round trips per strategy key and trading date, so the daily cap survives a restart."""

    def __init__(self, root: Path, bot_id: str) -> None: ...
    def record(self, strategy_key: str, trading_date: str) -> None: ...
    def count(self, strategy_key: str, trading_date: str) -> int: ...
