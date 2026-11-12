from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path


class DailyStore(ABC):
    @abstractmethod
    def load(self, strategy_key: str, trading_date: str) -> dict | None: ...

    @abstractmethod
    def save(self, strategy_key: str, trading_date: str, state: dict) -> None: ...


class FileDailyStore(DailyStore):
    def __init__(self, root: Path, bot_id: str, name: str) -> None:
        self.folder = Path(root) / bot_id
        self.name = name

    def path(self, strategy_key: str) -> Path:
        return self.folder / f"{self.name}_{strategy_key}.json"

    def load(self, strategy_key: str, trading_date: str) -> dict | None:
        try:
            stored = json.loads(self.path(strategy_key).read_text())
        except (OSError, ValueError):
            return None
        if not isinstance(stored, dict) or stored.get("trading_date") != trading_date:
            return None
        state = stored.get("state")
        return state if isinstance(state, dict) else None

    def save(self, strategy_key: str, trading_date: str, state: dict) -> None:
        path = self.path(strategy_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"trading_date": trading_date, "state": state}, sort_keys=True))


class InMemoryDailyStore(DailyStore):
    def __init__(self) -> None:
        self.records: dict[str, tuple[str, dict]] = {}

    def load(self, strategy_key: str, trading_date: str) -> dict | None:
        stored = self.records.get(strategy_key)
        return stored[1] if stored and stored[0] == trading_date else None

    def save(self, strategy_key: str, trading_date: str, state: dict) -> None:
        self.records[strategy_key] = (trading_date, state)


def round_trip_count(store: DailyStore, strategy_key: str, trading_date: str) -> int:
    return int((store.load(strategy_key, trading_date) or {}).get("count", 0))
