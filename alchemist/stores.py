from __future__ import annotations

import json
from pathlib import Path


def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True))


class CalcStateStore:
    def __init__(self, root: Path, bot_id: str) -> None:
        self.folder = Path(root) / bot_id

    def path(self, strategy_key: str) -> Path:
        return self.folder / f"calc_state_{strategy_key}.json"

    def save(self, strategy_key: str, trading_date: str, state: dict) -> None:
        write_json(self.path(strategy_key), {"trading_date": trading_date, "state": state})

    def load(self, strategy_key: str, trading_date: str) -> dict | None:
        stored = read_json(self.path(strategy_key))
        if not isinstance(stored, dict) or stored.get("trading_date") != trading_date:
            return None
        state = stored.get("state")
        return state if isinstance(state, dict) else None


class RoundTripStore:
    def __init__(self, root: Path, bot_id: str) -> None:
        self.folder = Path(root) / bot_id

    def path(self, strategy_key: str) -> Path:
        return self.folder / f"round_trips_{strategy_key}.json"

    def count(self, strategy_key: str, trading_date: str) -> int:
        stored = read_json(self.path(strategy_key))
        if not isinstance(stored, dict) or stored.get("trading_date") != trading_date:
            return 0
        return int(stored.get("count", 0))

    def record(self, strategy_key: str, trading_date: str) -> None:
        count = self.count(strategy_key, trading_date) + 1
        write_json(self.path(strategy_key), {"trading_date": trading_date, "count": count})
