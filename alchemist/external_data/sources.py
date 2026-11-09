from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Any

from alchemist.registry import get_aux_source, register_aux_source

__all__ = ["AuxSource", "ConfirmedBackupAuxSource", "FileAuxSource", "get_aux_source", "register_aux_source"]


class AuxSource(ABC):
    @abstractmethod
    async def fetch(self, name: str, start: date, end: date) -> Any: ...


@register_aux_source("file")
class FileAuxSource(AuxSource):
    def __init__(self, root: str) -> None:
        self.root = Path(root)

    async def fetch(self, name: str, start: date, end: date) -> list[dict]:
        path = self.root / f"{name}.csv"
        if not path.exists():
            return []
        with open(path, newline="") as handle:
            rows = [{"date": date.fromisoformat(r["date"]), "value": float(r["value"])} for r in csv.DictReader(handle)]
        return sorted((r for r in rows if start <= r["date"] <= end), key=lambda r: r["date"])


@register_aux_source("confirmed_backup")
class ConfirmedBackupAuxSource(AuxSource):
    def __init__(self, primary: AuxSource, backup: AuxSource, confirm: AuxSource) -> None:
        self.primary, self.backup, self.confirm = primary, backup, confirm

    async def fetch(self, name: str, start: date, end: date) -> list[dict]:
        rows = {r["date"]: r for r in await self.primary.fetch(name, start, end)}
        missing = [r for r in await self.backup.fetch(name, start, end) if r["date"] not in rows]
        if missing:
            confirmed = {r["date"]: r["value"] for r in await self.confirm.fetch(name, start, end)}
            for row in missing:
                if confirmed.get(row["date"]) == row["value"]:
                    rows[row["date"]] = row
        return [rows[d] for d in sorted(rows)]
