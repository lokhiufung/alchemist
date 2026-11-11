from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Any

__all__ = ["AUX_SOURCES", "AuxSource", "ConfirmedBackupAuxSource", "FileAuxSource", "build_aux_source"]


class AuxSource(ABC):
    @abstractmethod
    async def fetch(self, name: str, start: date, end: date) -> Any: ...


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


AUX_SOURCES: dict[str, type[AuxSource]] = {"file": FileAuxSource, "confirmed_backup": ConfirmedBackupAuxSource}


def build_aux_source(spec: dict) -> AuxSource:
    kind = spec["source"]
    if kind not in AUX_SOURCES:
        raise KeyError(f"no aux source named {kind!r}; known: {sorted(AUX_SOURCES)}")
    options = {key: build_aux_source(value) if isinstance(value, dict) and "source" in value else value
               for key, value in spec.items() if key != "source"}
    return AUX_SOURCES[kind](**options)
