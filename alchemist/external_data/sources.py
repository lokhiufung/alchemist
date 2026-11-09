from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Callable


class AuxSource(ABC):
    """Aux Data behaviour 1: one outside source of auxiliary daily series."""

    @abstractmethod
    async def fetch(self, name: str, start: date, end: date) -> Any: ...


AUX_SOURCES: dict[str, type[AuxSource]] = {}


def register_aux_source(name: str) -> Callable[[type[AuxSource]], type[AuxSource]]:
    def bind(cls: type[AuxSource]) -> type[AuxSource]:
        if name in AUX_SOURCES:
            raise ValueError(f"aux source {name!r} registered twice")
        AUX_SOURCES[name] = cls
        return cls
    return bind


@register_aux_source("file")
class FileAuxSource(AuxSource):
    """Replay: reads auxiliary series from files."""

    def __init__(self, root: str) -> None: ...
    async def fetch(self, name: str, start: date, end: date) -> Any: ...


@register_aux_source("confirmed_backup")
class ConfirmedBackupAuxSource(AuxSource):
    """A primary source whose missing values are filled from a backup only when a second source confirms."""

    def __init__(self, primary: AuxSource, backup: AuxSource, confirm: AuxSource) -> None: ...
    async def fetch(self, name: str, start: date, end: date) -> Any: ...
