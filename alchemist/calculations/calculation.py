from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Calculation(ABC):
    bar_fields: tuple[str, ...] = ()

    def setup(self, **params) -> None:
        return None

    def step(self, *inputs) -> Any:
        return self.value()

    def warmup(self, data) -> None:
        return None

    def value(self) -> Any:
        return None

    def bind_aux(self, names: frozenset[str]) -> None:
        return None

    def daily_months(self) -> int:
        return 0


class SessionFixedCalculation(Calculation):
    @abstractmethod
    def snapshot_state(self) -> dict | None: ...

    @abstractmethod
    def restore_state(self, state: dict) -> None: ...
