from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Calculation(ABC):
    """StrategySignal map: one registered calculation node."""

    def setup(self, **params) -> None:
        """Behaviour 1: take params; start cold."""

    def step(self, *inputs) -> Any:
        """Behaviour 2: value for this bar; None = unavailable; raise = failed."""
        return self.value()

    def warmup(self, data) -> None:
        """Behaviour 4: seed from history."""

    def value(self) -> Any:
        """Behaviour 4: the seeded value, or None if not seeded."""
        return None

    def bind_aux(self, names: frozenset[str]) -> None:
        """Behaviour 1: for calculations that read auxiliary history."""


class SessionFixedCalculation(Calculation):
    @abstractmethod
    def snapshot_state(self) -> dict | None: ...

    @abstractmethod
    def restore_state(self, state: dict) -> None: ...
