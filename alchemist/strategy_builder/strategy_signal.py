from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WarmupRequirements:
    daily_months: int = 0
    intraday: tuple[str, ...] = ()
    daily: tuple[str, ...] = ()
    aux: tuple[str, ...] = ()
    session_fixed: tuple[str, ...] = ()


class StrategySignal(ABC):
    """StrategySignal map: the abstract signal interface."""

    @classmethod
    @abstractmethod
    def from_config(cls, strategy_config: dict) -> StrategySignal:
        """Behaviours 1, 6: build cold from the slot config; refuse a bad signal."""

    @abstractmethod
    def warmup_requirements(self) -> WarmupRequirements: ...

    @abstractmethod
    def seed(self, history: dict) -> None: ...

    @abstractmethod
    def is_warm(self) -> bool: ...

    @abstractmethod
    def step(self, bars: deque, strategy_data: dict) -> None:
        """Behaviour 2 (and 3: drives the rearm gate every bar)."""

    @abstractmethod
    def evaluate_entry(self) -> bool: ...

    @abstractmethod
    def evaluate_exit_conditions(self) -> dict[str, bool]:
        """Behaviour 3: every named exit condition, in the order the exits were added."""

    def evaluate_exit(self) -> bool:
        return any(self.evaluate_exit_conditions().values())

    @property
    @abstractmethod
    def rearm_required(self) -> bool: ...

    @abstractmethod
    def exit_condition_level(self, name: str) -> float | None: ...

    @abstractmethod
    def level(self, name: str) -> float | None: ...

    @abstractmethod
    def has_level(self, name: str) -> bool: ...

    @abstractmethod
    def entry_conditions(self) -> dict[str, bool | None]:
        """Behaviour 3, decision 8."""

    @abstractmethod
    def exit_condition_names(self) -> list[str]: ...

    @abstractmethod
    def condition_names(self) -> set[str]: ...

    @abstractmethod
    def values(self) -> dict[str, Any]:
        """Behaviour 3, decision 8: a copy of every node's value on the last bar."""

    @abstractmethod
    def freeze_exit_inputs(self, bar: dict) -> None: ...

    @abstractmethod
    def release_exit_inputs(self) -> None: ...

    def snapshot_session_state(self) -> dict:
        return {}

    def restore_session_state(self, state: dict) -> None: ...
