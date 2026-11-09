from __future__ import annotations

from enum import Enum
from typing import Callable

EXECUTION_CLASSES: dict[str, type[StrategyExecution]] = {}


class Phase(str, Enum):
    """Glossary: phase. Outside code never reads it."""
    WAITING_FOR_WORKING_HOURS = "WAITING_FOR_WORKING_HOURS"
    WARMUP = "WARMUP"
    ENTRY_LOOP_ACTIVITY = "ENTRY_LOOP_ACTIVITY"
    PROCESSING_ENTRY_ORDER = "PROCESSING_ENTRY_ORDER"
    PROCESSING_PROTECTION_ORDERS = "PROCESSING_PROTECTION_ORDERS"
    IN_POSITION_ACTIVITY = "IN_POSITION_ACTIVITY"
    IN_POSITION_PROTECTED = "IN_POSITION_PROTECTED"
    PROCESSING_EXIT_ORDER = "PROCESSING_EXIT_ORDER"
    SESSION_END_FLATTENING = "SESSION_END_FLATTENING"
    DONE = "DONE"
    ERROR = "ERROR"


class ExecutionConfigError(ValueError): ...


def make_execution(execution_config: dict | None, signal, om, **kw) -> StrategyExecution:
    """Decision 21: the one plug point."""
    ...


class StrategyExecution:
    """StrategyExecution map: the slot's execution state machine."""

    TRANSITIONS: dict[Phase, set[Phase]] = {}

    warmup_ready: bool
    aux_ready: bool
    quarantine_reason: str | None

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        EXECUTION_CLASSES[cls.__name__] = cls

    @classmethod
    def options_from_config(cls, options: dict) -> dict: ...

    def on_bar(self, bar: dict, step_signal: Callable[[], None]) -> None:
        """Behaviours 1, 3-5, 8, 10, 12."""

    def on_order_update(self, update: dict) -> None:
        """Behaviours 1, 5-7, 11."""

    def is_processing_order(self) -> bool: ...
    def is_quarantined(self) -> bool: ...
    def quarantine(self, reason: str) -> None: ...
    def release_quarantine(self) -> None: ...
    def request_flatten(self, position: dict, reason: str) -> None: ...
    def flatten_wanted(self) -> bool: ...
    def consume_trade_closed(self) -> bool: ...
    def set_warmup_ready(self) -> None: ...
    def set_aux_ready(self) -> None: ...

    @property
    def ready_to_arm(self) -> bool:
        return self.warmup_ready and self.aux_ready


EXECUTION_CLASSES[StrategyExecution.__name__] = StrategyExecution


class ProtectedStrategyExecution(StrategyExecution):
    """Behaviours 3-7; adds PROCESSING_PROTECTION_ORDERS, IN_POSITION_PROTECTED."""
    resting_exits: dict[str, str] = {}


class StopLossProtectedStrategyExecution(ProtectedStrategyExecution):
    """Decision 22: one STP at level(name) +/- offset_points; masks nothing."""
    level_name: str
    offset_points: float
