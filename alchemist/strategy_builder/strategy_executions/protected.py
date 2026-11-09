from __future__ import annotations

import logging

from alchemist.managers.order_manager import CLOSED, OrderState
from alchemist.strategy_builder.strategy_executions.strategy_execution import (ExecutionConfigError, Phase,
                                                                              StrategyExecution)

logger = logging.getLogger(__name__)

LEG_TYPES = ("STP", "LMT")
WORKING = {OrderState.SUBMITTED, OrderState.OPEN, OrderState.PARTIALLY_FILLED}
TRADE_PHASES = {Phase.PROCESSING_PROTECTION_ORDERS, Phase.IN_POSITION_PROTECTED, Phase.PROCESSING_EXIT_ORDER,
                Phase.IN_POSITION_ACTIVITY}


def protected_transitions() -> dict[Phase, set[Phase]]:
    table = {phase: set(targets) for phase, targets in StrategyExecution.TRANSITIONS.items()}
    table[Phase.IN_POSITION_ACTIVITY] |= {Phase.PROCESSING_PROTECTION_ORDERS}
    table[Phase.PROCESSING_PROTECTION_ORDERS] = {Phase.IN_POSITION_PROTECTED, Phase.IN_POSITION_ACTIVITY,
                                                 Phase.PROCESSING_EXIT_ORDER, Phase.DONE,
                                                 Phase.ENTRY_LOOP_ACTIVITY}
    table[Phase.IN_POSITION_PROTECTED] = {Phase.PROCESSING_EXIT_ORDER, Phase.IN_POSITION_ACTIVITY, Phase.DONE,
                                          Phase.ENTRY_LOOP_ACTIVITY}
    table[Phase.PROCESSING_EXIT_ORDER] |= {Phase.IN_POSITION_PROTECTED}
    return table


class ProtectedStrategyExecution(StrategyExecution):
    TRANSITIONS = protected_transitions()
    resting_exits: dict[str, str]

    @classmethod
    def options_from_config(cls, options: dict) -> dict:
        unknown = set(options) - {"resting_exits"}
        if unknown:
            raise ExecutionConfigError(f"{cls.__name__} does not accept options {sorted(unknown)}")
        exits = options.get("resting_exits")
        if not isinstance(exits, dict) or not exits:
            raise ExecutionConfigError(f"{cls.__name__} needs a non-empty resting_exits map")
        bad = {name: kind for name, kind in exits.items() if kind not in LEG_TYPES}
        if bad:
            raise ExecutionConfigError(f"resting_exits values must be STP or LMT, got {bad}")
        if len(set(exits.values())) != len(exits):
            raise ExecutionConfigError("resting_exits may hold at most one STP and one LMT")
        return {"resting_exits": dict(exits)}

    def __init__(self, *args, resting_exits: dict[str, str], **kwargs) -> None:
        super().__init__(*args, **kwargs)
        unknown = [name for name in resting_exits if name not in self.signal.exit_condition_names()]
        if unknown:
            raise ExecutionConfigError(
                f"resting exit {unknown[0]!r} is not an exit condition of the signal "
                f"(known: {self.signal.exit_condition_names()})")
        self.resting_exits = resting_exits
        self.reset_trade_state()

    def reset_trade_state(self) -> None:
        self.protection_attempted = False
        self.group: str | None = None
        self.expected_legs = 0

    def masked_conditions(self) -> set[str]:
        return set(self.resting_exits)

    def handlers(self):
        table = super().handlers()
        table[Phase.IN_POSITION_ACTIVITY] = self.in_position_activity
        table[Phase.PROCESSING_PROTECTION_ORDERS] = self.processing_protection
        table[Phase.IN_POSITION_PROTECTED] = self.in_position_protected
        return table

    def in_position_activity(self, bar: dict) -> None:
        if not self.protection_attempted and not self.exit_failed_pending and not self.flatten_needed:
            if self.attempt_protection(bar):
                return
        self.judge(bar, masked=set())

    def processing_protection(self, bar: dict) -> None:
        self.promote_if_protected()
        self.judge(bar, masked=set())

    def in_position_protected(self, bar: dict) -> None:
        self.judge(bar, masked=self.masked_conditions())

    def judge(self, bar: dict, masked: set[str]) -> None:
        if self.handle_failed_exit() or self.trade_qty <= 0:
            return
        answers = self.signal.evaluate_exit_conditions()
        true_names = [name for name, value in answers.items() if value]
        if not true_names:
            return
        covered = [name for name in true_names if name in masked]
        if covered:
            logger.info("[%s] exit skipped: %s covered by a resting leg (raw answers %s)", self.slot, covered, answers)
            return
        logger.info("[%s] exit decision, conditions=%s", self.slot, answers)
        self.send_exit(bar)

    def attempt_protection(self, bar: dict) -> bool:
        self.protection_attempted = True
        legs = self.build_legs(bar)
        if legs is None:
            logger.warning("[%s] trade runs unprotected", self.slot)
            return False
        self.expected_legs = len(legs)
        self.set_phase(Phase.PROCESSING_PROTECTION_ORDERS)
        self.spawn(self.place_protection(legs))
        return True

    async def place_protection(self, legs: list[dict]) -> None:
        self.group = await self.om.place_group(legs)

    def leg(self, kind: str, price: float) -> dict:
        return {"instrument": self.instrument, "side": self.exit_side, "qty": self.trade_qty, "order_type": kind,
                "stop_price": price if kind == "STP" else None, "limit_price": price if kind == "LMT" else None}

    def stop_crossed(self, bar: dict, price: float) -> bool:
        return bar["close"] <= price if self.direction == "long" else bar["close"] >= price

    def build_legs(self, bar: dict) -> list[dict] | None:
        legs = []
        for name, kind in self.resting_exits.items():
            level = self.signal.exit_condition_level(name)
            if level is None:
                logger.warning("[%s] no level for %s", self.slot, name)
                return None
            if kind == "STP" and self.stop_crossed(bar, level):
                logger.warning("[%s] close %s already crossed the stop %s", self.slot, bar["close"], level)
                return None
            legs.append(self.leg(kind, level))
        return legs

    def group_legs(self) -> list[dict]:
        if self.group is None:
            return []
        return [o for o in self.om.get_order_group(self.group) if o["order_type"] in LEG_TYPES]

    def all_legs_open(self) -> bool:
        legs = self.group_legs()
        return len(legs) == self.expected_legs and all(o["status"] == OrderState.OPEN.value for o in legs)

    def promote_if_protected(self) -> None:
        if self.state is Phase.PROCESSING_PROTECTION_ORDERS and self.all_legs_open():
            self.set_phase(Phase.IN_POSITION_PROTECTED)

    def legs_alive(self) -> bool:
        return any(OrderState(o["status"]) in WORKING for o in self.group_legs())

    def send_exit(self, bar: dict) -> None:
        if self.state is Phase.PROCESSING_PROTECTION_ORDERS and self.group is None:
            return
        super().send_exit(bar)

    async def place_exit(self, qty: int) -> None:
        group = self.group if self.legs_alive() else None
        await self.om.place_order(self.instrument, self.exit_side, qty, "MKT", oca_group=group)

    def is_leg(self, order: dict) -> bool:
        if order["order_type"] not in LEG_TYPES or order["oca_group"] is None:
            return False
        if self.group is None and self.state is Phase.PROCESSING_PROTECTION_ORDERS:
            self.group = order["oca_group"]
        return order["oca_group"] == self.group

    def is_exit_order(self, order: dict) -> bool:
        return order["side"] == self.exit_side and order["order_type"] == "MKT" and order["oca_group"] in (None, self.group)

    def on_phase_update(self, order: dict) -> None:
        if self.state is Phase.SESSION_END_FLATTENING:
            self.on_flatten_update(order)
        elif self.is_leg(order):
            self.on_leg_update(order)
        else:
            super().on_phase_update(order)

    def on_leg_update(self, order: dict) -> None:
        if self.state not in TRADE_PHASES:
            return
        status = OrderState(order["status"])
        if status in CLOSED:
            if order["filled"] > 0:
                self.trade_qty -= order["filled"]
                if self.trade_qty <= 0:
                    self.complete_round_trip()
            elif self.state in (Phase.PROCESSING_PROTECTION_ORDERS, Phase.IN_POSITION_PROTECTED):
                self.lose_protection(order)
        else:
            self.promote_if_protected()

    def lose_protection(self, order: dict) -> None:
        logger.critical("[%s] protective leg %s ended %s; falling back to bar-close exits", self.slot,
                        order["order_id"], order["status"])
        self.set_phase(Phase.IN_POSITION_ACTIVITY)

    def on_exit_update(self, order: dict) -> None:
        if not self.is_exit_order(order) or not self.is_closed(order):
            return
        if order["filled"] > 0:
            self.trade_qty -= order["filled"]
            if self.trade_qty <= 0:
                self.complete_round_trip()
            else:
                self.set_phase(Phase.IN_POSITION_ACTIVITY)
            return
        logger.warning("[%s] exit order %s ended %s in position", self.slot, order["order_id"], order["status"])
        self.exit_failed_pending = True
        self.set_phase(Phase.IN_POSITION_PROTECTED if self.all_legs_open() else Phase.IN_POSITION_ACTIVITY)


class StopLossProtectedStrategyExecution(ProtectedStrategyExecution):
    level_name: str
    offset_points: float

    @classmethod
    def options_from_config(cls, options: dict) -> dict:
        unknown = set(options) - {"level_name", "offset_points"}
        if unknown:
            raise ExecutionConfigError(f"{cls.__name__} does not accept options {sorted(unknown)}")
        offset = options.get("offset_points")
        if "level_name" not in options or "offset_points" not in options:
            raise ExecutionConfigError(f"{cls.__name__} needs level_name and offset_points")
        if isinstance(offset, bool) or not isinstance(offset, (int, float)):
            raise ExecutionConfigError(f"offset_points must be a number, got {offset!r}")
        return {"level_name": options["level_name"], "offset_points": float(offset)}

    def __init__(self, *args, level_name: str, offset_points: float, **kwargs) -> None:
        StrategyExecution.__init__(self, *args, **kwargs)
        if not self.signal.has_level(level_name) or level_name in self.signal.condition_names():
            raise ExecutionConfigError(f"level {level_name!r} is not a node of the signal that carries a level")
        self.level_name, self.offset_points = level_name, offset_points
        self.resting_exits = {}
        self.reset_trade_state()

    def masked_conditions(self) -> set[str]:
        return set()

    def build_legs(self, bar: dict) -> list[dict] | None:
        level = self.signal.level(self.level_name)
        if level is None:
            logger.warning("[%s] no level for %s", self.slot, self.level_name)
            return None
        price = level - self.offset_points if self.direction == "long" else level + self.offset_points
        if self.stop_crossed(bar, price):
            logger.warning("[%s] close %s already crossed the stop %s", self.slot, bar["close"], price)
            return None
        return [self.leg("STP", price)]
