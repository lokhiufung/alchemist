from __future__ import annotations

import asyncio
from functools import cached_property
from datetime import date, datetime, timezone
from enum import Enum
from typing import Callable
from zoneinfo import ZoneInfo

from alchemist.config import ExecutionConfig
from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument
from alchemist.logger import strategy_logger
from alchemist.managers.order_manager import CLOSED, OrderManager, OrderState
from alchemist.stores import DailyStore, round_trip_count


EXECUTION_CLASSES: dict[str, type[StrategyExecution]] = {}


class Phase(str, Enum):
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


class ExecutionConfigError(ValueError):
    pass


def make_execution(execution_config: dict | None, signal, om, **kw) -> StrategyExecution:
    block = dict(execution_config or {})
    class_name = block.pop("class", "StrategyExecution")
    slot = kw.get("slot")
    cls = EXECUTION_CLASSES.get(class_name)
    try:
        if cls is None:
            raise ExecutionConfigError(f"unknown execution class {class_name!r}; known: {sorted(EXECUTION_CLASSES)}")
        options = cls.options_from_config(block)
        return cls(signal, om, **kw, **options)
    except ExecutionConfigError as exc:
        raise ExecutionConfigError(f"slot {slot}: {exc}") from exc


class StrategyExecution:
    TRANSITIONS: dict[Phase, set[Phase]] = {
        Phase.WARMUP: {Phase.ENTRY_LOOP_ACTIVITY, Phase.WAITING_FOR_WORKING_HOURS, Phase.DONE},
        Phase.WAITING_FOR_WORKING_HOURS: {Phase.WARMUP, Phase.ENTRY_LOOP_ACTIVITY, Phase.DONE},
        Phase.ENTRY_LOOP_ACTIVITY: {Phase.WARMUP, Phase.WAITING_FOR_WORKING_HOURS, Phase.PROCESSING_ENTRY_ORDER,
                                    Phase.DONE},
        Phase.PROCESSING_ENTRY_ORDER: {Phase.IN_POSITION_ACTIVITY, Phase.ENTRY_LOOP_ACTIVITY},
        Phase.IN_POSITION_ACTIVITY: {Phase.PROCESSING_EXIT_ORDER, Phase.ENTRY_LOOP_ACTIVITY, Phase.DONE},
        Phase.PROCESSING_EXIT_ORDER: {Phase.IN_POSITION_ACTIVITY, Phase.ENTRY_LOOP_ACTIVITY, Phase.DONE},
        Phase.SESSION_END_FLATTENING: {Phase.DONE},
        Phase.DONE: {Phase.ENTRY_LOOP_ACTIVITY},
        Phase.ERROR: {Phase.WAITING_FOR_WORKING_HOURS, Phase.DONE},
    }
    FROM_ANYWHERE = {Phase.ERROR, Phase.SESSION_END_FLATTENING}

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        EXECUTION_CLASSES[cls.__name__] = cls

    @classmethod
    def options_from_config(cls, options: dict) -> dict:
        if options:
            raise ExecutionConfigError(f"{cls.__name__} takes no options, got {sorted(options)}")
        return {}

    def __init__(self, signal, om: OrderManager, instrument: Instrument, account_id: str, slot: str,
                 session: str, watch_time, roundtrip_store: DailyStore, config: ExecutionConfig | None = None) -> None:
        self.signal = signal
        self.om = om
        self.instrument = instrument
        self.account_id = account_id
        self.slot = slot
        self.log = strategy_logger(slot)
        self.session = session
        self.watch_time = watch_time
        self.roundtrip_store = roundtrip_store
        self.config = config or ExecutionConfig()
        self.state = Phase.WARMUP
        self.warmup_ready = False
        self.aux_ready = False
        self.round_trips = 0
        self.trading_day: date | None = None
        self.trade_closed = False
        self.entry_order_bar_ts: float | None = None
        self.entry_order_id: int | None = None
        self.trade_qty = 0
        self.exit_failed_pending = False
        self.flatten_needed = False
        self.flatten_sent_day: date | None = None
        self.flatten_final = False
        self.flatten_sending = False
        self.quarantine_reason: str | None = None
        self.quarantined_at = 0.0
        self.last_reminder: float | None = None
        self.state_before_quarantine = Phase.WARMUP
        self.cached_window: tuple[float, float, date] | None = None
        self.tasks: set[asyncio.Task] = set()

    @property
    def direction(self) -> str:
        return self.signal.params.direction

    @property
    def entry_side(self) -> str:
        return "BUY" if self.direction == "long" else "SELL"

    @property
    def exit_side(self) -> str:
        return "SELL" if self.direction == "long" else "BUY"

    @property
    def daily_cap(self) -> int:
        return self.signal.params.max_trades_per_day

    @property
    def ready_to_arm(self) -> bool:
        return self.warmup_ready and self.aux_ready

    def set_warmup_ready(self, ready: bool = True) -> None:
        self.warmup_ready = ready

    def set_aux_ready(self, ready: bool = True) -> None:
        self.aux_ready = ready

    def set_phase(self, phase: Phase) -> None:
        if phase is self.state:
            return
        if phase not in self.FROM_ANYWHERE and phase not in self.TRANSITIONS.get(self.state, set()):
            raise RuntimeError(f"[{self.slot}] illegal phase change {self.state.value} -> {phase.value}")
        self.log.info("state transition", extra={
            "event": "state_transition", "from_state": self.state.value, "to_state": phase.value})
        self.state = phase

    def spawn(self, coroutine) -> None:
        task = asyncio.get_running_loop().create_task(self.guarded(coroutine))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def settle(self) -> None:
        while self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)

    async def guarded(self, coroutine) -> None:
        try:
            await coroutine
        except Exception:
            self.log.critical("order request failed", exc_info=True, extra={"event": "order_request_failed"})

    def session_state(self, bar: dict) -> tuple[bool, date | None]:
        start = bar["start_ts"]
        cached = self.cached_window
        if cached and cached[0] <= start < cached[1]:
            return True, cached[2]
        profile = exchange_profile(self.instrument)
        session = profile.session(self.session)
        window = profile.session_window_at(session.name, datetime.fromtimestamp(start, timezone.utc))
        if window is None:
            return False, None
        day = window[1].astimezone(ZoneInfo(session.tz)).date()
        if not session.crosses_midnight and day in profile.early_closes:
            return False, None
        self.cached_window = (window[0].timestamp(), window[1].timestamp(), day)
        return True, day

    def on_bar(self, bar: dict, step_signal: Callable[[], None]) -> None:
        in_session, day = self.session_state(bar)
        if not in_session:
            if self.state is Phase.ENTRY_LOOP_ACTIVITY:
                self.set_phase(Phase.WAITING_FOR_WORKING_HOURS)
            return
        if day != self.trading_day:
            self.start_trading_day(day)
        step_signal()
        self.handler_table[self.state](bar)

    @cached_property
    def handler_table(self) -> dict[Phase, Callable[[dict], None]]:
        return self.handlers()

    def handlers(self) -> dict[Phase, Callable[[dict], None]]:
        return {
            Phase.WARMUP: self.seek_entry,
            Phase.WAITING_FOR_WORKING_HOURS: self.seek_entry,
            Phase.ENTRY_LOOP_ACTIVITY: self.seek_entry,
            Phase.PROCESSING_ENTRY_ORDER: self.wait,
            Phase.IN_POSITION_ACTIVITY: self.judge_in_position,
            Phase.PROCESSING_EXIT_ORDER: self.wait,
            Phase.SESSION_END_FLATTENING: self.advance_flatten,
            Phase.DONE: self.wait,
            Phase.ERROR: self.remind_quarantine,
        }

    def wait(self, bar: dict) -> None:
        return None

    def start_trading_day(self, day: date) -> None:
        self.trading_day = day
        self.flatten_final = False
        self.round_trips = round_trip_count(self.roundtrip_store, self.slot, day.isoformat())
        if self.state in (Phase.WARMUP, Phase.WAITING_FOR_WORKING_HOURS, Phase.ENTRY_LOOP_ACTIVITY):
            if self.round_trips >= self.daily_cap:
                self.set_phase(Phase.DONE)
        elif self.state is Phase.DONE and self.round_trips < self.daily_cap:
            self.set_phase(Phase.ENTRY_LOOP_ACTIVITY)

    def seek_entry(self, bar: dict) -> None:
        if not self.ready_to_arm:
            self.set_phase(Phase.WARMUP)
            return
        self.set_phase(Phase.ENTRY_LOOP_ACTIVITY)
        if self.signal.evaluate_entry():
            self.send_entry(bar)

    def send_entry(self, bar: dict) -> None:
        self.log.info("entry decision", extra={
            "event": "entry_decision", "bar_ts": bar["end_ts"], "conditions": self.signal.entry_conditions()})
        self.signal.freeze_exit_inputs(bar)
        self.reset_trade_state()
        self.entry_order_bar_ts = bar["start_ts"]
        self.entry_order_id = None
        self.set_phase(Phase.PROCESSING_ENTRY_ORDER)
        self.spawn(self.place_entry())

    async def place_entry(self) -> None:
        order = await self.om.place_order(self.instrument, self.entry_side, 1, "MKT")
        self.entry_order_id = order["order_id"]

    def handle_failed_exit(self) -> bool:
        if self.exit_failed_pending:
            self.exit_failed_pending = False
            self.flatten_needed = True
            self.log.critical("exit failed in position; no further exit, flatten wanted",
                              extra={"event": "exit_failed_flatten_wanted"})
        return self.flatten_needed

    def judge_in_position(self, bar: dict) -> None:
        if self.handle_failed_exit() or self.trade_qty <= 0:
            return
        answers = self.signal.evaluate_exit_conditions()
        if any(answers.values()):
            self.log.info("exit decision", extra={"event": "exit_decision", "bar_ts": bar["end_ts"],
                                                   "conditions": answers})
            self.send_exit(bar)

    def send_exit(self, bar: dict) -> None:
        self.set_phase(Phase.PROCESSING_EXIT_ORDER)
        self.spawn(self.place_exit(self.trade_qty))

    async def place_exit(self, qty: int) -> None:
        await self.om.place_order(self.instrument, self.exit_side, qty, "MKT")

    def is_entry_order(self, order: dict) -> bool:
        return order["oca_group"] is None and order["side"] == self.entry_side

    def is_exit_order(self, order: dict) -> bool:
        return order["oca_group"] is None and order["side"] == self.exit_side

    @staticmethod
    def is_closed(order: dict) -> bool:
        return OrderState(order["status"]) in CLOSED

    def on_order_update(self, update: dict) -> None:
        order = self.om.get_order(update["order_id"])
        if order is None:
            return
        self.on_phase_update(order)

    def on_phase_update(self, order: dict) -> None:
        if self.state is Phase.PROCESSING_ENTRY_ORDER:
            self.on_entry_update(order)
        elif self.state is Phase.PROCESSING_EXIT_ORDER:
            self.on_exit_update(order)
        elif self.state is Phase.SESSION_END_FLATTENING:
            self.on_flatten_update(order)

    def on_entry_update(self, order: dict) -> None:
        if not self.is_entry_order(order) or not self.is_closed(order):
            return
        self.entry_order_id = order["order_id"]
        if order["filled"] > 0:
            self.trade_qty = order["filled"]
            self.set_phase(Phase.IN_POSITION_ACTIVITY)
        else:
            self.log.warning("entry order ended without a fill", extra={
                "event": "entry_order_unfilled", "order_id": order["order_id"], "status": order["status"]})
            self.signal.release_exit_inputs()
            self.set_phase(Phase.ENTRY_LOOP_ACTIVITY)

    def on_exit_update(self, order: dict) -> None:
        if not self.is_exit_order(order) or not self.is_closed(order):
            return
        if order["filled"] > 0:
            self.trade_qty -= order["filled"]
            if self.trade_qty <= 0:
                self.complete_round_trip()
            else:
                self.set_phase(Phase.IN_POSITION_ACTIVITY)
        else:
            self.log.warning("exit order ended unfilled in position", extra={
                "event": "exit_order_unfilled", "order_id": order["order_id"], "status": order["status"]})
            self.exit_failed_pending = True
            self.set_phase(Phase.IN_POSITION_ACTIVITY)

    def reset_trade_state(self) -> None:
        return None

    def complete_round_trip(self) -> None:
        self.reset_trade_state()
        self.trade_qty = 0
        self.entry_order_id = None
        self.exit_failed_pending = False
        self.round_trips += 1
        self.roundtrip_store.save(self.slot, self.trading_day.isoformat(), {"count": self.round_trips})
        self.signal.release_exit_inputs()
        self.trade_closed = True
        self.set_phase(Phase.DONE if self.round_trips >= self.daily_cap else Phase.ENTRY_LOOP_ACTIVITY)

    def consume_trade_closed(self) -> bool:
        closed, self.trade_closed = self.trade_closed, False
        return closed

    def live_orders(self) -> bool:
        return bool(self.om.get_submitted_orders() or self.om.get_open_orders() or self.om.get_cancelling_orders())

    def is_processing_order(self) -> bool:
        if self.state in (Phase.PROCESSING_ENTRY_ORDER, Phase.PROCESSING_EXIT_ORDER,
                          Phase.PROCESSING_PROTECTION_ORDERS):
            return True
        if self.state is Phase.SESSION_END_FLATTENING:
            return self.flatten_sending or self.live_orders()
        return False

    def request_flatten(self, position: dict, reason: str) -> None:
        if self.state is Phase.ERROR or (self.state is Phase.DONE and position["size"] == 0):
            return
        if self.state is not Phase.SESSION_END_FLATTENING:
            self.log.info("flatten requested", extra={"event": "flatten_requested", "reason": reason})
            self.set_phase(Phase.SESSION_END_FLATTENING)
        self.spawn(self.flatten_step(position["size"]))

    async def flatten_step(self, size: int) -> None:
        await self.om.cancel_all()
        if self.live_orders():
            return
        if size == 0:
            self.flatten_needed = False
            self.set_phase(Phase.DONE)
            return
        if self.flatten_sent_day == self.trading_day and self.flatten_sent_day is not None:
            return
        self.flatten_sent_day = self.trading_day
        self.flatten_sending = True
        try:
            side = "SELL" if size > 0 else "BUY"
            await self.om.place_order(self.instrument, side, abs(size), "MKT")
        finally:
            self.flatten_sending = False

    def on_flatten_update(self, order: dict) -> None:
        if order["oca_group"] is not None or order["order_type"] != "MKT" or not self.is_closed(order):
            return
        self.flatten_final = True
        self.flatten_needed = False
        if order["filled"] == 0:
            self.log.critical("flatten order ended unfilled; it will not be resent today", extra={
                "event": "flatten_order_unfilled", "order_id": order["order_id"], "status": order["status"]})

    def advance_flatten(self, bar: dict) -> None:
        if self.flatten_final:
            self.set_phase(Phase.DONE)

    def flatten_wanted(self) -> bool:
        return self.flatten_needed

    def quarantine(self, reason: str) -> None:
        if self.state is Phase.ERROR:
            return
        self.state_before_quarantine = self.state
        self.quarantine_reason = reason
        self.quarantined_at = self.watch_time.now()
        self.last_reminder = None
        self.log.error("strategy quarantined", extra={"event": "quarantined", "reason": reason})
        self.set_phase(Phase.ERROR)

    def release_quarantine(self) -> None:
        if self.state is not Phase.ERROR:
            return
        was_done = self.state_before_quarantine is Phase.DONE and self.round_trips >= self.daily_cap
        self.quarantine_reason = None
        self.set_phase(Phase.DONE if was_done else Phase.WAITING_FOR_WORKING_HOURS)

    def is_quarantined(self) -> bool:
        return self.state is Phase.ERROR

    def remind_quarantine(self, bar: dict) -> None:
        now = self.watch_time.now()
        if self.last_reminder is not None and now - self.last_reminder < self.config.reminder_interval_s:
            return
        self.last_reminder = now
        self.log.warning("still quarantined", extra={
            "event": "quarantine_reminder", "quarantined_s": round(now - self.quarantined_at),
            "reason": self.quarantine_reason})


EXECUTION_CLASSES[StrategyExecution.__name__] = StrategyExecution
