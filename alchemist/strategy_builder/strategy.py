from __future__ import annotations

import asyncio
import time
import zlib
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator
from zoneinfo import ZoneInfo

from alchemist.config import ExecutionConfig, StrategyConfig
from alchemist.exchanges import exchange_profile
from alchemist.logger import strategy_logger
from alchemist.managers.order_manager import OrderManager
from alchemist.registry import get_signal_dag
from alchemist.stores import DailyStore
from alchemist.strategy_builder.strategy_executions import make_execution
from alchemist.strategy_builder.strategy_signal import StrategySignal, WarmupRequirements
from alchemist.constants.strategy import REBUILD_YIELD_BUDGET_S, REPLAY_WINDOW


@dataclass
class Stores:
    calc_state: DailyStore
    round_trips: DailyStore


class Strategy:
    signal_dag: str
    resolution_s = 1.0

    def __init__(self, config: StrategyConfig, requests, watch_time, stores: Stores,
                 execution_config: ExecutionConfig | None = None,
                 rebuild_yield_budget_s: float = REBUILD_YIELD_BUDGET_S) -> None:
        self.config = config
        self.rebuild_yield_budget_s = rebuild_yield_budget_s
        self.name = config.name
        self.log = strategy_logger(config.name)
        self.watch_time = watch_time
        self.stores = stores
        self.signal_instrument = config.signal_instrument
        self.execution_instrument = config.execution_instrument
        self.position = self.flat_position()
        self.balance = {"currency": self.execution_instrument.currency, "available": None}
        self.om = OrderManager(requests, config.account_id, config.name)
        self.deferred_bars: list | None = None
        self.history: dict = {}
        self.aux_frames: dict = {}
        self.pending_warmup: dict | None = None
        self.pending_aux: dict | None = None
        self.seed_ready = False
        self.aux_armed = False
        self.persisted_day: str | None = None
        self.bars_stepped = 0
        self.snapshot_offset = zlib.crc32(config.name.encode()) % config.snapshot_interval if config.snapshot_interval else 0
        self.current_bar: dict | None = None
        self.signal: StrategySignal = self.build_signal()
        self.session = exchange_profile(self.signal_instrument).session(self.signal.params.session).name
        self.execution = make_execution(
            config.execution, self.signal, self.om, instrument=self.execution_instrument,
            account_id=config.account_id, slot=self.name, session=self.session, watch_time=watch_time,
            roundtrip_store=stores.round_trips, config=execution_config)
        if not self.signal.aux_names():
            self.execution.set_aux_ready()
        self.signal_state_restored = False
        if watch_time.now() is not None:
            self.restore_signal_state()

    @property
    def signal_name(self) -> str:
        return self.signal_dag

    @property
    def snapshot_names(self) -> tuple[str, ...]:
        return tuple(self.signal.values())

    def flat_position(self) -> dict:
        return {"size": 0, "avg_price": None, "last_price": None, "currency": self.execution_instrument.currency,
                "entry_ts": None, "side": "FLAT"}

    def build_signal(self) -> StrategySignal:
        return get_signal_dag(self.signal_dag).from_config(self.config)

    def reset_signal(self) -> None:
        self.signal = self.build_signal()
        self.execution.signal = self.signal

    def aux_names(self) -> frozenset[str]:
        return self.signal.aux_names()

    def trading_date(self) -> str:
        profile = exchange_profile(self.signal_instrument)
        return self.watch_time.moment().astimezone(ZoneInfo(profile.tz)).date().isoformat()

    def in_session(self, bar: dict) -> bool:
        profile = exchange_profile(self.signal_instrument)
        return profile.session_window_at(self.session, datetime.fromtimestamp(bar["start_ts"], timezone.utc)) is not None

    def on_bar(self, bars: deque, bar: dict) -> None:
        self.run_pending()
        if self.deferred_bars is not None:
            self.deferred_bars.append((tuple(bars), bar))
            return
        self.current_bar = bar
        self.execution.on_bar(bar, lambda: self.step_signal(bars, bar))
        if self.execution.flatten_wanted():
            self.execution.request_flatten(self.position_view(), "exit_failed")

    def step_signal(self, bars: deque, bar: dict) -> None:
        self.signal.step(bars, self.strategy_data(bar))
        self.bars_stepped += 1
        interval = self.config.snapshot_interval
        if interval and (self.bars_stepped + self.snapshot_offset) % interval == 0:
            self.log_snapshot()

    def strategy_data(self, bar: dict | None = None) -> dict:
        bar = bar or self.current_bar
        if self.position["size"] == 0 or self.position["avg_price"] is None:
            return {"entry_price": None, "secs_since_entry": None}
        entry_ts = self.position["entry_ts"]
        secs = None if entry_ts is None or bar is None else bar["start_ts"] - entry_ts
        return {"entry_price": self.position["avg_price"], "secs_since_entry": secs}

    def signal_snapshot(self) -> dict:
        return dict(self.signal.values())

    def log_snapshot(self) -> None:
        fields = {"event": "signal_snapshot", "instrument": self.signal_instrument.symbol,
                  "bar_ts": self.current_bar["end_ts"], "values": self.signal_snapshot()}
        if self.execution.is_quarantined():
            fields["stale"] = True
        self.log.info("signal snapshot", extra=fields)

    def on_order_update(self, update: dict) -> None:
        change = self.om.on_order_update(update)
        if change is None:
            return
        self.book_position(change, update)
        self.execution.on_order_update(update)
        self.run_pending()

    def book_position(self, change: dict, update: dict) -> None:
        delta = change["fill_delta"]
        if not delta:
            return
        signed = delta if change["side"] == "BUY" else -delta
        price = update.get("last_fill_price") or change["avg_fill_price"]
        old, held = self.position["size"], self.position
        new = old + signed
        if old == 0:
            held.update(avg_price=price, entry_ts=self.execution.entry_order_bar_ts)
        elif (old > 0) == (signed > 0):
            held["avg_price"] = (held["avg_price"] * abs(old) + price * abs(signed)) / abs(new)
        elif new == 0:
            held.update(avg_price=None, entry_ts=None)
        elif (new > 0) != (old > 0):
            held.update(avg_price=price, entry_ts=self.execution.entry_order_bar_ts)
        held.update(size=new, last_price=price, side="LONG" if new > 0 else "SHORT" if new < 0 else "FLAT")

    async def settle(self) -> None:
        await self.execution.settle()

    def position_view(self) -> dict:
        return dict(self.position)

    def on_position_update(self, update: dict) -> None:
        if update["account_id"] == self.config.account_id and update["instrument"] == self.execution_instrument:
            self.log.info("broker position logged, not booked", extra={
                "event": "broker_position", "instrument": self.execution_instrument.symbol,
                "size": update["size"], "avg_cost": update["avg_cost"]})

    def on_balance_update(self, update: dict) -> None:
        if update["account_id"] == self.config.account_id and update["currency"] == self.execution_instrument.currency:
            self.balance = {"currency": update["currency"], "available": update["available"]}

    def is_flat_idle(self) -> bool:
        return self.position["size"] == 0 and not (
            self.om.get_submitted_orders() or self.om.get_open_orders() or self.om.get_cancelling_orders())

    def warmup_requirements(self) -> WarmupRequirements:
        return self.signal.warmup_requirements()

    def seed_signal(self) -> None:
        self.signal.seed({**self.history, "aux": self.aux_frames})

    def warmup(self, history: dict) -> None:
        if not self.signal_state_restored:
            self.restore_signal_state()
        if not self.is_flat_idle():
            self.pending_warmup = history
            self.execution.set_warmup_ready(False)
            return
        self.history, self.pending_warmup = history, None
        self.reset_signal()
        self.seed_signal()
        self.execution.set_warmup_ready(False)

    def on_seed_state(self, ready: bool) -> None:
        self.seed_ready = ready
        self.sync_ready_flags()

    def on_aux_ready(self, ready: bool, frames: dict) -> None:
        if not ready:
            self.aux_armed, self.pending_aux = False, None
            self.execution.set_aux_ready(False)
            return
        if not self.is_flat_idle():
            self.aux_armed, self.pending_aux = True, frames
            self.execution.set_aux_ready(False)
            return
        self.aux_frames, self.pending_aux = frames, None
        self.reset_signal()
        try:
            self.seed_signal()
        except Exception:
            self.aux_armed = False
            self.execution.set_aux_ready(False)
            raise
        self.aux_armed = True
        self.start_replay_task()
        self.sync_ready_flags()

    def sync_ready_flags(self) -> None:
        warm = self.seed_ready and self.signal.is_warm() and self.pending_warmup is None
        self.execution.set_warmup_ready(warm)
        if self.signal.aux_names():
            self.execution.set_aux_ready(self.aux_armed and self.pending_aux is None)
        if warm:
            self.persist_signal_state()

    def run_pending(self) -> None:
        if (self.pending_warmup is None and self.pending_aux is None) or not self.is_flat_idle():
            return
        if self.pending_warmup is not None:
            self.history, self.pending_warmup = self.pending_warmup, None
        if self.pending_aux is not None:
            self.aux_frames, self.pending_aux = self.pending_aux, None
        self.reset_signal()
        self.seed_signal()
        self.start_replay_task()
        self.sync_ready_flags()

    def start_replay_task(self) -> None:
        seed_bars = self.history.get("intraday", [])
        if not seed_bars:
            return
        if self.deferred_bars is None:
            self.deferred_bars = []
        asyncio.get_running_loop().create_task(self.rebuild_intraday_state(seed_bars))

    async def rebuild_intraday_state(self, seed_bars: list) -> None:
        async with self.deferring_bars():
            window: deque = deque(maxlen=REPLAY_WINDOW)
            slice_began = time.perf_counter()
            for bar in seed_bars:
                if self.in_session(bar):
                    window.append(bar)
                    self.signal.step(window, {"entry_price": None, "secs_since_entry": None})
                if time.perf_counter() - slice_began >= self.rebuild_yield_budget_s:
                    await asyncio.sleep(0)
                    slice_began = time.perf_counter()
        self.sync_ready_flags()

    @asynccontextmanager
    async def deferring_bars(self) -> AsyncIterator[None]:
        if self.deferred_bars is None:
            self.deferred_bars = []
        try:
            yield
        finally:
            held, self.deferred_bars = self.deferred_bars, None
            for bars, bar in held:
                self.on_bar(deque(bars), bar)

    def request_flatten(self, reason: str, bar_ts: float) -> None:
        if reason == "session_end" and (self.pending_warmup is not None or self.pending_aux is not None):
            self.log.warning("rebuild still pending at session end; it never ran",
                             extra={"event": "rebuild_never_ran"})
        self.execution.request_flatten(self.position_view(), reason)

    def restore_signal_state(self) -> None:
        self.signal_state_restored = True
        state = self.stores.calc_state.load(self.name, self.trading_date())
        if state:
            self.log.warning("session-fixed signal state restored after a restart",
                             extra={"event": "signal_state_restored", "state": state})
            self.signal.restore_session_state(state)

    def persist_signal_state(self) -> None:
        today = self.trading_date()
        if self.execution.is_quarantined() or self.persisted_day == today:
            return
        state = self.signal.snapshot_session_state()
        if state:
            self.stores.calc_state.save(self.name, today, state)
        self.persisted_day = today

    def is_processing_order(self) -> bool:
        return self.execution.is_processing_order()

    def quarantine(self, reason: str) -> None:
        self.execution.quarantine(reason)

    def release_quarantine(self) -> None:
        self.execution.release_quarantine()

    def is_quarantined(self) -> bool:
        return self.execution.is_quarantined()

    def consume_trade_closed(self) -> bool:
        return self.execution.consume_trade_closed()

    def get_submitted_orders(self) -> list:
        return self.om.get_submitted_orders()

    def get_open_orders(self) -> list:
        return self.om.get_open_orders()

    def get_cancelling_orders(self) -> list:
        return self.om.get_cancelling_orders()
