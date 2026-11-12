from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from datetime import time as time_of_day
from typing import Callable

from alchemist.config import SafetyConfig
from alchemist.constants.messages import REPLY_EVENT_TYPES
from alchemist.constants.safety import FEED_EVENTS, ORDER_COMMANDS, TERMINAL_STATUSES
from alchemist.instruments import Instrument
from alchemist.messages import make_command
from alchemist.safety.fsm_repair import repair_orders
from alchemist.safety.freeze import FreezeDetector
from alchemist.service import Service

logger = logging.getLogger("safety")

__all__ = ["SafetyConfig", "SafetyMonitor", "WatchState"]

DRAIN_PAUSE_S = 0.02


def parse_time_of_day(text: str) -> time_of_day:
    hour, minute = text.split(":")
    return time_of_day(int(hour), int(minute))


@dataclass
class Ladder:
    resubscribes: int = 0
    reconnected: bool = False
    restarted: bool = False
    next_at: float = 0.0


@dataclass
class Repair:
    picked: list[int]
    executions_id: int
    open_orders_id: int
    replies: dict[int, dict] = field(default_factory=dict)


class WatchState:
    def __init__(self, started_at: float) -> None:
        self.watched: set[Instrument] = set()
        self.feed_arrival: dict[str, float] = {}
        self.feed_event_time: dict[str, float] = {}
        self.last_heartbeat = started_at
        self.last_event_time: float | None = None
        self.pending: dict[int, dict] = {}
        self.replies: dict[int, dict] = {}
        self.restored = 0
        self.lost = 0

    def observe_event(self, event: dict, arrived_at: float) -> None:
        kind, data = event["event_type"], event["data"]
        self.last_event_time = max(self.last_event_time or event["event_time"], event["event_time"])
        if kind in FEED_EVENTS:
            exchange = data["instrument"].exchange
            self.feed_arrival[exchange] = arrived_at
            self.feed_event_time[exchange] = event["event_time"]
        elif kind == "heartbeat":
            self.last_heartbeat = arrived_at
        elif kind in ("order_update", "repaired_fill"):
            entry = self.pending.get(data["order_id"])
            if entry is not None:
                entry["acked"] = True
                if data["status"] in TERMINAL_STATUSES:
                    del self.pending[data["order_id"]]
        elif kind in REPLY_EVENT_TYPES:
            self.replies[data["correlation_id"]] = data
        elif kind == "connection_restored":
            self.restored += 1
        elif kind == "connection_lost":
            self.lost += 1

    def observe_command(self, command: dict, sent_at: float) -> None:
        kind, data = command["command_type"], command["data"]
        if kind == "subscribe":
            self.watched.add(data["contract"]["instrument"])
        elif kind == "unsubscribe":
            self.watched.discard(data["contract"]["instrument"])
        elif kind in ORDER_COMMANDS:
            order = data["order"]
            self.pending[order["order_id"]] = {"order": order, "strategy": order["slot"], "sent_at": sent_at,
                                               "acked": False}


class SafetyMonitor(Service):
    def __init__(self, inbound, outbound, exchange_profiles: dict, config: SafetyConfig,
                 system_time: Callable[[], float] = time.time, freeze: FreezeDetector | None = None) -> None:
        self.inbound = inbound
        self.outbound = outbound
        self.exchange_profiles = exchange_profiles
        self.config = config
        self.system_time = system_time
        self.freeze = freeze or FreezeDetector(config)
        self.inbound_tap: deque = deque()
        self.outbound_tap: deque = deque()
        inbound.add_tap(self.inbound_tap)
        outbound.add_tap(self.outbound_tap)
        self.started_at = system_time()
        self.state = WatchState(self.started_at)
        self.ladders: dict[str, Ladder] = {}
        self.repairs: list[Repair] = []
        self.blind_resubscribes = 0
        self.seen_restored = 0
        self.seen_lost = 0
        self.last_reconnect_request: float | None = None
        self.last_check: float | None = None
        self.last_stuck_check = self.started_at
        self.last_poll = self.started_at
        self.stopped = threading.Event()
        self.thread: threading.Thread | None = None

    async def start(self) -> None:
        self.started_at = self.last_stuck_check = self.last_poll = self.system_time()
        self.state.last_heartbeat = self.started_at
        self.freeze.start()
        self.stopped.clear()
        self.thread = threading.Thread(target=self.run, daemon=True, name="safety-watcher")
        self.thread.start()

    async def stop(self) -> None:
        self.stopped.set()
        if self.thread is not None:
            self.thread.join(timeout=self.config.watch_check_interval_s * 2)
            self.thread = None
        self.freeze.stop()

    def check_in(self) -> None:
        self.freeze.check_in()

    def run(self) -> None:
        while not self.stopped.is_set():
            try:
                self.step(self.system_time())
            except Exception:
                logger.exception("safety step failed", extra={"event": "safety_step_failed"})
            self.stopped.wait(DRAIN_PAUSE_S)

    def step(self, now: float) -> None:
        self.drain(now)
        if self.last_check is None or now - self.last_check >= self.config.watch_check_interval_s:
            self.last_check = now
            self.check(now)

    def drain(self, now: float) -> None:
        while self.outbound_tap:
            self.state.observe_command(self.outbound_tap.popleft(), now)
        while self.inbound_tap:
            event = self.inbound_tap.popleft()
            self.state.observe_event(event, event.get("pushed_at", now))
            if event["event_type"] in FEED_EVENTS:
                self.ladders.pop(event["data"]["instrument"].exchange, None)
                self.blind_resubscribes = 0
        self.finish_repairs(now)

    def request(self, command_type: str, **data) -> int:
        command = make_command(command_type, data)
        self.outbound.push(command)
        return command["correlation_id"]

    def alert(self, message: str, **fields) -> None:
        self.request("alert", message=message, **fields)

    def check(self, now: float) -> None:
        self.on_connection_events(now)
        self.check_heartbeat(now)
        self.check_feeds(now)
        self.check_positions_poll(now)
        self.check_stuck_orders(now)

    def on_connection_events(self, now: float) -> None:
        while self.seen_lost < self.state.lost:
            self.seen_lost += 1
            self.alert("broker connection lost", reason="connection_lost")
        while self.seen_restored < self.state.restored:
            self.seen_restored += 1
            self.on_restored(now)

    def on_restored(self, now: float) -> None:
        self.blind_resubscribes += 1
        if self.blind_resubscribes > self.config.resubscribe_without_feed_cap:
            self.blind_resubscribes = 0
            self.request_reconnect(now, "restored_without_feed")
        elif self.state.watched:
            self.request("resubscribe", instruments=sorted(self.state.watched, key=lambda i: i.symbol))
        self.start_repair(list(self.state.pending))

    def request_reconnect(self, now: float, reason: str) -> None:
        self.last_reconnect_request = now
        self.request("reconnect")
        self.alert("reconnect requested", reason=reason)

    def check_heartbeat(self, now: float) -> None:
        reference = max(self.state.last_heartbeat, self.last_reconnect_request or self.started_at)
        if now - reference > self.config.heartbeat_interval_s + self.config.heartbeat_timeout_s:
            self.request_reconnect(now, "heartbeat_gap")

    def watched_by_exchange(self) -> dict[str, list[Instrument]]:
        grouped: dict[str, list[Instrument]] = {}
        for instrument in sorted(self.state.watched, key=lambda i: i.symbol):
            grouped.setdefault(instrument.exchange, []).append(instrument)
        return grouped

    def armed(self, exchange: str) -> bool:
        event_time = self.state.feed_event_time.get(exchange)
        if event_time is None:
            return False
        profile = self.exchange_profiles[exchange]
        return profile.is_open(profile.data_session, datetime.fromtimestamp(event_time, timezone.utc))

    def check_feeds(self, now: float) -> None:
        for exchange, instruments in self.watched_by_exchange().items():
            if not self.armed(exchange):
                continue
            threshold = self.config.stale_overrides.get(exchange, self.config.stale_feed_s)
            if now - self.state.feed_arrival[exchange] > threshold:
                self.climb_ladder(exchange, instruments, now)

    def climb_ladder(self, exchange: str, instruments: list[Instrument], now: float) -> None:
        ladder = self.ladders.setdefault(exchange, Ladder())
        if now < ladder.next_at:
            return
        resubscribe_limit = min(self.config.resubscribe_max_attempts, self.config.resubscribe_without_feed_cap)
        if ladder.resubscribes < resubscribe_limit:
            self.request("resubscribe", instruments=instruments)
            ladder.next_at = now + self.config.resubscribe_backoff_base_s * 2 ** ladder.resubscribes
            ladder.resubscribes += 1
        elif not ladder.reconnected:
            ladder.reconnected = True
            ladder.next_at = now + self.config.resubscribe_backoff_base_s * 2 ** ladder.resubscribes
            self.request_reconnect(now, f"stale_feed_{exchange}")
        elif not ladder.restarted and self.in_maintenance_window(now):
            ladder.restarted = True
            self.request("restart_broker_process")
            self.alert("broker process restart requested", reason=f"stale_feed_{exchange}")

    def in_maintenance_window(self, now: float) -> bool:
        moment = datetime.fromtimestamp(now, timezone.utc).time()
        for start, end in self.config.maintenance_windows:
            begin, finish = parse_time_of_day(start), parse_time_of_day(end)
            inside = begin <= moment < finish if begin <= finish else moment >= begin or moment < finish
            if inside:
                return True
        return False

    def check_positions_poll(self, now: float) -> None:
        if self.config.positions_poll_s and now - self.last_poll >= self.config.positions_poll_s:
            self.last_poll = now
            self.request("poll_positions")

    def check_stuck_orders(self, now: float) -> None:
        if now - self.last_stuck_check < self.config.check_interval_s:
            return
        self.last_stuck_check = now
        picked = [order_id for order_id, entry in self.state.pending.items() if not entry["acked"]]
        if picked:
            self.start_repair(picked)

    def start_repair(self, picked: list[int]) -> None:
        if not picked:
            return
        self.repairs.append(Repair(picked, self.request("get_executions", since=None),
                                   self.request("get_open_orders")))

    def finish_repairs(self, now: float) -> None:
        waiting = []
        for repair in self.repairs:
            executions = self.state.replies.get(repair.executions_id)
            open_orders = self.state.replies.get(repair.open_orders_id)
            if executions is None or open_orders is None:
                waiting.append(repair)
                continue
            self.state.replies.pop(repair.executions_id, None)
            self.state.replies.pop(repair.open_orders_id, None)
            if executions["error"] or open_orders["error"]:
                logger.warning("repair skipped, the broker state is unreadable", extra={
                    "event": "repair_skipped", "reason": executions["error"] or open_orders["error"]})
                continue
            self.on_replies(executions["result"], open_orders["result"], repair.picked, now)
        self.repairs = waiting

    def on_replies(self, executions: list, open_orders: list, picked: list[int], now: float | None = None) -> None:
        now = self.system_time() if now is None else now
        pending = [self.state.pending[i] for i in picked if i in self.state.pending]
        event_time = self.state.last_event_time or now
        for event in repair_orders(pending, executions, open_orders, now, event_time,
                                   self.config.reject_synthesis_min_age_s):
            if event["data"]["status"] in TERMINAL_STATUSES:
                self.state.pending.pop(event["data"]["order_id"], None)
            self.inbound.push(event)
