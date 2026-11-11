from __future__ import annotations

import asyncio
import hashlib
import heapq
import json
import logging
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator

from alchemist.clock import Clock
from alchemist.config import GatewayConfig
from alchemist.exchanges import EXCHANGES, exchange_profile
from alchemist.gateways.gateway import Gateway
from alchemist.gateways.utils import OrderIdSequence, check_order, round_price
from alchemist.gateways.simulation_gateway.tapes import (BAR_SECONDS, ReplayDataError, TapeSource, check_tape_in_session,
                                      tape_from_spec)
from alchemist.instruments import Instrument
from alchemist.messages import make_event
from alchemist.constants.simulation import (
    FILL_REFERENCES,
    FINGERPRINT_FILE,
    PRICE_KEYS,
    SEED_LOOKBACK_SESSIONS,
    SESSION_MARGIN,
    STARTING_FUNDS,
)


logger = logging.getLogger("gateway")


def canonical_bar(bar: dict) -> bytes:
    parts = {"instrument": f'"{bar["instrument"].symbol}"'}
    for key in ("broker_ts", "end_ts", "resolution_s", "start_ts"):
        parts[key] = f"{bar[key]:.3f}"
    for key in PRICE_KEYS:
        parts[key] = f"{bar[key]:.2f}"
    return ("{" + ",".join(f'"{k}":{parts[k]}' for k in sorted(parts)) + "}\n").encode()


def iso_ms(moment: datetime) -> str:
    utc = moment.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def journal_line(row: dict) -> str:
    parts = []
    for key in sorted(row):
        value = row[key]
        text = f"{value:.2f}" if isinstance(value, float) else json.dumps(value)
        parts.append(f'"{key}":{text}')
    return "{" + ",".join(parts) + "}\n"


class BrokerJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None

    def write(self, row: dict) -> None:
        if self.handle is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = open(self.path, "w")
        self.handle.write(journal_line(row))
        self.handle.flush()

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()
            self.handle = None


class NullJournal:
    def write(self, row: dict) -> None:
        return None

    def close(self) -> None:
        return None


class OrderFillingEngine:
    def __init__(self, fill_reference: str = "close", slippage_ticks: int = 0) -> None:
        if fill_reference not in FILL_REFERENCES:
            raise ValueError(f"fill_reference {fill_reference!r} must be one of {FILL_REFERENCES}")
        self.fill_reference = fill_reference
        self.slippage_ticks = slippage_ticks

    def market_price(self, side: str, bar: dict, instrument: Instrument) -> float:
        direction = 1 if side == "BUY" else -1
        return bar[self.fill_reference] + direction * self.slippage_ticks * instrument.tick_size

    @staticmethod
    def resting_price(order: dict, bar: dict) -> float | None:
        buying = order["side"] == "BUY"
        limit = order["order_type"] == "LMT"
        level = order["limit_price"] if limit else order["stop_price"]
        if limit == buying:
            touched, through = bar["low"] <= level, bar["open"] <= level
        else:
            touched, through = bar["high"] >= level, bar["open"] >= level
        if not touched:
            return None
        return bar["open"] if through else level


class SimulationGateway(Gateway):
    def __init__(self, event_buffer, config: GatewayConfig, clock: Clock, tape: TapeSource | None = None) -> None:
        super().__init__(event_buffer, config, clock)
        self.replay = config.replay
        self.tape = tape or tape_from_spec(self.replay.tape)
        self.day_bars: dict[Instrument, list[dict]] = {}
        self.tape_sha256 = hashlib.sha256()
        self.ids = OrderIdSequence(1)
        self.engine = OrderFillingEngine(self.replay.fill_reference, self.replay.slippage_ticks)
        self.journal = BrokerJournal(self.replay.journal) if self.replay.journal else NullJournal()
        self.current_bars: dict[Instrument, dict] = {}
        self.resting: dict[int, dict] = {}
        self.filled_groups: set[str] = set()
        self.executions: list[dict] = []
        self.positions: dict[tuple[str, Instrument], dict] = {}
        self.funds: dict[tuple[str, str], float] = {}
        self.feed_task: asyncio.Task | None = None

    def market_data_type(self, instrument: Instrument) -> tuple[str, float | None]:
        return ("bar_update", BAR_SECONDS)

    async def connect(self) -> None:
        profiles = [p for p in EXCHANGES.values() if self.replay.session in p.sessions or self.replay.session in p.aliases]
        if not profiles:
            raise ReplayDataError(f"replay session {self.replay.session} is not a session of any exchange")
        if not any(p.is_trading_day(self.replay.date) for p in profiles):
            raise ReplayDataError(
                f"replay date {self.replay.date.isoformat()} is not a trading day for session {self.replay.session}")

    async def disconnect(self) -> None:
        if self.feed_task is not None:
            self.feed_task.cancel()
        self.journal.close()

    async def reconnect(self) -> None:
        return None

    async def subscribe_order_update(self) -> None:
        return None

    async def subscribe_account_update(self) -> None:
        return None

    def session_window(self, instrument: Instrument, day: date) -> tuple[datetime, datetime]:
        profile = exchange_profile(instrument)
        try:
            return profile.session_window(self.replay.session, day)
        except KeyError:
            raise ReplayDataError(
                f"session {self.replay.session} is not a session of {profile.name} (replay date "
                f"{self.replay.date.isoformat()})") from None

    async def resolve_contract(self, instrument: Instrument) -> Any:
        start, end = self.session_window(instrument, self.replay.date)
        count = check_tape_in_session(self.tape, instrument, self.replay.session,
                                      start - SESSION_MARGIN, end + SESSION_MARGIN)
        if not count:
            raise ReplayDataError(
                f"tape has no bars for {instrument.symbol} on {self.replay.date.isoformat()} "
                f"session {self.replay.session}")
        return {"instrument": instrument}

    async def subscribe_tick(self, contract: Any) -> None:
        instrument = contract["instrument"]
        self.day_bars[instrument] = list(self.replay_bars(instrument))

    def next_order_id(self) -> int:
        return self.ids.next()

    def emit(self, event_type: str, data: dict, caller_depth: int = 1) -> None:
        self.push_broker_event(event_type, data, self.clock.timestamp(), caller_depth + 1)

    def emit_status(self, order: dict, status: str, ts: float, filled: int = 0,
                    price: float | None = None, reason: str | None = None) -> None:
        self.emit("order_update", caller_depth=2, data={
            "order_id": order["order_id"], "status": status, "filled": filled, "remaining": order["qty"] - filled,
            "avg_fill_price": price, "last_fill_price": price, "ts": ts, "account_id": order["account_id"],
            "instrument": order["instrument"], "reason": reason})

    async def place_order(self, order: dict[str, Any]) -> None:
        instrument = order["instrument"]
        reason = check_order(order, instrument.tick_size)
        if reason:
            raise ValueError(reason)
        if order["order_type"] in ("STP", "LMT"):
            field = "stop_price" if order["order_type"] == "STP" else "limit_price"
            order[field] = round_price(order[field], instrument.tick_size, order["side"], order["order_type"])
        now = self.clock.now()
        self.log_order_sent(order)
        self.journal.write({"instrument": instrument.symbol, "order_id": order["order_id"],
                            "order_type": order["order_type"], "qty": order["qty"], "side": order["side"],
                            "time": iso_ms(now)})
        group = order.get("oca_group")
        if group in self.filled_groups:
            self.emit_status(order, "REJECTED", now.timestamp(), reason="oca group already filled")
        elif order["order_type"] == "MKT":
            self.place_market(order, now.timestamp())
        else:
            self.resting[order["order_id"]] = order
            self.emit_status(order, "OPEN", now.timestamp())

    def place_market(self, order: dict, now_ts: float) -> None:
        bar = self.current_bars.get(order["instrument"])
        if bar is None:
            self.emit_status(order, "REJECTED", now_ts, reason="no bar to fill against")
            return
        if order.get("oca_group"):
            self.cancel_group_legs(order["oca_group"], now_ts)
        self.emit_status(order, "SUBMITTED", now_ts)
        self.fill(order, self.engine.market_price(order["side"], bar, order["instrument"]), bar["end_ts"])

    def cancel_group_legs(self, group: str, ts: float, except_id: int | None = None) -> None:
        for order_id in sorted(i for i, o in self.resting.items() if o.get("oca_group") == group and i != except_id):
            self.emit_status(self.resting.pop(order_id), "CANCELLED", ts)

    def fill(self, order: dict, price: float, ts: float) -> None:
        self.emit_status(order, "FILLED", ts, filled=order["qty"], price=price)
        self.journal.write({"order_id": order["order_id"], "price": float(price), "qty": order["qty"],
                            "time": iso_ms(datetime.fromtimestamp(ts, timezone.utc))})
        instrument = order["instrument"]
        self.executions.append({
            "order_id": order["order_id"], "exec_id": f"sim-{len(self.executions) + 1}", "ts": ts,
            "side": order["side"], "qty": order["qty"], "price": price, "instrument": instrument,
            "account_id": order["account_id"], "currency": instrument.currency})
        self.book_position(order, price)
        group = order.get("oca_group")
        if group:
            self.filled_groups.add(group)
            self.cancel_group_legs(group, ts, except_id=order["order_id"])

    def book_position(self, order: dict, price: float) -> None:
        instrument, account = order["instrument"], order["account_id"]
        position = self.positions.setdefault((account, instrument), {"size": 0, "avg_cost": 0.0})
        signed = order["qty"] if order["side"] == "BUY" else -order["qty"]
        size, cost = position["size"], position["avg_cost"]
        realized = 0.0
        if size and (size > 0) != (signed > 0):
            closed = min(abs(size), abs(signed))
            realized = (price - cost) * closed * (1 if size > 0 else -1) * instrument.multiplier
        new_size = size + signed
        if new_size == 0:
            new_cost = 0.0
        elif size == 0 or (size > 0) == (signed > 0):
            new_cost = (cost * abs(size) + price * abs(signed)) / abs(new_size)
        else:
            new_cost = cost if (new_size > 0) == (size > 0) else price
        position.update(size=new_size, avg_cost=new_cost)
        key = (account, instrument.currency)
        self.funds[key] = self.funds.get(key, STARTING_FUNDS) + realized
        self.emit("position_update", {"account_id": account, "instrument": instrument, "size": new_size,
                                      "avg_cost": new_cost, "currency": instrument.currency})
        self.emit("balance_update", {"account_id": account, "currency": instrument.currency,
                                     "available": self.funds[key]})

    def match_bar(self, instrument: Instrument, bar: dict[str, Any]) -> None:
        self.current_bars[instrument] = bar
        fires = []
        for order_id in sorted(self.resting):
            order = self.resting[order_id]
            if order["instrument"] != instrument:
                continue
            price = self.engine.resting_price(order, bar)
            if price is not None:
                fires.append((order["order_type"] != "STP", order_id, price))
        for _, order_id, price in sorted(fires):
            if order_id in self.resting:
                self.fill(self.resting.pop(order_id), price, bar["end_ts"])

    async def cancel_order(self, order_id: int) -> None:
        self.log_cancel_sent(order_id)
        order = self.resting.pop(order_id, None)
        if order is not None:
            self.emit_status(order, "CANCELLED", self.clock.timestamp())

    async def get_executions(self, since: float | None = None) -> list[dict[str, Any]]:
        return [dict(row) for row in self.executions if since is None or row["ts"] > since]

    async def get_open_orders(self, timeout: float = 5.0) -> list[dict[str, Any]]:
        return [dict(self.resting[i]) for i in sorted(self.resting)]

    async def heartbeat(self, timeout: float = 3.0) -> bool:
        return True

    async def get_positions(self, timeout: float = 5.0) -> list[dict]:
        return [{"account_id": account, "instrument": instrument, "size": p["size"], "avg_cost": p["avg_cost"],
                 "currency": instrument.currency}
                for (account, instrument), p in self.positions.items() if p["size"]]

    def completed_sessions_before_replay(self, instrument: Instrument, start: date, end: date) -> Iterator[date]:
        profile = exchange_profile(instrument)
        day, last = start, min(end, self.replay.date - timedelta(days=1))
        while day <= last:
            if profile.is_trading_day(day):
                yield day
            day += timedelta(days=1)

    async def get_daily_history(self, instrument: Instrument, start: date, end: date, session: str) -> list[dict]:
        frame = []
        for day in self.completed_sessions_before_replay(instrument, start, end):
            open_at, close_at = exchange_profile(instrument).session_window(session, day)
            bars = list(self.tape.bars(instrument, open_at, close_at))
            if bars:
                frame.append({
                    "instrument": instrument, "date": day.isoformat(),
                    "start_ts": open_at.timestamp(), "end_ts": close_at.timestamp(),
                    "open": bars[0]["open"], "high": max(b["high"] for b in bars),
                    "low": min(b["low"] for b in bars), "close": bars[-1]["close"],
                    "volume": sum(b["volume"] for b in bars), "resolution_s": 86_400.0,
                    "broker_ts": close_at.timestamp()})
        return frame

    async def get_intraday_seed_bars(self, instrument: Instrument, session: str) -> list:
        profile = exchange_profile(instrument)
        day, checked = self.replay.date, 0
        while checked < SEED_LOOKBACK_SESSIONS:
            day -= timedelta(days=1)
            if not profile.is_trading_day(day):
                continue
            checked += 1
            open_at, close_at = profile.session_window(session, day)
            bars = list(self.tape.bars(instrument, open_at, close_at))
            if bars:
                return bars
        return []

    def replay_bars(self, instrument: Instrument) -> Iterator[dict]:
        start, end = self.session_window(instrument, self.replay.date)
        bars = list(self.tape.bars(instrument, start, end))
        before, after = self.replay.pad_before_s, self.replay.pad_after_s
        if before and bars:
            yield from self.flat_bars(instrument, start.timestamp() - before, before, bars[0]["open"])
        yield from bars
        if after and bars:
            yield from self.flat_bars(instrument, bars[-1]["end_ts"], after, bars[-1]["close"])

    @staticmethod
    def flat_bars(instrument: Instrument, first_ts: float, count: int, price: float) -> Iterator[dict]:
        for index in range(count):
            begin = first_ts + index
            yield {"instrument": instrument, "start_ts": begin, "end_ts": begin + BAR_SECONDS, "open": price,
                   "high": price, "low": price, "close": price, "volume": 0.0, "resolution_s": BAR_SECONDS,
                   "broker_ts": begin + BAR_SECONDS}

    async def start_feed(self) -> None:
        self.feed_task = asyncio.get_running_loop().create_task(self.guarded_replay())

    async def guarded_replay(self) -> None:
        try:
            await self.start_replay()
        except Exception:
            logger.critical("replay feed failed", exc_info=True, extra={"event": "replay_feed_failed"})
            self.event_buffer.push(make_event("end_of_data", {}, self.clock.timestamp()))

    async def start_replay(self) -> None:
        for bar in heapq.merge(*self.day_bars.values(), key=lambda b: b["start_ts"]):
            await self.event_buffer.wait_idle()
            self.match_bar(bar["instrument"], bar)
            self.tape_sha256.update(canonical_bar(bar))
            self.event_buffer.push(make_event("bar_update", bar, self.clock.timestamp()))
        await self.event_buffer.wait_idle()
        self.event_buffer.push(make_event("end_of_data", {}, self.clock.timestamp()))
        self.write_tape_fingerprint()

    def write_tape_fingerprint(self) -> None:
        if self.replay.journal is None:
            return
        folder = self.replay.journal.parent
        folder.mkdir(parents=True, exist_ok=True)
        (folder / FINGERPRINT_FILE).write_text(self.tape_sha256.hexdigest() + "\n")
