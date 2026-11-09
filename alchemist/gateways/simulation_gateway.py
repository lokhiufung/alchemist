from __future__ import annotations

import hashlib
import heapq
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator

from alchemist.clock import Clock
from alchemist.config import GatewayConfig
from alchemist.exchanges import EXCHANGES, exchange_profile
from alchemist.gateways.gateway import Gateway
from alchemist.gateways.tapes import (BAR_SECONDS, ReplayDataError, TapeSource, check_tape_in_session,
                                      tape_from_spec)
from alchemist.instruments import Instrument
from alchemist.messages import make_event
from alchemist.registry import register_gateway

SEED_LOOKBACK_SESSIONS = 10
SESSION_MARGIN = timedelta(hours=6)
PRICE_KEYS = ("open", "high", "low", "close", "volume")
FINGERPRINT_FILE = "tape.sha256"


def canonical_bar(bar: dict) -> bytes:
    parts = {"instrument": f'"{bar["instrument"].symbol}"'}
    for key in ("broker_ts", "end_ts", "resolution_s", "start_ts"):
        parts[key] = f"{bar[key]:.3f}"
    for key in PRICE_KEYS:
        parts[key] = f"{bar[key]:.2f}"
    return ("{" + ",".join(f'"{k}":{parts[k]}' for k in sorted(parts)) + "}\n").encode()


class OrderFillingEngine:
    def __init__(self, fill_reference: str = "close", slippage_ticks: int = 0) -> None:
        self.fill_reference = fill_reference
        self.slippage_ticks = slippage_ticks


@register_gateway("simulation")
class SimulationGateway(Gateway):
    def __init__(self, event_buffer, config: GatewayConfig, clock: Clock, tape: TapeSource | None = None) -> None:
        super().__init__(event_buffer, config, clock)
        self.replay = config.replay
        self.tape = tape or tape_from_spec(self.replay.tape)
        self.instruments: list[Instrument] = []
        self.tape_sha256 = hashlib.sha256()

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
        return None

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
        self.instruments.append(contract["instrument"])

    def next_order_id(self) -> int:
        raise NotImplementedError

    async def place_order(self, order: dict[str, Any]) -> None:
        raise NotImplementedError

    async def cancel_order(self, order_id: int) -> None:
        raise NotImplementedError

    async def get_executions(self, since: float | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def get_open_orders(self, timeout: float = 5.0) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def heartbeat(self, timeout: float = 3.0) -> bool:
        return True

    async def get_positions(self, timeout: float = 5.0) -> list[dict]:
        raise NotImplementedError

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

    async def start_replay(self) -> None:
        streams = [self.replay_bars(instrument) for instrument in self.instruments]
        for bar in heapq.merge(*streams, key=lambda b: b["start_ts"]):
            self.tape_sha256.update(canonical_bar(bar))
            self.event_buffer.push(make_event("bar_update", bar, self.clock.timestamp()))
        self.event_buffer.push(make_event("end_of_data", {}, self.clock.timestamp()))
        self.write_tape_fingerprint()

    def write_tape_fingerprint(self) -> None:
        if self.replay.journal is None:
            return
        folder = self.replay.journal.parent
        folder.mkdir(parents=True, exist_ok=True)
        (folder / FINGERPRINT_FILE).write_text(self.tape_sha256.hexdigest() + "\n")
