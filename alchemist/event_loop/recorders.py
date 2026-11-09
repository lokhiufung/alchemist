from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument, instrument as lookup_instrument

BAR_KEYS = ("start_ts", "end_ts", "open", "high", "low", "close", "volume", "resolution_s", "broker_ts")


def session_day(instrument: Instrument, session: str, at_ts: float) -> str:
    profile = exchange_profile(instrument)
    moment = datetime.fromtimestamp(at_ts, timezone.utc)
    window = profile.session_window_at(session, moment)
    if window is None:
        return moment.astimezone(ZoneInfo(profile.tz)).date().isoformat()
    return window[1].astimezone(ZoneInfo(profile.session(session).tz)).date().isoformat()


def bar_path(root: Path, instrument: Instrument, day: str) -> Path:
    return Path(root) / "bars" / f"{instrument.symbol}_{day}.jsonl"


class BarRecorder:
    def __init__(self, root: Path, book) -> None:
        self.root = Path(root)
        self.book = book
        self.handles: dict[Path, object] = {}

    def record(self, instrument: Instrument, bar: dict) -> None:
        profile = exchange_profile(instrument)
        session = self.book.sessions_by_instrument.get(instrument, profile.data_session)
        path = bar_path(self.root, instrument, session_day(instrument, session, bar["start_ts"]))
        handle = self.handles.get(path)
        if handle is None:
            path.parent.mkdir(parents=True, exist_ok=True)
            handle = self.handles[path] = open(path, "a")
        row = {"instrument": instrument.symbol, **{key: bar[key] for key in BAR_KEYS}}
        handle.write(json.dumps(row) + "\n")
        handle.flush()

    def finalize(self) -> None:
        for handle in self.handles.values():
            handle.close()
        self.handles.clear()


def load_recorded_bars(root: Path, instrument: Instrument, session: str, clock) -> list[dict]:
    day = session_day(instrument, session, clock.timestamp())
    path = bar_path(root, instrument, day)
    if not path.exists():
        return []
    bars = []
    for line in path.read_text().splitlines():
        row = json.loads(line)
        row["instrument"] = lookup_instrument(row["instrument"])
        bars.append(row)
    return bars


class TradeRecorder:
    def __init__(self, root: Path, gateway, book) -> None:
        self.root = Path(root)
        self.gateway = gateway
        self.book = book
        self.seen: dict[int, tuple[int, float]] = {}
        self.open_trades: dict[str, dict] = {}
        self.pending: list[dict] = []

    def owner(self, order_id: int):
        for strategy in self.book.strategies:
            order = strategy.om.get_order(order_id)
            if order is not None:
                return strategy, order
        return None

    def record_update(self, update: dict) -> None:
        if update["status"] not in ("FILLED", "PARTIALLY_FILLED"):
            return
        order_id = update["order_id"]
        seen_qty, seen_cost = self.seen.get(order_id, (0, 0.0))
        delta = update["filled"] - seen_qty
        if delta <= 0:
            return
        found = self.owner(order_id)
        if found is None:
            return
        strategy, order = found
        cost = update["avg_fill_price"] * update["filled"]
        price = (cost - seen_cost) / delta
        self.seen[order_id] = (update["filled"], cost)
        self.apply_fill(strategy.name, order, delta, price, update["ts"])

    def apply_fill(self, name: str, order: dict, qty: int, price: float, ts: float) -> None:
        trade = self.open_trades.get(name)
        if trade is None:
            trade = self.open_trades[name] = {
                "strategy": name, "instrument": order["instrument"].symbol, "currency": order["instrument"].currency,
                "side": order["side"], "entry_qty": 0, "entry_cost": 0.0, "entry_ts": ts, "exit_qty": 0,
                "exit_cost": 0.0, "exit_ts": ts}
        if order["side"] == trade["side"]:
            trade["entry_qty"] += qty
            trade["entry_cost"] += price * qty
            return
        trade["exit_qty"] += qty
        trade["exit_cost"] += price * qty
        trade["exit_ts"] = ts
        if trade["exit_qty"] >= trade["entry_qty"]:
            self.pending.append({
                "strategy": name, "instrument": trade["instrument"], "currency": trade["currency"],
                "side": trade["side"], "qty": trade["entry_qty"], "entry_price": trade["entry_cost"] / trade["entry_qty"],
                "exit_price": trade["exit_cost"] / trade["exit_qty"], "entry_ts": trade["entry_ts"],
                "exit_ts": trade["exit_ts"], "commission": None})
            del self.open_trades[name]

    def write_pending(self) -> None:
        if not self.pending:
            return
        for row in self.pending:
            day = datetime.fromtimestamp(row["exit_ts"], timezone.utc).date().isoformat()
            path = self.root / "trades" / f"round_trips_{day}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a") as handle:
                handle.write(json.dumps(row) + "\n")
        self.pending = []

    async def flush(self) -> None:
        self.write_pending()

    def finalize(self) -> None:
        self.write_pending()


class NullRecorder:
    def record(self, instrument: Instrument, bar: dict) -> None:
        return None

    def record_update(self, update: dict) -> None:
        return None

    async def flush(self) -> None:
        return None

    def finalize(self) -> None:
        return None
