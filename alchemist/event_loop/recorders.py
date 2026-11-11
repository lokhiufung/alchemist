from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument, instrument as lookup_instrument
from alchemist.constants.event_loop import BAR_KEYS


def session_day(instrument: Instrument, session: str, at_ts: float) -> str:
    profile = exchange_profile(instrument)
    moment = datetime.fromtimestamp(at_ts, timezone.utc)
    window = profile.session_window_at(session, moment)
    if window is None:
        return moment.astimezone(ZoneInfo(profile.tz)).date().isoformat()
    return window[1].astimezone(ZoneInfo(profile.session(session).tz)).date().isoformat()


BAR_SCHEMA = pa.schema([("instrument", pa.string())] + [(key, pa.float64()) for key in BAR_KEYS])
ROUND_TRIP_SCHEMA = pa.schema([
    ("strategy", pa.string()), ("instrument", pa.string()), ("currency", pa.string()), ("side", pa.string()),
    ("qty", pa.int64()), ("entry_price", pa.float64()), ("exit_price", pa.float64()), ("entry_ts", pa.float64()),
    ("exit_ts", pa.float64()), ("commission", pa.float64())])


def bar_path(root: Path, instrument: Instrument, day: str) -> Path:
    return Path(root) / "bars" / f"{instrument.symbol}_{day}.parquet"


def staging_path(path: Path) -> Path:
    return path.with_name(f".{path.stem}.staging.jsonl")


def read_parquet_rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist() if path.exists() else []


def read_bar_rows(path: Path) -> list[dict]:
    rows = read_parquet_rows(path)
    staging = staging_path(path)
    if staging.exists():
        rows += [json.loads(line) for line in staging.read_text().splitlines() if line]
    return rows


def write_parquet_rows(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), temp)
    temp.replace(path)


def roll_up(path: Path) -> None:
    staging = staging_path(path)
    if not staging.exists():
        return
    write_parquet_rows(path, read_bar_rows(path), BAR_SCHEMA)
    staging.unlink()


class BarRecorder:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.handles: dict[Path, TextIO] = {}
        self.current: dict[str, Path] = {}

    def record(self, instrument: Instrument, bar: dict, session: str) -> None:
        path = bar_path(self.root, instrument, session_day(instrument, session, bar["start_ts"]))
        previous = self.current.get(instrument.symbol)
        if previous is not None and previous != path:
            self.close(previous)
        self.current[instrument.symbol] = path
        handle = self.handles.get(path)
        if handle is None:
            path.parent.mkdir(parents=True, exist_ok=True)
            handle = self.handles[path] = open(staging_path(path), "a")
        row = {"instrument": instrument.symbol, **{key: bar[key] for key in BAR_KEYS}}
        handle.write(json.dumps(row) + "\n")
        handle.flush()

    def close(self, path: Path) -> None:
        handle = self.handles.pop(path, None)
        if handle is not None:
            handle.close()
        roll_up(path)

    def finalize(self) -> None:
        for path in list(self.handles):
            self.close(path)
        self.current.clear()


def load_recorded_bars(root: Path, instrument: Instrument, session: str, at_ts: float) -> list[dict]:
    path = bar_path(root, instrument, session_day(instrument, session, at_ts))
    return [{**row, "instrument": lookup_instrument(row["instrument"])} for row in read_bar_rows(path)]


class TradeRecorder:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.seen: dict[int, tuple[int, float]] = {}
        self.open_trades: dict[str, dict] = {}
        self.pending: list[dict] = []

    def record_update(self, update: dict, strategy_key: str, order: dict) -> None:
        if update["status"] not in ("FILLED", "PARTIALLY_FILLED"):
            return
        order_id = update["order_id"]
        seen_qty, seen_cost = self.seen.get(order_id, (0, 0.0))
        delta = update["filled"] - seen_qty
        if delta <= 0:
            return
        cost = update["avg_fill_price"] * update["filled"]
        price = (cost - seen_cost) / delta
        self.seen[order_id] = (update["filled"], cost)
        self.apply_fill(strategy_key, order, delta, price, update["ts"])

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
        by_path: dict[Path, list[dict]] = {}
        for row in self.pending:
            day = datetime.fromtimestamp(row["exit_ts"], timezone.utc).date().isoformat()
            by_path.setdefault(self.root / "trades" / f"round_trips_{day}.parquet", []).append(row)
        for path, rows in by_path.items():
            write_parquet_rows(path, read_parquet_rows(path) + rows, ROUND_TRIP_SCHEMA)
        self.pending = []

    async def flush(self) -> None:
        self.write_pending()

    def finalize(self) -> None:
        self.write_pending()
