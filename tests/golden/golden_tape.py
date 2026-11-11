from __future__ import annotations

import atexit
import shutil
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from alchemist.exchanges import exchange_profile
from alchemist.instruments import MES, Instrument

REPLAY_DATE = date(2026, 4, 15)
PRIOR_SESSIONS = 60
SESSION_BARS = 23_400
BASE_PRICE = 5000.0
HIGH_PRICE = 5010.0
LOW_PRICE = 4990.0
TICK = 0.25
QUIET_BARS = 600
STEP_UP = 2.0
STEP_DOWN = 1.0
TARGET_PRICE = 5011.0
BAR_VOLUME = 100.0
PARQUET_COLUMNS = ("open", "high", "low", "close", "volume")


def on_tick(price: float) -> float:
    return round(price / TICK) * TICK


def leg(first: float, last: float, bars: int) -> list[float]:
    return [on_tick(first + (last - first) * k / bars) for k in range(1, bars + 1)]


@lru_cache(maxsize=1)
def prior_session_closes() -> tuple[float, ...]:
    quarter = SESSION_BARS // 4
    return tuple(leg(BASE_PRICE, HIGH_PRICE, quarter) + leg(HIGH_PRICE, LOW_PRICE, 2 * quarter)
                 + leg(LOW_PRICE, BASE_PRICE, quarter))


@lru_cache(maxsize=1)
def replay_session_closes() -> tuple[float, ...]:
    closes = [BASE_PRICE] * QUIET_BARS
    price, up = BASE_PRICE, True
    while price < TARGET_PRICE:
        price += STEP_UP if up else -STEP_DOWN
        up = not up
        closes.append(price)
    return tuple(closes + [price] * (SESSION_BARS - len(closes)))


def prior_session_dates(profile) -> list[date]:
    dates, day = [], REPLAY_DATE
    while len(dates) < PRIOR_SESSIONS:
        day -= timedelta(days=1)
        if profile.is_trading_day(day):
            dates.append(day)
    return dates


def session_bars(instrument: Instrument, open_ts: float, closes: tuple[float, ...]) -> Iterator[dict]:
    previous = BASE_PRICE
    for index, close in enumerate(closes):
        start = open_ts + index
        yield {"instrument": instrument, "start_ts": start, "end_ts": start + 1.0, "open": previous,
               "high": max(previous, close), "low": min(previous, close), "close": close,
               "volume": BAR_VOLUME, "resolution_s": 1.0, "broker_ts": start + 1.0}
        previous = close


def golden_session(instrument: Instrument, start: datetime, end: datetime) -> Iterator[dict]:
    profile = exchange_profile(instrument)
    session = profile.session("regular").name
    prior = set(prior_session_dates(profile))
    local_start = start.astimezone(ZoneInfo(profile.tz)).date() - timedelta(days=1)
    local_end = end.astimezone(ZoneInfo(profile.tz)).date() + timedelta(days=1)
    day = local_start
    while day <= local_end:
        if day in prior or day == REPLAY_DATE:
            open_at, close_at = profile.session_window(session, day)
            if open_at < end and close_at > start:
                closes = replay_session_closes() if day == REPLAY_DATE else prior_session_closes()
                for bar in session_bars(instrument, open_at.timestamp(), closes):
                    if start.timestamp() <= bar["start_ts"] < end.timestamp():
                        yield bar
        day += timedelta(days=1)


def write_golden_tape(path: Path, instrument: Instrument = MES) -> Path:
    first = min(prior_session_dates(exchange_profile(instrument)))
    start = datetime.combine(first - timedelta(days=1), time(), timezone.utc)
    end = datetime.combine(REPLAY_DATE + timedelta(days=2), time(), timezone.utc)
    bars = list(golden_session(instrument, start, end))
    columns = {"symbol": [instrument.symbol] * len(bars), "start_ts": [b["start_ts"] for b in bars]}
    columns.update({key: [b[key] for b in bars] for key in PARQUET_COLUMNS})
    pq.write_table(pa.table(columns), path)
    return path


@lru_cache(maxsize=1)
def golden_tape_path() -> str:
    folder = Path(tempfile.mkdtemp(prefix="golden_tape_"))
    atexit.register(shutil.rmtree, folder, ignore_errors=True)
    return str(write_golden_tape(folder / "golden_session.parquet"))
