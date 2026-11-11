import asyncio
from datetime import date, datetime, timedelta, timezone

from time_helpers import ManualWatchTime
from alchemist.config import AuxConfig
from alchemist.aux_data.provider import AuxProvider
from alchemist.aux_data.sources import AuxSource
from alchemist.instruments import FDAX, MES

TARGET = date(2026, 4, 15)


def utc(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 4, day, tzinfo=timezone.utc) + timedelta(hours=hour, minutes=minute)


def ny(hour: int, minute: int = 0, day: int = 15) -> datetime:
    return utc(day, hour + 4, minute)


def berlin(hour: int, minute: int = 0, day: int = 15) -> datetime:
    return utc(day, hour - 2, minute)


def row(day: date, value: float) -> dict:
    return {"date": day, "value": value}


class ScriptedSource(AuxSource):
    def __init__(self, clock) -> None:
        self.clock = clock
        self.publications: list[tuple[datetime, dict]] = []
        self.fetch_times: list[datetime] = []
        self.fail_from: datetime | None = None
        self.always_fail = False

    def publish(self, available_from: datetime, day: date, value: float) -> None:
        self.publications.append((available_from, row(day, value)))

    @property
    def fetches(self) -> int:
        return len(self.fetch_times)

    async def fetch(self, name, start, end):
        now = self.clock.moment()
        self.fetch_times.append(now)
        if self.always_fail or (self.fail_from is not None and now >= self.fail_from):
            raise ConnectionError("source down")
        latest: dict[date, dict] = {}
        for available_from, published in self.publications:
            if available_from <= now and start <= published["date"] <= end:
                latest[published["date"]] = published
        return [latest[d] for d in sorted(latest)]


class StubStrategy:
    def __init__(self, name, instrument, names, raise_on_ready_call: int | None = None) -> None:
        self.name, self.signal_instrument, self.names = name, instrument, frozenset(names)
        self.calls: list[tuple[bool, dict]] = []
        self.ready_calls = 0
        self.raise_on_ready_call = raise_on_ready_call

    def aux_names(self):
        return self.names

    def on_aux_ready(self, ready, frames):
        if ready:
            self.ready_calls += 1
            if self.raise_on_ready_call == self.ready_calls:
                raise RuntimeError("seed failed")
        self.calls.append((ready, frames))


def make_provider(sources, strategies, clock, sleep=asyncio.sleep, **config):
    provider = AuxProvider(sources, clock, AuxConfig(**config), sleep=sleep)
    for strategy in strategies:
        provider.member_joined(strategy)
    return provider


async def run_minutes(provider, clock, start: datetime, end: datetime, on_step=None, interval: int = 60):
    moment = start
    while moment <= end:
        clock.advance(moment)
        await provider.step_all()
        if on_step:
            on_step(moment)
        moment += timedelta(seconds=interval)


def first_ready_time(strategy, history):
    return next((at for at, count in history if count >= 1), None)


def tape_bar(instrument, start_ts: float, close: float) -> dict:
    return {"instrument": instrument, "start_ts": start_ts, "end_ts": start_ts + 1.0, "open": close, "high": close,
            "low": close, "close": close, "volume": 1.0, "resolution_s": 1.0, "broker_ts": start_ts + 1.0}
