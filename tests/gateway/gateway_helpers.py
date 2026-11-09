from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from alchemist.clock import ReplayClock
from alchemist.config import GatewayConfig, ReplayConfig
from alchemist.exchanges import exchange_profile
from alchemist.gateways.simulation_gateway import SimulationGateway
from alchemist.gateways.tapes import TapeSource
from alchemist.instruments import MES

REPLAY_DATE = date(2026, 4, 15)


class RecordingBuffer:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def push(self, event) -> None:
        self.events.append(event)

    def of_type(self, event_type: str) -> list[dict]:
        return [e["data"] for e in self.events if e["event_type"] == event_type]


class ListTape(TapeSource):
    def __init__(self, bars: list[dict]) -> None:
        self.rows = sorted(bars, key=lambda b: b["start_ts"])

    def bars(self, instrument, start, end):
        low, high = start.timestamp(), end.timestamp()
        return (b for b in self.rows if b["instrument"] == instrument and low <= b["start_ts"] < high)


def tape_bar(instrument, start_ts: float, close: float, high=None, low=None, open_=None) -> dict:
    return {"instrument": instrument, "start_ts": start_ts, "end_ts": start_ts + 1.0,
            "open": close if open_ is None else open_, "high": close if high is None else high,
            "low": close if low is None else low, "close": close, "volume": 1.0,
            "resolution_s": 1.0, "broker_ts": start_ts + 1.0}


def session_seconds(instrument, session: str, day: date, count: int, start_offset: int = 0, close: float = 100.0):
    start, _ = exchange_profile(instrument).session_window(session, day)
    return [tape_bar(instrument, start.timestamp() + start_offset + i, close) for i in range(count)]


def make_gateway(replay_kwargs=None, tape=None, journal=None):
    replay = ReplayConfig(**{"tape": "synthetic:golden_session", "date": REPLAY_DATE, "session": "NY_RTH",
                             "journal": journal, **(replay_kwargs or {})})
    buffer = RecordingBuffer()
    start, _ = exchange_profile(MES).session_window("NY_RTH", replay.date)
    gateway = SimulationGateway(buffer, GatewayConfig("simulation", replay=replay),
                                ReplayClock(start.astimezone(timezone.utc)), tape=tape)
    return gateway, buffer
