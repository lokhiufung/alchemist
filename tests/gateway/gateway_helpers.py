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


def make_order(gateway, order_type="MKT", side="BUY", instrument=MES, qty=1, **fields) -> dict:
    return {"order_id": gateway.next_order_id(), "account_id": "SIM", "instrument": instrument, "side": side,
            "qty": qty, "order_type": order_type, "stop_price": None, "limit_price": None, "oca_group": None,
            "oca_type": None, "tif": "DAY", "outside_rth": True, "slot": "T", **fields}


class Driver:
    def __init__(self, gateway, buffer, instrument=MES) -> None:
        self.gateway, self.buffer, self.instrument = gateway, buffer, instrument
        self.index = 0
        start, _ = exchange_profile(instrument).session_window(gateway.replay.session, gateway.replay.date)
        self.open_ts = start.timestamp()

    def show(self, close, high=None, low=None, open_=None, instrument=None):
        instrument = instrument or self.instrument
        bar = tape_bar(instrument, self.open_ts + self.index, close, high=high, low=low, open_=open_)
        self.index += 1
        self.gateway.clock.advance(datetime.fromtimestamp(bar["end_ts"], timezone.utc))
        self.gateway.on_bar(instrument, bar)
        return bar

    def updates(self, order_id=None):
        rows = self.buffer.of_type("order_update")
        return [u for u in rows if order_id is None or u["order_id"] == order_id]

    def statuses(self, order_id):
        return [u["status"] for u in self.updates(order_id)]


def driver(replay_kwargs=None, session="NY_RTH", instrument=MES, journal=None):
    gateway, buffer = make_gateway({"session": session, **(replay_kwargs or {})}, tape=ListTape([]), journal=journal)
    return Driver(gateway, buffer, instrument)
