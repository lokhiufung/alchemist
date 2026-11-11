from __future__ import annotations

import itertools
from typing import Any, TypedDict

from alchemist.constants.messages import REPLY_EVENTS
from alchemist.instruments import Instrument


class Bar(TypedDict):
    instrument: Instrument
    start_ts: float
    end_ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    resolution_s: float
    broker_ts: float


class TickUpdate(TypedDict):
    instrument: Instrument
    broker_ts: float
    recv_ts: float
    price: float
    size: float


class OrderUpdate(TypedDict):
    order_id: int
    status: str
    filled: int
    remaining: int
    avg_fill_price: float | None
    last_fill_price: float | None
    ts: float
    account_id: str
    instrument: Instrument
    reason: str | None


class PositionUpdate(TypedDict):
    account_id: str
    instrument: Instrument
    size: int
    avg_cost: float
    currency: str


class BalanceUpdate(TypedDict):
    account_id: str
    currency: str
    available: float


class Order(TypedDict):
    order_id: int
    account_id: str
    instrument: Instrument
    side: str
    qty: int
    order_type: str
    stop_price: float | None
    limit_price: float | None
    oca_group: str | None
    oca_type: int | None
    tif: str
    outside_rth: bool
    slot: str


class Execution(TypedDict):
    order_id: int
    exec_id: str
    ts: float
    side: str
    qty: int
    price: float
    instrument: Instrument
    account_id: str
    currency: str


class Event(TypedDict):
    event_type: str
    data: dict[str, Any]
    event_time: float


class Command(TypedDict):
    command_type: str
    data: dict[str, Any]
    correlation_id: int | None


def make_event(event_type: str, data: dict[str, Any], event_time: float) -> Event:
    return {"event_type": event_type, "data": data, "event_time": event_time}


correlation_ids = itertools.count(1)


def make_command(command_type: str, data: dict[str, Any] | None = None) -> Command:
    return {"command_type": command_type, "data": data or {}, "correlation_id": next(correlation_ids)}


def make_reply(command: Command, event_time: float, result: Any = None, error: str | None = None,
               terminal: bool = False) -> Event:
    return make_event(REPLY_EVENTS.get(command["command_type"], "reply"), {"correlation_id": command["correlation_id"], "request": command["command_type"],
                                "result": result, "error": error, "terminal": terminal}, event_time)
