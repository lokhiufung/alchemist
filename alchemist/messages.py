from __future__ import annotations

from typing import Any, TypedDict

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
    enqueued_at: float


EVENT_TYPES = ("tick_update", "bar_update", "order_update", "position_update",
               "balance_update", "repaired_fill", "book_op", "end_of_data")


def make_event(event_type: str, data: dict[str, Any], now: float) -> Event:
    return {"event_type": event_type, "data": data, "enqueued_at": now}
