from __future__ import annotations

import asyncio
import itertools
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from alchemist.clock import Clock
from alchemist.config import GatewayConfig
from alchemist.exchanges import exchange_profile
from alchemist.gateways.gateway import Gateway
from alchemist.gateways.ibkr.errors import (CANCEL_REFUSAL_CODES, CONNECTION_LOST_CODE, CONNECTION_RESTORED_CODES,
                                            DATA_FARM_OK_CODES, ORDER_REJECT_CODES, REQUEST_ID_FLOOR)
from alchemist.gateways.utils import OrderIdSequence, check_order, round_price
from alchemist.instruments import Instrument
from alchemist.messages import make_event
from alchemist.registry import register_gateway

logger = logging.getLogger("gateway")

ROLL_THIN_DAYS = 5
SEED_CHUNK_BARS = 2000
BOT, SLD = "BOT", "SLD"
STATUS_TO_UPDATE = {"Submitted": "OPEN", "PreSubmitted": "OPEN", "Filled": "FILLED", "Cancelled": "CANCELLED",
                    "ApiCancelled": "CANCELLED"}


class IbTransport(ABC):
    @abstractmethod
    def connect(self, host: str, port: int, client_id: int) -> None: ...
    @abstractmethod
    def disconnect(self) -> None: ...
    @abstractmethod
    def req_current_time(self) -> None: ...
    @abstractmethod
    def req_contract_details(self, req_id: int, spec: dict) -> None: ...
    @abstractmethod
    def req_tick_by_tick(self, req_id: int, contract: dict) -> None: ...
    @abstractmethod
    def cancel_tick_by_tick(self, req_id: int) -> None: ...
    @abstractmethod
    def req_order_updates(self) -> None: ...
    @abstractmethod
    def req_account_updates(self, accounts: list[str]) -> None: ...
    @abstractmethod
    def place_order(self, order_id: int, contract: dict, spec: dict) -> None: ...
    @abstractmethod
    def cancel_order(self, order_id: int) -> None: ...
    @abstractmethod
    def req_executions(self, req_id: int, since: float | None) -> None: ...
    @abstractmethod
    def req_open_orders(self) -> None: ...
    @abstractmethod
    def req_positions(self) -> None: ...
    @abstractmethod
    def req_historical_data(self, req_id: int, contract: dict, end: datetime, duration: str, bar_size: str) -> None: ...


@register_gateway("ibkr")
class IbkrGateway(Gateway):
    def __init__(self, event_buffer, config: GatewayConfig, clock: Clock, transport: IbTransport | None = None) -> None:
        super().__init__(event_buffer, config, clock)
        options = config.options
        self.host, self.port, self.client_id = options["host"], options["port"], options["client_id"]
        self.accounts: list[str] = list(options.get("accounts", []))
        self.request_timeout_s = options.get("request_timeout_s", 10.0)
        self.position_timeout_s = options.get("position_timeout_s", 5.0)
        if transport is None:
            from alchemist.gateways.ibkr.client import create_transport
            transport = create_transport(self)
        self.transport = transport
        self.ids = OrderIdSequence(1)
        self.request_ids = itertools.count(REQUEST_ID_FLOOR)
        self.generation = 0
        self.connected = False
        self.loop: asyncio.AbstractEventLoop | None = None
        self.contracts: dict[Instrument, dict] = {}
        self.identity: dict[tuple, Instrument] = {}
        self.tick_requests: dict[int, Instrument] = {}
        self.subscribed_in: dict[Instrument, int] = {}
        self.orders: dict[int, dict] = {}
        self.rejected: set[int] = set()
        self.waiters: dict[Any, asyncio.Future] = {}
        self.collected: dict[Any, list] = {}
        self.instrument_sessions: dict[Instrument, str] = {}
        self.last_tick: float | None = None
        self.resubscribe_requested = False
        self.roll_mismatch: set[str] = set()
        self.commissions: dict[str, dict] = {}
        self.sender: queue.Queue = queue.Queue()
        self.sender_thread: threading.Thread | None = None

    def next_request_id(self) -> int:
        return next(self.request_ids)

    def next_order_id(self) -> int:
        return self.ids.next()

    @property
    def last_tick_ts(self) -> float | None:
        return self.last_tick

    def set_instrument_sessions(self, sessions: dict[Instrument, str]) -> None:
        self.instrument_sessions = dict(sessions)

    def roll_mismatch_dates(self) -> frozenset:
        return frozenset(self.roll_mismatch)

    async def get_commissions(self) -> dict[str, dict]:
        return dict(self.commissions)

    def consume_resubscribe_request(self) -> bool:
        requested, self.resubscribe_requested = self.resubscribe_requested, False
        return requested

    def resolve_waiter(self, key: Any, result: Any = None, error: Exception | None = None) -> None:
        future = self.waiters.get(key)
        if future is None or self.loop is None:
            return

        def settle() -> None:
            if future.done():
                return
            future.set_exception(error) if error else future.set_result(result)
        self.loop.call_soon_threadsafe(settle)

    async def wait_for(self, key: Any, timeout: float, send) -> Any:
        future = self.loop.create_future()
        self.waiters[key] = future
        self.collected[key] = []
        try:
            send()
            return await asyncio.wait_for(future, timeout)
        finally:
            self.waiters.pop(key, None)
            self.collected.pop(key, None)

    async def connect(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.generation += 1
        key = ("connect", self.generation)
        await self.wait_for(key, self.request_timeout_s,
                            lambda: self.transport.connect(self.host, self.port, self.client_id))
        self.connected = True
        self.start_sender()

    def start_sender(self) -> None:
        if self.sender_thread is None or not self.sender_thread.is_alive():
            self.sender_thread = threading.Thread(target=self.send_loop, daemon=True, name="ibkr-send")
            self.sender_thread.start()

    def send_loop(self) -> None:
        while True:
            job = self.sender.get()
            if job is None:
                return
            action, future = job
            try:
                action()
                error = None
            except Exception as exc:
                error = exc
            if self.loop is not None:
                self.loop.call_soon_threadsafe(lambda f=future, e=error: f.done() or (f.set_exception(e) if e else f.set_result(None)))

    async def send(self, action) -> None:
        future = self.loop.create_future()
        self.sender.put((action, future))
        await future

    async def disconnect(self) -> None:
        self.connected = False
        self.transport.disconnect()
        self.sender.put(None)
        self.sender_thread = None

    async def reconnect(self) -> None:
        self.connected = False
        self.transport.disconnect()
        await self.connect()
        await self.resubscribe_streams()

    async def resubscribe_streams(self) -> None:
        await self.subscribe_order_update()
        await self.subscribe_account_update()
        for instrument in list(self.subscribed_in):
            await self.subscribe_tick(self.contracts[instrument])

    async def subscribe_order_update(self) -> None:
        self.transport.req_order_updates()

    async def subscribe_account_update(self) -> None:
        self.transport.req_account_updates(self.accounts)

    async def resolve_contract(self, instrument: Instrument) -> dict:
        profile = exchange_profile(instrument)
        spec = dict(profile.broker_identity(instrument))
        if instrument.asset_class.value == "future":
            spec["contract_month"] = profile.front_contract(instrument, self.clock.now().date())
        details = await self.request_details(spec)
        contract = {**spec, **details, "instrument": instrument}
        self.contracts[instrument] = contract
        self.identity[(spec["sec_type"], spec["trading_class"], spec["venue"])] = instrument
        return contract

    async def request_details(self, spec: dict) -> dict:
        req_id = self.next_request_id()
        return await self.wait_for(("details", req_id), self.request_timeout_s,
                                   lambda: self.transport.req_contract_details(req_id, spec))

    def on_contract_details(self, req_id: int, details: dict) -> None:
        self.collected.setdefault(("details", req_id), []).append(details)

    def on_contract_details_end(self, req_id: int) -> None:
        rows = self.collected.get(("details", req_id), [])
        if rows:
            self.resolve_waiter(("details", req_id), rows[0])
        else:
            self.resolve_waiter(("details", req_id), error=LookupError("no contract details"))

    async def subscribe_tick(self, contract: dict) -> None:
        instrument = contract["instrument"]
        if self.subscribed_in.get(instrument) == self.generation:
            return
        req_id = self.next_request_id()
        self.tick_requests = {r: i for r, i in self.tick_requests.items() if i != instrument}
        self.tick_requests[req_id] = instrument
        self.subscribed_in[instrument] = self.generation
        self.transport.req_tick_by_tick(req_id, contract)

    async def unsubscribe_tick(self, contract: dict) -> None:
        instrument = contract["instrument"]
        for req_id, held in list(self.tick_requests.items()):
            if held == instrument:
                self.transport.cancel_tick_by_tick(req_id)
                del self.tick_requests[req_id]
        self.subscribed_in.pop(instrument, None)

    def on_tick(self, generation: int, req_id: int, broker_ts: float, price: float, size: float,
                now: float | None = None) -> None:
        instrument = self.tick_requests.get(req_id)
        if generation != self.generation or instrument is None:
            return
        recv_ts = time.time() if now is None else now
        self.event_buffer.push(make_event("tick_update", {"instrument": instrument, "broker_ts": broker_ts,
                                                          "recv_ts": recv_ts, "price": price, "size": size},
                                          recv_ts))
        session = self.instrument_sessions.get(instrument)
        profile = exchange_profile(instrument)
        if session is not None and profile.is_open(session, datetime.fromtimestamp(recv_ts, timezone.utc)):
            self.last_tick = broker_ts

    def on_next_valid_id(self, order_id: int) -> None:
        self.ids.raise_floor(order_id)
        self.resolve_waiter(("connect", self.generation), True)

    async def heartbeat(self, timeout: float = 3.0) -> bool:
        if not self.connected:
            return False
        try:
            await self.wait_for(("time", self.next_request_id()), timeout, self.transport.req_current_time)
        except (asyncio.TimeoutError, TimeoutError):
            return False
        return True

    def on_current_time(self, epoch: float) -> None:
        for key in [k for k in self.waiters if isinstance(k, tuple) and k[0] == "time"]:
            self.resolve_waiter(key, epoch)

    def on_error(self, req_id: int, code: int, message: str) -> None:
        if code in DATA_FARM_OK_CODES or code in CONNECTION_RESTORED_CODES:
            self.resubscribe_requested = True
        elif code == CONNECTION_LOST_CODE:
            self.connected = False
        is_request = req_id >= REQUEST_ID_FLOOR
        if code in CANCEL_REFUSAL_CODES and req_id in self.orders:
            self.push_update(self.orders[req_id], "CANCEL_REJECTED", reason=f"{code} {message}")
        elif code in ORDER_REJECT_CODES and not is_request and req_id in self.orders:
            if req_id not in self.rejected:
                self.rejected.add(req_id)
                self.push_update(self.orders[req_id], "REJECTED", reason=f"{code} {message}")
        elif is_request:
            for key in [k for k in self.waiters if isinstance(k, tuple) and len(k) > 1 and k[1] == req_id]:
                self.resolve_waiter(key, error=RuntimeError(f"broker error {code}: {message}"))

    def push_update(self, order: dict, status: str, filled: int = 0, remaining: int | None = None,
                    avg: float | None = None, last: float | None = None, ts: float | None = None,
                    reason: str | None = None) -> None:
        now = time.time() if ts is None else ts
        update = {"order_id": order["order_id"], "status": status, "filled": filled,
                  "remaining": order["qty"] - filled if remaining is None else remaining,
                  "avg_fill_price": avg, "last_fill_price": last, "ts": now, "account_id": order["account_id"],
                  "instrument": order["instrument"], "reason": reason}
        self.event_buffer.push(make_event("order_update", update, now))

    def on_order_status(self, order_id: int, status: str, filled: int, remaining: int, avg: float,
                        last: float, ts: float | None = None) -> None:
        order = self.orders.get(order_id)
        translated = STATUS_TO_UPDATE.get(status)
        if order is None or translated is None:
            return
        if translated == "OPEN" and filled > 0:
            translated = "PARTIALLY_FILLED"
        self.push_update(order, translated, filled, remaining, avg or None, last or None, ts)

    async def place_order(self, order: dict[str, Any]) -> None:
        contract = self.contracts[order["instrument"]]
        reason = check_order(order, contract.get("min_tick"))
        if reason:
            raise ValueError(reason)
        if order["order_type"] in ("STP", "LMT"):
            field = "stop_price" if order["order_type"] == "STP" else "limit_price"
            order[field] = round_price(order[field], contract["min_tick"], order["side"], order["order_type"])
        self.orders[order["order_id"]] = order
        spec = {"action": order["side"], "qty": order["qty"], "order_type": order["order_type"],
                "stop_price": order["stop_price"], "limit_price": order["limit_price"], "tif": order["tif"],
                "outside_rth": order["outside_rth"], "oca_group": order["oca_group"], "oca_type": order["oca_type"],
                "account": order["account_id"]}
        await self.send(lambda: self.transport.place_order(order["order_id"], contract, spec))

    async def cancel_order(self, order_id: int) -> None:
        await self.send(lambda: self.transport.cancel_order(order_id))

    def instrument_of(self, row: dict) -> Instrument | None:
        return self.identity.get((row.get("sec_type"), row.get("trading_class"), row.get("venue")))

    async def get_executions(self, since: float | None = None) -> list[dict[str, Any]]:
        req_id = self.next_request_id()
        rows = await self.wait_for(("executions", req_id), self.request_timeout_s,
                                   lambda: self.transport.req_executions(req_id, since))
        return rows

    def on_exec_details(self, req_id: int, row: dict) -> None:
        instrument = self.instrument_of(row)
        if instrument is None or row.get("order_id") is None or not row.get("currency"):
            logger.warning("execution row dropped, it cannot be attributed",
                           extra={"event": "execution_row_dropped", "row": row})
            return
        side = "BUY" if row["side"] == BOT else "SELL"
        self.collected.setdefault(("executions", req_id), []).append({
            "order_id": row["order_id"], "exec_id": row["exec_id"], "ts": row["ts"], "side": side,
            "qty": row["qty"], "price": row["price"], "instrument": instrument, "account_id": row["account"],
            "currency": row["currency"]})

    def on_exec_details_end(self, req_id: int) -> None:
        self.resolve_waiter(("executions", req_id), list(self.collected.get(("executions", req_id), [])))

    async def get_open_orders(self, timeout: float = 5.0) -> list[dict[str, Any]]:
        return await self.wait_for(("open_orders", 0), timeout, self.transport.req_open_orders)

    def on_open_order(self, row: dict) -> None:
        instrument = self.instrument_of(row)
        if instrument is not None:
            self.collected.setdefault(("open_orders", 0), []).append({**row, "instrument": instrument})

    def on_open_order_end(self) -> None:
        self.resolve_waiter(("open_orders", 0), list(self.collected.get(("open_orders", 0), [])))

    async def get_positions(self, timeout: float | None = None) -> list[dict]:
        timeout = self.position_timeout_s if timeout is None else timeout
        return await self.wait_for(("positions", 0), timeout, self.transport.req_positions)

    def on_position(self, row: dict) -> None:
        instrument = self.instrument_of(row)
        if instrument is not None:
            self.collected.setdefault(("positions", 0), []).append({
                "account_id": row["account"], "instrument": instrument, "size": int(row["size"]),
                "avg_cost": float(row["avg_cost"]), "currency": instrument.currency})

    def on_position_end(self) -> None:
        self.resolve_waiter(("positions", 0), list(self.collected.get(("positions", 0), [])))

    def on_account_value(self, account: str, currency: str, key: str, value: str) -> None:
        if key == "AvailableFunds" and currency:
            self.event_buffer.push(make_event("balance_update", {"account_id": account, "currency": currency,
                                                                 "available": float(value)}, time.time()))

    def history_instrument(self, instrument: Instrument, today: date) -> dict:
        contract = self.contracts[instrument]
        profile = exchange_profile(instrument)
        if instrument.asset_class.value != "future":
            return contract
        earlier = today - timedelta(days=ROLL_THIN_DAYS)
        previous = profile.front_contract(instrument, earlier)
        if previous != contract["contract_month"]:
            for offset in range(ROLL_THIN_DAYS + 1):
                self.roll_mismatch.add((earlier + timedelta(days=offset)).isoformat())
            return {**contract, "contract_month": previous}
        return contract

    async def get_daily_history(self, instrument: Instrument, start: date, end: date, session: str) -> list[dict]:
        profile = exchange_profile(instrument)
        zone = ZoneInfo(profile.session(session).tz)
        now = self.clock.now()
        last_close = self.last_completed_close(profile, session, now)
        window_end = min(datetime.combine(end, datetime.max.time(), zone), last_close)
        days = (window_end.date() - start).days + 1
        contract = self.history_instrument(instrument, now.date())
        rows = await self.request_history(contract, window_end, f"{days} D", "1 min")
        return self.aggregate_daily(instrument, profile, session, rows, start, window_end)

    def last_completed_close(self, profile, session: str, now: datetime) -> datetime:
        day = now.astimezone(ZoneInfo(profile.session(session).tz)).date()
        for offset in range(0, 10):
            candidate = day - timedelta(days=offset)
            if profile.is_trading_day(candidate):
                close = profile.session_window(session, candidate)[1]
                if close <= now:
                    return close
        return now

    async def request_history(self, contract: dict, end: datetime, duration: str, bar_size: str) -> list[dict]:
        req_id = self.next_request_id()
        return await self.wait_for(("history", req_id), self.request_timeout_s,
                                   lambda: self.transport.req_historical_data(req_id, contract, end, duration, bar_size))

    def on_historical_bar(self, req_id: int, row: dict) -> None:
        self.collected.setdefault(("history", req_id), []).append(row)

    def on_historical_end(self, req_id: int) -> None:
        self.resolve_waiter(("history", req_id), list(self.collected.get(("history", req_id), [])))

    def aggregate_daily(self, instrument, profile, session, rows, start: date, window_end: datetime) -> list[dict]:
        days: dict[date, list[dict]] = {}
        for row in sorted(rows, key=lambda r: r["start_ts"]):
            moment = datetime.fromtimestamp(row["start_ts"], timezone.utc)
            window = profile.session_window_at(session, moment)
            if window is None or moment >= window_end:
                continue
            trade_day = window[1].astimezone(ZoneInfo(profile.session(session).tz)).date()
            if trade_day >= start:
                days.setdefault(trade_day, []).append({**row, "window": window})
        frame = []
        for day in sorted(days):
            bars, window = days[day], days[day][0]["window"]
            frame.append({"instrument": instrument, "date": day.isoformat(), "start_ts": window[0].timestamp(),
                          "end_ts": window[1].timestamp(), "open": bars[0]["open"],
                          "high": max(b["high"] for b in bars), "low": min(b["low"] for b in bars),
                          "close": bars[-1]["close"], "volume": sum(b["volume"] for b in bars),
                          "resolution_s": 86_400.0, "broker_ts": window[1].timestamp()})
        return frame

    async def get_intraday_seed_bars(self, instrument: Instrument, session: str) -> list:
        profile = exchange_profile(instrument)
        now = self.clock.now()
        close = self.last_completed_close(profile, session, now)
        open_at = profile.session_window_at(session, close - timedelta(seconds=1))[0]
        contract = self.history_instrument(instrument, now.date())
        bars: list[dict] = []
        chunk_end = close
        while chunk_end > open_at:
            rows = await self.request_history(contract, chunk_end, f"{SEED_CHUNK_BARS} S", "1 secs")
            bars = [{**r, "instrument": instrument, "end_ts": r["start_ts"] + 1.0, "resolution_s": 1.0,
                     "broker_ts": r["start_ts"] + 1.0} for r in rows] + bars
            chunk_end -= timedelta(seconds=SEED_CHUNK_BARS)
        return [b for b in bars if open_at.timestamp() <= b["start_ts"] < close.timestamp()]
