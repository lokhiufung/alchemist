from __future__ import annotations

import asyncio
import inspect
import logging
import time
from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Awaitable, Callable

from alchemist.config import GatewayConfig
from alchemist.constants.gateway import COMMAND_TYPES, ORDER_SENT_KEYS
from alchemist.instruments import Instrument
from alchemist.messages import Command, make_event, make_reply

__all__ = ["Gateway", "log_broker", "reconnect_delay"]

broker_logger = logging.getLogger("gateway")

Sink = Callable[[Command], Awaitable[None] | None]


def log_broker(event: str, fields: dict[str, Any], caller_depth: int = 1) -> None:
    broker_logger.info(event.replace("_", " "), stacklevel=caller_depth + 1,
                       extra={"event": event, **fields})


def reconnect_delay(attempt: int, base_s: float, cap_s: float) -> float:
    return min(cap_s, base_s * 2 ** (attempt - 1))


class Gateway(ABC):
    """Gateway map: base of every broker gateway and of the simulated broker."""

    def __init__(self, inbound, outbound, config: GatewayConfig,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        self.inbound = inbound
        self.outbound = outbound
        self.config = config
        self.sleep = sleep
        self.subscriptions: dict[Instrument, Any] = {}
        self.sinks: dict[str, Sink] = {}
        self.tasks: list[asyncio.Task] = []
        self.reconnecting = False

    def event_time(self) -> float:
        return time.time()

    def push(self, event_type: str, data: dict[str, Any], event_time: float | None = None) -> None:
        self.inbound.push(make_event(event_type, data, self.event_time() if event_time is None else event_time))

    def log_order_sent(self, order: dict[str, Any]) -> None:
        log_broker("order_sent", {key: order.get(key) for key in ORDER_SENT_KEYS}, caller_depth=2)

    def log_cancel_sent(self, order_id: int) -> None:
        log_broker("cancel_sent", {"order_id": order_id}, caller_depth=2)

    def push_broker_event(self, event_type: str, data: dict[str, Any], ts: float, caller_depth: int = 1) -> None:
        log_broker(f"broker_{event_type}", data, caller_depth + 1)
        self.inbound.push(make_event(event_type, data, ts))

    async def start(self) -> None:
        await self.connect()
        await self.subscribe_order_update()
        await self.subscribe_account_update()
        loop = asyncio.get_running_loop()
        self.outbound.bind_loop(loop)
        self.tasks = [loop.create_task(self.run_commands()), loop.create_task(self.run_heartbeat())]

    async def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        await self.disconnect()

    async def run_commands(self) -> None:
        while True:
            command = await self.outbound.get()
            try:
                await self.run_command(command)
            except Exception:
                broker_logger.exception("command consumer failed", extra={
                    "event": "command_consumer_failed", "command_type": command["command_type"]})
            finally:
                self.outbound.done()

    async def run_command(self, command: Command) -> None:
        kind = command["command_type"]
        sink = self.sinks.get(kind)
        if sink is not None:
            result = sink(command)
            if asyncio.iscoroutine(result):
                await result
            return
        if kind not in COMMAND_TYPES:
            broker_logger.error("unknown command", extra={"event": "unknown_command", "command_type": kind})
            self.inbound.push(make_reply(command, self.event_time(), error=f"unknown command {kind}"))
            return
        try:
            result = getattr(self, kind)(**command["data"])
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            self.on_command_failed(command, exc)
            self.inbound.push(make_reply(command, self.event_time(), error=repr(exc),
                                         terminal=self.failure_is_terminal(kind)))
            return
        self.inbound.push(make_reply(command, self.event_time(), result=result))

    def on_command_failed(self, command: Command, exc: Exception) -> None:
        broker_logger.warning("command failed", extra={
            "event": "command_failed", "command_type": command["command_type"], "reason": repr(exc)})
        if command["command_type"] == "place_order":
            order = command["data"]["order"]
            self.push("order_update", {
                "order_id": order["order_id"], "status": "REJECTED", "filled": 0, "remaining": order["qty"],
                "avg_fill_price": None, "last_fill_price": None, "ts": self.event_time(),
                "account_id": order["account_id"], "instrument": order["instrument"], "reason": str(exc)})

    def failure_is_terminal(self, command_type: str) -> bool:
        return command_type == "get_intraday_seed_bars" and self.intraday_seed_failure_is_fatal()

    async def run_heartbeat(self) -> None:
        while True:
            if not self.reconnecting:
                if await self.ping(self.config.heartbeat_timeout_s):
                    self.push("heartbeat", {})
                else:
                    await self.reconnect_forever()
            await self.sleep(self.config.heartbeat_interval_s)

    async def reconnect(self) -> None:
        await self.reconnect_forever()

    async def reconnect_forever(self) -> None:
        if self.reconnecting:
            return
        self.reconnecting = True
        try:
            self.push("connection_lost", {})
            attempt = 0
            while True:
                attempt += 1
                try:
                    await self.disconnect()
                    await self.connect()
                    break
                except Exception as exc:
                    level = logging.CRITICAL if attempt > 1 else logging.ERROR
                    broker_logger.log(level, "reconnect failed", extra={
                        "event": "reconnect_failed", "attempt": attempt, "reason": repr(exc)})
                    await self.sleep(reconnect_delay(attempt, self.config.reconnect_base_s,
                                                     self.config.reconnect_cap_s))
            await self.restore_streams()
            self.push("connection_restored", {"next_order_id": self.order_id_floor()})
        finally:
            self.reconnecting = False

    async def restore_streams(self) -> None:
        await self.subscribe_order_update()
        await self.subscribe_account_update()
        for contract in list(self.subscriptions.values()):
            await self.subscribe_tick(contract)

    async def resubscribe(self, instruments: list[Instrument]) -> None:
        for instrument in instruments:
            contract = self.subscriptions.get(instrument)
            if contract is not None:
                await self.unsubscribe_tick(contract)
                await self.subscribe_tick(contract)

    async def poll_positions(self) -> list[dict]:
        return await self.get_positions()

    async def subscribe(self, contract: Any) -> None:
        self.subscriptions[contract["instrument"]] = contract
        await self.subscribe_tick(contract)

    async def unsubscribe(self, contract: Any) -> None:
        self.subscriptions.pop(contract["instrument"], None)
        await self.unsubscribe_tick(contract)

    @abstractmethod
    async def restart_broker_process(self) -> None: ...

    @abstractmethod
    def order_id_floor(self) -> int: ...

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def subscribe_order_update(self) -> None: ...

    @abstractmethod
    async def resolve_contract(self, instrument: Instrument) -> Any: ...

    @abstractmethod
    async def subscribe_tick(self, contract: Any) -> None: ...

    @abstractmethod
    def next_order_id(self) -> int: ...

    @abstractmethod
    async def place_order(self, order: dict[str, Any]) -> None: ...

    @abstractmethod
    async def cancel_order(self, order_id: int) -> None: ...

    @abstractmethod
    async def get_executions(self, since: float | None = None) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def get_open_orders(self, timeout: float = 5.0) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def ping(self, timeout: float) -> bool: ...

    @abstractmethod
    async def get_daily_history(self, instrument: Instrument, start: date,
                                end: date, session: str) -> Any:
        """Behaviours 7, 11: daily frame in broker-neutral units, no look-ahead."""

    @abstractmethod
    async def get_intraday_seed_bars(self, instrument: Instrument, session: str) -> list: ...

    @abstractmethod
    async def subscribe_account_update(self) -> None: ...

    @abstractmethod
    async def get_positions(self, timeout: float = 5.0) -> list[dict]: ...

    async def market_data_type(self, instrument: Instrument) -> tuple[str, float | None]:
        return ("tick_update", None)

    def market_data_ready(self) -> bool:
        return True

    def set_instrument_sessions(self, sessions: dict[Instrument, str]) -> None:
        return None

    def roll_mismatch_dates(self) -> frozenset:
        return frozenset()

    async def get_commissions(self) -> dict[str, dict]:
        return {}

    def intraday_seed_failure_is_fatal(self) -> bool:
        return False

    async def unsubscribe_tick(self, contract: Any) -> None: ...

    async def start_feed(self) -> None:
        return None
