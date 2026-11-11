from __future__ import annotations

import hashlib
import logging
import time
from enum import Enum

from alchemist.constants.order_manager import (
    CANCEL_REJECTED_INTERNALLY,
    OCA_SLOT_PREFIX_LEN,
    OCA_TYPE,
    ORDER_RECORD_ONLY,
    STORE_NAMES,
)
from alchemist.instruments import Instrument

logger = logging.getLogger("om")


class OrderState(str, Enum):
    SUBMITTED = "SUBMITTED"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLING = "CANCELLING"
    CANCEL_REJECTED = "CANCEL_REJECTED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


CLOSED = {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED}
ACKNOWLEDGED = {OrderState.SUBMITTED, OrderState.OPEN, OrderState.PARTIALLY_FILLED}


class OrderManager:
    def __init__(self, gateway, account_id: str, slot: str) -> None:
        self.gateway = gateway
        self.account_id = account_id
        self.slot = slot
        self.stores: dict[str, dict[int, dict]] = {name: {} for name in STORE_NAMES}
        self.group_sequence = 0

    async def place_order(self, instrument: Instrument, side: str, qty: int,
                          order_type: str, stop_price: float | None = None,
                          limit_price: float | None = None,
                          oca_group: str | None = None) -> dict:
        order = self.stamp(self.gateway.next_order_id(), instrument, side, qty, order_type,
                           stop_price, limit_price, oca_group)
        await self.send(order)
        return dict(order)

    async def place_group(self, orders: list[dict]) -> str:
        ids = [self.gateway.next_order_id() for _ in orders]
        group = self.fresh_group_name(ids[0])
        stamped = [
            self.stamp(order_id, o["instrument"], o["side"], o["qty"], o["order_type"],
                       o.get("stop_price"), o.get("limit_price"), group)
            for order_id, o in zip(ids, orders)
        ]
        for order in stamped:
            await self.send(order)
        return group

    def fresh_group_name(self, first_order_id: int) -> str:
        self.group_sequence += 1
        digest = hashlib.sha1(f"{self.slot}|{self.group_sequence}|{first_order_id}".encode()).hexdigest()[:12]
        return f"{self.slot[:OCA_SLOT_PREFIX_LEN]}-oca-{digest}"

    def stamp(self, order_id: int, instrument: Instrument, side: str, qty: int, order_type: str,
              stop_price: float | None, limit_price: float | None, oca_group: str | None) -> dict:
        order = {
            "order_id": order_id, "account_id": self.account_id, "instrument": instrument,
            "side": side, "qty": qty, "order_type": order_type,
            "stop_price": stop_price, "limit_price": limit_price,
            "oca_group": oca_group, "oca_type": OCA_TYPE if oca_group else None,
            "tif": "DAY", "outside_rth": True, "slot": self.slot,
            "status": OrderState.SUBMITTED.value, "filled": 0, "avg_fill_price": None,
            "submitted_at": time.time(),
        }
        self.stores["submitted"][order_id] = order
        return order

    async def send(self, order: dict) -> None:
        logger.info("order placed", extra={
            "event": "order_placed", "strategy": self.slot, "order_id": order["order_id"],
            "instrument": order["instrument"].symbol, "side": order["side"], "qty": order["qty"],
            "order_type": order["order_type"], "stop_price": order["stop_price"],
            "limit_price": order["limit_price"], "oca_group": order["oca_group"]})
        await self.gateway.place_order({k: v for k, v in order.items()
                                        if k not in ORDER_RECORD_ONLY})

    def locate(self, order_id: int) -> str | None:
        return next((name for name, store in self.stores.items() if order_id in store), None)

    def move(self, order_id: int, source: str, target: str) -> dict:
        order = self.stores[source].pop(order_id)
        self.stores[target][order_id] = order
        return order

    def on_order_update(self, update: dict) -> dict | None:
        order_id = update["order_id"]
        where = self.locate(order_id)
        if where is None or where == "closed":
            return None
        try:
            status = OrderState(str(update["status"]).upper())
        except ValueError:
            logger.warning("order update has an unknown status", extra={
                "event": "unknown_order_status", "strategy": self.slot, "order_id": order_id,
                "status": update["status"]})
            return None
        order = self.stores[where][order_id]
        delta = self.apply_fill(order, update)
        moved = self.apply_status(order, order_id, where, status, update)
        if not delta and not moved:
            return None
        return {**order, "fill_delta": delta}

    def apply_fill(self, order: dict, update: dict) -> int:
        reported = update.get("filled") or 0
        delta = max(reported - order["filled"], 0)
        if not delta:
            return 0
        if update.get("avg_fill_price") is not None:
            order["avg_fill_price"] = update["avg_fill_price"]
        elif update.get("last_fill_price") is not None:
            previous = (order["avg_fill_price"] or 0.0) * order["filled"]
            order["avg_fill_price"] = (previous + update["last_fill_price"] * delta) / (order["filled"] + delta)
        order["filled"] += delta
        return delta

    def apply_status(self, order: dict, order_id: int, where: str, status: OrderState, update: dict) -> bool:
        if status in CLOSED:
            self.move(order_id, where, "closed")
            order["status"] = status.value
            self.log_closed(order, status, update)
            return True
        if status is OrderState.CANCEL_REJECTED:
            if where != "cancelling":
                return False
            self.move(order_id, "cancelling", "open")
            order["status"] = OrderState.OPEN.value
            return True
        if where == "submitted" and status in ACKNOWLEDGED:
            self.move(order_id, "submitted", "open")
            order["status"] = OrderState.OPEN.value
        if where != "cancelling" and order["filled"]:
            order["status"] = OrderState.PARTIALLY_FILLED.value
        return where == "submitted"

    def log_closed(self, order: dict, status: OrderState, update: dict) -> None:
        fields = {"strategy": self.slot, "order_id": order["order_id"], "instrument": order["instrument"].symbol}
        if status is OrderState.FILLED:
            logger.info("order filled", extra={"event": "order_filled", **fields, "side": order["side"],
                                               "filled": order["filled"], "avg_fill_price": order["avg_fill_price"]})
        elif status is OrderState.REJECTED:
            logger.warning("order rejected", extra={"event": "order_rejected", **fields, "reason": update.get("reason")})
        elif status is OrderState.CANCELLED:
            logger.info("order cancelled", extra={"event": "order_cancelled", **fields, "filled": order["filled"]})

    def get_order(self, order_id: int) -> dict | None:
        where = self.locate(order_id)
        return dict(self.stores[where][order_id]) if where else None

    def listing(self, store: str, group: str | None) -> list[dict]:
        orders = self.stores[store].values()
        return [dict(o) for o in sorted(orders, key=lambda o: o["order_id"])
                if group is None or o["oca_group"] == group]

    def get_submitted_orders(self, group: str | None = None) -> list[dict]:
        return self.listing("submitted", group)

    def get_open_orders(self, group: str | None = None) -> list[dict]:
        return self.listing("open", group)

    def get_cancelling_orders(self, group: str | None = None) -> list[dict]:
        return self.listing("cancelling", group)

    def get_order_group(self, group: str) -> list[dict]:
        found = [o for store in STORE_NAMES for o in self.listing(store, group)]
        return sorted(found, key=lambda o: o["order_id"])

    def is_group_closed(self, group: str) -> bool:
        orders = self.get_order_group(group)
        return bool(orders) and all(OrderState(o["status"]) in CLOSED for o in orders)

    async def cancel_order(self, order_id: int) -> str | None:
        if self.locate(order_id) != "open":
            logger.warning("cancel refused, the order is not open", extra={
                "event": "cancel_refused", "strategy": self.slot, "order_id": order_id})
            return CANCEL_REJECTED_INTERNALLY
        await self.send_cancel(order_id)
        return None

    async def send_cancel(self, order_id: int) -> None:
        order = self.move(order_id, "open", "cancelling")
        order["status"] = OrderState.CANCELLING.value
        await self.gateway.cancel_order(order_id)

    async def cancel_group(self, group: str) -> None:
        for order in self.get_open_orders(group):
            await self.send_cancel(order["order_id"])

    async def cancel_all(self) -> None:
        for order in self.get_open_orders():
            await self.send_cancel(order["order_id"])
