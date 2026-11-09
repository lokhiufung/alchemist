from __future__ import annotations

import math
import threading
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

PRICED_FIELD = {"STP": "stop_price", "LMT": "limit_price"}
OCA_TYPES = (1, 2, 3)
ROUND_UP = {("BUY", "STP"), ("SELL", "LMT")}


def round_price(price: float, tick_size: float, side: str, order_type: str) -> float:
    mode = ROUND_CEILING if (side, order_type) in ROUND_UP else ROUND_FLOOR
    ticks = (Decimal(repr(price)) / Decimal(repr(tick_size))).to_integral_value(rounding=mode)
    return float(ticks * Decimal(repr(tick_size)))


def invalid_price(price: Any) -> bool:
    if isinstance(price, bool) or not isinstance(price, (int, float)):
        return True
    return math.isnan(price) or price <= 0


def check_order(order: dict[str, Any], tick: float | None) -> str | None:
    if tick is None or not tick > 0:
        return f"order {order.get('order_id')}: contract has no known minimum tick"
    field = PRICED_FIELD.get(order["order_type"])
    if field and invalid_price(order.get(field)):
        return f"order {order.get('order_id')}: {field} {order.get(field)!r} is not a positive number"
    if order.get("oca_group") and order.get("oca_type") not in OCA_TYPES:
        return f"order {order.get('order_id')}: oca_type {order.get('oca_type')!r} must be one of {OCA_TYPES}"
    return None


class OrderIdSequence:
    def __init__(self, start: int = 1) -> None:
        self.following = start
        self.lock = threading.Lock()

    def next(self) -> int:
        with self.lock:
            taken = self.following
            self.following += 1
            return taken

    def raise_floor(self, floor: int) -> None:
        with self.lock:
            self.following = max(self.following, floor)
