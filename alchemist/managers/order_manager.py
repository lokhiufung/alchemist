from __future__ import annotations

from enum import Enum

from alchemist.instruments import Instrument

OCA_GROUP_MAX_LEN = 48


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
CANCEL_REJECTED_INTERNALLY = "CANCEL_REJECTED_INTERNALLY"


class OrderManager:
    """Order Manager map."""

    def __init__(self, gateway, account_id: str, slot: str) -> None: ...

    async def place_order(self, instrument: Instrument, side: str, qty: int,
                          order_type: str, stop_price: float | None = None,
                          limit_price: float | None = None,
                          oca_group: str | None = None) -> dict: ...

    async def place_group(self, orders: list[dict]) -> str:
        """One fresh OCA type 2 DAY group; returns its name."""
        ...

    def on_order_update(self, update: dict) -> dict | None:
        """Behaviour 3: the changed order with its fill delta, or None."""
        ...

    def get_order(self, order_id: int) -> dict | None: ...
    def get_submitted_orders(self, group: str | None = None) -> list[dict]: ...
    def get_open_orders(self, group: str | None = None) -> list[dict]: ...
    def get_cancelling_orders(self, group: str | None = None) -> list[dict]: ...
    def get_order_group(self, group: str) -> list[dict]: ...
    def is_group_closed(self, group: str) -> bool: ...

    async def cancel_order(self, order_id: int) -> str | None: ...
    async def cancel_group(self, group: str) -> None: ...
    async def cancel_all(self) -> None: ...
