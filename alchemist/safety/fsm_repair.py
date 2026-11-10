from __future__ import annotations

import logging

from alchemist.config import SafetyConfig
from alchemist.messages import make_event

logger = logging.getLogger("safety")


def pending_orders(strategy) -> list[dict]:
    return strategy.get_submitted_orders() + strategy.get_open_orders() + strategy.get_cancelling_orders()


def filled_update(order: dict, rows: list[dict]) -> dict:
    qty = sum(r["qty"] for r in rows)
    average = sum(r["price"] * r["qty"] for r in rows) / qty
    return {"order_id": order["order_id"], "status": "FILLED" if qty >= order["qty"] else "PARTIALLY_FILLED",
            "filled": qty, "remaining": max(order["qty"] - qty, 0), "avg_fill_price": average,
            "last_fill_price": rows[-1]["price"], "ts": max(r["ts"] for r in rows),
            "account_id": order["account_id"], "instrument": order["instrument"], "reason": None}


def rejected_update(order: dict, now: float) -> dict:
    return {"order_id": order["order_id"], "status": "REJECTED", "filled": 0, "remaining": order["qty"],
            "avg_fill_price": None, "last_fill_price": None, "ts": now, "account_id": order["account_id"],
            "instrument": order["instrument"], "reason": "absent at the broker after the minimum age"}


async def repair_stuck_strategies(strategies: list, gateway, event_buffer, now: float,
                                  config: SafetyConfig | None = None) -> int:
    config = config or SafetyConfig()
    pending = [order for strategy in strategies for order in pending_orders(strategy)]
    if not pending:
        return 0
    try:
        executions = await gateway.get_executions(None)
        open_orders = await gateway.get_open_orders()
    except Exception as exc:
        logger.warning("repair skipped, the broker state is unreadable",
                       extra={"event": "repair_skipped", "reason": repr(exc)})
        return 0
    by_order: dict[int, list[dict]] = {}
    for row in executions:
        by_order.setdefault(row["order_id"], []).append(row)
    open_ids = {o["order_id"] for o in open_orders}
    repaired = 0
    for order in pending:
        rows = by_order.get(order["order_id"])
        if rows:
            event_buffer.push(make_event("repaired_fill", filled_update(order, rows), now))
            repaired += 1
        elif order["order_id"] not in open_ids and now - order["submitted_at"] >= config.reject_synthesis_min_age_s:
            event_buffer.push(make_event("order_update", rejected_update(order, now), now))
            repaired += 1
    return repaired
