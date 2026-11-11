from __future__ import annotations

EVENT_TYPES = ("tick_update", "bar_update", "order_update", "position_update", "balance_update", "repaired_fill",
               "heartbeat", "connection_lost", "connection_restored", "reply", "executions", "open_orders", "positions",
               "deploy_request", "os_signal",
               "end_of_data")
REPLY_EVENTS = {"get_executions": "executions", "get_open_orders": "open_orders", "get_positions": "positions",
                "poll_positions": "positions"}
REPLY_EVENT_TYPES = frozenset({"reply", *REPLY_EVENTS.values()})
