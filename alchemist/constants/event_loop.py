from __future__ import annotations

from alchemist.constants.messages import REPLY_EVENT_TYPES

BAR_HISTORY = 5000
FLUSH_INTERVAL_S = 60.0
POLL_S = 0.005
BAR_KEYS = ("start_ts", "end_ts", "open", "high", "low", "close", "volume", "resolution_s", "broker_ts")
DAYS_PER_MONTH = 31
CONTROL_EVENTS = REPLY_EVENT_TYPES | frozenset({"order_update", "balance_update", "repaired_fill", "position_update",
                            "deploy_request", "connection_lost", "connection_restored", "os_signal",
                            "end_of_data"})
