from __future__ import annotations

BAR_HISTORY = 5000
FLUSH_INTERVAL_S = 60.0
TERMINAL_SUCCESS = ("registered", "noop")
NEXT_SESSION_LOOKAHEAD_DAYS = 10
POLL_S = 0.005
RECHECK_OUTSIDE_SESSION_S = 30.0
BAR_KEYS = ("start_ts", "end_ts", "open", "high", "low", "close", "volume", "resolution_s", "broker_ts")
DAYS_PER_MONTH = 31
CONTROL_EVENTS = frozenset({"order_update", "balance_update", "repaired_fill", "position_update", "book_op",
                            "end_of_data"})
