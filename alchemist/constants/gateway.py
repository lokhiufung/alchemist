from __future__ import annotations

ORDER_SENT_KEYS = ("order_id", "account_id", "instrument", "side", "qty", "order_type", "stop_price", "limit_price",
                   "oca_group", "tif", "outside_rth")
PRICED_FIELD = {"STP": "stop_price", "LMT": "limit_price"}
OCA_TYPES = (1, 2, 3)
ROUND_UP = {("BUY", "STP"), ("SELL", "LMT")}
RECONNECT_BASE_S = 15.0
RECONNECT_CAP_S = 120.0
REPLY_TIMEOUT_S = 30.0
COMMAND_TYPES = frozenset({
    "place_order", "cancel_order", "resolve_contract", "subscribe", "unsubscribe", "market_data_type",
    "get_daily_history", "get_intraday_seed_bars", "get_executions", "get_open_orders", "get_positions",
    "poll_positions", "get_commissions", "resubscribe", "reconnect", "restart_broker_process", "order_id_floor",
    "set_instrument_sessions",
})
