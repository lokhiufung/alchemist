from __future__ import annotations

ORDER_SENT_KEYS = ("order_id", "account_id", "instrument", "side", "qty", "order_type", "stop_price", "limit_price",
                   "oca_group", "tif", "outside_rth")
PRICED_FIELD = {"STP": "stop_price", "LMT": "limit_price"}
OCA_TYPES = (1, 2, 3)
ROUND_UP = {("BUY", "STP"), ("SELL", "LMT")}
