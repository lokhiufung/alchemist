from __future__ import annotations

from datetime import timedelta

SEED_LOOKBACK_SESSIONS = 10
SESSION_MARGIN = timedelta(hours=6)
PRICE_KEYS = ("open", "high", "low", "close", "volume")
FINGERPRINT_FILE = "tape.sha256"
STARTING_FUNDS = 1_000_000.0
FILL_REFERENCES = ("close",)
BAR_SECONDS = 1.0
