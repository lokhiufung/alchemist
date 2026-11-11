from __future__ import annotations

from datetime import date, timedelta

SEED_LOOKBACK_SESSIONS = 10
SESSION_MARGIN = timedelta(hours=6)
PRICE_KEYS = ("open", "high", "low", "close", "volume")
FINGERPRINT_FILE = "tape.sha256"
STARTING_FUNDS = 1_000_000.0
FILL_REFERENCES = ("close",)
BAR_SECONDS = 1.0
SYNTHETIC_PREFIX = "synthetic:"
REPLAY_DATE = date(2026, 4, 15)
PRIOR_SESSIONS = 60
SESSION_BARS = 23_400
BASE_PRICE = 5000.0
HIGH_PRICE = 5010.0
LOW_PRICE = 4990.0
TICK = 0.25
QUIET_BARS = 600
STEP_UP = 2.0
STEP_DOWN = 1.0
TARGET_PRICE = 5011.0
BAR_VOLUME = 100.0
