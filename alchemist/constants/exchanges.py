from __future__ import annotations

from datetime import date, time

CME_NAME = "CME"
EUREX_NAME = "EUREX"
NASDAQ_NAME = "NASDAQ"
QUARTERLY_MONTHS = (3, 6, 9, 12)
US_HOLIDAYS_2026 = frozenset(date(2026, m, d) for m, d in (
    (1, 1), (1, 19), (2, 16), (4, 3), (5, 25), (6, 19), (7, 3), (9, 7), (11, 26), (12, 25)))
US_EARLY_CLOSES_2026 = {date(2026, 11, 27): time(13, 0), date(2026, 12, 24): time(13, 0)}
EUREX_HOLIDAYS_2026 = frozenset(date(2026, m, d) for m, d in (
    (1, 1), (4, 3), (4, 6), (5, 1), (12, 24), (12, 25), (12, 31)))
