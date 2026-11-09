from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Callable
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from alchemist.instruments import Instrument

CME_NAME = "CME"
EUREX_NAME = "EUREX"
NASDAQ_NAME = "NASDAQ"

QUARTERLY_MONTHS = (3, 6, 9, 12)


@dataclass(frozen=True)
class Session:
    name: str
    open: time
    close: time
    tz: str

    @property
    def crosses_midnight(self) -> bool:
        return self.close <= self.open


@dataclass(frozen=True)
class RollRule:
    days_before_expiry: int
    trading_days: bool


def third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    first_friday = first + timedelta(days=(4 - first.weekday()) % 7)
    return first_friday + timedelta(days=14)


@dataclass(frozen=True)
class ExchangeProfile:
    name: str
    currency: str
    tz: str
    venue: str
    sessions: dict[str, Session]
    data_session: str
    holidays: frozenset[date]
    early_closes: dict[date, time]
    roll: RollRule | None
    session_end_cutoff_s: float
    broker_identity: Callable[["Instrument"], dict]
    aliases: dict[str, str] = field(default_factory=dict)

    def session(self, name: str) -> Session:
        return self.sessions[self.aliases.get(name, name)]

    def is_trading_day(self, d: date) -> bool:
        return d.weekday() < 5 and d not in self.holidays

    def session_window(self, name: str, d: date) -> tuple[datetime, datetime]:
        session = self.session(name)
        zone = ZoneInfo(session.tz)
        close = session.close
        if not session.crosses_midnight:
            close = self.early_closes.get(d, close)
        open_day = d - timedelta(days=1) if session.crosses_midnight else d
        return (datetime.combine(open_day, session.open, zone),
                datetime.combine(d, close, zone))

    def is_open(self, name: str, at: datetime) -> bool:
        local_day = at.astimezone(ZoneInfo(self.session(name).tz)).date()
        for d in (local_day, local_day + timedelta(days=1)):
            if not self.is_trading_day(d):
                continue
            start, end = self.session_window(name, d)
            if start <= at < end:
                return True
        return False

    def _roll_date(self, expiry: date) -> date:
        assert self.roll is not None
        if not self.roll.trading_days:
            return expiry - timedelta(days=self.roll.days_before_expiry)
        day, remaining = expiry, self.roll.days_before_expiry
        while remaining:
            day -= timedelta(days=1)
            if self.is_trading_day(day):
                remaining -= 1
        return day

    def front_contract(self, instrument: "Instrument", on: date) -> str:
        if self.roll is None or instrument.asset_class.value != "future":
            raise ValueError(f"{instrument.symbol} has no futures roll on {self.name}")
        year, month = on.year, on.month
        while True:
            if month in QUARTERLY_MONTHS and on < self._roll_date(third_friday(year, month)):
                return f"{year}{month:02d}"
            month += 1
            if month > 12:
                year, month = year + 1, 1


def _future_identity(venue: str) -> Callable[["Instrument"], dict]:
    def identity(instrument: "Instrument") -> dict:
        return {"venue": venue, "currency": instrument.currency,
                "trading_class": instrument.symbol, "sec_type": instrument.asset_class.value}
    return identity


def _stock_identity(instrument: "Instrument") -> dict:
    return {"venue": "SMART", "currency": instrument.currency,
            "trading_class": instrument.symbol, "sec_type": instrument.asset_class.value}


US_HOLIDAYS_2026 = frozenset(date(2026, m, d) for m, d in (
    (1, 1), (1, 19), (2, 16), (4, 3), (5, 25), (6, 19), (7, 3), (9, 7), (11, 26), (12, 25)))
US_EARLY_CLOSES_2026 = {date(2026, 11, 27): time(13, 0), date(2026, 12, 24): time(13, 0)}

EUREX_HOLIDAYS_2026 = frozenset(date(2026, m, d) for m, d in (
    (1, 1), (4, 3), (4, 6), (5, 1), (12, 24), (12, 25), (12, 31)))

_NY = "America/New_York"

CME = ExchangeProfile(
    name=CME_NAME, currency="USD", tz=_NY, venue="CME",
    sessions={"NY_RTH": Session("NY_RTH", time(9, 30), time(16, 0), _NY),
              "NY_GLOBEX": Session("NY_GLOBEX", time(18, 0), time(17, 0), _NY)},
    data_session="NY_GLOBEX", holidays=US_HOLIDAYS_2026, early_closes=US_EARLY_CLOSES_2026,
    roll=RollRule(days_before_expiry=8, trading_days=False), session_end_cutoff_s=60.0,
    broker_identity=_future_identity("CME"), aliases={"regular": "NY_RTH"})

EUREX = ExchangeProfile(
    name=EUREX_NAME, currency="EUR", tz="Europe/Berlin", venue="EUREX",
    sessions={"FRA_DAY": Session("FRA_DAY", time(8, 0), time(22, 0), "Europe/Berlin")},
    data_session="FRA_DAY", holidays=EUREX_HOLIDAYS_2026, early_closes={},
    roll=RollRule(days_before_expiry=3, trading_days=True), session_end_cutoff_s=60.0,
    broker_identity=_future_identity("EUREX"), aliases={"regular": "FRA_DAY"})

NASDAQ = ExchangeProfile(
    name=NASDAQ_NAME, currency="USD", tz=_NY, venue="NASDAQ",
    sessions={"NY_RTH": Session("NY_RTH", time(9, 30), time(16, 0), _NY)},
    data_session="NY_RTH", holidays=US_HOLIDAYS_2026, early_closes=US_EARLY_CLOSES_2026,
    roll=None, session_end_cutoff_s=60.0,
    broker_identity=_stock_identity, aliases={"regular": "NY_RTH"})

EXCHANGES: dict[str, ExchangeProfile] = {p.name: p for p in (CME, EUREX, NASDAQ)}


def exchange_profile(instrument_or_name: "Instrument | str") -> ExchangeProfile:
    name = instrument_or_name if isinstance(instrument_or_name, str) else instrument_or_name.exchange
    if name not in EXCHANGES:
        raise ValueError(f"unknown exchange {name!r}; known: {sorted(EXCHANGES)}")
    return EXCHANGES[name]
