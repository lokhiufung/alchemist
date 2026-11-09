import re
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from alchemist.exchanges import exchange_profile
from alchemist.instruments import FDAX, MES, AssetClass, Instrument


def test_mes_on_cme_in_usd():
    assert exchange_profile(MES).currency == "USD"
    assert exchange_profile(FDAX).tz == "Europe/Berlin"


def test_golden_date_window():
    start, end = exchange_profile(MES).session_window("NY_RTH", date(2026, 4, 15))
    assert (end - start).total_seconds() == 23_400


def test_stock_has_no_roll():
    stock = Instrument(AssetClass.STOCK, "AAPL", "NASDAQ", "USD", 1.0, 0.01)
    with pytest.raises(ValueError):
        exchange_profile("NASDAQ").front_contract(stock, date(2026, 4, 15))


def test_cme_front_month_rolls_before_expiry():
    cme = exchange_profile("CME")
    assert cme.front_contract(MES, date(2026, 3, 13)) == "202606"
    assert cme.front_contract(MES, date(2026, 3, 11)) == "202603"


def test_eurex_rolls_three_trading_days_before_expiry():
    eurex = exchange_profile("EUREX")
    assert eurex.front_contract(FDAX, date(2026, 3, 17)) == "202606"
    assert eurex.front_contract(FDAX, date(2026, 3, 16)) == "202603"


def test_early_close_shortens_window():
    start, end = exchange_profile("CME").session_window("NY_RTH", date(2026, 11, 27))
    assert (end - start).total_seconds() == 3.5 * 3600


def test_weekend_and_holiday_are_not_trading_days():
    cme = exchange_profile("CME")
    assert not cme.is_trading_day(date(2026, 4, 18))
    assert not cme.is_trading_day(date(2026, 12, 25))
    assert cme.is_trading_day(date(2026, 4, 15))


def test_is_open_inside_and_outside_rth():
    cme = exchange_profile("CME")
    ny = ZoneInfo("America/New_York")
    assert cme.is_open("NY_RTH", datetime(2026, 4, 15, 9, 30, tzinfo=ny))
    assert not cme.is_open("NY_RTH", datetime(2026, 4, 15, 16, 0, tzinfo=ny))


def test_globex_crosses_midnight_and_is_open_in_the_evening():
    cme = exchange_profile("CME")
    ny = ZoneInfo("America/New_York")
    assert cme.session("NY_GLOBEX").crosses_midnight
    assert cme.is_open("NY_GLOBEX", datetime(2026, 4, 15, 20, 0, tzinfo=ny))
    assert not cme.is_open("NY_GLOBEX", datetime(2026, 4, 15, 17, 30, tzinfo=ny))


def test_regular_alias_maps_to_rth():
    assert exchange_profile("EUREX").session("regular").name == "FRA_DAY"
    assert exchange_profile("NASDAQ").session("regular").open == time(9, 30)


def test_broker_identity_follows_profile():
    identity = exchange_profile("EUREX").broker_identity(FDAX)
    assert identity["currency"] == "EUR" and identity["trading_class"] == "FDAX"


def test_no_branch_on_exchange_name():
    offenders = [p for p in Path("alchemist").rglob("*.py")
                 if p.name != "exchanges.py" and re.search(r"['\"](CME|EUREX|NASDAQ)['\"]", p.read_text())]
    assert offenders == []


def test_session_window_at_finds_window_containing_instant():
    cme = exchange_profile("CME")
    ny = ZoneInfo("America/New_York")
    start, end = cme.session_window_at("NY_RTH", datetime(2026, 4, 15, 12, 0, tzinfo=ny))
    assert (start.hour, end.hour) == (9, 16)
    assert cme.session_window_at("NY_RTH", datetime(2026, 4, 15, 16, 0, tzinfo=ny)) is None
    globex = cme.session_window_at("NY_GLOBEX", datetime(2026, 4, 15, 20, 0, tzinfo=ny))
    assert globex[1].date() == date(2026, 4, 16)
