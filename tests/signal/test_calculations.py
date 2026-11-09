from datetime import datetime, timezone

import pytest

from alchemist.exchanges import exchange_profile
from alchemist.instruments import MES
from alchemist.registry import get_calc

import alchemist.calculations.library  # noqa: F401  registers the built-ins

CME = exchange_profile(MES)


def make(key, **params):
    calc = get_calc(key, "anywhere")()
    calc.setup(**params)
    return calc


def feed(calc, rows):
    return [calc.step(*row) for row in rows]


def ts(hour, minute, second=0, day=15):
    return datetime(2026, 4, day, hour, minute, second, tzinfo=timezone.utc).timestamp()


def test_sma_step_is_mean_of_last_period_values():
    assert feed(make("sma", period=3), [(1.0,), (2.0,), (3.0,), (6.0,)]) == [None, None, 2.0, 11 / 3]


def test_sma_warmup_seeds_window_and_makes_value_available():
    calc = make("sma", period=3)
    assert calc.value() is None
    calc.warmup([(1.0,), (2.0,), (3.0,), (4.0,), (5.0,)])
    assert calc.value() == 4.0
    assert calc.step(7.0) == 16 / 3


def test_sma_warmup_with_too_few_rows_stays_unavailable():
    calc = make("sma", period=5)
    calc.warmup([(1.0,)] * 4)
    assert calc.value() is None


def test_sma_refuses_bad_period():
    with pytest.raises(ValueError, match="period"):
        make("sma", period=0)
    with pytest.raises(ValueError, match="period"):
        make("sma", period=True)


def test_ema_starts_at_first_value_then_smooths():
    assert feed(make("ema", period=3), [(10.0,), (20.0,), (20.0,)]) == [10.0, 15.0, 17.5]


def test_atr_is_mean_of_first_period_true_ranges_then_wilder():
    rows = [(12.0, 10.0, 11.0), (13.0, 11.0, 12.0), (15.0, 12.0, 14.0), (16.0, 14.0, 15.0)]
    result = feed(make("atr", period=3), rows)
    assert result[:2] == [None, None]
    assert result[2] == pytest.approx((2 + 2 + 3) / 3)
    assert result[3] == pytest.approx(((2 + 2 + 3) / 3 * 2 + 2) / 3)


def test_vwap_is_volume_weighted_and_resets_each_day():
    calc = make("vwap", profile=CME)
    first = calc.step(ts(14, 0), 100.0, 1.0)
    second = calc.step(ts(14, 1), 110.0, 3.0)
    next_day = calc.step(ts(14, 0, day=16), 50.0, 2.0)
    assert (first, second, next_day) == (100.0, 107.5, 50.0)


def test_vwap_without_volume_is_unavailable():
    assert make("vwap", profile=CME).step(ts(14, 0), 100.0, 0.0) is None


def test_lowest_and_highest_over_rolling_window():
    assert feed(make("lowest", period=3), [(5.0,), (4.0,), (6.0,), (7.0,), (8.0,)]) == [None, None, 4.0, 4.0, 6.0]
    assert feed(make("highest", period=2), [(5.0,), (4.0,), (6.0,)]) == [None, 5.0, 6.0]


def test_session_bars_elapsed_counts_one_second_bars_from_open():
    calc = make("session_bars_elapsed", session="NY_RTH", profile=CME)
    open_ts = ts(13, 30)
    assert calc.step(open_ts, open_ts + 1) == 1
    assert calc.step(open_ts + 600, open_ts + 601) == 601


def test_session_bars_left_is_zero_on_the_last_bar():
    calc = make("session_bars_left", session="NY_RTH", profile=CME)
    close_ts = ts(20, 0)
    assert calc.step(close_ts - 1, close_ts) == 0
    assert calc.step(close_ts - 1801, close_ts - 1800) == 1800


def test_session_bars_outside_session_are_unavailable():
    calc = make("session_bars_left", session="NY_RTH", profile=CME)
    assert calc.step(ts(5, 0), ts(5, 0) + 1) is None


def test_session_bars_refuse_session_the_profile_lacks():
    with pytest.raises(KeyError):
        make("session_bars_elapsed", session="FRA_DAY", profile=CME)


def test_early_close_shortens_bars_left():
    calc = make("session_bars_left", session="NY_RTH", profile=CME)
    early_close = datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc).timestamp()
    assert calc.step(early_close - 1, early_close) == 0
