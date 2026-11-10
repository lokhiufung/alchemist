from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import alchemist.gateways  # noqa: F401  registers the golden tape
from alchemist.exchanges import exchange_profile
from alchemist.gateways.simulation_gateway.tapes import ParquetTape, ReplayDataError, SyntheticTape, check_tape_in_session, tape_from_spec
from alchemist.instruments import FDAX, MES
from alchemist.registry import get_tape

from gateway_helpers import REPLAY_DATE, ListTape, session_seconds, tape_bar

CME = exchange_profile(MES)


def day_bars(day=REPLAY_DATE):
    tape = tape_from_spec("synthetic:golden_session")
    start, end = CME.session_window("NY_RTH", day)
    return list(tape.bars(MES, start, end))


def test_golden_tape_has_23400_bars():
    bars = day_bars()
    start, end = CME.session_window("NY_RTH", REPLAY_DATE)
    assert len(bars) == 23_400
    assert bars[0]["start_ts"] == start.timestamp() and bars[-1]["end_ts"] == end.timestamp()
    assert all(b["end_ts"] - b["start_ts"] == 1.0 and b["resolution_s"] == 1.0 for b in bars[:50])


def test_golden_phase_a_flat():
    assert {(b["open"], b["high"], b["low"], b["close"]) for b in day_bars()[:600]} == {(5000.0,) * 4}


def test_golden_phase_b_zigzag_reaches_5011():
    closes = [b["close"] for b in day_bars()]
    assert closes[600:605] == [5002.0, 5001.0, 5003.0, 5002.0, 5004.0]
    first_target = next(i for i, c in enumerate(closes) if c >= 5011.0)
    assert first_target == 618
    assert set(closes[618:]) == {5011.0}
    assert all(c * 4 == round(c * 4) for c in closes)


def test_golden_prior_sessions_have_designed_shape():
    tape = tape_from_spec("synthetic:golden_session")
    sessions = []
    day = date(2026, 4, 14)
    while len(sessions) < 60:
        if CME.is_trading_day(day):
            start, end = CME.session_window("NY_RTH", day)
            sessions.append(list(tape.bars(MES, start, end)))
        day = date.fromordinal(day.toordinal() - 1)
    assert all(len(s) == 23_400 for s in sessions)
    assert {(s[0]["open"], max(b["high"] for b in s), min(b["low"] for b in s), s[-1]["close"]) for s in sessions} == {
        (5000.0, 5010.0, 4990.0, 5000.0)}


def test_golden_tape_has_nothing_before_sixty_sessions_or_after_replay_day():
    tape = tape_from_spec("synthetic:golden_session")
    far_start, far_end = CME.session_window("NY_RTH", date(2025, 12, 1))
    next_start, next_end = CME.session_window("NY_RTH", date(2026, 4, 16))
    assert list(tape.bars(MES, far_start, far_end)) == []
    assert list(tape.bars(MES, next_start, next_end)) == []


def test_golden_tape_is_deterministic():
    assert day_bars() == day_bars()


def test_tape_from_spec_picks_synthetic_or_parquet(tmp_path):
    assert isinstance(tape_from_spec("synthetic:golden_session"), SyntheticTape)
    assert isinstance(tape_from_spec(str(tmp_path / "x.parquet")), ParquetTape)
    with pytest.raises(KeyError, match="golden_session"):
        tape_from_spec("synthetic:nope")
    assert get_tape("golden_session") is not None


def test_parquet_tape_filters_instrument_and_window(tmp_path):
    start, end = CME.session_window("NY_RTH", REPLAY_DATE)
    rows = [tape_bar(MES, start.timestamp() + i, 100.0 + i) for i in range(5)] + [tape_bar(FDAX, start.timestamp(), 1.0)]
    table = pa.table({"symbol": [r["instrument"].symbol for r in rows], "start_ts": [r["start_ts"] for r in rows],
                      "open": [r["open"] for r in rows], "high": [r["high"] for r in rows], "low": [r["low"] for r in rows],
                      "close": [r["close"] for r in rows], "volume": [r["volume"] for r in rows]})
    pq.write_table(table, tmp_path / "tape.parquet")
    got = list(ParquetTape(str(tmp_path / "tape.parquet")).bars(MES, start, end))
    assert [b["close"] for b in got] == [100.0, 101.0, 102.0, 103.0, 104.0]
    assert got[0]["instrument"] == MES and got[0]["end_ts"] == got[0]["start_ts"] + 1.0
    narrowed = list(ParquetTape(str(tmp_path / "tape.parquet")).bars(
        MES, datetime.fromtimestamp(start.timestamp() + 2, timezone.utc), end))
    assert len(narrowed) == 3


def test_check_counts_rows_inside_session():
    start, end = CME.session_window("NY_RTH", REPLAY_DATE)
    tape = ListTape(session_seconds(MES, "NY_RTH", REPLAY_DATE, 10))
    assert check_tape_in_session(tape, MES, "NY_RTH", start, end) == 10


@pytest.mark.parametrize("instrument,session", [(MES, "NY_RTH"), (FDAX, "FRA_DAY")])
def test_replay_refuses_tape_rows_outside_session(instrument, session):
    profile = exchange_profile(instrument)
    start, end = profile.session_window(session, REPLAY_DATE)
    rows = session_seconds(instrument, session, REPLAY_DATE, 5) + [tape_bar(instrument, end.timestamp() + 60, 1.0)]
    with pytest.raises(ReplayDataError, match="outside session"):
        check_tape_in_session(ListTape(rows), instrument, session, start, end + (end - start))


def test_day_session_only_tape_refused_for_overnight_session():
    rth_start, rth_end = CME.session_window("NY_RTH", REPLAY_DATE)
    globex_start, globex_end = CME.session_window("NY_GLOBEX", REPLAY_DATE)
    tape = ListTape(session_seconds(MES, "NY_RTH", REPLAY_DATE, 100))
    with pytest.raises(ReplayDataError, match="regular session only"):
        check_tape_in_session(tape, MES, "NY_GLOBEX", globex_start, globex_end)
