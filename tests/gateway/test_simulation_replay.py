import hashlib
from datetime import date, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from alchemist.exchanges import exchange_profile
from alchemist.gateways import get_gateway
from alchemist.gateways.simulation_gateway import SimulationGateway
from alchemist.gateways.simulation_gateway.tapes import ParquetTape, ReplayDataError
from alchemist.instruments import FDAX, MES, MNQ

from gateway_helpers import REPLAY_DATE, ListTape, make_gateway, session_seconds, tape_bar

CME = exchange_profile(MES)


async def started(gateway, instrument=MES):
    await gateway.connect()
    contract = await gateway.resolve_contract(instrument)
    await gateway.subscribe_tick(contract)
    return gateway


def atr(daily, period=14):
    rows = daily[-(period + 1):]
    ranges = [max(c["high"], p["close"]) - min(c["low"], p["close"]) for p, c in zip(rows, rows[1:])]
    return sum(ranges) / period


def test_simulated_broker_registered_and_declares_one_second_bars():
    assert get_gateway("simulation") is SimulationGateway
    gateway, _ = make_gateway()
    assert gateway.market_data_type(MES) == ("bar_update", 1.0)


async def test_weekend_date_refused():
    gateway, _ = make_gateway({"date": date(2026, 4, 18)})
    with pytest.raises(ReplayDataError, match="2026-04-18.*NY_RTH"):
        await gateway.connect()


async def test_trading_day_with_no_tape_bars_refused_naming_date_and_session():
    gateway, _ = make_gateway(tape=ListTape([]))
    await gateway.connect()
    with pytest.raises(ReplayDataError, match="2026-04-15.*NY_RTH"):
        await gateway.resolve_contract(MES)


async def test_golden_daily_atr_is_20():
    gateway, _ = make_gateway()
    await gateway.connect()
    daily = await gateway.get_daily_history(MES, REPLAY_DATE - timedelta(days=120), REPLAY_DATE, "NY_RTH")
    assert len(daily) >= 15 and atr(daily) == 20.0
    assert all(c["close"] == 5000.0 and c["high"] == 5010.0 and c["low"] == 4990.0 for c in daily)


@pytest.mark.parametrize("window", ["months", "dates"])
@pytest.mark.parametrize("tape_kind", ["golden", "no_prior_session"])
async def test_scenario_history_and_seed_bars(window, tape_kind):
    tape = None if tape_kind == "golden" else ListTape(session_seconds(MES, "NY_RTH", REPLAY_DATE, 600))
    gateway, _ = make_gateway(tape=tape)
    await gateway.connect()
    start = REPLAY_DATE - timedelta(days=61 if window == "months" else 20)
    end = REPLAY_DATE + timedelta(days=5)
    daily = await gateway.get_daily_history(MES, start, end, "NY_RTH")
    seed = await gateway.get_intraday_seed_bars(MES, "NY_RTH")
    if tape_kind == "no_prior_session":
        assert daily == [] and seed == []
        return
    starts = [d["start_ts"] for d in daily]
    assert starts == sorted(starts) and len(set(starts)) == len(starts)
    assert all(d["date"] >= start.isoformat() and d["date"] < REPLAY_DATE.isoformat() for d in daily)
    assert daily[-1]["date"] == "2026-04-14"
    prior_start, prior_end = CME.session_window("NY_RTH", date(2026, 4, 14))
    assert len(seed) == 23_400 and seed[0]["start_ts"] == prior_start.timestamp()
    assert all(prior_start.timestamp() <= b["start_ts"] < prior_end.timestamp() and b["resolution_s"] == 1.0 for b in seed)
    assert atr(daily) == 20.0 if len(daily) > 14 else True


@pytest.mark.parametrize("instrument,session", [(MES, "NY_RTH"), (FDAX, "FRA_DAY")])
async def test_daily_frame_has_no_look_ahead_past_last_completed_close(instrument, session):
    profile = exchange_profile(instrument)
    prior = date(2026, 4, 14)
    rows = (session_seconds(instrument, session, prior, 60, close=10.0)
            + session_seconds(instrument, session, REPLAY_DATE, 60, close=99.0))
    gateway, _ = make_gateway({"session": session}, tape=ListTape(rows))
    await gateway.connect()
    daily = await gateway.get_daily_history(instrument, prior, REPLAY_DATE + timedelta(days=3), session)
    _, prior_close = profile.session_window(session, prior)
    assert [d["date"] for d in daily] == ["2026-04-14"]
    assert all(d["end_ts"] <= prior_close.timestamp() + 1 for d in daily)


def reference_rows():
    rows = []
    for offset, day in enumerate((date(2026, 4, 13), date(2026, 4, 14))):
        start, _ = CME.session_window("NY_RTH", day)
        rows += [tape_bar(MES, start.timestamp() + i, 100.0 + offset + i % 3, high=105.0 + offset, low=95.0 - i % 2)
                 for i in range(30)]
    return rows


@pytest.mark.parametrize("tape_kind", ["parquet", "synthetic"])
async def test_replay_daily_history_matches_broker_definitions(tape_kind, tmp_path):
    rows = reference_rows()
    if tape_kind == "parquet":
        pq.write_table(pa.table({"symbol": ["MES"] * len(rows), "start_ts": [r["start_ts"] for r in rows],
                                 "open": [r["open"] for r in rows], "high": [r["high"] for r in rows],
                                 "low": [r["low"] for r in rows], "close": [r["close"] for r in rows],
                                 "volume": [r["volume"] for r in rows]}), tmp_path / "t.parquet")
        tape = ParquetTape(str(tmp_path / "t.parquet"))
    else:
        tape = ListTape(rows)
    gateway, _ = make_gateway(tape=tape)
    await gateway.connect()
    daily = await gateway.get_daily_history(MES, date(2026, 4, 13), date(2026, 4, 14), "NY_RTH")
    assert [(d["date"], d["open"], d["high"], d["low"], d["close"], d["volume"]) for d in daily] == [
        ("2026-04-13", 100.0, 105.0, 94.0, 100.0 + 29 % 3, 30.0),
        ("2026-04-14", 101.0, 106.0, 94.0, 101.0 + 29 % 3, 30.0)]


async def test_replay_no_prior_session_returns_empty_seed():
    gateway, _ = make_gateway(tape=ListTape(session_seconds(MES, "NY_RTH", REPLAY_DATE, 10)))
    await gateway.connect()
    assert await gateway.get_intraday_seed_bars(MES, "NY_RTH") == []


@pytest.mark.parametrize("instrument,session", [(MES, "NY_RTH"), (FDAX, "FRA_DAY")])
async def test_replay_start_refuses_out_of_session_row(instrument, session):
    _, close = exchange_profile(instrument).session_window(session, REPLAY_DATE)
    rows = (session_seconds(instrument, session, REPLAY_DATE, 5)
            + [tape_bar(instrument, close.timestamp() + 3600, 1.0)])
    gateway, _ = make_gateway({"session": session}, tape=ListTape(rows))
    await gateway.connect()
    with pytest.raises(ReplayDataError, match="outside session"):
        await gateway.resolve_contract(instrument)


async def test_golden_replay_delivers_every_bar_then_end_of_data_in_time_order(tmp_path):
    gateway, buffer = make_gateway(journal=tmp_path / "broker_journal.jsonl")
    await started(gateway)
    await gateway.start_replay()
    kinds = [e["event_type"] for e in buffer.events]
    assert kinds == ["bar_update"] * 23_400 + ["end_of_data"]
    starts = [e["data"]["start_ts"] for e in buffer.events[:-1]]
    assert starts == sorted(starts) and len(set(starts)) == 23_400
    assert {e["data"]["resolution_s"] for e in buffer.events[:-1]} == {1.0}
    assert {e["data"]["instrument"] for e in buffer.events[:-1]} == {MES}
    assert (tmp_path / "tape.sha256").read_text().strip() == gateway.tape_sha256.hexdigest()


async def test_golden_tape_is_deterministic_across_runs(tmp_path):
    digests = []
    for name in ("a", "b"):
        gateway, _ = make_gateway(journal=tmp_path / name / "broker_journal.jsonl")
        await started(gateway)
        await gateway.start_replay()
        digests.append((tmp_path / name / "tape.sha256").read_text())
    assert digests[0] == digests[1] and len(digests[0].strip()) == 64


async def test_flat_padding_surrounds_the_session():
    rows = session_seconds(MES, "NY_RTH", REPLAY_DATE, 4, close=77.0)
    gateway, buffer = make_gateway({"pad_before_s": 3, "pad_after_s": 2}, tape=ListTape(rows))
    await started(gateway)
    await gateway.start_replay()
    bars = buffer.of_type("bar_update")
    open_ts = CME.session_window("NY_RTH", REPLAY_DATE)[0].timestamp()
    assert [b["start_ts"] - open_ts for b in bars] == [-3, -2, -1, 0, 1, 2, 3, 4, 5]
    assert {b["close"] for b in bars} == {77.0} and bars[0]["volume"] == 0.0


async def test_two_instruments_merge_in_time_order():
    rows = (session_seconds(MES, "NY_RTH", REPLAY_DATE, 3)
            + [tape_bar(MNQ, r["start_ts"] + 0.5, 1.0) for r in session_seconds(MES, "NY_RTH", REPLAY_DATE, 3)])
    gateway, buffer = make_gateway(tape=ListTape(rows))
    await gateway.connect()
    for instrument in (MES, MNQ):
        await gateway.subscribe_tick(await gateway.resolve_contract(instrument))
    await gateway.start_replay()
    stamps = [b["start_ts"] for b in buffer.of_type("bar_update")]
    assert stamps == sorted(stamps) and len(stamps) == 6
