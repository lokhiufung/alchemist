import json
from collections import deque
from datetime import datetime, timezone

import pytest

from alchemist.clock import ReplayClock
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.recorders import BarRecorder, NullRecorder, TradeRecorder, load_recorded_bars
from alchemist.instruments import MES

from el_helpers import RecordingStrategy
from sd_helpers import at


class OwnedOrders:
    def __init__(self, sides):
        self.sides = dict(sides)

    def get_order(self, order_id):
        if order_id not in self.sides:
            return None
        return {"order_id": order_id, "side": self.sides[order_id], "instrument": MES}


def book_with(session):
    book = RuntimeBook()
    book.sessions_by_instrument[MES] = session
    return book


def bar(ts, close=100.0):
    return {"instrument": MES, "start_ts": ts, "end_ts": ts + 1, "open": close, "high": close, "low": close,
            "close": close, "volume": 1.0, "resolution_s": 1.0, "broker_ts": ts + 1}


def files(root):
    return sorted(p.name for p in (root / "bars").glob("*"))


@pytest.mark.parametrize("session,start", [("NY_RTH", at(MES, "NY_RTH", 15, 0)), ("NY_GLOBEX", at(MES, "NY_GLOBEX", 23, 59, 0, day=14))])
def test_scenario_recording_bars(session, start, tmp_path):
    recorder = BarRecorder(tmp_path, book_with(session))
    for index in range(180):
        recorder.record(MES, bar(start.timestamp() + index))
    recorder.finalize()
    assert len(files(tmp_path)) == 1
    rows = [json.loads(line) for line in (tmp_path / "bars" / files(tmp_path)[0]).read_text().splitlines()]
    assert len(rows) == 180 and rows[0]["instrument"] == "MES" and rows[0]["close"] == 100.0


def test_overnight_session_bars_not_split_at_midnight(tmp_path):
    recorder = BarRecorder(tmp_path, book_with("NY_GLOBEX"))
    before = at(MES, "NY_GLOBEX", 23, 59, 30, day=14).timestamp()
    for index in range(120):
        recorder.record(MES, bar(before + index))
    recorder.finalize()
    assert len(files(tmp_path)) == 1


def test_next_overnight_session_gets_its_own_file(tmp_path):
    recorder = BarRecorder(tmp_path, book_with("NY_GLOBEX"))
    recorder.record(MES, bar(at(MES, "NY_GLOBEX", 20, 0, 0, day=14).timestamp()))
    recorder.record(MES, bar(at(MES, "NY_GLOBEX", 20, 0, 0, day=15).timestamp()))
    recorder.finalize()
    assert len(files(tmp_path)) == 2


def test_recorded_bar_durable_before_finalize(tmp_path):
    recorder = BarRecorder(tmp_path, book_with("NY_RTH"))
    start = at(MES, "NY_RTH", 10, 0).timestamp()
    for index in range(10):
        recorder.record(MES, bar(start + index))
    lines = (tmp_path / "bars" / files(tmp_path)[0]).read_text().splitlines()
    assert len(lines) == 10


def test_recorded_bars_load_back_for_a_mid_session_start(tmp_path):
    recorder = BarRecorder(tmp_path, book_with("NY_RTH"))
    start = at(MES, "NY_RTH", 10, 0).timestamp()
    for index in range(5):
        recorder.record(MES, bar(start + index, 100.0 + index))
    recorder.finalize()
    clock = ReplayClock(datetime.fromtimestamp(start + 10, timezone.utc))
    loaded = load_recorded_bars(tmp_path, MES, "NY_RTH", clock)
    assert [b["close"] for b in loaded] == [100.0 + i for i in range(5)] and loaded[0]["instrument"] == MES
    assert load_recorded_bars(tmp_path / "missing", MES, "NY_RTH", clock) == []


def fill(order_id, filled, avg, ts, side="BUY"):
    return {"order_id": order_id, "status": "FILLED", "filled": filled, "remaining": 0, "avg_fill_price": avg,
            "last_fill_price": avg, "ts": ts, "account_id": "ACC", "instrument": MES, "reason": None}


def recorder_for(tmp_path, owned):
    book = RuntimeBook()
    strategy = RecordingStrategy("slot")
    strategy.om = OwnedOrders(owned)
    strategy.execution_instrument = MES
    book.strategies.append(strategy)
    return TradeRecorder(tmp_path, None, book)


def rows(root):
    path = next((root / "trades").glob("*.jsonl"))
    return [json.loads(line) for line in path.read_text().splitlines()]


async def test_scenario_recording_round_trip_row_has_prices_and_currency(tmp_path):
    recorder = recorder_for(tmp_path, {1: "BUY", 2: "SELL"})
    recorder.record_update(fill(1, 1, 5001.0, 100.0))
    recorder.record_update(fill(2, 1, 5011.0, 200.0))
    await recorder.flush()
    row = rows(tmp_path)[0]
    assert (row["strategy"], row["currency"], row["entry_price"], row["exit_price"], row["qty"]) == (
        "slot", "USD", 5001.0, 5011.0, 1)


async def test_round_trip_weighted_average_for_multi_fill_leg(tmp_path):
    recorder = recorder_for(tmp_path, {1: "BUY", 2: "SELL"})
    recorder.record_update({**fill(1, 2, 100.0, 1.0), "status": "PARTIALLY_FILLED"})
    recorder.record_update(fill(1, 3, 101.0, 2.0))
    recorder.record_update(fill(2, 3, 110.0, 3.0))
    await recorder.flush()
    row = rows(tmp_path)[0]
    assert row["entry_price"] == 101.0 and row["qty"] == 3 and row["exit_price"] == 110.0


async def test_open_position_is_not_written_until_closed(tmp_path):
    recorder = recorder_for(tmp_path, {1: "BUY"})
    recorder.record_update(fill(1, 1, 5001.0, 1.0))
    await recorder.flush()
    assert list((tmp_path / "trades").glob("*.jsonl")) == []


def test_unowned_updates_are_ignored(tmp_path):
    recorder = recorder_for(tmp_path, {})
    recorder.record_update(fill(9, 1, 1.0, 1.0))
    assert recorder.pending == []


async def test_null_recorder_does_nothing():
    recorder = NullRecorder()
    recorder.record(MES, {})
    recorder.record_update({})
    await recorder.flush()
    recorder.finalize()
