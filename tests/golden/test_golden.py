import json
import os
import subprocess
import sys
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from golden_tape import golden_tape_path

ROOT = Path(__file__).resolve().parents[2]
EXPECTED = Path(__file__).parent / "expected"


def run_golden(out: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "GOLDEN_OUT": str(out), "GOLDEN_TAPE": golden_tape_path()}
    return subprocess.run(
        [sys.executable, "run_strategies.py", "--config", "tests/golden/config.yaml", "--mode", "replay"],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=600)


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    a, b = tmp_path_factory.mktemp("a"), tmp_path_factory.mktemp("b")
    return (run_golden(a), a), (run_golden(b), b)


def rows(out): return [json.loads(l) for l in (out / "broker_journal.jsonl").read_text().splitlines()]


def test_1_exit_code_zero(runs):
    (proc, _), _ = runs
    assert proc.returncode == 0, proc.stderr


def test_2_tape_fingerprint(runs):
    (_, out), _ = runs
    assert (out / "tape.sha256").read_text().strip() == (EXPECTED / "tape.sha256").read_text().strip()


def test_3_journal_byte_equal(runs):
    (_, out), _ = runs
    got = (out / "broker_journal.jsonl").read_bytes().splitlines()
    want = (EXPECTED / "broker_journal.jsonl").read_bytes().splitlines()
    first_diff = next((i for i, (g, w) in enumerate(zip(got, want)) if g != w), None)
    assert got == want, f"first different row: {first_diff}"


def test_4_four_rows_in_order(runs):
    (_, out), _ = runs
    r = rows(out)
    assert len(r) == 4
    assert [("side" in x, x.get("side")) for x in r] == [(True, "BUY"), (False, None), (True, "SELL"), (False, None)]
    assert all(x["qty"] == 1 for x in r)
    assert all(x["order_type"] == "MKT" and x["instrument"] == "MES" for x in r[0::2])


def test_5_buy_inside_entry_window(runs):
    (_, out), _ = runs
    t = datetime.fromisoformat(rows(out)[0]["time"]).astimezone(ZoneInfo("America/New_York")).time()
    assert time(9, 40) <= t < time(15, 30)


def test_6_profit_at_least_ten_points(runs):
    (_, out), _ = runs
    r = rows(out)
    assert r[3]["price"] - r[1]["price"] >= 10.00


def test_7_second_run_identical(runs):
    (_, a), (_, b) = runs
    assert (a / "broker_journal.jsonl").read_bytes() == (b / "broker_journal.jsonl").read_bytes()


def log_lines(out):
    return {f.name: [json.loads(l) for l in f.read_text().splitlines()] for f in (out / "logs").glob("*.log")}


def test_8_no_error_lines(runs):
    (_, out), _ = runs
    levels = {line["level"] for lines in log_lines(out).values() for line in lines}
    assert not levels & {"ERROR", "CRITICAL"}


def test_9_logs_are_json_lines_per_component(runs):
    (_, out), _ = runs
    logs = log_lines(out)
    assert set(logs) == {"startup.log", "shutdown.log", "gateway.log", "event_loop.log", "aux_data.log", "safety.log",
                         "om.log", "macd_rsi_rth_long_strategy.log"}
    for name, lines in logs.items():
        assert lines, name
        for line in lines:
            assert {"timestamp", "event_time", "level", "logger", "method", "message", "event"} <= set(line), (name, line)
            assert f"{line['logger']}.log" == name
    strategy_lines, order_lines = logs["macd_rsi_rth_long_strategy.log"], logs["om.log"]
    assert {"signal_snapshot", "state_transition", "entry_decision", "exit_decision"} <= {
        line["event"] for line in strategy_lines}
    assert [line["event"] for line in order_lines] == ["order_placed", "order_filled"] * 2
    assert {line["strategy"] for line in order_lines} == {"macd_rsi_rth_long"}
    broker_lines = [line for line in logs["gateway.log"] if line["event"] in ("order_sent", "broker_order_update")]
    assert [(line["event"], line.get("status")) for line in broker_lines] == [
        ("order_sent", None), ("broker_order_update", "SUBMITTED"), ("broker_order_update", "FILLED")] * 2
    tape_lines = strategy_lines + order_lines
    assert {line["event_time"][:10] for line in tape_lines} == {"2026-04-15"}
    assert [line["event_time"] for line in order_lines][::2] == [
        "2026-04-15T13:40:02+00:00", "2026-04-15T13:40:19+00:00"]


def test_10_safety_runs_in_replay_and_stays_quiet(runs):
    (_, out), _ = runs
    safety = log_lines(out)["safety.log"]
    assert {line["event"] for line in safety} == {"service_started", "service_stopped"}
    events = {line["event"] for lines in log_lines(out).values() for line in lines}
    assert events.isdisjoint({"alert", "repaired_fill", "connection_lost", "command_failed"})
