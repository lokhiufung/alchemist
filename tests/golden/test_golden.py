import json
import os
import subprocess
import sys
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXPECTED = Path(__file__).parent / "expected"


def run_golden(out: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "GOLDEN_OUT": str(out)}
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


def test_8_no_error_lines(runs):
    (_, out), _ = runs
    log = (out / "engine.log").read_text()
    assert " ERROR " not in log and " CRITICAL " not in log
