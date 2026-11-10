import logging
import re
from pathlib import Path

import pytest

from alchemist.event_loop import loop as loop_module
from alchemist.event_loop.loop import HANDLERS

from el_helpers import (OPEN, Harness, RecordingStrategy, balance_update, bar_update, end_of_data, order_update,
                        tick)


@pytest.mark.parametrize("strategies", [1, 15])
async def test_bars_in_order_exactly_once(strategies):
    members = [RecordingStrategy(f"s{i}") for i in range(strategies)]
    harness = Harness(members)
    harness.push(*[bar_update(OPEN + i) for i in range(20)], end_of_data())
    await harness.run()
    for member in members:
        assert member.bars == [OPEN + i for i in range(20)]


async def test_aux_then_gateway_then_strategies():
    log = []
    strategy = RecordingStrategy(log=log)
    harness = Harness([strategy], log=log)
    harness.push(bar_update(OPEN), end_of_data())
    await harness.run()
    assert [entry[0] for entry in log] == ["aux", "gateway", "strategy"]


async def test_final_drain_uses_same_handlers():
    strategy = RecordingStrategy()
    harness = Harness([strategy])
    dispatched = []
    original = harness.loop.dispatch

    async def spy(event):
        dispatched.append(event["event_type"])
        await original(event)

    harness.loop.dispatch = spy
    harness.push(order_update(1), end_of_data())
    await harness.run()
    harness.push(order_update(2))
    await harness.loop.drain_remaining()
    assert dispatched[-1] == "order_update" and strategy.events[-1] == ("order", 2, "FILLED")
    assert set(HANDLERS) >= {"tick_update", "bar_update", "bar", "order_update", "balance_update",
                             "position_update", "repaired_fill", "book_op", "end_of_data"}


@pytest.mark.parametrize("cause", ["rollover", "flush", "end_of_data", "bar_update"])
async def test_every_bar_close_passes_one_function(cause):
    strategy = RecordingStrategy()
    clock = None
    harness = Harness([strategy], message_type="bar_update" if cause == "bar_update" else "tick_update")
    emitted = []
    original = harness.loop.emit_bar

    async def spy(instrument, bar, source, origin=None):
        emitted.append(source)
        await original(instrument, bar, source, origin)

    harness.loop.emit_bar = spy
    bar_events = []
    original_push = harness.buffer.push
    harness.buffer.push = lambda event, wake=True: (bar_events.append(event) if event["event_type"] == "bar" else None,
                                                   original_push(event, wake))[1]
    if cause == "bar_update":
        harness.push(bar_update(OPEN), end_of_data())
    elif cause == "rollover":
        harness.push(tick(OPEN + 0.1, 1.0, 1), tick(OPEN + 1.1, 1.0, 1), end_of_data())
    elif cause == "flush":
        harness.push(tick(OPEN + 0.1, 1.0, 1))
        harness.clock.__class__  # replay clock: flush due immediately once time passes
        harness.clock.advance(__import__("datetime").datetime.fromtimestamp(OPEN + 5, __import__("datetime").timezone.utc))
        harness.push(end_of_data())
    else:
        harness.push(tick(OPEN + 0.1, 1.0, 1), end_of_data())
    await harness.run()
    assert len(bar_events) == len(emitted) >= 1


async def test_bar_update_feed_bypasses_resampler():
    strategy = RecordingStrategy()
    harness = Harness([strategy])
    calls = []
    harness.book.resamplers = {strategy.signal_instrument: type("R", (), {
        "update_from_tick": lambda *a: calls.append(1), "flush_due": lambda *a, **k: calls.append(1),
        "flush": lambda *a: calls.append(1)})()}
    harness.push(bar_update(OPEN), end_of_data())
    await harness.run()
    assert calls == [] and strategy.bars == [OPEN]


@pytest.mark.parametrize("state", ["in_session", "flattening", "session_ended"])
async def test_order_update_reaches_every_strategy(state):
    strategies = [RecordingStrategy(state), RecordingStrategy("other")]
    harness = Harness(strategies)
    harness.push(order_update(3), end_of_data())
    await harness.run()
    assert all(s.events == [("order", 3, "FILLED")] for s in strategies)


async def test_position_update_and_repaired_fill_reach_every_strategy():
    from alchemist.messages import make_event
    strategy = RecordingStrategy()
    harness = Harness([strategy])
    repaired = order_update(9)
    harness.push(make_event("position_update", {"account_id": "A", "instrument": strategy.signal_instrument,
                                                "size": 2, "avg_cost": 1.0, "currency": "USD"}, 0.0),
                 make_event("repaired_fill", repaired["data"], 0.0), end_of_data())
    await harness.run()
    assert strategy.events == [("order", 9, "FILLED"), ("position", 2)] or sorted(strategy.events) == sorted(
        [("order", 9, "FILLED"), ("position", 2)])


async def test_round_trip_close_flushes_trade_recorder():
    strategy = RecordingStrategy()
    harness = Harness([strategy])
    strategy.closed_trades = 1
    harness.push(order_update(1), order_update(2), end_of_data())
    await harness.run()
    assert harness.trade_recorder.flushes == 1 and harness.trade_recorder.updates == [1, 2]


async def test_bars_are_recorded():
    harness = Harness([RecordingStrategy()])
    harness.push(bar_update(OPEN), bar_update(OPEN + 1), end_of_data())
    await harness.run()
    assert harness.bar_recorder.bars == [OPEN, OPEN + 1]


async def test_safety_checked_in_every_pass():
    harness = Harness([RecordingStrategy()])
    harness.push(bar_update(OPEN), end_of_data())
    await harness.run()
    assert harness.safety.check_ins >= 1


async def test_strategy_quarantined_at_threshold_others_continue():
    healthy, broken = RecordingStrategy("ok"), RecordingStrategy("bad", raises_on_bar=True)
    harness = Harness([broken, healthy])
    attempts = []
    original = broken.on_bar
    broken.on_bar = lambda bars, bar: (attempts.append(bar["start_ts"]), original(bars, bar))[1]
    harness.push(*[bar_update(OPEN + i) for i in range(6)], end_of_data())
    await harness.run()
    assert len(attempts) == 3 and broken.quarantined and len(healthy.bars) == 6


@pytest.mark.parametrize("failing", ["one", "all"])
async def test_fan_out_logs_and_counts_each_strategy_exception(failing, caplog):
    members = [RecordingStrategy("a", raises_on_bar=True), RecordingStrategy("b", raises_on_bar=failing == "all")]
    harness = Harness(members)
    harness.push(bar_update(OPEN), end_of_data())
    with caplog.at_level(logging.ERROR):
        await harness.run()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == (1 if failing == "one" else 2)


async def test_success_resets_the_consecutive_error_count():
    flaky = RecordingStrategy("flaky")
    calls = {"n": 0}
    original = flaky.on_bar

    def sometimes(bars, bar):
        calls["n"] += 1
        if calls["n"] % 3 != 0:
            raise RuntimeError("odd")
        original(bars, bar)

    flaky.on_bar = sometimes
    harness = Harness([flaky])
    harness.push(*[bar_update(OPEN + i) for i in range(9)], end_of_data())
    await harness.run()
    assert not flaky.quarantined and len(flaky.bars) == 3


def test_no_run_mode_read_in_event_loop():
    offenders = [p for p in Path("alchemist/event_loop").glob("*.py")
                 if re.search(r"RunMode|\bMode\b|is_replay|\.mode\b|run_mode", p.read_text())]
    assert offenders == []


async def test_unknown_event_type_is_logged_and_skipped(caplog):
    harness = Harness([RecordingStrategy()])
    harness.push({"event_type": "mystery", "data": {}, "enqueued_at": 0.0}, bar_update(OPEN), end_of_data())
    with caplog.at_level(logging.ERROR):
        await harness.run()
    assert any(getattr(r, "event_type", None) == "mystery" for r in caplog.records)
