import logging

import pytest

from alchemist.event_loop.metrics import LatencyMetrics
from alchemist.instruments import ES, MES, NQ

from el_helpers import (OPEN, FakeWallClock, Harness, RecordingStrategy, bar_update, end_of_data, order_update,
                        tick)


def keys_of(record):
    return set(record)


@pytest.mark.parametrize("kind", ["tick_closing_bar", "bar_update", "flushed_bar", "order_update"])
async def test_processing_and_feed_latency_logged_per_message(kind):
    strategy = RecordingStrategy()
    clock = FakeWallClock(OPEN + 10)
    harness = Harness([strategy], message_type="bar_update" if kind in ("bar_update", "order_update") else "tick_update",
                      clock=clock)
    if kind == "tick_closing_bar":
        harness.push(tick(OPEN + 0.5, 1.0, 1, recv_ts=OPEN + 0.51), tick(OPEN + 1.5, 1.0, 1, recv_ts=OPEN + 1.52),
                     end_of_data())
    elif kind == "bar_update":
        harness.push(bar_update(OPEN), end_of_data())
    elif kind == "flushed_bar":
        harness.push(tick(OPEN + 0.5, 1.0, 1, recv_ts=OPEN + 0.51))
    else:
        harness.push(order_update(1), end_of_data())

    async def stop_when_recorded():
        while not harness.loop.metrics.records:
            await asyncio.sleep(0)
        harness.loop.stopping = True

    import asyncio
    task = asyncio.create_task(stop_when_recorded())
    await harness.run()
    await task
    record = harness.loop.metrics.records[0]
    assert {"processing", "queue_wait", "dispatch", "slot_cost"} <= keys_of(record)
    assert record["processing"] >= 0 and record["slot_cost"] >= 0
    if kind == "flushed_bar":
        assert record["feed"] is None and record["feed_precision_s"] is None
    elif kind == "order_update":
        assert record["kind"] == "order_update"
    else:
        assert record["feed"] is not None and record["feed_precision_s"] in (1.0, 0.001)


def test_percentiles_per_instrument_per_minute_are_logged(caplog):
    metrics = LatencyMetrics()
    for instrument in (MES, ES, NQ):
        for index in range(100):
            metrics.record_feed(instrument, 1000.0, 1000.0 + 0.001 * (index + 1))
            metrics.record_processing(instrument, 2000.0, 2000.0 + 0.0001 * (index + 1), 0.00001, 0.00005,
                                      0.00002 * (index + 1))
            metrics.record_decode(instrument, 0.00001 * (index + 1))
    with caplog.at_level(logging.INFO, logger="alchemist.latency"):
        metrics.flush(3000.0)
    rows = [r.latency_row for r in caplog.records if hasattr(r, "latency_row")]
    assert sorted(r["instrument"] for r in rows) == ["ES", "MES", "NQ"]
    for row in rows:
        assert {"feed_p50", "feed_p99", "proc_p50", "proc_p99", "queue_wait_p99", "dispatch_p99", "slot_cost_p50",
                "slot_cost_p99", "decode_p50", "decode_p99", "ts_precision_s"} <= set(row)
        assert row["feed_p50"] == pytest.approx(0.05, abs=0.002) and row["proc_p99"] <= 0.0101


def test_metrics_reset_after_a_flush(caplog):
    metrics = LatencyMetrics()
    metrics.record_processing(MES, 1.0, 1.001, 0.0, 0.001, 0.0005)
    with caplog.at_level(logging.INFO, logger="alchemist.latency"):
        metrics.flush(100.0)
        caplog.clear()
        metrics.flush(160.0)
    assert [r for r in caplog.records if hasattr(r, "latency_row")] == []


def test_maybe_flush_runs_once_a_minute(caplog):
    metrics = LatencyMetrics()
    metrics.record_processing(MES, 1.0, 1.001, 0.0, 0.001, 0.0005)
    with caplog.at_level(logging.INFO, logger="alchemist.latency"):
        metrics.maybe_flush(10.0)
        metrics.maybe_flush(30.0)
        metrics.record_processing(MES, 1.0, 1.001, 0.0, 0.001, 0.0005)
        metrics.maybe_flush(71.0)
    assert len([r for r in caplog.records if hasattr(r, "latency_row")]) == 1


def test_timestamp_precision_follows_the_broker_timestamps():
    metrics = LatencyMetrics()
    metrics.record_feed(MES, 1000.0, 1000.02)
    metrics.record_feed(MES, 1001.0, 1001.03)
    assert metrics.precision(MES) == 1.0
    metrics.record_feed(MES, 1002.123, 1002.15)
    assert metrics.precision(MES) == 0.001


@pytest.mark.parametrize("instruments", [1, 3])
def test_slot_cost_sum_and_decode_time_logged_per_instrument(instruments, caplog):
    metrics = LatencyMetrics()
    chosen = (MES, ES, NQ)[:instruments]
    for instrument in chosen:
        for index in range(50):
            metrics.record_processing(instrument, 1.0, 1.001, 0.0, 0.001, 0.0001 * (index + 1))
            metrics.record_decode(instrument, 0.00002 * (index + 1))
    with caplog.at_level(logging.INFO, logger="alchemist.latency"):
        metrics.flush(100.0)
    rows = [r.latency_row for r in caplog.records if hasattr(r, "latency_row")]
    assert len(rows) == instruments
    assert all(row[k] is not None for row in rows for k in ("slot_cost_p50", "slot_cost_p99", "decode_p50", "decode_p99"))


def test_sample_memory_and_flush_cost_stay_bounded_under_a_tick_flood(caplog):
    import time as time_module
    metrics = LatencyMetrics()
    for index in range(200_000):
        metrics.record_feed(MES, 1000.0, 1000.0 + (index % 100) / 1000.0)
    assert len(metrics.series["feed"][MES]) <= metrics.MAX_SAMPLES
    started = time_module.perf_counter()
    with caplog.at_level(logging.INFO, logger="alchemist.latency"):
        metrics.flush(2000.0)
    assert time_module.perf_counter() - started < 0.005
    row = next(r.latency_row for r in caplog.records if hasattr(r, "latency_row"))
    assert row["feed_p50"] == pytest.approx(0.05, abs=0.01) and row["feed_p99"] == pytest.approx(0.099, abs=0.005)
