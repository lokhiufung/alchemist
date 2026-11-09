import pytest

from alchemist.event_loop.resampler import Resampler


def feed(resampler, ticks):
    return [bar for bar in (resampler.update_from_tick(*t) for t in ticks) if bar is not None]


@pytest.mark.parametrize("resolution", [1])
def test_bar_ohlcv_matches_ticks_in_bucket(resolution):
    ticks = [(100.1, 5000.0, 1), (100.5, 5001.0, 2), (100.9, 4999.0, 1), (101.2, 5000.5, 3), (101.9, 5002.0, 2),
             (102.4, 4998.0, 5)]
    resampler = Resampler(resolution)
    bars = feed(resampler, ticks)
    bars.append(resampler.flush())
    assert [(b["start_ts"], b["open"], b["high"], b["low"], b["close"], b["volume"]) for b in bars] == [
        (100, 5000.0, 5001.0, 4999.0, 4999.0, 4), (101, 5000.5, 5002.0, 5000.5, 5002.0, 5),
        (102, 4998.0, 4998.0, 4998.0, 4998.0, 5)]
    assert all(b["end_ts"] == b["start_ts"] + resolution and b["resolution_s"] == resolution for b in bars)


@pytest.mark.parametrize("resolution", [1])
def test_out_of_order_tick_rejected(resolution):
    resampler = Resampler(resolution)
    resampler.update_from_tick(100.5, 5000.0, 1)
    resampler.update_from_tick(100.9, 5001.0, 1)
    assert resampler.update_from_tick(100.7, 9999.0, 50) is None
    bar = resampler.flush()
    assert (bar["high"], bar["volume"]) == (5001.0, 2)


@pytest.mark.parametrize("resolution", [1])
def test_late_tick_for_emitted_bucket_ignored(resolution):
    resampler = Resampler(resolution)
    resampler.update_from_tick(100.5, 5000.0, 1)
    flushed = resampler.flush_due(now=102.5, grace_s=1.0)
    assert flushed["start_ts"] == 100
    assert resampler.update_from_tick(100.9, 5001.0, 1) is None
    assert resampler.flush() is None


@pytest.mark.parametrize("grace", [1.0])
def test_quiet_bucket_flushed_after_grace(grace):
    resampler = Resampler(1)
    resampler.update_from_tick(100.2, 5000.0, 1)
    assert resampler.flush_due(now=101.9, grace_s=grace) is None
    bar = resampler.flush_due(now=102.0, grace_s=grace)
    assert bar["close"] == 5000.0 and bar["volume"] == 1
    assert resampler.flush_due(now=105.0, grace_s=grace) is None


def test_end_of_data_flushes_open_bucket():
    resampler = Resampler(1)
    resampler.update_from_tick(100.2, 5000.0, 1)
    assert resampler.flush()["close"] == 5000.0
    assert resampler.flush() is None
