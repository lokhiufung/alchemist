import asyncio

import pytest

from alchemist.event_loop.seeder import Backoff, SeedState, SharedFetch, TerminalSeedError
from alchemist.instruments import MES

from seed_helpers import SeedGateway, SeedStrategy, make_seeder


async def wait_for_tasks(seeder):
    await asyncio.gather(*seeder.retry_tasks.values(), return_exceptions=True)


@pytest.mark.parametrize("case", ["fetch_fails_once", "terminal_failure"])
async def test_scenario_seed_retry(case):
    gateway = SeedGateway(daily_failures=1 if case == "fetch_fails_once" else 0, terminal=case == "terminal_failure")
    seeder, delays = make_seeder(gateway)
    first, second = SeedStrategy("a"), SeedStrategy("b")
    assert seeder.state_of("a") is SeedState.PENDING
    await seeder.seed_all([first, second])
    assert gateway.daily_calls == 1
    if case == "terminal_failure":
        assert {seeder.state_of("a"), seeder.state_of("b")} == {SeedState.QUARANTINED_TERMINAL}
        assert seeder.retry_tasks == {}
        await asyncio.sleep(0)
        assert gateway.daily_calls == 1
        return
    assert {seeder.state_of("a"), seeder.state_of("b")} == {SeedState.QUARANTINED_RECOVERABLE}
    assert set(seeder.retry_tasks) == {"a", "b"}
    await wait_for_tasks(seeder)
    assert len(delays) == 2 and all(4.0 <= d <= 6.0 for d in delays)
    assert gateway.daily_calls == 2
    assert {seeder.state_of("a"), seeder.state_of("b")} == {SeedState.READY}
    assert first.seed_states == [True] and second.seed_states == [True]


@pytest.mark.parametrize("slots", [1, 5])
async def test_one_seed_fetch_shared_between_slots(slots):
    gateway = SeedGateway()
    gateway.delay = 0.01
    seeder, _ = make_seeder(gateway)
    await seeder.seed_all([SeedStrategy(f"s{i}") for i in range(slots)])
    assert gateway.daily_calls == 1 and gateway.seed_calls == 1


async def test_cache_serves_a_later_attempt_within_ttl_only_for_non_empty_results():
    gateway = SeedGateway()
    seeder, _ = make_seeder(gateway)
    await seeder.attempt(SeedStrategy("a"))
    await seeder.attempt(SeedStrategy("b"))
    assert gateway.daily_calls == 1


async def test_failed_seed_quarantines_only_that_slot():
    seeder, _ = make_seeder(SeedGateway())
    bad, good = SeedStrategy("bad", warmup_error=ConnectionError("x")), SeedStrategy("good")
    await seeder.seed_all([bad, good])
    assert seeder.state_of("bad") is SeedState.QUARANTINED_RECOVERABLE and seeder.state_of("good") is SeedState.READY
    seeder.cancel_retries()


async def test_startup_and_retry_run_the_same_attempt():
    gateway = SeedGateway(daily_failures=1)
    seeder, _ = make_seeder(gateway)
    calls = []
    original = seeder.attempt

    async def spy(strategy):
        calls.append(strategy.name)
        return await original(strategy)

    seeder.attempt = spy
    await seeder.seed_all([SeedStrategy("a")])
    await wait_for_tasks(seeder)
    assert calls == ["a", "a"] and seeder.state_of("a") is SeedState.READY


def test_retry_backoff_jittered_and_capped():
    draws = iter([0.0, 1.0, 0.5, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0])
    backoff = Backoff(5.0, 300.0, 0.2, rng=lambda: next(draws))
    delays = [backoff.next_delay() for _ in range(9)]
    assert delays[0] == 5.0 and delays[1] == pytest.approx(12.0)
    assert all(5.0 <= d <= 300.0 for d in delays) and delays[-1] == 300.0


async def test_terminal_quarantine_never_overwritten_by_retry():
    gateway = SeedGateway(terminal=True)
    seeder, _ = make_seeder(gateway)
    strategy = SeedStrategy("a")
    await seeder.seed_all([strategy])
    gateway.terminal = False
    assert await seeder.attempt(strategy) is SeedState.QUARANTINED_TERMINAL
    assert seeder.state_of("a") is SeedState.QUARANTINED_TERMINAL and strategy.seed_states == []


async def test_two_attempts_never_seed_one_slot_at_once():
    gateway = SeedGateway()
    gateway.delay = 0.02
    seeder, _ = make_seeder(gateway)
    strategy = SeedStrategy("a")
    await asyncio.gather(seeder.attempt(strategy), seeder.attempt(strategy))
    assert len(strategy.histories) == 1


async def test_history_handed_over_in_fixed_order_then_recorded_bars_replayed():
    gateway = SeedGateway()
    order = []
    strategy = SeedStrategy("a")
    original_warmup, original_replay = strategy.warmup, strategy.rebuild_intraday_state
    strategy.warmup = lambda h: (order.append("warmup"), original_warmup(h))[1]

    async def replay(bars):
        order.append(f"replay{len(bars)}")
        await original_replay(bars)

    strategy.rebuild_intraday_state = replay
    strategy.on_seed_state = lambda ready: order.append("ready")
    seeder, _ = make_seeder(gateway, recorded=lambda instrument, session, at_ts: [{"start_ts": 5.0}])
    await seeder.attempt(strategy)
    assert order == ["warmup", "replay3", "replay1", "ready"]
    assert strategy.histories[0]["daily"] and len(strategy.histories[0]["intraday"]) == 3


async def test_empty_needed_history_is_not_ready():
    class Empty(SeedGateway):
        async def get_daily_history(self, *args):
            self.daily_calls += 1
            return []

    seeder, _ = make_seeder(Empty())
    await seeder.attempt(SeedStrategy("a"))
    assert seeder.state_of("a") is SeedState.QUARANTINED_RECOVERABLE
    seeder.cancel_retries()


async def test_slot_without_history_needs_still_gets_ready():
    gateway = SeedGateway()
    seeder, _ = make_seeder(gateway)
    strategy = SeedStrategy("a", daily_months=0, intraday=())
    await seeder.attempt(strategy)
    assert seeder.state_of("a") is SeedState.READY and gateway.daily_calls == 0


async def test_fatal_seed_failure_is_terminal():
    seeder, _ = make_seeder(SeedGateway(seed_fails=True, fatal_seed=True))
    await seeder.attempt(SeedStrategy("a"))
    assert seeder.state_of("a") is SeedState.QUARANTINED_TERMINAL


async def test_shared_fetch_does_not_keep_empty_results():
    fetch = SharedFetch(60.0, lambda: 0.0)
    calls = []

    async def empty():
        calls.append(1)
        return []

    await fetch.get(("k",), empty)
    await fetch.get(("k",), empty)
    assert len(calls) == 2
