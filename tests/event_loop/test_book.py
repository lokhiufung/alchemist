import asyncio
import logging

import pytest

from alchemist.event_loop.book import BookError, RuntimeBook
from alchemist.event_loop.resampler import Resampler
from alchemist.instruments import ES, MES, NQ

from el_helpers import Harness, RecordingStrategy, bar_update, end_of_data, OPEN
from seed_helpers import SeedGateway, SeedStrategy, make_seeder


def test_reserve_is_idempotent_and_keeps_the_existing_partition():
    book = RuntimeBook()
    assert book.reserve_instrument(MES, {"c": 1}, fed=True) is True
    book.bars[MES].append({"start_ts": 1})
    assert book.reserve_instrument(MES, {"c": 2}, fed=True) is False
    assert list(book.bars[MES]) == [{"start_ts": 1}] and book.contracts[MES] == {"c": 1}


def test_release_only_when_no_strategy_uses_the_instrument():
    book = RuntimeBook()
    book.reserve_instrument(MES, {}, fed=True)
    book.reserve_instrument(ES, {}, fed=True)
    book.commit(SeedStrategy("a", MES), [])
    book.release_instrument(MES)
    book.release_instrument(ES)
    assert MES in book.contracts and ES not in book.contracts and ES not in book.fed_instruments


def test_duplicate_key_commit_leaves_book_unchanged():
    book = RuntimeBook()
    book.commit(SeedStrategy("a"), [])
    snapshot = (list(book.strategies), dict(book.sessions_by_strategy), dict(book.retry_tasks))
    with pytest.raises(BookError, match="a"):
        book.commit(SeedStrategy("a", ES), [object()])
    assert (list(book.strategies), dict(book.sessions_by_strategy), dict(book.retry_tasks)) == snapshot


def test_commit_records_session_maps():
    book = RuntimeBook()
    book.commit(SeedStrategy("a", MES, execution_instrument=NQ), [])
    assert book.sessions_by_strategy["a"] == "NY_RTH" and book.sessions_by_execution_instrument[NQ] == "NY_RTH"
    assert book.strategies_for(MES) == book.strategies and book.strategies_for(ES) == []


async def test_dispatch_sees_one_strategy_list():
    first = RecordingStrategy("a")
    harness = Harness([first])
    called = []
    late = RecordingStrategy("late")
    original = first.on_bar

    def on_bar(bars, bar):
        harness.book.strategies.append(late)
        original(bars, bar)

    first.on_bar = on_bar
    harness.push(bar_update(OPEN), end_of_data())
    await harness.run()
    assert late.bars == []


def build_book(message_type="bar_update", resolution=1.0, slot_resolution=1.0):
    gateway = SeedGateway(message_type=message_type, resolution=resolution)
    seeder, _ = make_seeder(gateway)
    book = RuntimeBook()
    book.attach(seeder.requests, seeder)
    strategy = SeedStrategy("slot")
    strategy.resolution_s = slot_resolution
    return book, gateway, seeder, strategy


@pytest.mark.parametrize("slot_resolution", [1.0, 0.5])
async def test_bar_update_feed_bypasses_resampler_and_finer_resolution_refused(slot_resolution):
    book, gateway, seeder, strategy = build_book(slot_resolution=slot_resolution)
    if slot_resolution < 1.0:
        with pytest.raises(BookError, match="slot"):
            await book.admit(strategy)
        assert book.strategies == [] and MES not in book.contracts
        return
    await book.admit(strategy)
    assert MES not in book.resamplers and book.strategies == [strategy]
    assert gateway.subscribed == [MES]


async def test_admit_on_tick_feed_creates_a_resampler_and_subscribes_once():
    book, gateway, seeder, strategy = build_book(message_type="tick_update")
    await book.admit(strategy)
    second = SeedStrategy("second")
    await book.admit(second)
    assert isinstance(book.resamplers[MES], Resampler)
    assert gateway.subscribed == [MES] and len(book.strategies) == 2
    assert MES in book.sessions_by_instrument


async def test_admit_subscribes_only_new_signal_instruments_and_reserves_execution_instrument():
    book, gateway, seeder, _ = build_book()
    strategy = SeedStrategy("cross", MES, execution_instrument=NQ)
    await book.admit(strategy)
    assert gateway.subscribed == [MES] and NQ in book.contracts and NQ not in book.fed_instruments


async def test_failed_admit_releases_only_what_it_reserved():
    book, gateway, seeder, strategy = build_book()
    await book.admit(strategy)

    async def explode(instrument):
        if instrument is NQ:
            raise RuntimeError("no contract")
        return {"instrument": instrument}

    gateway.resolve_contract = explode
    with pytest.raises(RuntimeError):
        await book.admit(SeedStrategy("bad", MES, execution_instrument=NQ))
    assert MES in book.contracts and NQ not in book.contracts and len(book.strategies) == 1


async def test_admit_with_a_fetch_miss_still_commits_and_retries():
    book, gateway, seeder, strategy = build_book()
    gateway.daily_failures = 1
    await book.admit(strategy)
    assert book.strategies == [strategy] and "slot" in book.retry_tasks and book.retry_tasks["slot"]
    seeder.cancel_retries()


def test_commit_sends_a_member_event_to_each_listener():
    joined = []
    listener = type("Listener", (), {"member_joined": lambda self, strategy: joined.append(strategy.name)})()
    book = RuntimeBook()
    book.member_listeners.append(listener)
    book.commit(SeedStrategy("a"), [])
    with pytest.raises(BookError):
        book.commit(SeedStrategy("a"), [])
    assert joined == ["a"]


async def test_book_op_runs_between_events_and_resolves_its_future():
    from alchemist.messages import make_event, make_reply
    first = RecordingStrategy("a")
    harness = Harness([first])
    trace = []
    original = first.on_bar
    first.on_bar = lambda bars, bar: (trace.append(("bar", bar["start_ts"])), original(bars, bar))[1]

    async def deploy_step(request):
        contract = await harness.requests.request("resolve_contract", instrument=first.signal_instrument)
        trace.append("op")
        harness.book.reserve_instrument(first.signal_instrument, contract, fed=False)
        return contract

    step = None

    def handle(request):
        nonlocal step
        step = asyncio.ensure_future(deploy_step(request))

    harness.deploy.handle = handle
    harness.push(make_event("deploy_request", {"request_id": 1, "key": "b", "config_sha256": "x"}, 0.0))
    runner = asyncio.create_task(harness.loop.run())
    command = await harness.outbound.get()
    harness.push(bar_update(OPEN), make_reply(command, 0.0, result={"instrument": first.signal_instrument}),
                 bar_update(OPEN + 1))
    contract = await step
    harness.push(end_of_data())
    await runner
    assert contract == {"instrument": first.signal_instrument} and trace.count("op") == 1
    assert first.bars == [OPEN, OPEN + 1] and trace.index("op") > trace.index(("bar", OPEN))
