import asyncio
import logging
from datetime import date

import pytest

import alchemist.gateways  # noqa: F401
from alchemist.config import EventLoopConfig, GatewayConfig, ReplayConfig
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.buffer import CommandBuffer, EventBuffer
from alchemist.event_loop.loop import EventLoop
from alchemist.event_loop.seeder import Seeder
from alchemist.event_loop.shutdown import ShutdownCoordinator
from alchemist.event_loop.watch_time import WatchTime
from alchemist.gateways.simulation_gateway import SimulationGateway
from alchemist.instruments import MES
from alchemist.requests import Requests

from el_helpers import RecordingStrategy, StubAux, StubDeploy, StubRecorder, StubSafety
from seed_helpers import SeedStrategy

ENTRY_INDEX, TARGET_INDEX = 601, 618


class TradingStrategy(RecordingStrategy, SeedStrategy):
    def __init__(self, requests, watch_time):
        RecordingStrategy.__init__(self, "golden", watch_time=watch_time)
        SeedStrategy.__init__(self, "golden")
        self.requests = requests
        self.count = 0
        self.seeded_before_first_bar = None
        self.order_updates = []

    async def on_bar(self, bars, bar):
        if self.count == 0:
            self.seeded_before_first_bar = bool(self.histories) and self.seed_states == [True]
        self.watch_at_bar.append(self.watch_time.now())
        self.bars.append(bar["start_ts"])
        if self.count in (ENTRY_INDEX, TARGET_INDEX):
            side = "BUY" if self.count == ENTRY_INDEX else "SELL"
            self.requests.command("place_order", order={
                "order_id": self.requests.next_order_id(), "account_id": "SIM", "instrument": MES, "side": side,
                "qty": 1, "order_type": "MKT", "stop_price": None, "limit_price": None, "oca_group": None,
                "oca_type": None, "tif": "DAY", "outside_rth": True, "slot": "golden"})
        self.count += 1

    def on_order_update(self, update):
        self.order_updates.append(update["status"])


async def test_golden_bar_updates_fills_and_end_of_data(caplog, tmp_path):
    replay = ReplayConfig("synthetic:golden_session", date(2026, 4, 15), "NY_RTH", journal=tmp_path / "j.jsonl")
    config = EventLoopConfig()
    buffer, outbound = EventBuffer(config), CommandBuffer()
    watch_time = WatchTime(system_time=lambda: 0.0)
    requests = Requests(outbound)
    gateway = SimulationGateway(buffer, outbound, GatewayConfig("simulation", replay=replay))
    book = RuntimeBook()
    seeder = Seeder(requests, watch_time, config)
    book.attach(requests, seeder)
    strategy = TradingStrategy(requests, watch_time)
    loop = EventLoop(buffer, requests, book, StubAux(), StubRecorder(), StubRecorder(),
                     ShutdownCoordinator(book, requests, watch_time, config), watch_time, StubDeploy(),
                     StubSafety().check_in, config)
    buffer.bind_loop(asyncio.get_running_loop())
    await gateway.start()
    running = asyncio.create_task(loop.run())
    await watch_time.ready()
    await book.admit(strategy)
    assert seeder.state_of("golden").value == "ready" and MES not in book.resamplers
    await gateway.start_feed()
    with caplog.at_level(logging.INFO):
        await running
    await gateway.stop()
    assert strategy.seeded_before_first_bar is True
    assert len(strategy.bars) == 23_400 and strategy.bars == sorted(set(strategy.bars))
    assert strategy.watch_at_bar == [b + 1.0 for b in strategy.bars]
    assert strategy.order_updates.count("FILLED") == 2
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(buffer) == 0 and loop.end_of_data_seen
