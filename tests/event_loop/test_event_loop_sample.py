import logging
from datetime import date, datetime, timezone

import pytest

import alchemist.gateways  # noqa: F401
from alchemist.clock import ReplayClock
from alchemist.config import EventLoopConfig, GatewayConfig, ReplayConfig
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.buffer import EventBuffer
from alchemist.event_loop.loop import EventLoop
from alchemist.event_loop.seeder import Seeder
from alchemist.event_loop.shutdown import ShutdownCoordinator
from alchemist.exchanges import exchange_profile
from alchemist.gateways.simulation_gateway import SimulationGateway
from alchemist.instruments import MES

from el_helpers import RecordingStrategy, StubAux, StubRecorder, StubSafety
from seed_helpers import SeedStrategy

ENTRY_INDEX, TARGET_INDEX = 601, 618


class TradingStrategy(RecordingStrategy, SeedStrategy):
    def __init__(self, gateway, clock):
        RecordingStrategy.__init__(self, "golden")
        SeedStrategy.__init__(self, "golden")
        self.gateway, self.clock = gateway, clock
        self.count = 0
        self.seeded_before_first_bar = None
        self.order_updates = []

    async def on_bar(self, bars, bar):
        if self.count == 0:
            self.seeded_before_first_bar = bool(self.histories) and self.seed_states == [True]
        self.clock_at_bar.append(self.clock.timestamp())
        self.bars.append(bar["start_ts"])
        if self.count in (ENTRY_INDEX, TARGET_INDEX):
            side = "BUY" if self.count == ENTRY_INDEX else "SELL"
            await self.gateway.place_order({
                "order_id": self.gateway.next_order_id(), "account_id": "SIM", "instrument": MES, "side": side,
                "qty": 1, "order_type": "MKT", "stop_price": None, "limit_price": None, "oca_group": None,
                "oca_type": None, "tif": "DAY", "outside_rth": True, "slot": "golden"})
        self.count += 1

    def on_order_update(self, update):
        self.order_updates.append(update["status"])


async def test_golden_bar_updates_fills_and_end_of_data(caplog, tmp_path):
    replay = ReplayConfig("synthetic:golden_session", date(2026, 4, 15), "NY_RTH", journal=tmp_path / "j.jsonl")
    start, _ = exchange_profile(MES).session_window("NY_RTH", replay.date)
    clock = ReplayClock(start.astimezone(timezone.utc))
    config = EventLoopConfig()
    buffer = EventBuffer(config)
    gateway = SimulationGateway(buffer, GatewayConfig("simulation", replay=replay), clock)
    book = RuntimeBook()
    seeder = Seeder(gateway, clock, config)
    book.attach(gateway, seeder)
    strategy = TradingStrategy(gateway, clock)
    await gateway.connect()
    await book.admit(strategy)
    assert seeder.state_of("golden").value == "ready" and MES not in book.resamplers
    loop = EventLoop(buffer, book, gateway, StubAux(), StubSafety(), StubRecorder(), StubRecorder(),
                     ShutdownCoordinator(book, gateway, clock, config), clock, config, clock_advance=clock.advance)
    await gateway.start_replay()
    with caplog.at_level(logging.INFO):
        await loop.run()
    assert strategy.seeded_before_first_bar is True
    assert len(strategy.bars) == 23_400 and strategy.bars == sorted(set(strategy.bars))
    assert strategy.clock_at_bar == [b + 1.0 for b in strategy.bars]
    assert strategy.order_updates.count("FILLED") == 2
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(buffer) == 0 and loop.end_of_data_seen
