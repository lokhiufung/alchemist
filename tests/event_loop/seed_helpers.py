import asyncio
from datetime import date, datetime, timezone

from alchemist.config import EventLoopConfig
from alchemist.event_loop.seeder import Seeder
from alchemist.instruments import ES, MES

from el_helpers import OPEN
from request_helpers import DirectRequests
from time_helpers import ManualWatchTime


class SeedGateway:
    def __init__(self, daily_failures=0, terminal=False, message_type="bar_update", resolution=1.0, seed_fails=False,
                 fatal_seed=False) -> None:
        self.daily_failures = daily_failures
        self.terminal = terminal
        self.daily_calls = 0
        self.seed_calls = 0
        self.message_type, self.resolution = message_type, resolution
        self.seed_fails, self.fatal_seed = seed_fails, fatal_seed
        self.subscribed, self.resolved, self.sessions = [], [], []
        self.unsubscribed = []
        self.delay = 0.0
        self.positions, self.open_orders = [], []
        self.exposure_unreadable = False

    async def market_data_type(self, instrument):
        return (self.message_type, self.resolution if self.message_type == "bar_update" else None)

    def intraday_seed_failure_is_fatal(self):
        return self.fatal_seed

    async def get_daily_history(self, instrument, start, end, session):
        self.daily_calls += 1
        await asyncio.sleep(self.delay)
        if self.terminal:
            from alchemist.event_loop.seeder import TerminalSeedError
            raise TerminalSeedError("history can never arrive")
        if self.daily_calls <= self.daily_failures:
            raise ConnectionError("history unavailable")
        return [{"date": "d", "start_ts": 1.0, "close": 100.0}]

    async def get_intraday_seed_bars(self, instrument, session):
        self.seed_calls += 1
        if self.seed_fails:
            raise ConnectionError("no seed bars")
        return [{"start_ts": OPEN - 10 + i, "close": 100.0} for i in range(3)]

    async def resolve_contract(self, instrument):
        self.resolved.append(instrument)
        return {"instrument": instrument}

    async def subscribe_tick(self, contract):
        self.subscribed.append(contract["instrument"])

    async def unsubscribe_tick(self, contract):
        self.unsubscribed.append(contract["instrument"])

    async def get_positions(self, timeout=5.0):
        if self.exposure_unreadable:
            raise TimeoutError("no answer")
        return list(self.positions)

    async def get_open_orders(self, timeout=5.0):
        if self.exposure_unreadable:
            raise TimeoutError("no answer")
        return list(self.open_orders)

    def set_instrument_sessions(self, sessions):
        self.sessions.append(dict(sessions))


class SeedStrategy:
    resolution_s = 1.0

    def __init__(self, name="s", instrument=MES, execution_instrument=None, warmup_error=None, daily_months=2,
                 intraday=("sma5",), account="ACC", session="NY_RTH") -> None:
        from alchemist.strategy_builder.strategy_signal import WarmupRequirements
        self.name = name
        self.signal_instrument = instrument
        self.execution_instrument = execution_instrument or instrument
        self.session = session
        from alchemist.config import StrategyConfig
        self.config = StrategyConfig(name, "stub", account, (instrument,), (self.execution_instrument,))
        self.warmup_error = warmup_error
        self.requirements = WarmupRequirements(daily_months=daily_months, intraday=intraday)
        self.histories, self.replayed, self.seed_states = [], [], []

    def warmup_requirements(self):
        return self.requirements

    def warmup(self, history):
        if self.warmup_error is not None:
            raise self.warmup_error
        self.histories.append(history)

    async def rebuild_intraday_state(self, bars):
        self.replayed.append(list(bars))

    def on_seed_state(self, ready):
        self.seed_states.append(ready)


def make_seeder(gateway, config=None, delays=None, rng=None, recorded=None):
    watch_time = ManualWatchTime(OPEN)
    slept = delays if delays is not None else []

    async def sleep(delay):
        slept.append(delay)

    kwargs = {"sleep": sleep}
    if rng is not None:
        kwargs["rng"] = rng
    if recorded is not None:
        kwargs["recorded_bars"] = recorded
    return Seeder(DirectRequests(gateway), watch_time, config or EventLoopConfig(), **kwargs), slept
