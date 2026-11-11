from datetime import date

from time_helpers import ManualWatchTime
from alchemist.instruments import MES

from aux_helpers import tape_bar, ScriptedSource, StubStrategy, make_provider, ny


async def test_golden_no_aux_names_is_inert():
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.always_fail = True
    strategy = StubStrategy("macd", MES, [])
    provider = make_provider({"unused": source}, [strategy], clock)
    await provider.start()
    bar = tape_bar(MES, ny(9, 30).timestamp(), 5000.0)
    before = dict(bar)
    provider.annotate(MES, bar)
    await provider.step_all()
    await provider.stop()
    assert bar == before
    assert source.fetches == 0 and strategy.calls == [] and provider.groups == []
