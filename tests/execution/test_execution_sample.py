from datetime import date, datetime, timezone

from alchemist.clock import ReplayClock
from alchemist.exchanges import exchange_profile
from alchemist.gateways.tapes import tape_from_spec
from alchemist.instruments import MES

import alchemist.gateways  # noqa: F401
from exec_helpers import ScriptedSignal, Slot, settle

ENTRY_INDEX, TARGET_INDEX = 601, 618


async def test_golden_macd_entry_then_target_exit():
    signal = ScriptedSignal(cap=1, exit_names=("stop", "target", "end_of_day"))
    signal.entries = {ENTRY_INDEX}
    signal.exits = {TARGET_INDEX: {"target": True}}
    slot = Slot(signal)
    start, end = exchange_profile(MES).session_window("NY_RTH", date(2026, 4, 15))
    bars = list(tape_from_spec("synthetic:golden_session").bars(MES, start, end))
    assert len(bars) == 23_400

    processing = []
    for index, bar in enumerate(bars):
        slot.clock.advance(datetime.fromtimestamp(bar["end_ts"], timezone.utc))
        signal.index = index
        slot.execution.on_bar(bar, signal.step)
        await settle()
        if slot.orders and index in (ENTRY_INDEX, TARGET_INDEX):
            processing.append(slot.execution.is_processing_order())
            await slot.fill_last(bar["close"])
            processing.append(slot.execution.is_processing_order())
        elif index > TARGET_INDEX + 5:
            assert not slot.execution.is_processing_order()

    assert signal.steps == 23_400
    assert slot.sides() == [("BUY", "MKT", 1), ("SELL", "MKT", 1)]
    assert processing == [True, False, True, False]
    assert slot.execution.state.value == "DONE"
    assert slot.execution.consume_trade_closed() is True
