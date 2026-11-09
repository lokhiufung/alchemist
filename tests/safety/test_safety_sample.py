from datetime import timedelta

import pytest

from alchemist.config import SafetyConfig
from alchemist.instruments import MES, NQ

from exec_helpers import Slot, status_update
from safety_helpers import PendingStrategy, Rig, execution, order, utc

START = utc(15, 15, 0)


class SlotStrategy:
    def __init__(self, slot):
        self.slot, self.name = slot, slot.name
        self.signal_instrument = self.execution_instrument = MES
        self.session = "NY_RTH"

    def get_submitted_orders(self):
        return self.slot.om.get_submitted_orders()

    def get_open_orders(self):
        return self.slot.om.get_open_orders()

    def get_cancelling_orders(self):
        return self.slot.om.get_cancelling_orders()

    def is_processing_order(self):
        return self.slot.execution.is_processing_order()


@pytest.mark.parametrize("base,cap", [(15.0, 120.0), (120.0, 120.0)])
async def test_outage_fill_repaired_by_order_id(base, cap):
    slot = Slot()
    slot.signal.entries = {1}
    await slot.bar(1)
    entry_id = slot.orders[0]["order_id"]
    assert slot.execution.state.value == "PROCESSING_ENTRY_ORDER"
    rig = Rig(START, config=SafetyConfig(reconnect_base_s=base, reconnect_cap_s=cap),
              strategies=[SlotStrategy(slot), PendingStrategy("other", NQ, [order(43, NQ, 5.0, START.timestamp())])])
    drop = START + timedelta(seconds=2)
    rig.gateway.heartbeat_ok = lambda now: now < drop.timestamp() or rig.gateway.recovered
    rig.gateway.last_tick = lambda: rig.clock.timestamp()
    rig.gateway.reconnect_failures = 1
    rig.gateway.executions = [execution(entry_id, MES, 5000.0, START.timestamp() + 5),
                              execution(43, NQ, 18000.0, START.timestamp() + 6)]
    await rig.steps(START, START + timedelta(seconds=5))
    fills = rig.buffer.of_type("repaired_fill")
    assert sorted(f["order_id"] for f in fills) == sorted([entry_id, 43])
    entry_fill = next(f for f in fills if f["order_id"] == entry_id)
    assert (entry_fill["status"], entry_fill["avg_fill_price"], entry_fill["filled"]) == ("FILLED", 5000.0, 1)
    assert rig.sleeps[0] == base
    change = slot.om.on_order_update(entry_fill)
    assert change is not None
    slot.execution.on_order_update(entry_fill)
    assert slot.execution.state.value == "IN_POSITION_ACTIVITY"
