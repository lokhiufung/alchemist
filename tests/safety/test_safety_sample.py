from safety_helpers import Rig, execution


def heartbeat(rig):
    rig.event("heartbeat")


async def test_outage_fill_repaired_by_order_id():
    rig = Rig()
    rig.place(42, "A")
    rig.event("connection_lost")
    rig.run(1, on_tick=heartbeat)
    assert len(rig.alerts()) == 1
    rig.run(10, on_tick=heartbeat)
    rig.event("connection_restored", {"next_order_id": 100})
    rig.broker_executions = [execution(42, 5000.0)]
    rig.auto_reply = True
    rig.run(1, on_tick=heartbeat)
    assert len(rig.requests("get_executions")) == 1 and len(rig.requests("get_open_orders")) == 1
    repaired = [e["data"] for e in rig.repairs()]
    assert [(u["order_id"], u["status"], u["strategy"], u["avg_fill_price"]) for u in repaired] == [
        (42, "FILLED", "A", 5000.0)]
