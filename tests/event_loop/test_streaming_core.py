import asyncio
from alchemist.constants.deploy import DeployStatus
from alchemist.instruments import MES
from alchemist.messages import make_event, make_reply

from el_helpers import Harness, RecordingStrategy, end_of_data, order_update
from sd_helpers import FlattenStrategy, Rig, at


async def test_flatten_starts_with_no_bars():
    strategy = FlattenStrategy("s", MES, flat_on_request=None)
    start = at(MES, "NY_RTH", 15, 58, 30)
    rig = Rig([strategy], start)
    rig.push_second_bars(MES, start, 1)
    runner = asyncio.create_task(rig.run())
    while not strategy.bar_log:
        await asyncio.sleep(0)
    assert strategy.request_times == []
    rig.watch_time.advance(at(MES, "NY_RTH", 15, 59, 0))
    while not strategy.request_times:
        await asyncio.sleep(0)
    rig.loop.stopping = True
    await runner
    assert strategy.request_times[0] == at(MES, "NY_RTH", 15, 59, 0).timestamp()


async def test_repair_event_reaches_only_its_strategy():
    owner, other = RecordingStrategy("A"), RecordingStrategy("B")
    harness = Harness([owner, other])
    repaired = order_update(42)
    repaired["data"]["strategy"] = "A"
    harness.push(make_event("repaired_fill", repaired["data"], 0.0), end_of_data())
    await harness.run()
    assert owner.events == [("order", 42, "FILLED")] and other.events == []


async def test_reply_event_resolves_the_waiting_request():
    harness = Harness([RecordingStrategy()])
    waiting = asyncio.ensure_future(harness.requests.request("get_positions"))
    await asyncio.sleep(0)
    command = await harness.outbound.get()
    harness.push(make_reply(command, 0.0, result=[{"size": 0}]), end_of_data())
    await harness.run()
    assert await waiting == [{"size": 0}]


async def test_connection_restored_raises_the_order_id_floor():
    harness = Harness([RecordingStrategy()])
    harness.push(make_event("connection_restored", {"next_order_id": 500}, 0.0), end_of_data())
    await harness.run()
    assert harness.requests.next_order_id() == 500


async def test_deploy_request_event_goes_to_the_deploy_service():
    harness = Harness([RecordingStrategy()])
    request = {"request_id": 3, "key": "b", "config_sha256": "abc"}
    harness.push(make_event("deploy_request", request, 0.0), end_of_data())
    await harness.run()
    assert harness.deploy.requests == [request]


async def test_os_signal_event_requests_shutdown():
    harness = Harness([RecordingStrategy()])
    harness.push(make_event("os_signal", {"signal": "SIGTERM"}, 0.0))
    await harness.run()
    assert harness.shutdown.shutdown_requested


async def test_every_deploy_status_line_is_a_deploy_result_command(tmp_path):
    from test_deploy import Rig as DeployRig
    rig = DeployRig(tmp_path)
    await rig.start_a()
    rig.service.handle({"request_id": 9, "key": "b", "config_sha256": rig.service.file_sha()})
    await asyncio.gather(*rig.service.tasks)
    results = [data for kind, data in rig.requests.commands if kind == "deploy_result"]
    assert [(data["request_id"], data["line"]["status"], data["final"]) for data in results] == [
        (9, DeployStatus.PREPARING, False), (9, DeployStatus.REGISTERED, True)]
