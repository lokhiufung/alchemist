import asyncio

import pytest

from alchemist.event_loop.buffer import CommandBuffer
from alchemist.messages import make_reply
from alchemist.requests import Requests, RequestFailed, TerminalRequestError


async def test_request_pushes_command_with_fresh_correlation_id_and_waits_for_its_reply():
    outbound = CommandBuffer()
    requests = Requests(outbound)
    waiting = asyncio.ensure_future(requests.request("get_positions"))
    await asyncio.sleep(0)
    command = await outbound.get()
    assert command["command_type"] == "get_positions" and command["correlation_id"] is not None
    requests.resolve(make_reply(command, 0.0, result=[{"size": 1}])["data"])
    assert await waiting == [{"size": 1}]


async def test_error_reply_raises():
    outbound = CommandBuffer()
    requests = Requests(outbound)
    waiting = asyncio.ensure_future(requests.request("get_open_orders"))
    await asyncio.sleep(0)
    command = await outbound.get()
    requests.resolve(make_reply(command, 0.0, error="socket closed")["data"])
    with pytest.raises(RequestFailed):
        await waiting


async def test_terminal_error_reply_raises_terminal_error():
    outbound = CommandBuffer()
    requests = Requests(outbound)
    waiting = asyncio.ensure_future(requests.request("get_intraday_seed_bars"))
    await asyncio.sleep(0)
    command = await outbound.get()
    requests.resolve(make_reply(command, 0.0, error="no data", terminal=True)["data"])
    with pytest.raises(TerminalRequestError):
        await waiting


def test_command_gets_correlation_id_and_order_ids_never_repeat():
    outbound = CommandBuffer()
    requests = Requests(outbound)
    requests.command("cancel_order", order_id=3)
    first, second = requests.next_order_id(), requests.next_order_id()
    requests.raise_order_id_floor(100)
    assert outbound.queue[0]["data"] == {"order_id": 3}
    assert outbound.queue[0]["correlation_id"] is not None
    assert second == first + 1 and requests.next_order_id() == 100


def test_reply_for_unknown_id_is_ignored():
    Requests(CommandBuffer()).resolve({"correlation_id": 999, "result": None, "error": None, "terminal": False})
