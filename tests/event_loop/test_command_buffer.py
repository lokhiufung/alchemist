import asyncio
import threading
from collections import deque

import pytest

from alchemist.event_loop.buffer import CommandBuffer


def command(name, correlation_id):
    return {"command_type": name, "correlation_id": correlation_id}


async def test_consumer_gets_commands_in_push_order():
    buffer = CommandBuffer()
    pushed = [command("subscribe", 1), command("place_order", 2), command("cancel", 3)]
    for item in pushed:
        buffer.push(item)
    taken = []
    for _ in pushed:
        taken.append(await buffer.get())
        buffer.done()
    assert taken == pushed


@pytest.mark.parametrize("taps", [0, 2])
async def test_tap_never_changes_consumer_order(taps):
    buffer = CommandBuffer()
    tap_queues = [deque() for _ in range(taps)]
    for tap in tap_queues:
        buffer.add_tap(tap)
    pushed = [command("place_order", index) for index in range(20)]
    for item in pushed:
        buffer.push(item)
    taken = []
    for _ in pushed:
        taken.append(await buffer.get())
        buffer.done()
    assert taken == pushed
    assert all(list(tap) == pushed for tap in tap_queues)


async def test_idle_only_after_consumer_ran_every_command_it_took():
    buffer = CommandBuffer()
    assert buffer.idle.is_set()
    buffer.push(command("subscribe", 1))
    assert not buffer.idle.is_set()
    await buffer.get()
    assert not buffer.idle.is_set()
    buffer.done()
    assert buffer.idle.is_set()


async def test_get_waits_for_a_push_from_another_thread():
    buffer = CommandBuffer()
    buffer.bind_loop(asyncio.get_running_loop())
    threading.Timer(0.01, lambda: buffer.push(command("reconnect", 9))).start()
    taken = await asyncio.wait_for(buffer.get(), timeout=1.0)
    assert taken["correlation_id"] == 9
