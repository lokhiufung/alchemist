from __future__ import annotations

import asyncio
from typing import Any

from alchemist.messages import make_command


class RequestFailed(RuntimeError): ...


class TerminalRequestError(RequestFailed): ...


class Requests:
    def __init__(self, outbound) -> None:
        self.outbound = outbound
        self.waiting: dict[int, asyncio.Future] = {}
        self.order_id = 1

    def command(self, command_type: str, **data: Any) -> int:
        command = make_command(command_type, data)
        self.outbound.push(command)
        return command["correlation_id"]

    async def request(self, command_type: str, **data: Any) -> Any:
        command = make_command(command_type, data)
        future = asyncio.get_running_loop().create_future()
        self.waiting[command["correlation_id"]] = future
        self.outbound.push(command)
        try:
            return await future
        finally:
            self.waiting.pop(command["correlation_id"], None)

    def resolve(self, reply: dict) -> None:
        future = self.waiting.get(reply["correlation_id"])
        if future is None or future.done():
            return
        if reply["error"] is None:
            future.set_result(reply["result"])
        else:
            error = TerminalRequestError if reply["terminal"] else RequestFailed
            future.set_exception(error(reply["error"]))

    def next_order_id(self) -> int:
        order_id, self.order_id = self.order_id, self.order_id + 1
        return order_id

    def raise_order_id_floor(self, floor: int) -> None:
        self.order_id = max(self.order_id, floor)
