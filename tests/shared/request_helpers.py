import asyncio
import inspect

from alchemist.event_loop.seeder import TerminalSeedError
from alchemist.requests import RequestFailed, TerminalRequestError

METHODS = {"subscribe": "subscribe_tick", "unsubscribe": "unsubscribe_tick", "poll_positions": "get_positions"}


class DirectRequests:
    def __init__(self, gateway) -> None:
        self.gateway = gateway
        self.commands: list[tuple[str, dict]] = []
        self.order_id = 1

    async def call(self, command_type: str, data: dict):
        if not hasattr(self.gateway, METHODS.get(command_type, command_type)):
            return None
        result = getattr(self.gateway, METHODS.get(command_type, command_type))(**data)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def request(self, command_type: str, **data):
        try:
            return await self.call(command_type, data)
        except TerminalSeedError as exc:
            raise TerminalRequestError(repr(exc)) from exc
        except Exception as exc:
            if command_type == "get_intraday_seed_bars" and self.gateway.intraday_seed_failure_is_fatal():
                raise TerminalRequestError(repr(exc)) from exc
            raise RequestFailed(repr(exc)) from exc

    def command(self, command_type: str, **data) -> None:
        self.commands.append((command_type, data))
        asyncio.get_running_loop().create_task(self.call(command_type, data))

    def resolve(self, reply: dict) -> None:
        return None

    def next_order_id(self) -> int:
        order_id, self.order_id = self.order_id, self.order_id + 1
        return order_id

    def raise_order_id_floor(self, floor: int) -> None:
        self.order_id = max(self.order_id, floor)


class OrderSink:
    def __init__(self, first_id: int = 1) -> None:
        self.next_id = first_id
        self.placed: list[dict] = []
        self.cancelled: list[int] = []
        self.on_place = None

    def next_order_id(self) -> int:
        self.next_id += 1
        return self.next_id - 1

    def command(self, command_type: str, **data) -> None:
        if command_type == "place_order":
            if self.on_place is not None:
                self.on_place(data["order"])
            self.placed.append(dict(data["order"]))
        elif command_type == "cancel_order":
            self.cancelled.append(data["order_id"])
