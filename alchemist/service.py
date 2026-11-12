from __future__ import annotations

from abc import ABC, abstractmethod


class Service(ABC):
    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...
