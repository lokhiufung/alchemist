from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class DeploySocketServer:
    def __init__(self, path: Path, service) -> None:
        self.path = Path(path)
        self.service = service
        self.server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        if self.path.exists():
            self.path.unlink()
        previous = os.umask(0o177)
        try:
            self.server = await asyncio.start_unix_server(self.handle, path=str(self.path))
        finally:
            os.umask(previous)
        os.chmod(self.path, 0o600)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        if self.path.exists():
            self.path.unlink()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = json.loads(await reader.readline())
            async for line in self.service.deploy(request["key"], request["config_sha256"]):
                writer.write((json.dumps(line) + "\n").encode())
                await writer.drain()
        except Exception as exc:
            logger.exception("deploy request failed")
            writer.write((json.dumps({"status": "failed", "reason": str(exc), "revision": None}) + "\n").encode())
        finally:
            writer.close()
