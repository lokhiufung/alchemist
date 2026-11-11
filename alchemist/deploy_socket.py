from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import time
from pathlib import Path

from alchemist.messages import Command, make_event

logger = logging.getLogger("deploy")


class DeploySocketServer:
    def __init__(self, path: Path, inbound) -> None:
        self.path = Path(path)
        self.inbound = inbound
        self.server: asyncio.AbstractServer | None = None
        self.request_ids = itertools.count(1)
        self.writers: dict[int, asyncio.StreamWriter] = {}
        self.finished: dict[int, asyncio.Event] = {}

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
        request_id = next(self.request_ids)
        try:
            request = json.loads(await reader.readline())
            self.writers[request_id] = writer
            self.finished[request_id] = asyncio.Event()
            self.inbound.push(make_event("deploy_request", {
                "request_id": request_id, "key": request["key"], "config_sha256": request["config_sha256"]},
                time.time()))
            await self.finished[request_id].wait()
        except Exception as exc:
            logger.exception("deploy request failed", extra={"event": "deploy_request_failed"})
            writer.write((json.dumps({"status": "failed", "reason": str(exc), "revision": None}) + "\n").encode())
        finally:
            self.writers.pop(request_id, None)
            self.finished.pop(request_id, None)
            writer.close()

    async def write_result(self, command: Command) -> None:
        data = command["data"]
        writer = self.writers.get(data["request_id"])
        if writer is None:
            return
        writer.write((json.dumps(data["line"]) + "\n").encode())
        await writer.drain()
        if data["final"]:
            self.finished[data["request_id"]].set()
