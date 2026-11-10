from __future__ import annotations

import faulthandler
import logging
import sys
import threading
from pathlib import Path
from typing import Callable

from alchemist.config import SafetyConfig

logger = logging.getLogger("freeze")


class FreezeDetector:
    def __init__(self, config: SafetyConfig, dump: Callable[[], None] | None = None,
                 stackdump_path: Path | None = None) -> None:
        self.config = config
        self.stackdump_path = stackdump_path
        self.dump = dump or self.dump_all_stacks
        self.missed = 0
        self.dumped = False
        self.checked_in = False
        self.stopped = threading.Event()
        self.thread: threading.Thread | None = None

    def check_in(self) -> None:
        self.checked_in = True

    def tick(self) -> None:
        if self.checked_in:
            self.checked_in = False
            self.missed, self.dumped = 0, False
            return
        self.missed += 1
        if self.missed >= self.config.freeze_threshold and not self.dumped:
            self.dumped = True
            logger.critical("event loop frozen; dumping every thread's stack",
                            extra={"event": "event_loop_frozen", "missed_check_ins": self.missed})
            self.dump()

    def dump_all_stacks(self) -> None:
        if self.stackdump_path is None:
            faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
            return
        self.stackdump_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.stackdump_path, "a") as handle:
            faulthandler.dump_traceback(file=handle, all_threads=True)

    def start(self) -> None:
        self.stopped.clear()
        self.thread = threading.Thread(target=self.watch, daemon=True, name="freeze-detector")
        self.thread.start()

    def watch(self) -> None:
        while not self.stopped.wait(self.config.check_in_interval_s):
            self.tick()

    def stop(self) -> None:
        self.stopped.set()
        if self.thread is not None:
            self.thread.join(timeout=self.config.check_in_interval_s * 2)
            self.thread = None
