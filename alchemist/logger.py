from __future__ import annotations

import logging
import logging.handlers
import queue
from pathlib import Path

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def setup_logging(log_path: Path, level: int = logging.INFO) -> logging.handlers.QueueListener:
    log_path = Path(log_path)
    log_path.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path / "engine.log")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    records: queue.SimpleQueue = queue.SimpleQueue()
    root = logging.getLogger()
    for handler in [h for h in root.handlers if isinstance(h, logging.handlers.QueueHandler)]:
        root.removeHandler(handler)
    root.addHandler(logging.handlers.QueueHandler(records))
    root.setLevel(level)
    listener = logging.handlers.QueueListener(records, file_handler)
    listener.start()
    return listener
