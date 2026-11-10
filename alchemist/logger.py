from __future__ import annotations

import copy
import json
import logging
import logging.handlers
import queue
from datetime import datetime, timezone
from pathlib import Path

LOGS_DIR = "logs"
RECORD_FIELDS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "clock_time"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": iso_utc(record.created),
            "clock_time": vars(record).get("clock_time"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            payload["exc_info"] = record.exc_text
        payload.update(fields_of(record))
        return json.dumps(payload, default=str)


def iso_utc(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def fields_of(record: logging.LogRecord) -> dict:
    return {key: value for key, value in vars(record).items() if key not in RECORD_FIELDS}


class NoClock:
    def timestamp(self) -> None:
        return None


class RecordQueue(logging.handlers.QueueHandler):
    clock = NoClock()

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        record = copy.copy(record)
        epoch = self.clock.timestamp()
        record.clock_time = None if epoch is None else iso_utc(epoch)
        record.msg, record.args = record.getMessage(), None
        if record.exc_info:
            record.exc_text, record.exc_info = JsonFormatter().formatException(record.exc_info), None
        return record


class FilePerLogger(logging.Handler):
    def __init__(self, logs_dir: Path) -> None:
        super().__init__()
        self.logs_dir = logs_dir
        self.files: dict[str, logging.Handler] = {}
        self.formatter = JsonFormatter()

    def emit(self, record: logging.LogRecord) -> None:
        file_name = record.name.split(".")[0] or "root"
        handler = self.files.get(file_name)
        if handler is None:
            handler = logging.handlers.TimedRotatingFileHandler(
                self.logs_dir / f"{file_name}.log", when="midnight", backupCount=30, utc=True, encoding="utf-8")
            handler.setFormatter(self.formatter)
            self.files[file_name] = handler
        handler.handle(record)

    def close(self) -> None:
        for handler in self.files.values():
            handler.close()
        super().close()


class Listener(logging.handlers.QueueListener):
    def stop(self) -> None:
        super().stop()
        for handler in self.handlers:
            handler.close()


def strategy_logger(strategy_key: str) -> logging.Logger:
    return logging.getLogger(f"{strategy_key}_strategy")


def attach_clock(clock) -> None:
    for handler in logging.getLogger().handlers:
        if isinstance(handler, RecordQueue):
            handler.clock = clock


def setup_logging(log_path: Path, level: int = logging.INFO) -> Listener:
    logs_dir = Path(log_path) / LOGS_DIR
    logs_dir.mkdir(parents=True, exist_ok=True)
    records: queue.SimpleQueue = queue.SimpleQueue()
    root = logging.getLogger()
    for handler in [h for h in root.handlers if isinstance(h, logging.handlers.QueueHandler)]:
        root.removeHandler(handler)
    root.addHandler(RecordQueue(records))
    root.setLevel(level)
    listener = Listener(records, FilePerLogger(logs_dir))
    listener.start()
    return listener
