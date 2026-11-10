import json
import logging

from alchemist.logger import attach_clock, setup_logging, strategy_logger


def read(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_logging_creates_missing_dir_and_one_file_per_logger(tmp_path):
    root = tmp_path / "does" / "not" / "exist"
    listener = setup_logging(root)
    try:
        logging.getLogger("startup").warning("hello", extra={"event": "greeting", "who": "world"})
        strategy_logger("macd_long").info("snapshot", extra={"event": "signal_snapshot", "values": {"rsi": 50.0}})
    finally:
        listener.stop()
    [startup] = read(root / "logs" / "startup.log")
    assert startup.pop("timestamp").endswith("+00:00")
    assert startup == {"clock_time": None, "level": "WARNING", "logger": "startup",
                       "method": "test_logging_creates_missing_dir_and_one_file_per_logger", "message": "hello",
                       "event": "greeting", "who": "world"}
    [snapshot] = read(root / "logs" / "macd_long_strategy.log")
    assert snapshot["values"] == {"rsi": 50.0}


def test_dotted_logger_names_share_the_top_level_file(tmp_path):
    listener = setup_logging(tmp_path)
    try:
        logging.getLogger("ib_async.client").info("connected")
    finally:
        listener.stop()
    assert read(tmp_path / "logs" / "ib_async.log")[0]["logger"] == "ib_async.client"


def test_exception_is_a_field_not_part_of_the_message(tmp_path):
    listener = setup_logging(tmp_path)
    try:
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            logging.getLogger("event_loop").exception("dispatch failed", extra={"event": "dispatch_failed"})
    finally:
        listener.stop()
    [line] = read(tmp_path / "logs" / "event_loop.log")
    assert line["message"] == "dispatch failed" and "RuntimeError: boom" in line["exc_info"]


def test_setup_twice_does_not_duplicate_lines(tmp_path):
    first = setup_logging(tmp_path / "a")
    first.stop()
    second = setup_logging(tmp_path / "b")
    try:
        logging.getLogger("startup").error("once")
    finally:
        second.stop()
    assert len(read(tmp_path / "b" / "logs" / "startup.log")) == 1


class StoppedClock:
    def __init__(self, epoch):
        self.epoch = epoch

    def timestamp(self):
        return self.epoch


def test_clock_time_is_the_engine_clock_at_the_moment_of_logging(tmp_path):
    listener = setup_logging(tmp_path)
    clock = StoppedClock(1776260402.0)
    attach_clock(clock)
    try:
        logging.getLogger("event_loop").info("first")
        clock.epoch += 17
        logging.getLogger("event_loop").info("second")
    finally:
        listener.stop()
    first, second = read(tmp_path / "logs" / "event_loop.log")
    assert (first["clock_time"], second["clock_time"]) == ("2026-04-15T13:40:02+00:00", "2026-04-15T13:40:19+00:00")
    assert first["timestamp"] != first["clock_time"]
