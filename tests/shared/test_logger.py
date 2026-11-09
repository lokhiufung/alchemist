import logging

from alchemist.logger import setup_logging


def test_logging_creates_missing_dir(tmp_path):
    log_dir = tmp_path / "does" / "not" / "exist"
    listener = setup_logging(log_dir)
    try:
        logging.getLogger("alchemist.test").warning("hello")
    finally:
        listener.stop()
    line = (log_dir / "engine.log").read_text().splitlines()[0]
    assert " WARNING alchemist.test hello" in line


def test_setup_twice_does_not_duplicate_lines(tmp_path):
    first = setup_logging(tmp_path / "a")
    first.stop()
    second = setup_logging(tmp_path / "b")
    try:
        logging.getLogger("alchemist.test").error("once")
    finally:
        second.stop()
    assert (tmp_path / "b" / "engine.log").read_text().count("once") == 1
