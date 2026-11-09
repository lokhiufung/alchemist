from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path


def setup_logging(log_path: Path, level: int = logging.INFO) -> logging.handlers.QueueListener:
    """System map decision 29: one queue handler; a listener thread writes the files."""
    ...
