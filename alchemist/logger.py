import os
import logging
from logging.handlers import TimedRotatingFileHandler  # Import the TimedRotatingFileHandler
from alchemist.constants import LOGGING_DIR


if not os.path.exists(LOGGING_DIR):
    os.makedirs(LOGGING_DIR)


def parse_level(level: str):
    level = level.lower()
    if level == 'info':
        level = logging.INFO
    elif level == 'error':
        level = logging.ERROR
    elif level == 'warning':
        level = logging.WARNING
    else:
        level = logging.DEBUG
    return level


def get_logger(name, console_logger_lv='info', file_logger_lv='debug', logger_lv='debug'):
    # Create a logger object with the name "my_logger"
    logger = logging.getLogger(name)
    if not logger.handlers:

        # Set the logging level
        logger_lv = parse_level(logger_lv)
        console_logger_lv = parse_level(console_logger_lv)
        file_logger_lv = parse_level(file_logger_lv)

        logger.setLevel(logger_lv)

        # Create a handler for logging to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_logger_lv)

        # Create a handler for logging to a rotating file with the date in the filename
        file_handler = TimedRotatingFileHandler(
            os.path.join(LOGGING_DIR, f'{name}.log'), 
            when='midnight',  # Rotate the log file daily at midnight
            interval=1,       # Interval is 1 day
            backupCount=0     # Keep all log files, do not delete old ones
        )
        
        # This will add the date to the log file name when it rotates
        file_handler.suffix = "%Y-%m-%d"  # Add the date as a suffix to the file
        file_handler.setLevel(file_logger_lv)

        # Create a formatter for the log messages
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s")
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add the console handler to the logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger