import logging
import os
import sys
from datetime import datetime

BASE_FORMAT = "%(asctime)s %(script)s [%(levelname)s] %(message)s"
FORMATS = {
    logging.WARNING: f"\033[33m{BASE_FORMAT}\033[00m",
    logging.ERROR: f"\033[31m{BASE_FORMAT}\033[00m",
}


class LevelFormatter(logging.Formatter):
    def format(self, record):
        fmt = FORMATS.get(record.levelno, BASE_FORMAT)
        formatter = logging.Formatter(fmt)
        return formatter.format(record)


class LogHandler(logging.Handler):
    def __init__(self, script_name: str) -> None:
        super().__init__()
        self.script_name: str = os.path.splitext(os.path.basename(script_name))[0]
        self.file_handler: logging.FileHandler | None = None

        self.stream_handler: logging.StreamHandler = logging.StreamHandler()
        self.stream_handler.setFormatter(LevelFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        # Top level script name
        record.script = self.script_name
        self.stream_handler.emit(record)

        # Save Warnings and Errors to a file
        if record.levelno >= logging.WARNING:
            if self.file_handler is None:
                date_str = datetime.now().strftime("%Y-%m-%d")
                log_filename = f"{self.script_name}_{date_str}.log"
                self.file_handler = logging.FileHandler(log_filename, encoding="utf-8")
                self.file_handler.setFormatter(logging.Formatter(BASE_FORMAT))
            self.file_handler.emit(record)


def get_top_level_script() -> str:
    """Returns the name of the top-level script."""
    if hasattr(sys, "argv") and sys.argv and sys.argv[0]:
        return sys.argv[0]
    return "__main__"


def setup_logging() -> None:
    """Sets up logging configuration for the root logger."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    root_logger.addHandler(LogHandler(get_top_level_script()))
