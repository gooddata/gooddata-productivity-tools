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
    def __init__(self) -> None:
        super().__init__()
        self.script_name: str = self.get_top_level_script()
        self.modules: list[str] = self.get_module_names()

        self.file_handler: logging.FileHandler | None = None
        self.stream_handler: logging.StreamHandler = logging.StreamHandler()
        self.stream_handler.setFormatter(LevelFormatter())

    @staticmethod
    def get_module_names() -> list[str]:
        """Returns a list of module names in the scripts directory."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_dir = current_dir.split("scripts")[0] + "scripts"
        modules = os.listdir(scripts_dir)
        return modules

    @staticmethod
    def get_top_level_script() -> str:
        """Returns the name of the top-level script - i.e., the script that was executed."""
        if hasattr(sys, "argv") and sys.argv and sys.argv[0]:
            return sys.argv[0].split(os.sep)[-1]
        return "__main__"

    def emit(self, record: logging.LogRecord) -> None:
        # Top level script name
        record.script = self.script_name
        self.stream_handler.emit(record)

        # Save Warnings and Errors to a file
        # Only if the script name is in the modules list (we don't need to log pytest errors etc.)
        if record.levelno >= logging.WARNING and self.script_name in self.modules:
            if self.file_handler is None:
                date_str = datetime.now().strftime("%Y-%m-%d")
                log_filename = f"{self.script_name}_{date_str}.log"
                self.file_handler = logging.FileHandler(log_filename, encoding="utf-8")
                self.file_handler.setFormatter(logging.Formatter(BASE_FORMAT))
            self.file_handler.emit(record)


def setup_logging(verbose: bool = False) -> None:
    """
    Sets up logging configuration for the root logger.
    Terminal logs will be formatted with colors based on the log level.
    Warnings and errors will also be saved to a file named
    `<script_name>_<date>.log` in the current working directory.
    """
    root_logger = logging.getLogger()

    min_level = logging.DEBUG if verbose else logging.INFO

    root_logger.setLevel(min_level)
    root_logger.handlers.clear()
    root_logger.addHandler(LogHandler())
