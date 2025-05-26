import logging


class LevelFormatter(logging.Formatter):
    BASE_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
    FORMATS = {
        logging.WARNING: "\033[33m%(asctime)s [%(levelname)s] %(message)s\033[00m",
        logging.ERROR: "\033[31m%(asctime)s [%(levelname)s] %(message)s\033[00m",
    }

    def format(self, record):
        fmt = self.FORMATS.get(record.levelno, self.BASE_FORMAT)
        formatter = logging.Formatter(fmt)
        return formatter.format(record)


logger = logging.getLogger(__name__)
logging.getLogger(__name__).setLevel(logging.INFO)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(LevelFormatter())
logger.addHandler(ch)
