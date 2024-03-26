import logging
import os
from os.path import join, dirname
from dotenv import load_dotenv


class Formatter(logging.Formatter):
    grey = "\x1b[29;20m"
    yellow = "\x1b[33;2m"
    red = "\x1b[31;2m"
    bold_red = "\x1b[31;21m"
    reset = "\x1b[0m"
    format_info = "%(asctime)s: %(levelname)s: %(module)s: %(message)s"
    format_rest = "%(levelname)s: %(module)s: %(funcName)s: %(lineno)d: %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format_rest + reset,
        logging.INFO: grey + format_info,
        logging.WARNING: yellow + format_rest + reset,
        logging.ERROR: red + format_rest + reset,
        logging.CRITICAL: bold_red + format_rest + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


dotenv_path = join(dirname(dirname(__file__)), 'letterboxd.env')
load_dotenv(dotenv_path)

log_level = logging.getLevelName(os.getenv("LOGLEVEL", "INFO"))
log = logging.getLogger(__name__)
log.setLevel(log_level)

_ch = logging.StreamHandler()
_ch.setFormatter(Formatter())
log.addHandler(_ch)
