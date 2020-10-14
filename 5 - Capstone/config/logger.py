""" A simple logger configuration

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import os

import config

FORMATTER = logging.Formatter(
    "%(asctime)s — %(name)s — %(levelname)s —" "%(funcName)s:%(lineno)d — %(message)s"
)
LOG_DIR = config.ROOT + "/logs"
LOG_FILE = LOG_DIR + "/log.log"

def get_logger(*, logger_name):
    """Get logger with prepared handlers."""
    
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logger = logging.getLogger(logger_name)

    logger.setLevel(logging.INFO)

    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    logger.propagate = False

    return logger

def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    file_handler = TimedRotatingFileHandler(LOG_FILE, when="midnight")
    file_handler.setFormatter(FORMATTER)
    file_handler.setLevel(logging.INFO)
    return file_handler