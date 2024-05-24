import colorlog
import logging
import os, sys

if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)

from config.secrets import LOGGING_LEVEL


def configure_logger(logger):
    # Set level for the logger
    logger.setLevel(logging.getLevelName(LOGGING_LEVEL))

    # Create a color formatter
    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
    )

    # Create a stream handler and set the formatter
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)


def init_logger(name):
    logger = logging.getLogger(name)
    configure_logger(logger)
    return logger