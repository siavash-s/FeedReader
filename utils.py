import logging
import sys
from typing import Optional


def init_logging(
        logger_name: Optional[str] = None,
        level: str = 'INFO',
):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        style='%'
    ))
    logger.addHandler(handler)
