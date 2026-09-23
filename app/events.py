"""统一日志。

单测通过 configure_logging(level="DEBUG") 打印处理位点与事件流水，
例如：[cursor=3] accepted height=4 hash=0xa...
"""

from __future__ import annotations

import logging

LOGGER_NAME = "chain_indexer"

_configured = False


def configure_logging(level: str = "INFO") -> logging.Logger:
    global _configured
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    if not _configured:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)
        logger.propagate = False
        _configured = True
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
