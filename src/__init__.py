import logging
import os
from typing import Final

_LOG_FORMAT: Final[str] = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def _configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    logging.basicConfig(
        level=logging._nameToLevel.get(_LOG_LEVEL, logging.INFO), format=_LOG_FORMAT, datefmt=_DATE_FORMAT
    )


_configure_logging()
