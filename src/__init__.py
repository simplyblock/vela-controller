import logging
from typing import Final

_LOG_FORMAT: Final[str] = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"


def _configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, datefmt=_DATE_FORMAT)


_configure_logging()
