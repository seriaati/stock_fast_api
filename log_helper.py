import contextlib
import logging
import logging.handlers
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

__all__ = ("setup_logging",)


@contextlib.contextmanager
def setup_logging() -> "Generator[None, None, None]":
    log = logging.getLogger()

    try:
        max_bytes = 32 * 1024 * 1024  # 32MB
        log.setLevel(logging.INFO)

        file_handler = logging.handlers.RotatingFileHandler(
            filename="stock-api.log",
            encoding="utf-8",
            mode="w",
            maxBytes=max_bytes,
            backupCount=5,
        )
        stream_handler = logging.StreamHandler()

        dt_fmt = "%Y-%m-%d %H:%M:%S"
        fmt = logging.Formatter(
            "[{asctime}] [{levelname:<7}] {name}: {message}", dt_fmt, style="{"
        )

        file_handler.setFormatter(fmt)
        stream_handler.setFormatter(fmt)
        log.addHandler(file_handler)
        log.addHandler(stream_handler)

        yield
    finally:
        handlers = log.handlers[:]
        for handler in handlers:
            handler.close()
            log.removeHandler(handler)
