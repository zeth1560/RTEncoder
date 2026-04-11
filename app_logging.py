"""Rotating file logging + optional Tk queue handler for the encoder operator."""

from __future__ import annotations

import logging
import queue
from logging.handlers import RotatingFileHandler
from pathlib import Path


class TkQueueLogHandler(logging.Handler):
    """Push formatted log lines to a queue consumed by the Tk text widget."""

    def __init__(self, log_q: queue.Queue[str]) -> None:
        super().__init__()
        self._q = log_q

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record) + "\n"
            self._q.put(msg)
        except Exception:
            self.handleError(record)


def setup_encoder_logging(
    log_file: Path,
    *,
    ui_queue: queue.Queue[str] | None = None,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 7,
) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger("replaytrove.encoder")
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    if ui_queue is not None:
        qh = TkQueueLogHandler(ui_queue)
        qh.setLevel(logging.INFO)
        qh.setFormatter(fmt)
        root.addHandler(qh)

    return root
