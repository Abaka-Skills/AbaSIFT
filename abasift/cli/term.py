"""Terminal colour, and the one rule that matters: only when someone is watching.

Colour goes on when the destination is a TTY and `NO_COLOR` is unset. Anything else — a
piped log, a CI capture, a file, a worker's redirected stdout — gets clean text, because
escape codes in a log a machine will grep are worse than no colour at all.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import TextIO

_CODES = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "dim": "\033[2m",
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "magenta": "\033[35m",
    "cyan": "\033[36m",
}

#: Level -> style for log records.
_LEVEL_STYLE = {
    "DEBUG": ("dim",),
    "INFO": ("cyan",),
    "WARNING": ("yellow",),
    "ERROR": ("red", "bold"),
    "CRITICAL": ("red", "bold"),
}


def color_enabled(stream: TextIO | None = None) -> bool:
    """https://no-color.org — honoured, plus the usual "is it a terminal" test."""
    stream = stream or sys.stdout
    if os.environ.get("NO_COLOR"):
        return False
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):  # closed or exotic stream
        return False


def paint(text: str, *styles: str, enabled: bool = True) -> str:
    """Wrap ``text`` in ANSI styles. ``enabled=False`` returns it untouched."""
    if not text or not enabled or not styles:
        return text
    return "".join(_CODES[s] for s in styles) + text + _CODES["reset"]


class ColorFormatter(logging.Formatter):
    """The stdlib formatter with the level and the logger name tinted.

    The *message* is never touched: only the framing around it, so a line stays as
    greppable as it was.
    """

    def __init__(self, fmt: str, datefmt: str | None = None, enabled: bool = True):
        super().__init__(fmt, datefmt)
        self.enabled = enabled

    def format(self, record: logging.LogRecord) -> str:
        level, name = record.levelname, record.name
        # Pad before painting: escape codes are characters too, and `%(levelname)-7s`
        # would count them and knock every coloured line out of alignment.
        record.levelname = paint(level.ljust(7), *_LEVEL_STYLE.get(level, ()), enabled=self.enabled)
        record.name = paint(name, "dim", enabled=self.enabled)
        try:
            return super().format(record)
        finally:  # a formatter must not leave the record mutated for other handlers
            record.levelname, record.name = level, name


def setup_logging(verbose: bool = False, stream: TextIO | None = None) -> None:
    """Configure the root logger for the CLI, coloured when the destination is a terminal."""
    stream = stream or sys.stderr
    handler = logging.StreamHandler(stream)
    handler.setFormatter(
        ColorFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s", enabled=color_enabled(stream))
    )
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=[handler], force=True)
