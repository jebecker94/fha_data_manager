"""Utility helpers for configuring project-wide logging."""

from __future__ import annotations

import logging
from typing import Mapping


_LOG_LEVEL_NAMES: Mapping[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def resolve_log_level(level: str | int | None) -> int:
    """Return a numeric logging level from ``level``.

    Parameters
    ----------
    level:
        May be an integer logging level, a string matching one of the standard
        logging level names (case insensitive), or ``None``. ``None`` defaults
        to :data:`logging.INFO`.

    Returns
    -------
    int
        The numeric logging level that should be applied to the root logger.

    Raises
    ------
    ValueError
        If ``level`` cannot be interpreted as a valid logging level.
    """

    if level is None:
        return logging.INFO

    if isinstance(level, int):
        return level

    normalized = level.strip().upper()
    if normalized in _LOG_LEVEL_NAMES:
        return _LOG_LEVEL_NAMES[normalized]

    if normalized.isdigit():
        return int(normalized)

    raise ValueError(f"Unknown logging level: {level!r}")


def configure_logging(
    level: str | int | None = logging.INFO,
    *,
    log_format: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt: str | None = "%Y-%m-%d %H:%M:%S",
    force: bool = False,
) -> int:
    """Configure the root logger with project defaults.

    The helper centralises logging configuration so command-line interfaces and
    legacy entry points all produce consistent, timestamped output. Repeated
    calls update the logging level without creating duplicate handlers unless
    ``force`` is set to ``True``.

    Parameters
    ----------
    level:
        Desired logging level. Accepts the same values as
        :func:`resolve_log_level` and defaults to ``"INFO"``.
    log_format:
        Format string passed to :func:`logging.basicConfig` when handlers need
        to be created.
    datefmt:
        Optional date format passed to :func:`logging.basicConfig`.
    force:
        When ``True``, forces :func:`logging.basicConfig` to recreate handlers
        even if logging has already been configured elsewhere.

    Returns
    -------
    int
        The resolved numeric logging level that was applied.
    """

    resolved_level = resolve_log_level(level)

    root_logger = logging.getLogger()
    if force or not root_logger.handlers:
        logging.basicConfig(level=resolved_level, format=log_format, datefmt=datefmt)
    else:
        root_logger.setLevel(resolved_level)
        for handler in root_logger.handlers:
            handler.setLevel(resolved_level)

    return resolved_level


__all__ = ["configure_logging", "resolve_log_level"]
