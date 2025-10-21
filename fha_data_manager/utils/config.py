# -*- coding: utf-8 -*-
"""Configuration management helpers for FHA Data Manager."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from decouple import config


class Config:
    """Centralised configuration for FHA Data Manager."""

    PROJECT_DIR: Path = Path(
        config('PROJECT_DIR', default=Path(__file__).resolve().parent.parent.parent)
    )
    DATA_DIR: Path = Path(config('DATA_DIR', default=PROJECT_DIR / 'data'))
    RAW_DIR: Path = Path(config('RAW_DIR', default=DATA_DIR / 'raw'))
    CLEAN_DIR: Path = Path(config('CLEAN_DIR', default=DATA_DIR / 'clean'))
    DATABASE_DIR: Path = Path(config('DATABASE_DIR', default=DATA_DIR / 'database'))
    OUTPUT_DIR: Path = Path(config('OUTPUT_DIR', default=PROJECT_DIR / 'output'))

    @classmethod
    def ensure_directories(cls) -> None:
        """Create necessary directories if they don't exist."""

        required_paths: Iterable[Path] = (
            cls.DATA_DIR,
            cls.RAW_DIR,
            cls.CLEAN_DIR,
            cls.DATABASE_DIR,
            cls.OUTPUT_DIR,
        )
        for dir_path in required_paths:
            dir_path.mkdir(parents=True, exist_ok=True)


PROJECT_DIR: Path = Config.PROJECT_DIR
DATA_DIR: Path = Config.DATA_DIR
RAW_DIR: Path = Config.RAW_DIR
CLEAN_DIR: Path = Config.CLEAN_DIR
DATABASE_DIR: Path = Config.DATABASE_DIR
OUTPUT_DIR: Path = Config.OUTPUT_DIR
