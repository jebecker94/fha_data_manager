# -*- coding: utf-8 -*-
"""
Configuration management for FHA Data Manager.

Created on Sun Dec 29 08:46:50 2024
@author: Jebecker3
"""

from pathlib import Path
from decouple import config


class Config:
    """Centralized configuration for FHA Data Manager."""
    
    # Directory paths
    PROJECT_DIR = Path(config('PROJECT_DIR', default=Path(__file__).resolve().parent.parent.parent))
    DATA_DIR = Path(config('DATA_DIR', default=PROJECT_DIR / 'data'))
    RAW_DIR = Path(config('RAW_DIR', default=DATA_DIR / 'raw'))
    CLEAN_DIR = Path(config('CLEAN_DIR', default=DATA_DIR / 'clean'))
    DATABASE_DIR = Path(config('DATABASE_DIR', default=DATA_DIR / 'database'))
    OUTPUT_DIR = Path(config('OUTPUT_DIR', default=PROJECT_DIR / 'output'))
    
    @classmethod
    def ensure_directories(cls):
        """Create necessary directories if they don't exist."""
        for dir_path in [cls.DATA_DIR, cls.RAW_DIR, cls.CLEAN_DIR, 
                        cls.DATABASE_DIR, cls.OUTPUT_DIR]:
            dir_path.mkdir(parents=True, exist_ok=True)


# Legacy constants for backward compatibility
PROJECT_DIR = Config.PROJECT_DIR
DATA_DIR = Config.DATA_DIR
RAW_DIR = Config.RAW_DIR
CLEAN_DIR = Config.CLEAN_DIR
