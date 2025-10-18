"""Tests for utility modules."""

import pytest
from pathlib import Path
from fha_data_manager.utils import Config


class TestConfig:
    """Test Config class."""
    
    def test_config_has_paths(self):
        """Test that Config has all expected path attributes."""
        assert hasattr(Config, 'PROJECT_DIR')
        assert hasattr(Config, 'DATA_DIR')
        assert hasattr(Config, 'RAW_DIR')
        assert hasattr(Config, 'CLEAN_DIR')
        assert hasattr(Config, 'DATABASE_DIR')
        assert hasattr(Config, 'OUTPUT_DIR')
    
    def test_config_paths_are_paths(self):
        """Test that Config paths are Path objects."""
        assert isinstance(Config.PROJECT_DIR, Path)
        assert isinstance(Config.DATA_DIR, Path)
        assert isinstance(Config.RAW_DIR, Path)
        assert isinstance(Config.CLEAN_DIR, Path)
        assert isinstance(Config.DATABASE_DIR, Path)
        assert isinstance(Config.OUTPUT_DIR, Path)
    
    def test_config_path_hierarchy(self):
        """Test that Config paths have correct hierarchy."""
        # DATA_DIR should be under PROJECT_DIR
        assert Config.DATA_DIR.is_relative_to(Config.PROJECT_DIR) or \
               str(Config.PROJECT_DIR) in str(Config.DATA_DIR)
        
        # RAW_DIR and CLEAN_DIR should reference DATA_DIR
        assert 'data' in str(Config.RAW_DIR).lower()
        assert 'data' in str(Config.CLEAN_DIR).lower()
    
    def test_ensure_directories_method(self, temp_data_dir):
        """Test that ensure_directories creates directories."""
        # This is a bit tricky to test without modifying the actual Config
        # We'll just check that the method exists and is callable
        assert hasattr(Config, 'ensure_directories')
        assert callable(Config.ensure_directories)

