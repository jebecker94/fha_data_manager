"""Tests for the analysis modules."""

import pytest
import polars as pl
from fha_data_manager.analysis import (
    load_combined_data,
    analyze_lender_activity,
    analyze_sponsor_activity,
    analyze_loan_characteristics,
)
from fha_data_manager.analysis.institutions import InstitutionAnalyzer


class TestExploratoryAnalysis:
    """Test exploratory analysis functions."""
    
    def test_load_combined_data(self, sample_data_file):
        """Test loading data."""
        df = load_combined_data(sample_data_file)
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
    
    def test_analyze_lender_activity(self, sample_single_family_data):
        """Test lender activity analysis."""
        results = analyze_lender_activity(sample_single_family_data)
        assert isinstance(results, dict)
        assert 'lender_volume' in results
        assert 'yearly_lenders' in results
        assert isinstance(results['lender_volume'], pl.DataFrame)
        assert len(results['lender_volume']) > 0
    
    def test_analyze_sponsor_activity(self, sample_single_family_data):
        """Test sponsor activity analysis."""
        results = analyze_sponsor_activity(sample_single_family_data)
        assert isinstance(results, dict)
        assert 'sponsor_volume' in results
        assert 'yearly_sponsors' in results
        assert isinstance(results['sponsor_volume'], pl.DataFrame)
    
    def test_analyze_loan_characteristics(self, sample_single_family_data):
        """Test loan characteristics analysis."""
        results = analyze_loan_characteristics(sample_single_family_data)
        assert isinstance(results, dict)
        assert 'loan_purpose' in results
        assert 'down_payment' in results
        assert 'yearly_loan_size' in results
        assert all(isinstance(v, pl.DataFrame) for v in results.values())


class TestInstitutionAnalyzer:
    """Test InstitutionAnalyzer class."""
    
    def test_analyzer_initialization(self, sample_data_file):
        """Test analyzer can be initialized."""
        analyzer = InstitutionAnalyzer(sample_data_file)
        assert analyzer.data_path == sample_data_file
        assert analyzer.df is None
    
    def test_analyzer_load_data(self, sample_data_file):
        """Test analyzer can load data."""
        analyzer = InstitutionAnalyzer(sample_data_file)
        analyzer.load_data()
        assert analyzer.df is not None
    
    def test_build_crosswalk(self, sample_data_file):
        """Test building institution crosswalk."""
        analyzer = InstitutionAnalyzer(sample_data_file)
        analyzer.load_data()
        crosswalk = analyzer.build_institution_crosswalk()
        assert isinstance(crosswalk, pl.DataFrame)
        assert 'institution_number' in crosswalk.columns
        assert 'institution_name' in crosswalk.columns
        assert 'type' in crosswalk.columns
    
    def test_find_mapping_errors(self, sample_data_file):
        """Test finding mapping errors."""
        analyzer = InstitutionAnalyzer(sample_data_file)
        analyzer.load_data()
        errors = analyzer.find_mapping_errors()
        assert isinstance(errors, pl.DataFrame)
        # Sample data should have no errors
        assert len(errors) == 0
    
    def test_analyze_id_spaces(self, sample_data_file):
        """Test ID space analysis."""
        analyzer = InstitutionAnalyzer(sample_data_file)
        analyzer.load_data()
        stats = analyzer.analyze_id_spaces()
        assert isinstance(stats, dict)
        assert 'unique_originator_names' in stats
        assert 'unique_originator_ids' in stats
        assert 'overlapping_names' in stats

