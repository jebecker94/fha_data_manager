import unittest
import os
from pathlib import Path
import glob
import datetime
import pandas as pd
from download_fha_data import find_years_in_string, find_month_in_string, standardize_filename
from import_fha_data import clean_sf_sheets, clean_hecm_sheets
import tempfile
import shutil

class TestFHADataProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment with temporary directories"""
        cls.temp_dir = tempfile.mkdtemp()
        cls.data_dir = Path(cls.temp_dir) / "data"
        cls.data_dir.mkdir(exist_ok=True)
        cls.output_dir = Path(cls.temp_dir) / "output"
        cls.output_dir.mkdir(exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        """Clean up temporary test directories"""
        shutil.rmtree(cls.temp_dir)

    def test_find_years_in_string(self):
        """Test year extraction from various filename patterns"""
        test_cases = [
            ("FHA_SFSnapshot_Aug2023.xlsx", 2023),
            ("fha_snapshot_20230801.xlsx", 2023),
            ("fha_0113.zip", 2013),  # Legacy format
            ("fha_hecm_snapshot_202401.xlsx", 2024),
        ]
        for filename, expected_year in test_cases:
            with self.subTest(filename=filename):
                self.assertEqual(find_years_in_string(filename), expected_year)

    def test_find_month_in_string(self):
        """Test month extraction from various filename patterns"""
        test_cases = [
            ("FHA_SFSnapshot_Aug2023.xlsx", 8),
            ("FHA_SFSnapshot_Dec2023.xlsx", 12),
            ("fha_snapshot_20230801.xlsx", 8),
            ("fha_0113.zip", 1),  # Legacy format
        ]
        for filename, expected_month in test_cases:
            with self.subTest(filename=filename):
                self.assertEqual(find_month_in_string(filename), expected_month)

    def test_standardize_filename(self):
        """Test filename standardization for different file types"""
        test_cases = [
            ("FHA_SFSnapshot_Aug2023.xlsx", "sf", "fha_snapshot_20230801.xlsx"),
            ("HECM_Aug2023.xlsx", "hecm", "fha_hecm_snapshot_20230801.xlsx"),
            ("fha_0113.zip", "sf", "fha_snapshot_20130101.zip"),
        ]
        for original, file_type, expected in test_cases:
            with self.subTest(original=original):
                self.assertEqual(standardize_filename(original, file_type), expected)

    def test_date_sequence_integrity(self):
        """Test for gaps in monthly data files"""
        # Create some test files
        test_files = [
            "fha_snapshot_20230101.parquet",
            "fha_snapshot_20230201.parquet",
            "fha_snapshot_20230301.parquet",
            # Skip April to test gap detection
            "fha_snapshot_20230501.parquet",
        ]
        
        for file in test_files:
            (self.data_dir / file).touch()

        # Get all snapshot files
        files = sorted(glob.glob(str(self.data_dir / "fha_snapshot_*.parquet")))
        
        # Extract dates from filenames
        dates = []
        for file in files:
            filename = os.path.basename(file)
            date_str = filename.split("_")[2].split(".")[0]
            dates.append(datetime.datetime.strptime(date_str, "%Y%m%d"))

        # Check for gaps
        for i in range(len(dates) - 1):
            month_diff = (dates[i + 1].year - dates[i].year) * 12 + (dates[i + 1].month - dates[i].month)
            self.assertEqual(month_diff, 1, f"Gap detected between {dates[i]} and {dates[i + 1]}")

    def test_clean_sf_sheets(self):
        """Test cleaning of single-family data sheets"""
        # Create sample data
        sample_data = pd.DataFrame({
            'Endorsement Month': [1, 2],
            'Original Mortgage Amount': [200000, 300000],
            'Origination Mortgagee/Sponsor Originator': ['Bank A', 'Bank B'],
            'Property Zip': ['12345', '67890'],
            'Loan Purpose': ['Purchase', 'Fixed Rate'],  # Test correction of 2016 data
            'Down Payment Source': ['NonProfit', 'Other'],  # Test standardization
            'Unnamed: 0': ['drop me', 'drop me too'],
        })

        # Clean the data
        cleaned_df = clean_sf_sheets(sample_data)

        # Verify cleaning results
        self.assertNotIn('Unnamed: 0', cleaned_df.columns)
        self.assertIn('Mortgage Amount', cleaned_df.columns)
        self.assertEqual(cleaned_df['Loan Purpose'].iloc[1], 'Purchase')
        self.assertEqual(cleaned_df['Down Payment Source'].iloc[0], 'Non Profit')

    def test_clean_hecm_sheets(self):
        """Test cleaning of HECM data sheets"""
        # Create sample data
        sample_data = pd.DataFrame({
            'NMLS*': [123, 456],
            'Sponosr Number': [789, 101],
            'Standard Saver': ['Standard', 'Saver'],
            'Property Zip': ['12345', '67890'],
            'Originating Mortgagee/Sponsor Originator': ['Bank A', 'Not Available'],
        })

        # Clean the data
        cleaned_df = clean_hecm_sheets(sample_data)

        # Verify cleaning results
        self.assertIn('NMLS', cleaned_df.columns)
        self.assertIn('Sponsor Number', cleaned_df.columns)
        self.assertIn('Standard/Saver', cleaned_df.columns)
        self.assertTrue(pd.isna(cleaned_df['Originating Mortgagee'].iloc[1]))

if __name__ == '__main__':
    unittest.main() 