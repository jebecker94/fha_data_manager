import glob
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)

def log_message(message, log_file=None, level=logging.INFO):
    logger.log(level, message)
    if log_file:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(message + '\n')

if __name__ == '__main__':
    try:
        # Create output directory if it doesn't exist
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        # Initialize log file
        log_file = output_dir / 'institution_analysis_log.txt'
        if log_file.exists():
            log_file.unlink()
        
        logging.basicConfig(level=logging.INFO)

        log_message("Starting institution mapping analysis...", log_file)
        
        # Get all parquet files
        files = glob.glob('data/clean/single_family/fha_snapshot_*.parquet')
        log_message(f"Found {len(files)} parquet files to process", log_file)
        
        if not files:
            log_message("No parquet files found! Please check the data directory.", log_file, level=logging.WARNING)
            raise FileNotFoundError("No parquet files found")
        
        # Initialize lists to store mapping data
        mapping_data = []
        
        # Process each file
        for file in sorted(files):
            date_str = file.split('_')[-1].replace('.parquet', '')
            date = datetime.strptime(date_str, '%Y%m%d')
            log_message(f"Processing {date_str}...", log_file)
            
            try:
                df = pd.read_parquet(file)
                
                # Process originating mortgagees
                orig_pairs = df[['Originating Mortgagee Number', 'Originating Mortgagee']].drop_duplicates()
                for _, row in orig_pairs.iterrows():
                    if pd.notna(row['Originating Mortgagee Number']) and pd.notna(row['Originating Mortgagee']):
                        mapping_data.append({
                            'institution_number': str(row['Originating Mortgagee Number']).strip(),
                            'institution_name': str(row['Originating Mortgagee']).strip(),
                            'type': 'Originator',
                            'date': date
                        })
                
                # Process sponsors
                sponsor_pairs = df[['Sponsor Number', 'Sponsor Name']].drop_duplicates()
                for _, row in sponsor_pairs.iterrows():
                    if pd.notna(row['Sponsor Number']) and pd.notna(row['Sponsor Name']):
                        mapping_data.append({
                            'institution_number': str(row['Sponsor Number']).strip(),
                            'institution_name': str(row['Sponsor Name']).strip(),
                            'type': 'Sponsor',
                            'date': date
                        })
            except Exception as e:
                log_message(f"Error processing {file}: {str(e)}", log_file, level=logging.ERROR)
                continue
        
        if not mapping_data:
            log_message("No mapping data collected! Please check the file contents.", log_file, level=logging.WARNING)
            raise ValueError("No mapping data collected")
            
        log_message(f"\nCreating mapping DataFrame from {len(mapping_data)} records...", log_file)
        mapping_df = pd.DataFrame(mapping_data)
        
        # Create summary of mappings
        log_message("\nGenerating institution mapping summary...", log_file)
        summary = []
        for num in mapping_df['institution_number'].unique():
            num_data = mapping_df[mapping_df['institution_number'] == num]
            for name in num_data['institution_name'].unique():
                name_data = num_data[num_data['institution_name'] == name]
                for type_ in name_data['type'].unique():
                    type_data = name_data[name_data['type'] == type_]
                    summary.append({
                        'institution_number': num,
                        'institution_name': name,
                        'type': type_,
                        'first_date': type_data['date'].min(),
                        'last_date': type_data['date'].max(),
                        'num_months': len(type_data['date'].unique())
                    })
        
        summary_df = pd.DataFrame(summary)
        
        # Analyze potential mapping errors
        log_message("\nAnalyzing potential mapping errors...", log_file)
        errors = []
        
        # Group by institution number and date
        for num in mapping_df['institution_number'].unique():
            num_data = mapping_df[mapping_df['institution_number'] == num].sort_values('date')
            
            # Look for months where the same number maps to multiple names
            monthly_groups = num_data.groupby([num_data['date'].dt.year, num_data['date'].dt.month])
            
            for (year, month), group in monthly_groups:
                unique_names = group['institution_name'].unique()
                if len(unique_names) > 1:
                    errors.append({
                        'institution_number': num,
                        'date': f"{year}-{month:02d}",
                        'names': list(unique_names),
                        'issue': 'Multiple names for same number in one month'
                    })
            
            # Look for sudden changes in mapping
            prev_name = None
            prev_date = None
            for _, row in num_data.iterrows():
                if prev_name is not None and row['institution_name'] != prev_name:
                    # Check if this is a temporary change
                    future_data = num_data[num_data['date'] > row['date']]
                    if not future_data.empty and any(future_data['institution_name'] == prev_name):
                        errors.append({
                            'institution_number': num,
                            'date': row['date'].strftime('%Y-%m'),
                            'old_name': prev_name,
                            'new_name': row['institution_name'],
                            'issue': 'Temporary name change'
                        })
                prev_name = row['institution_name']
                prev_date = row['date']
        
        error_df = pd.DataFrame(errors)

        # Save results
        log_message("\nSaving results...", log_file)
        summary_df.to_csv(output_dir / 'institution_crosswalk.csv', index=False)
        error_df.to_csv(output_dir / 'institution_mapping_errors.csv', index=False)

        # Write summary statistics
        log_message("\nCrosswalk Summary:", log_file)
        log_message(f"Total unique institution numbers: {len(summary_df['institution_number'].unique())}", log_file)
        log_message(f"Total unique institution names: {len(summary_df['institution_name'].unique())}", log_file)
        log_message(f"Potential Errors Found: {len(error_df)}", log_file)
        
        # Analyze specific time periods
        log_message("\nAnalyzing specific time periods...", log_file)
        for year in range(2014, 2015):
            for month in range(1, 13):
                date = datetime(year, month, 1)
                period_data = mapping_df[mapping_df['date'] == date]
                if not period_data.empty:
                    log_message(f"\n{date.strftime('%B %Y')} Analysis:", log_file)
                    log_message(f"Number of unique institutions: {len(period_data['institution_number'].unique())}", log_file)
                    log_message(f"Number of unique names: {len(period_data['institution_name'].unique())}", log_file)
                    
                    # Find institutions with multiple names
                    issues = period_data.groupby('institution_number').agg({
                        'institution_name': lambda x: list(set(x))
                    }).reset_index()
                    issues = issues[issues['institution_name'].map(len) > 1]
                    
                    if not issues.empty:
                        log_message(f"\nInstitutions with multiple names in {date.strftime('%B %Y')}:", log_file)
                        for _, row in issues.iterrows():
                            log_message(f"Institution {row['institution_number']}:", log_file)
                            for name in row['institution_name']:
                                log_message(f"  - {name}", log_file)
        
        log_message("\nAnalysis complete. Results saved in output directory.", log_file)
        
    except Exception as e:
        log_message(f"Error during analysis: {str(e)}", log_file, level=logging.ERROR)
        import traceback
        log_message("\nTraceback:", log_file, level=logging.ERROR)
        log_message(traceback.format_exc(), log_file, level=logging.ERROR)
        raise  # Re-raise the exception for interactive debugging
