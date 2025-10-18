import logging
from pathlib import Path

import polars as pl


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

        log_message("Starting institution mapping analysis (Polars version)...", log_file)

        # Load Data
        data_folder = 'data/database/single_family'
        df = pl.scan_parquet(data_folder)
                
        # Process originating mortgagees
        orig_pairs = (
            df.select(['Originating Mortgagee Number', 'Originating Mortgagee','Date'])
            .unique()
            .filter(
                pl.col('Originating Mortgagee Number').is_not_null() &
                pl.col('Originating Mortgagee').is_not_null()
            )
            .with_columns([
                pl.col('Originating Mortgagee Number').cast(pl.Utf8).str.strip_chars().alias('institution_number'),
                pl.col('Originating Mortgagee').cast(pl.Utf8).str.strip_chars().alias('institution_name'),
                pl.lit('Originator').alias('type'),
            ])
            .select(['institution_number', 'institution_name', 'type', 'Date'])
        )
        
        # Process sponsors
        sponsor_pairs = (
            df.select(['Sponsor Number', 'Sponsor Name','Date'])
            .unique()
            .filter(
                pl.col('Sponsor Number').is_not_null() &
                pl.col('Sponsor Name').is_not_null()
            )
            .with_columns([
                pl.col('Sponsor Number').cast(pl.Utf8).str.strip_chars().alias('institution_number'),
                pl.col('Sponsor Name').cast(pl.Utf8).str.strip_chars().alias('institution_name'),
                pl.lit('Sponsor').alias('type'),
            ])
            .select(['institution_number', 'institution_name', 'type', 'Date'])
        )

        # Combine originators and sponsors
        institution_pairs = pl.concat([orig_pairs, sponsor_pairs]).collect()

        #  Drop missing institution numbers or names
        institution_pairs = institution_pairs.filter(
            pl.col('institution_number') != '',
            pl.col('institution_name') != '',
        )
        log_message(f"Total records: {len(institution_pairs)}", log_file)

        # Create summary of mappings
        log_message("\nGenerating institution mapping summary...", log_file)
        summary_df = (
            institution_pairs
            .group_by(['institution_number', 'institution_name', 'type'])
            .agg([
                pl.col('Date').min().alias('first_date'),
                pl.col('Date').max().alias('last_date'),
                pl.col('Date').n_unique().alias('num_months')
            ])
            .sort(['institution_number', 'type', 'first_date'])
        )
        
        # Analyze potential mapping errors
        log_message("\nAnalyzing potential mapping errors...", log_file)
        errors = []
        
        # Find months where the same number maps to multiple names
        monthly_mappings = (
            institution_pairs
            .with_columns([
                pl.col('Date').dt.year().alias('year'),
                pl.col('Date').dt.month().alias('month')
            ])
            .group_by(['institution_number', 'year', 'month'])
            .agg(pl.col('institution_name').unique().alias('names'))
            .with_columns(pl.col('names').list.len().alias('name_count'))
            .filter(pl.col('name_count') > 1)
        )
        
        for row in monthly_mappings.iter_rows(named=True):
            errors.append({
                'institution_number': row['institution_number'],
                'date': f"{row['year']}-{row['month']:02d}",
                'names': row['names'],
                'issue': 'Multiple names for same number in one month'
            })
        
        # Look for temporary name changes (oscillations)
        for inst_num in institution_pairs['institution_number'].unique():
            num_data = (
                institution_pairs
                .filter(pl.col('institution_number') == inst_num)
                .sort('Date')
            )
            
            # Convert to list for easier iteration
            records = num_data.to_dicts()
            
            for i, row in enumerate(records):
                if i == 0:
                    continue
                    
                prev_name = records[i-1]['institution_name']
                curr_name = row['institution_name']
                curr_date = row['Date']
                
                if curr_name != prev_name:
                    # Check if previous name appears again in future
                    future_names = [r['institution_name'] for r in records[i+1:]]
                    if prev_name in future_names:
                        errors.append({
                            'institution_number': inst_num,
                            'date': curr_date.strftime('%Y-%m'),
                            'old_name': prev_name,
                            'new_name': curr_name,
                            'issue': 'Temporary name change'
                        })

        error_df = pl.DataFrame(errors) if errors else pl.DataFrame({
            'institution_number': [],
            'Date': [],
            'names': [],
            'issue': []
        })

        # Cast names to string
        # This is necessary because the names are a list of strings
        error_df = error_df.with_columns(
            pl.col('names').map_elements(lambda x: ",".join(x) if isinstance(x, (list, pl.Series)) else x),
        )

        # Save results
        log_message("\nSaving results...", log_file)
        summary_df.write_csv(output_dir / 'institution_crosswalk.csv')
        error_df.write_csv(output_dir / 'institution_mapping_errors.csv')

        # Write summary statistics
        log_message("\nCrosswalk Summary:", log_file)
        log_message(f"Total unique institution numbers: {institution_pairs['institution_number'].n_unique()}", log_file)
        log_message(f"Total unique institution names: {institution_pairs['institution_name'].n_unique()}", log_file)
        log_message(f"Potential Errors Found: {len(error_df)}", log_file)
        log_message("\nAnalysis complete. Results saved in output directory.", log_file)
        
    except Exception as e:
        log_message(f"Error during analysis: {str(e)}", log_file, level=logging.ERROR)
        import traceback
        log_message("\nTraceback:", log_file, level=logging.ERROR)
        log_message(traceback.format_exc(), log_file, level=logging.ERROR)
        raise  # Re-raise the exception for interactive debugging

