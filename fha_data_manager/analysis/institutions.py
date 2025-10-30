"""
Comprehensive analysis of institution identity, naming, and mapping patterns.

This module consolidates analysis of:
- Institution ID-name mappings and crosswalks
- Temporal name changes
- Name oscillations (back-and-forth changes)
- Originator and sponsor ID spaces
"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import polars as pl

from fha_data_manager.utils.logging import configure_logging

logger = logging.getLogger(__name__)


def log_message(message: str, log_file=None, level=logging.INFO):
    """Log message to both logger and optional file."""
    logger.log(level, message)
    if log_file:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(message + '\n')


class InstitutionAnalyzer:
    """Analyze institution identity and mapping patterns in FHA data."""
    
    def __init__(self, data_path: str | Path):
        self.data_path = Path(data_path)
        self.df = None
        self.institution_pairs = None
        self.last_name_change_event_log = None
    
    def load_data(self):
        """Load data from hive structure."""
        logger.info("Loading data from %s...", self.data_path)
        self.df = pl.scan_parquet(str(self.data_path))
        return self
    
    def build_institution_crosswalk(self) -> pl.DataFrame:
        """
        Build a comprehensive crosswalk of institution IDs and names.
        
        Returns:
            DataFrame with columns: institution_number, institution_name, type, 
                                   first_date, last_date, num_months
        """
        logger.info("Building institution crosswalk...")
        
        # Add Date column if not present
        df_with_date = self.df.with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        )
        
        # Process originating mortgagees
        orig_pairs = (
            df_with_date.select(['Originating Mortgagee Number', 'Originating Mortgagee', 'Date'])
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
            df_with_date.select(['Sponsor Number', 'Sponsor Name', 'Date'])
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
        self.institution_pairs = pl.concat([orig_pairs, sponsor_pairs]).collect()
        
        # Drop empty entries
        self.institution_pairs = self.institution_pairs.filter(
            (pl.col('institution_number') != '') &
            (pl.col('institution_name') != '')
        )
        
        logger.info("Total institution-period records: %s", f"{len(self.institution_pairs):,}")
        
        # Create summary with temporal info
        crosswalk = (
            self.institution_pairs
            .group_by(['institution_number', 'institution_name', 'type'])
            .agg([
                pl.col('Date').min().alias('first_date'),
                pl.col('Date').max().alias('last_date'),
                pl.col('Date').n_unique().alias('num_months')
            ])
            .sort(['institution_number', 'type', 'first_date'])
        )
        
        return crosswalk
    
    def find_mapping_errors(self) -> pl.DataFrame:
        """
        Find potential mapping errors in institution data.
        
        Looks for:
        - Multiple names for same ID in one month
        - Temporary name changes (oscillations)
        
        Returns:
            DataFrame with error details
        """
        logger.info("Analyzing potential mapping errors...")
        
        if self.institution_pairs is None:
            self.build_institution_crosswalk()
        
        errors = []
        
        # Find months where the same number maps to multiple names
        monthly_mappings = (
            self.institution_pairs
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
                'names': ','.join(row['names']),
                'issue': 'Multiple names for same number in one month'
            })
        
        logger.info("Found %s instances of multiple names in same month", len(errors))
        
        # Look for temporary name changes (oscillations)
        oscillation_errors = self._find_oscillations()
        errors.extend(oscillation_errors)
        
        logger.info("Found %s instances of name oscillations", len(oscillation_errors))
        
        if errors:
            error_df = pl.DataFrame(errors)
        else:
            error_df = pl.DataFrame({
                'institution_number': [],
                'date': [],
                'names': [],
                'issue': []
            })
        
        return error_df
    
    def _find_oscillations(self) -> List[Dict]:
        """Find cases where institution names oscillate back and forth."""
        errors = []
        
        for inst_num in self.institution_pairs['institution_number'].unique():
            num_data = (
                self.institution_pairs
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
                            'names': f"{prev_name} -> {curr_name} -> {prev_name}",
                            'issue': 'Temporary name change (oscillation)'
                        })
                        break  # Only report once per ID
        
        return errors
    
    def analyze_name_changes_over_time(self,
                                       notable_ids: List[int] = None,
                                       log_file=None) -> Dict[str, Any]:
        """
        Analyze how institution names change over time.
        
        Args:
            notable_ids: Specific IDs to analyze in detail (e.g., [71970, 75159])
            log_file: Optional file to log detailed output
            
        Returns:
            Dictionary with two keys:
                ``id_name_changes``: Mapping of institution IDs to their name
                    change sequences.
                ``event_log``: Polars DataFrame describing rename and
                    ownership transition events for lenders and sponsors.

        Side Effects:
            Populates ``self.last_name_change_event_log`` with the event log
            for later reuse.
        """
        log_message("\n=== Analyzing Name Changes Over Time ===", log_file)
        
        # Create period column
        df_with_period = self.df.with_columns(
            pl.concat_str([
                pl.col("Year").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("Month").cast(pl.Utf8).str.zfill(2)
            ]).alias("period")
        )
        
        # Analyze specific notable cases first
        if notable_ids:
            log_message("\n=== Notable Cases ===", log_file)
            for id_num in notable_ids:
                self._analyze_single_id(id_num, df_with_period, log_file)
        
        # Analyze all originator name changes
        log_message("\n=== Overall Name Change Patterns ===", log_file)
        originator_changes = (
            df_with_period
            .select([
                "period",
                "Originating Mortgagee",
                "Originating Mortgagee Number"
            ])
            .filter(
                pl.col("Originating Mortgagee Number").is_not_null()
            )
            .group_by([
                "period",
                "Originating Mortgagee Number"
            ])
            .agg([
                pl.col("Originating Mortgagee").unique().alias("names_in_period")
            ])
            .sort(["Originating Mortgagee Number", "period"])
            .collect()
        )
        
        # Find IDs with multiple names
        id_name_changes = {}
        for row in originator_changes.iter_rows(named=True):
            id_num = row["Originating Mortgagee Number"]
            period = row["period"]
            names = row["names_in_period"]
            
            if len(names) > 1 or (id_num in id_name_changes and 
                                 names[0] not in [n for _, n_list in id_name_changes[id_num]["transitions"] 
                                                 for n in n_list]):
                if id_num not in id_name_changes:
                    id_name_changes[id_num] = {"names": set(), "transitions": []}
                
                current_names = set(names)
                if not id_name_changes[id_num]["transitions"] or \
                   current_names != set(id_name_changes[id_num]["transitions"][-1][1]):
                    id_name_changes[id_num]["names"].update(current_names)
                    id_name_changes[id_num]["transitions"].append((period, names))
        
        # Report top IDs with most changes
        log_message(f"\nFound {len(id_name_changes)} IDs with name changes", log_file)
        log_message("\nTop 10 IDs with most name changes:", log_file)
        
        for id_num, data in sorted(id_name_changes.items(),
                                   key=lambda x: len(x[1]["transitions"]),
                                   reverse=True)[:10]:
            log_message(f"\nID {id_num} ({len(data['transitions'])} changes):", log_file)
            for period, names in data["transitions"][:5]:  # Show first 5
                log_message(f"  {period}: {', '.join(names)}", log_file)
            if len(data["transitions"]) > 5:
                log_message(f"  ... and {len(data['transitions']) - 5} more", log_file)
        
        event_log = self._build_name_change_event_log(df_with_period)

        log_message(
            f"\nGenerated {event_log.height} structured rename/ownership events",
            log_file,
        )

        # Store for downstream access while returning a richer structure
        self.last_name_change_event_log = event_log

        return {
            "id_name_changes": id_name_changes,
            "event_log": event_log,
        }

    def _build_name_change_event_log(self, df_with_period: pl.LazyFrame) -> pl.DataFrame:
        """Create a structured event log of rename and ownership transitions."""

        event_records: List[Dict] = []

        originator_events = self._build_entity_name_events(
            df_with_period,
            entity_type="Originator",
            name_col="Originating Mortgagee",
            id_col="Originating Mortgagee Number",
        )
        sponsor_events = self._build_entity_name_events(
            df_with_period,
            entity_type="Sponsor",
            name_col="Sponsor Name",
            id_col="Sponsor Number",
        )

        event_records.extend(originator_events)
        event_records.extend(sponsor_events)

        ownership_events = self._build_ownership_transition_events(df_with_period)
        event_records.extend(ownership_events)

        if not event_records:
            return pl.DataFrame({
                "entity_type": [],
                "event_type": [],
                "institution_number": [],
                "effective_period": [],
                "previous_names": [],
                "new_names": [],
                "previous_start_period": [],
                "previous_end_period": [],
                "new_start_period": [],
                "new_end_period": [],
                "previous_duration_months": [],
                "new_duration_months": [],
                "previous_observation_count": [],
                "new_observation_count": [],
                "metadata": [],
            })

        return (
            pl.DataFrame(event_records)
            .with_columns([
                pl.col("institution_number").cast(pl.Utf8),
                pl.col("effective_period").cast(pl.Utf8),
            ])
            .sort(["institution_number", "effective_period", "event_type"])
        )

    def _build_entity_name_events(
        self,
        df_with_period: pl.LazyFrame,
        *,
        entity_type: str,
        name_col: str,
        id_col: str,
    ) -> List[Dict]:
        """Construct rename events for a single entity type."""

        aggregated = (
            df_with_period
            .select([
                "period",
                name_col,
                id_col,
            ])
            .filter(pl.col(id_col).is_not_null())
            .group_by(["period", id_col])
            .agg([
                pl.col(name_col).drop_nulls().unique().alias("names_in_period"),
                pl.count().alias("record_count"),
            ])
            .sort([id_col, "period"])
            .collect()
        )

        events: List[Dict] = []

        grouped = aggregated.partition_by(id_col, as_dict=True)
        for id_value, frame in grouped.items():
            rows = frame.sort("period").to_dicts()
            segments, ambiguous = self._segment_sequences(
                rows,
                value_key="names_in_period",
            )

            events.extend(
                self._segments_to_events(
                    segments,
                    ambiguous,
                    entity_type=entity_type,
                    institution_number=id_value,
                )
            )

        return events

    def _build_ownership_transition_events(
        self,
        df_with_period: pl.LazyFrame,
    ) -> List[Dict]:
        """Identify sponsor ownership transitions for each originator."""

        aggregated = (
            df_with_period
            .select([
                "period",
                "Originating Mortgagee Number",
                "Sponsor Number",
                "Sponsor Name",
            ])
            .filter(
                pl.col("Originating Mortgagee Number").is_not_null()
            )
            .group_by(["period", "Originating Mortgagee Number"])
            .agg([
                pl.col("Sponsor Number").drop_nulls().unique().alias("sponsor_numbers"),
                pl.col("Sponsor Name").drop_nulls().unique().alias("sponsor_names"),
                pl.count().alias("record_count"),
            ])
            .sort(["Originating Mortgagee Number", "period"])
            .collect()
        )

        if aggregated.is_empty():
            return []

        sponsor_name_lookup = (
            df_with_period
            .select([
                "Sponsor Number",
                "Sponsor Name",
            ])
            .filter(
                pl.col("Sponsor Number").is_not_null()
                & pl.col("Sponsor Name").is_not_null()
            )
            .group_by("Sponsor Number")
            .agg([
                pl.col("Sponsor Name").drop_nulls().mode().alias("canonical_name"),
            ])
            .with_columns(
                pl.when(pl.col("canonical_name").list.len() > 0)
                .then(pl.col("canonical_name").list.first())
                .otherwise(None)
                .alias("canonical_name")
            )
            .select(["Sponsor Number", "canonical_name"])
            .to_dict(as_series=False)
        )

        sponsor_lookup = dict(zip(
            sponsor_name_lookup.get("Sponsor Number", []),
            sponsor_name_lookup.get("canonical_name", []),
        ))

        events: List[Dict] = []

        grouped = aggregated.partition_by("Originating Mortgagee Number", as_dict=True)
        for id_value, frame in grouped.items():
            rows = frame.sort("period").to_dicts()
            segments, ambiguous = self._segment_sequences(
                rows,
                value_key="sponsor_numbers",
            )

            if segments:
                # Ownership transition events compare sponsor number sets
                for prev_seg, curr_seg in zip(segments, segments[1:]):
                    prev_numbers_raw = list(prev_seg["value"])
                    curr_numbers_raw = list(curr_seg["value"])
                    prev_numbers = [str(num) for num in prev_numbers_raw]
                    curr_numbers = [str(num) for num in curr_numbers_raw]

                    events.append({
                        "entity_type": "Originator",
                        "event_type": "ownership_transition",
                        "institution_number": str(id_value),
                        "effective_period": curr_seg["start_period"],
                        "previous_names": [
                            sponsor_lookup.get(num, None) for num in prev_numbers_raw
                        ],
                        "new_names": [
                            sponsor_lookup.get(num, None) for num in curr_numbers_raw
                        ],
                        "previous_start_period": prev_seg["start_period"],
                        "previous_end_period": prev_seg["end_period"],
                        "new_start_period": curr_seg["start_period"],
                        "new_end_period": curr_seg["end_period"],
                        "previous_duration_months": len(prev_seg["periods"]),
                        "new_duration_months": len(curr_seg["periods"]),
                        "previous_observation_count": prev_seg["total_records"],
                        "new_observation_count": curr_seg["total_records"],
                        "metadata": {
                            "previous_sponsor_numbers": prev_numbers,
                            "new_sponsor_numbers": curr_numbers,
                            "previous_segment_periods": prev_seg["periods"],
                            "new_segment_periods": curr_seg["periods"],
                        },
                    })

            for ambiguous_event in ambiguous:
                raw_numbers = list(ambiguous_event["raw_values"])
                numbers = [str(num) for num in raw_numbers]
                events.append({
                    "entity_type": "Originator",
                    "event_type": "multiple_sponsors_in_period",
                    "institution_number": str(id_value),
                    "effective_period": ambiguous_event["period"],
                    "previous_names": [],
                    "new_names": [
                        sponsor_lookup.get(num, None) for num in raw_numbers
                    ],
                    "previous_start_period": None,
                    "previous_end_period": None,
                    "new_start_period": ambiguous_event["period"],
                    "new_end_period": ambiguous_event["period"],
                    "previous_duration_months": 0,
                    "new_duration_months": 1,
                    "previous_observation_count": 0,
                    "new_observation_count": ambiguous_event.get("record_count", 0),
                    "metadata": {
                        "sponsor_numbers": numbers,
                        "observed_sponsor_names": [
                            sponsor_lookup.get(num, None) for num in raw_numbers
                        ],
                    },
                })

        return events

    def _segment_sequences(
        self,
        rows: List[Dict],
        *,
        value_key: str,
    ) -> Tuple[List[Dict], List[Dict]]:
        """Convert period-level observations into contiguous segments."""

        segments: List[Dict] = []
        ambiguous_events: List[Dict] = []

        current_value = None
        period_buffer: List[str] = []
        total_records = 0

        for row in rows:
            period = row["period"]
            values = row[value_key]
            values = [v for v in values if v is not None]
            value_tuple = tuple(sorted(values))

            if len(row[value_key]) > 1:
                ambiguous_events.append({
                    "period": period,
                    "value": value_tuple,
                    "raw_values": row[value_key],
                    "record_count": row.get("record_count", 0),
                })

            if current_value is None:
                current_value = value_tuple
                period_buffer = [period]
                total_records = row.get("record_count", 0)
                continue

            if value_tuple == current_value:
                period_buffer.append(period)
                total_records += row.get("record_count", 0)
            else:
                segments.append({
                    "value": current_value,
                    "start_period": period_buffer[0],
                    "end_period": period_buffer[-1],
                    "periods": period_buffer.copy(),
                    "total_records": total_records,
                })
                current_value = value_tuple
                period_buffer = [period]
                total_records = row.get("record_count", 0)

        if current_value is not None and period_buffer:
            segments.append({
                "value": current_value,
                "start_period": period_buffer[0],
                "end_period": period_buffer[-1],
                "periods": period_buffer.copy(),
                "total_records": total_records,
            })

        return segments, ambiguous_events

    def _segments_to_events(
        self,
        segments: List[Dict],
        ambiguous_events: List[Dict],
        *,
        entity_type: str,
        institution_number,
    ) -> List[Dict]:
        """Convert contiguous name segments into structured events."""

        events: List[Dict] = []

        if not segments:
            return events

        first_segment = segments[0]
        events.append({
            "entity_type": entity_type,
            "event_type": "appearance",
            "institution_number": str(institution_number),
            "effective_period": first_segment["start_period"],
            "previous_names": [],
            "new_names": list(first_segment["value"]),
            "previous_start_period": None,
            "previous_end_period": None,
            "new_start_period": first_segment["start_period"],
            "new_end_period": first_segment["end_period"],
            "previous_duration_months": 0,
            "new_duration_months": len(first_segment["periods"]),
            "previous_observation_count": 0,
            "new_observation_count": first_segment["total_records"],
            "metadata": {
                "segment_periods": first_segment["periods"],
            },
        })

        for prev_segment, curr_segment in zip(segments, segments[1:]):
            events.append({
                "entity_type": entity_type,
                "event_type": "rename",
                "institution_number": str(institution_number),
                "effective_period": curr_segment["start_period"],
                "previous_names": list(prev_segment["value"]),
                "new_names": list(curr_segment["value"]),
                "previous_start_period": prev_segment["start_period"],
                "previous_end_period": prev_segment["end_period"],
                "new_start_period": curr_segment["start_period"],
                "new_end_period": curr_segment["end_period"],
                "previous_duration_months": len(prev_segment["periods"]),
                "new_duration_months": len(curr_segment["periods"]),
                "previous_observation_count": prev_segment["total_records"],
                "new_observation_count": curr_segment["total_records"],
                "metadata": {
                    "previous_segment_periods": prev_segment["periods"],
                    "new_segment_periods": curr_segment["periods"],
                },
            })

        for ambiguous in ambiguous_events:
            events.append({
                "entity_type": entity_type,
                "event_type": "ambiguous_identity",
                "institution_number": str(institution_number),
                "effective_period": ambiguous["period"],
                "previous_names": list(ambiguous["value"]),
                "new_names": list(ambiguous["raw_values"]),
                "previous_start_period": None,
                "previous_end_period": None,
                "new_start_period": ambiguous["period"],
                "new_end_period": ambiguous["period"],
                "previous_duration_months": 0,
                "new_duration_months": 1,
                "previous_observation_count": 0,
                "new_observation_count": ambiguous.get("record_count", 0),
                "metadata": {
                    "observed_names": ambiguous["raw_values"],
                },
            })

        return events
    
    def _analyze_single_id(self, id_num: int, df_with_period, log_file=None):
        """Analyze a single institution ID in detail."""
        log_message(f"\nDetailed analysis for ID {id_num}:", log_file)
        
        timeline = (
            df_with_period
            .filter(pl.col("Originating Mortgagee Number") == id_num)
            .group_by("period")
            .agg([
                pl.col("Originating Mortgagee").unique().alias("names"),
                pl.count().alias("loan_count")
            ])
            .sort("period")
            .collect()
        )
        
        if len(timeline) == 0:
            log_message(f"  No data found for ID {id_num}", log_file)
            return
        
        # Track distinct name sets
        previous_names = set()
        changes = []
        for row in timeline.iter_rows(named=True):
            current_names = set(row["names"])
            if current_names != previous_names:
                changes.append((row["period"], current_names, row["loan_count"]))
                previous_names = current_names
        
        log_message(f"  Found {len(changes)} distinct name periods:", log_file)
        for period, names, count in changes:
            log_message(
                f"    {period}: {', '.join(names)} ({count:,} loans)",
                log_file
            )
    
    def detect_oscillations(self, log_file=None) -> Dict[str, List[Dict]]:
        """
        Detect oscillating name patterns for both originators and sponsors.
        
        Returns:
            Dictionary with 'originators' and 'sponsors' keys, each containing
            a list of oscillation details
        """
        results = {}
        
        df_with_period = self.df.with_columns(
            pl.concat_str([
                pl.col("Year").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("Month").cast(pl.Utf8).str.zfill(2)
            ]).alias("period")
        )
        
        for entity_type in ["Originator", "Sponsor"]:
            log_message(f"\n=== Analyzing {entity_type} Name Oscillations ===", log_file)
            
            name_col = "Originating Mortgagee" if entity_type == "Originator" else "Sponsor Name"
            id_col = "Originating Mortgagee Number" if entity_type == "Originator" else "Sponsor Number"
            
            # Get name changes over time
            name_changes = (
                df_with_period
                .select([
                    "period",
                    name_col,
                    id_col
                ])
                .filter(
                    pl.col(id_col).is_not_null()
                )
                .group_by([
                    "period",
                    id_col
                ])
                .agg([
                    pl.col(name_col).unique().alias("names_in_period")
                ])
                .sort([id_col, "period"])
                .collect()
            )
            
            # Track name sequences for each ID
            id_sequences = defaultdict(list)
            for row in name_changes.iter_rows(named=True):
                id_num = row[id_col]
                period = row["period"]
                names = tuple(sorted(row["names_in_period"]))
                
                # Only add if different from previous
                if not id_sequences[id_num] or names != id_sequences[id_num][-1][1]:
                    id_sequences[id_num].append((period, names))
            
            # Find oscillating patterns
            oscillating = []
            for id_num, sequence in id_sequences.items():
                if len(sequence) < 3:
                    continue
                
                # Look for any name that appears multiple times non-consecutively
                name_periods = defaultdict(list)
                for period, names in sequence:
                    for name in names:
                        name_periods[name].append(period)
                
                # Check for oscillations
                for name, periods in name_periods.items():
                    if len(periods) < 2:
                        continue
                    
                    # Check if there are other names between occurrences
                    for i in range(len(periods) - 1):
                        current_period = periods[i]
                        next_period = periods[i + 1]
                        
                        intermediate_names = set()
                        for p, names_tuple in sequence:
                            if current_period < p < next_period:
                                intermediate_names.update(names_tuple)
                        
                        if intermediate_names and name not in intermediate_names:
                            oscillating.append({
                                "id": id_num,
                                "oscillating_name": name,
                                "periods": periods,
                                "intermediate_names": list(intermediate_names)
                            })
                            break
                    break
            
            results[entity_type.lower() + 's'] = oscillating
            
            if oscillating:
                log_message(
                    f"\nFound {len(oscillating)} {entity_type}s with oscillating names",
                    log_file
                )
                for item in oscillating[:5]:  # Show first 5
                    log_message(f"\nID {item['id']}:", log_file)
                    log_message(
                        f"  Name '{item['oscillating_name']}' appears in: {', '.join(item['periods'][:5])}",
                        log_file
                    )
                    log_message(
                        f"  With intermediate names: {', '.join(item['intermediate_names'][:3])}",
                        log_file
                    )
            else:
                log_message(f"\nNo oscillating patterns found for {entity_type}s", log_file)
        
        return results
    
    def analyze_id_spaces(self, log_file=None) -> Dict[str, any]:
        """
        Analyze originator and sponsor ID/name spaces.
        
        Returns:
            Dictionary with statistics about ID spaces and overlaps
        """
        log_message("\n=== ID Space Analysis ===", log_file)
        
        # Get unique values
        orig_names = set(self.df.select("Originating Mortgagee").unique().collect()["Originating Mortgagee"])
        orig_ids = set(self.df.select("Originating Mortgagee Number").unique().collect()["Originating Mortgagee Number"])
        sponsor_names = set(self.df.select("Sponsor Name").unique().collect()["Sponsor Name"])
        sponsor_ids = set(self.df.select("Sponsor Number").unique().collect()["Sponsor Number"])
        
        # Remove nulls
        orig_names = {x for x in orig_names if x is not None}
        orig_ids = {x for x in orig_ids if x is not None}
        sponsor_names = {x for x in sponsor_names if x is not None}
        sponsor_ids = {x for x in sponsor_ids if x is not None}
        
        # Check overlaps
        name_overlap = orig_names.intersection(sponsor_names)
        id_overlap = orig_ids.intersection(sponsor_ids)
        
        results = {
            "unique_originator_names": len(orig_names),
            "unique_originator_ids": len(orig_ids),
            "unique_sponsor_names": len(sponsor_names),
            "unique_sponsor_ids": len(sponsor_ids),
            "overlapping_names": len(name_overlap),
            "overlapping_ids": len(id_overlap),
            "sample_overlapping_names": sorted(list(name_overlap))[:10],
            "sample_overlapping_ids": sorted(list(id_overlap))[:10]
        }
        
        log_message(f"\nUnique originator names: {results['unique_originator_names']:,}", log_file)
        log_message(f"Unique originator IDs: {results['unique_originator_ids']:,}", log_file)
        log_message(f"Unique sponsor names: {results['unique_sponsor_names']:,}", log_file)
        log_message(f"Unique sponsor IDs: {results['unique_sponsor_ids']:,}", log_file)
        log_message(f"\nNames appearing as both originator and sponsor: {results['overlapping_names']:,}", log_file)
        log_message(f"IDs appearing as both originator and sponsor: {results['overlapping_ids']:,}", log_file)
        
        if name_overlap:
            log_message("\nSample overlapping names:", log_file)
            for name in results['sample_overlapping_names']:
                log_message(f"  - {name}", log_file)
        
        if id_overlap:
            log_message("\nSample overlapping IDs:", log_file)
            for id_num in results['sample_overlapping_ids']:
                log_message(f"  - {id_num}", log_file)
        
        return results
    
    def generate_full_report(self, output_dir: str | Path = "output"):
        """
        Generate a comprehensive report with all analyses.
        
        Saves:
        - institution_crosswalk.csv: Complete ID-name crosswalk
        - institution_mapping_errors.csv: Detected mapping issues
        - institution_analysis_report.txt: Detailed text report
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)
        
        log_file = output_dir / 'institution_analysis_report.txt'
        if log_file.exists():
            log_file.unlink()
        
        log_message("=" * 80, log_file)
        log_message("COMPREHENSIVE INSTITUTION ANALYSIS REPORT", log_file)
        log_message("=" * 80, log_file)
        
        # Build crosswalk
        log_message("\n1. Building Institution Crosswalk...", log_file)
        crosswalk = self.build_institution_crosswalk()
        crosswalk_path = output_dir / 'institution_crosswalk.csv'
        crosswalk.write_csv(crosswalk_path)
        log_message(f"   Saved to: {crosswalk_path}", log_file)
        log_message(f"   Total unique institution-name pairs: {len(crosswalk):,}", log_file)
        
        # Find mapping errors
        log_message("\n2. Detecting Mapping Errors...", log_file)
        errors = self.find_mapping_errors()
        errors_path = output_dir / 'institution_mapping_errors.csv'
        errors.write_csv(errors_path)
        log_message(f"   Saved to: {errors_path}", log_file)
        log_message(f"   Total errors detected: {len(errors):,}", log_file)
        
        # Analyze ID spaces
        log_message("\n3. Analyzing ID Spaces...", log_file)
        id_space_stats = self.analyze_id_spaces(log_file)
        
        # Analyze name changes
        log_message("\n4. Analyzing Name Changes Over Time...", log_file)
        notable_ids = [71970, 75159]  # Quicken/Rocket, Freedom
        name_changes = self.analyze_name_changes_over_time(
            notable_ids=notable_ids,
            log_file=log_file
        )
        
        # Detect oscillations
        log_message("\n5. Detecting Name Oscillations...", log_file)
        oscillations = self.detect_oscillations(log_file)
        
        # Summary statistics
        log_message("\n" + "=" * 80, log_file)
        log_message("SUMMARY STATISTICS", log_file)
        log_message("=" * 80, log_file)
        log_message(f"Unique institution numbers: {self.institution_pairs['institution_number'].n_unique()}", log_file)
        log_message(f"Unique institution names: {self.institution_pairs['institution_name'].n_unique()}", log_file)
        log_message(f"IDs with name changes: {len(name_changes):,}", log_file)
        log_message(f"Mapping errors detected: {len(errors):,}", log_file)
        log_message(f"Originator oscillations: {len(oscillations.get('originators', [])):,}", log_file)
        log_message(f"Sponsor oscillations: {len(oscillations.get('sponsors', [])):,}", log_file)
        log_message("=" * 80, log_file)
        
        logger.info("\nReport complete. All results saved to %s", output_dir)


def main():
    """Run comprehensive institution analysis from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Comprehensive analysis of FHA institution identities and mappings"
    )
    parser.add_argument(
        "--data-path",
        default="data/database/single_family",
        help="Path to hive-structured data (default: data/database/single_family)"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory for results (default: output)"
    )
    parser.add_argument(
        "--crosswalk-only",
        action="store_true",
        help="Only build the crosswalk, don't run full analysis"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help=(
            "Logging verbosity (default: %(default)s). Accepts standard level "
            "names or numeric values."
        ),
    )

    args = parser.parse_args()

    configure_logging(args.log_level)
    
    analyzer = InstitutionAnalyzer(args.data_path)
    analyzer.load_data()
    
    if args.crosswalk_only:
        logger.info("Building crosswalk only...")
        crosswalk = analyzer.build_institution_crosswalk()
        output_path = Path(args.output_dir) / 'institution_crosswalk.csv'
        Path(args.output_dir).mkdir(exist_ok=True)
        crosswalk.write_csv(output_path)
        logger.info("Crosswalk saved to %s", output_path)
    else:
        analyzer.generate_full_report(args.output_dir)


if __name__ == "__main__":
    main()

