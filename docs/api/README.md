# API Reference

Comprehensive documentation for the public interfaces exposed by the FHA Data Manager
package. Signatures list default values and types using the annotations present in the
codebase. Throughout the reference `PathLike` refers to objects accepted by
:class:`pathlib.Path`, typically strings or ``Path`` instances.

## Package Exports (`fha_data_manager`)

The package root re-exports download, import, and enrichment helpers for convenience.
The following sections document each public function or class.

### Download helpers (`fha_data_manager.download`)

- `download_excel_files_from_url(page_url: str, destination_folder: PathLike, pause_length: int = 5, include_zip: bool = False, file_type: str | None = None) -> None`
  - **Parameters**
    - `page_url`: HUD landing page that lists snapshot workbooks.
    - `destination_folder`: Directory where downloaded files are written. It is created when missing.
    - `pause_length`: Courtesy delay (seconds) inserted after each download to reduce server load.
    - `include_zip`: When ``True`` also downloads ``.zip`` archives and extracts contained workbooks.
    - `file_type`: Optional snapshot type token (``"sf"`` or ``"hecm"``) used to standardise filenames.
  - **Returns**: ``None``; files are downloaded for their side effects.
  - **Raises**: Propagates ``requests`` and I/O errors when network or filesystem operations fail.

- `find_years_in_string(text: str) -> int`
  - **Parameters**: `text` – string containing a year fragment (four-digit or legacy two-digit month/year pattern).
  - **Returns**: The resolved four-digit year.
  - **Raises**: ``TypeError`` if `text` is not a string; ``ValueError`` when no year-like token is found.

- `find_month_in_string(text: str) -> int | None`
  - **Parameters**: `text` – input string potentially containing a month abbreviation or two-digit code.
  - **Returns**: Numeric month ``1``–``12`` when detected; ``None`` otherwise.

- `handle_file_dates(file_name: str | Path) -> str`
  - **Parameters**: `file_name` – filename (with or without path) that embeds a month/year marker.
  - **Returns**: ``_YYYYMM`` suffix derived from the first matching month and year tokens.

- `standardize_filename(original_filename: str | Path, file_type: str | None) -> str`
  - **Parameters**
    - `original_filename`: Name to standardise.
    - `file_type`: Snapshot type token (``"sf"`` or ``"hecm"``). ``None`` returns the original filename.
  - **Returns**: Filename rewritten to ``fha_<type>_snapshot_YYYYMM01.<ext>`` when possible; otherwise the unmodified name.
  - **Raises**: ``ValueError`` if `file_type` is unrecognised or the filename cannot be parsed.

  - `process_zip_file(zip_path: PathLike, destination_folder: PathLike, file_type: str | None) -> list[Path]`
    - **Parameters**
      - `zip_path`: Location of the downloaded archive.
      - `destination_folder`: Extraction target; created if necessary.
      - `file_type`: Snapshot type used when renaming extracted spreadsheets; ``None`` keeps original names.
    - **Returns**: List of extracted spreadsheet paths written under `destination_folder`.

### Import helpers (`fha_data_manager.import_data`)

- `standardize_county_names(df: pl.LazyFrame, county_col: str = "Property County", state_col: str = "Property State") -> pl.LazyFrame`
  - **Parameters**
    - `df`: LazyFrame containing state and county columns.
    - `county_col`: Name of the county column to normalise.
    - `state_col`: State abbreviation column used for contextual fixes.
  - **Returns**: New ``LazyFrame`` with harmonised county naming suitable for FIPS lookups.

- `add_county_fips(df: pl.LazyFrame, state_col: str = "Property State", county_col: str = "Property County", fips_col: str = "FIPS") -> pl.LazyFrame`
  - **Parameters**
    - `df`: LazyFrame to enrich.
    - `state_col`: State abbreviation column.
    - `county_col`: County name column.
    - `fips_col`: Destination column for the derived five-digit FIPS code.
  - **Returns**: LazyFrame with an added `fips_col` column.

- `build_county_fips_crosswalk(bronze_folder: PathLike, crosswalk_path: PathLike, problematic_path: PathLike, state_col: str = "Property State", county_col: str = "Property County", fips_col: str = "FIPS", manual_overrides: Mapping[tuple[str, str], str] | None = None) -> tuple[pl.DataFrame, pl.DataFrame]`
  - **Parameters**
    - `bronze_folder`: Directory that stores bronze-level parquet snapshots by dataset.
    - `crosswalk_path`: Output path (CSV or parquet) for successful FIPS mappings.
    - `problematic_path`: Output path for unresolved county/state combinations.
    - `state_col`, `county_col`, `fips_col`: Column names used during matching.
    - `manual_overrides`: Optional dictionary of manual FIPS corrections keyed by ``(state, county)``.
  - **Returns**: Tuple ``(crosswalk_df, problematic_df)`` containing updated mappings and unresolved rows.

- `create_lender_id_to_name_crosswalk(clean_data_folder: PathLike) -> pl.DataFrame`
  - **Parameters**: `clean_data_folder` – root directory containing cleaned single-family and HECM parquet files.
  - **Returns**: Polars ``DataFrame`` documenting lender and sponsor ID/name combinations with observation date ranges.

- `clean_sf_sheets(df: pl.DataFrame) -> pl.DataFrame`
  - **Parameters**: `df` – raw single-family worksheet converted to a Polars ``DataFrame``.
  - **Returns**: Cleaned ``DataFrame`` with standardised column names, types, and value normalisations.

- `clean_hecm_sheets(df: pl.DataFrame) -> pl.DataFrame`
  - **Parameters**: `df` – raw HECM worksheet as a Polars ``DataFrame``.
  - **Returns**: Cleaned ``DataFrame`` with consistent naming and type coercions.

- `convert_fha_sf_snapshots(data_folder: Path, save_folder: Path, overwrite: bool = False) -> None`
  - **Parameters**
    - `data_folder`: Directory containing raw single-family Excel workbooks.
    - `save_folder`: Destination for cleaned parquet exports.
    - `overwrite`: When ``True`` regenerates parquet files even if they exist.
  - **Returns**: ``None``. Parquet snapshots are written to `save_folder`.

- `convert_fha_hecm_snapshots(data_folder: Path, save_folder: Path, overwrite: bool = False) -> None`
  - **Parameters** mirror `convert_fha_sf_snapshots` but operate on HECM workbooks.
  - **Returns**: ``None``. HECM parquet snapshots are written to `save_folder`.

- `save_clean_snapshots_to_db(data_folder: Path, save_folder: Path, min_year: int = 2010, max_year: int = 2025, file_type: SnapshotType = "single_family", add_fips: bool = True, add_date: bool = True) -> None`
  - **Parameters**
    - `data_folder`: Directory containing cleaned monthly parquet snapshots.
    - `save_folder`: Hive-structured output directory (partitioned by ``Year``/``Month``).
    - `min_year` / `max_year`: Inclusive endorsement year bounds to process.
    - `file_type`: Snapshot type literal (``"single_family"`` or ``"hecm"``) controlling schema tweaks.
    - `add_fips`: When ``True`` enriches rows with county FIPS codes.
    - `add_date`: When ``True`` synthesises a ``Date`` column from year/month.
  - **Returns**: ``None``. Partitioned parquet files are persisted in `save_folder`.

### CLI entry points

#### Download CLI (`fha_data_manager.download_cli`)

- `download_single_family_snapshots(destination: Path | str = DEFAULT_SINGLE_FAMILY_DESTINATION, *, pause_length: int = DEFAULT_PAUSE_LENGTH, include_zip: bool = True, url: str = SINGLE_FAMILY_SNAPSHOT_URL) -> None`
  - **Parameters** mirror the download helper and determine destination, request pacing, and source URL.
  - **Returns**: ``None``. Delegates to `download_excel_files_from_url` with ``file_type="sf"``.

- `download_hecm_snapshots(destination: Path | str = DEFAULT_HECM_DESTINATION, *, pause_length: int = DEFAULT_PAUSE_LENGTH, include_zip: bool = True, url: str = HECM_SNAPSHOT_URL) -> None`
  - Equivalent to `download_single_family_snapshots` but passes ``file_type="hecm"``.

- `get_argument_parser() -> argparse.ArgumentParser`
  - **Returns**: Configured parser with subcommands for ``single-family`` and ``hecm`` downloads.

- `main(argv: Sequence[str] | None = None) -> int`
  - **Parameters**: Optional `argv` sequence for testing.
  - **Returns**: Exit status code after running the requested download command.

#### Import CLI (`fha_data_manager.import_cli`)

- `import_single_family_snapshots(raw_dir: Path | str = ..., bronze_dir: Path | str = ..., silver_dir: Path | str = ..., *, overwrite: bool = False, min_year: int = 2010, max_year: int = 2025, add_fips: bool = True, add_date: bool = True) -> None`
  - **Parameters**
    - `raw_dir`: Location of downloaded Excel files.
    - `bronze_dir`: Staging directory for cleaned parquet snapshots.
    - `silver_dir`: Destination for the hive-structured database.
    - `overwrite`, `min_year`, `max_year`, `add_fips`, `add_date`: Import pipeline tuning flags mirroring `save_clean_snapshots_to_db`.
  - **Returns**: ``None``. Runs the single-family import pipeline.

- `import_hecm_snapshots(... same parameters ...) -> None`
  - HECM equivalent of `import_single_family_snapshots`.

- `get_argument_parser() -> argparse.ArgumentParser`
  - **Returns**: Parser configured with dataset-specific subcommands and shared flags.

- `main(argv: Sequence[str] | None = None) -> int`
  - **Parameters**: Optional argument vector.
  - **Returns**: Exit status code after executing the selected import routine.

## Analysis Modules (`fha_data_manager.analysis`)

### Exploratory analysis (`fha_data_manager.analysis.exploratory`)

- `load_combined_data(data_path: Union[str, Path], *, lazy: bool = True) -> pl.LazyFrame | pl.DataFrame`
  - **Parameters**: `data_path` – hive-partitioned parquet directory to scan lazily.
  - **Returns**: Materialised Polars ``DataFrame`` containing the combined dataset.

- `analyze_lender_activity(df: pl.DataFrame | pl.LazyFrame) -> Dict[str, pl.DataFrame]`
  - **Parameters**: `df` – snapshot dataset with lender information.
  - **Returns**: Dictionary with ``"lender_volume"`` and ``"yearly_lenders"`` summary tables.

- `build_lender_panel(df: pl.DataFrame | pl.LazyFrame, frequency: str = "annual", output_path: str | Path | None = None) -> pl.DataFrame`
  - **Parameters**
    - `df`: DataFrame or LazyFrame containing single-family records.
    - `frequency`: ``"annual"`` or ``"quarterly"`` aggregation cadence.
    - `output_path`: Optional path (file or directory) where the panel is written as Parquet.
  - **Returns**: Aggregated Polars ``DataFrame`` of lender-level metrics.

- `analyze_sponsor_activity(df: pl.DataFrame | pl.LazyFrame) -> Dict[str, pl.DataFrame]`
  - **Parameters**: `df` – dataset containing sponsor fields.
  - **Returns**: Dictionary keyed by ``"sponsor_volume"`` and ``"yearly_sponsors"``.

- `analyze_refinance_share(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame`
  - **Parameters**: `df` – dataset containing loan purpose information.
  - **Returns**: ``DataFrame`` with columns ``Date``, ``purchase_loan_count``, ``refinance_loan_count``, and ``refinance_share``.

- `analyze_fixed_rate_share(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame`
  - **Parameters**: `df` – dataset including product type and interest rate columns.
  - **Returns**: ``DataFrame`` detailing fixed versus adjustable rate counts, share, and rate statistics by ``Date``.

- `print_summary_statistics(stats_dict: Dict[str, pl.DataFrame], section: str) -> None`
  - **Parameters**
    - `stats_dict`: Mapping from summary label to Polars ``DataFrame``.
    - `section`: Heading printed above the summaries.
  - **Returns**: ``None``; results are logged/printed.

- `plot_active_lenders_over_time(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None`
  - **Parameters**: `data_path` – hive directory; `output_dir` – destination directory for the Plotly HTML file.
  - **Returns**: ``None``. Writes ``active_lenders_trend.html``.

- `plot_average_loan_size_over_time(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None`
  - **Parameters / Returns**: Same semantics as above, producing ``avg_loan_size_trend.html`` with mean/median lines.

- `plot_purchase_and_refinance_trend(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None`
  - **Parameters / Returns**: Same as other plotting helpers; outputs ``purchase_and_refinance_trend.html``.

- `plot_down_payment_source_trend(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None`
  - **Parameters / Returns**: Identical semantics; writes ``down_payment_source_trend.html`` showing borrower-funded share.

- `plot_interest_rate_and_loan_amount_by_product_type(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None`
  - **Parameters**: Adds interest rate and loan amount comparisons split by product type.
  - **Returns**: ``None``. Saves ``interest_rate_by_product_type.html`` and ``loan_amount_by_product_type.html``.

- `plot_top_lender_group_averages(data_path: Union[str, Path], output_dir: Union[str, Path] = "output", top_n: int = 20) -> None`
  - **Parameters**: Includes `top_n` specifying how many lenders to treat as the "top" group each period.
  - **Returns**: ``None``; writes comparison plots for interest rates and loan sizes.

- `plot_interest_rate_and_loan_amount_by_property_type(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None`
  - **Parameters**: Same as other plotting helpers, comparing property types.
  - **Returns**: ``None`` with two HTML outputs (interest rate and loan amount).

- `plot_interest_rate_and_loan_amount_by_loan_purpose(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None`
  - **Parameters / Returns**: Mirrors the property-type helper but splits by loan purpose, generating two HTML files.

- `plot_categorical_counts_over_time(data_path: Union[str, Path], output_dir: Union[str, Path] = "output", normalized: bool = False) -> None`
  - **Parameters**
    - `data_path`: Hive directory to scan.
    - `output_dir`: Directory for the generated Plotly HTML files.
    - `normalized`: When ``True`` plots proportions instead of counts.
  - **Returns**: ``None``. Writes stacked area charts for several categorical variables.

- `create_all_trend_plots(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None`
  - **Parameters**: `data_path`, `output_dir` as above.
  - **Returns**: ``None``. Calls every plotting helper, producing a full suite of HTML files.

- `main(log_level: str | int = "INFO", create_plots: bool = True, output_dir: Union[str, Path] = "output") -> None`
  - **Parameters**
    - `log_level`: Logging verbosity passed to the project logger.
    - `create_plots`: Toggle for generating Plotly outputs.
    - `output_dir`: Destination for plot files when `create_plots` is ``True``.
  - **Returns**: ``None``. Orchestrates exploratory analysis and optional chart creation.

### Geographic summaries (`fha_data_manager.analysis.geo`)

- `summarize_county_metrics(df: pl.DataFrame | pl.LazyFrame, frequency: str = "annual", *, fips_col: str = "FIPS", state_col: str = "Property State", county_col: str = "Property County", output_path: str | Path | None = None) -> pl.DataFrame`
  - **Parameters**
    - `df`: DataFrame or LazyFrame enriched with county FIPS codes.
    - `frequency`: ``"annual"`` or ``"quarterly"`` aggregation cadence.
    - `fips_col`, `state_col`, `county_col`: Column names required for grouping and labeling.
    - `output_path`: Optional file or directory for Parquet output.
  - **Returns**: Polars ``DataFrame`` summarising loan counts, mortgage statistics, and rate dispersion by county and period.

- `summarize_metro_metrics(df: pl.DataFrame | pl.LazyFrame, frequency: str = "annual", *, county_fips_col: str = "FIPS", cbsa_col: str = "CBSA Code", cbsa_name_col: str | None = "CBSA Title", cbsa_crosswalk: pl.DataFrame | pl.LazyFrame | None = None, output_path: str | Path | None = None) -> pl.DataFrame`
  - **Parameters**
    - `df`: Dataset containing county FIPS identifiers.
    - `frequency`: Aggregation cadence.
    - `county_fips_col`, `cbsa_col`, `cbsa_name_col`: Column names used when joining to a CBSA crosswalk.
    - `cbsa_crosswalk`: Optional crosswalk mapping counties to CBSAs.
    - `output_path`: Optional Parquet destination.
  - **Returns**: Polars ``DataFrame`` with metro-level loan counts and mortgage metrics.

- `create_state_loan_count_choropleth(df: pl.DataFrame | pl.LazyFrame, *, state_col: str = "Property State", title: str | None = None, color_scale: str | list = "Viridis") -> plotly.graph_objects.Figure`
  - **Parameters**
    - `df`: Dataset containing state information.
    - `state_col`: Column with state abbreviations.
    - `title`: Optional Plotly title string.
    - `color_scale`: Continuous color scale name or custom list.
  - **Returns**: Plotly ``Figure`` visualising loan counts by state.

- `create_county_loan_count_choropleth(df: pl.DataFrame | pl.LazyFrame, *, fips_col: str = "FIPS", state_col: str = "Property State", county_col: str = "Property County", title: str | None = None, color_scale: str | list = "Viridis", geojson: dict | None = None) -> plotly.graph_objects.Figure`
  - **Parameters**
    - `df`: Dataset containing FIPS, state, and county columns.
    - `fips_col`, `state_col`, `county_col`: Column names used in the choropleth.
    - `title`: Optional Plotly title string.
    - `color_scale`: Continuous color scale specification.
    - `geojson`: Optional GeoJSON describing US counties; defaults to Plotly's sample if ``None``.
  - **Returns**: Plotly ``Figure`` mapping county-level loan counts.

### Institutional analysis (`fha_data_manager.analysis.institutions`)

- `log_message(message: str, log_file: str | Path | None = None, level: int = logging.INFO) -> None`
  - **Parameters**: `message` to record, optional `log_file` for mirrored output, and logging `level`.
  - **Returns**: ``None``.

- `class InstitutionAnalyzer`
  - **Constructor**: `InstitutionAnalyzer(data_path: str | Path)` – initialises with the hive-parquet directory to analyse.
  - **Public methods**
    - `load_data() -> InstitutionAnalyzer`: Loads parquet snapshots lazily and returns ``self`` for chaining.
    - `build_institution_crosswalk() -> pl.DataFrame`: Generates a crosswalk of institution IDs to names with first/last appearance metadata.
    - `find_mapping_errors() -> pl.DataFrame`: Detects monthly ID/name conflicts and oscillations.
    - `analyze_name_changes_over_time(notable_ids: List[int] | None = None, log_file: str | Path | None = None) -> Dict[int, Dict[str, Any]]`: Builds a detailed event log of name changes, optionally focusing on selected IDs and emitting verbose output to `log_file`.
    - `detect_oscillations(log_file: str | Path | None = None) -> Dict[str, List[Dict[str, Any]]]`: Highlights back-and-forth name oscillations separately for originators and sponsors.
    - `analyze_id_spaces(log_file: str | Path | None = None) -> Dict[str, Any]`: Summarises overlaps and coverage of originator versus sponsor ID spaces.
    - `generate_full_report(output_dir: str | Path = "output") -> None`: Runs the full analysis suite and writes CSV/text summaries into `output_dir`.

### Network analysis (`fha_data_manager.analysis.network`)

- `load_originator_sponsor_edges(data_path: str | Path, *, start_year: int | None = None, end_year: int | None = None, min_loans: int = 1) -> pl.DataFrame`
  - **Parameters**
    - `data_path`: Hive-parquet directory.
    - `start_year` / `end_year`: Optional inclusive filters on endorsement years.
    - `min_loans`: Minimum loan count threshold for keeping a sponsor–originator pair.
  - **Returns**: Polars ``DataFrame`` aggregating relationship statistics (counts, volume, average loan amount, etc.).

- `build_bipartite_graph(edges: pl.DataFrame, *, weight_col: str = "loan_count") -> tuple[nx.Graph, tuple[set[str], set[str]]]`
  - **Parameters**
    - `edges`: Output of `load_originator_sponsor_edges` or similar table.
    - `weight_col`: Edge attribute used when computing weights.
  - **Returns**: Tuple containing the constructed bipartite ``networkx.Graph`` and the partition sets (originators, sponsors).

- `compute_centrality_metrics(graph: nx.Graph, node_sets: tuple[set[str], set[str]], *, weight_attr: str = "weight") -> Dict[str, pl.DataFrame]`
  - **Parameters**
    - `graph`: Bipartite graph from `build_bipartite_graph`.
    - `node_sets`: Tuple of originator and sponsor node sets.
    - `weight_attr`: Edge attribute to use when computing weighted degree and betweenness.
  - **Returns**: Dictionary with Polars ``DataFrame`` entries for originator and sponsor centrality metrics.

- `project_affiliation_graphs(graph: nx.Graph, node_sets: tuple[set[str], set[str]], *, weight_attr: str = "weight") -> Dict[str, pl.DataFrame]`
  - **Parameters** mirror `compute_centrality_metrics`.
  - **Returns**: Dictionary with weighted one-mode projection edge lists keyed by ``"originator_projection"`` and ``"sponsor_projection"``.

- `analyze_sponsor_originator_network(data_path: str | Path, *, start_year: int | None = None, end_year: int | None = None, min_loans: int = 1, weight_col: str = "loan_count") -> Dict[str, Any]`
  - **Parameters** combine those of `load_originator_sponsor_edges` and `build_bipartite_graph`.
  - **Returns**: Dictionary containing the aggregated edges, constructed graph, centrality tables, projections, and a summary of node/edge counts.

## Validation (`fha_data_manager.validation.validators`)

- `class ValidationResult`
  - **Constructor**: `ValidationResult(name: str, passed: bool, details: Dict[str, Any], warning: bool = False)` stores metadata for a single check.
  - **Attributes**: ``name``, ``passed``, ``details``, ``warning`` (denoting non-critical failures).
  - ``__repr__`` summarises the status string for interactive use.

- `class FHADataValidator`
  - **Constructor**: `FHADataValidator(data_path: str | Path)` initialises a validator pointed at the hive-parquet directory.
  - **Public methods**
    - `load_data() -> FHADataValidator`: Lazily loads the dataset and returns ``self`` for chaining.
    - `check_required_columns() -> ValidationResult`: Ensures mandatory columns exist.
    - `check_fha_index_uniqueness() -> ValidationResult`: Confirms ``FHA_Index`` uniqueness.
    - `check_missing_originator_ids(threshold_pct: float = 5.0) -> ValidationResult`: Flags when missing originator IDs exceed the threshold (warning-level result).
    - `check_missing_originator_names(threshold_pct: float = 5.0) -> ValidationResult`: Same as above for originator names.
    - `check_orphaned_sponsors() -> ValidationResult`: Detects sponsor-linked loans lacking originator IDs (warning-level).
    - `check_sponsor_coverage() -> ValidationResult`: Reports informational sponsor coverage statistics.
    - `check_id_name_consistency_monthly() -> ValidationResult`: Verifies IDs map to a single name within each year-month period.
    - `check_overlapping_id_spaces() -> ValidationResult`: Highlights overlaps between originator and sponsor ID pools (warning-level).
    - `check_name_oscillations(min_changes: int = 3) -> ValidationResult`: Searches for repeated back-and-forth name changes (warning-level).
    - `check_originator_sponsor_relationships() -> ValidationResult`: Summarises complex originator-to-ID relationships (warning-level).
    - `check_date_coverage() -> ValidationResult`: Reports the temporal range of observations (informational result).
    - `check_mortgage_amounts() -> ValidationResult`: Validates mortgage amount ranges and counts of problematic values (warning-level).
    - `run_all(include_warnings: bool = True) -> Dict[str, ValidationResult]`: Executes all checks and stores results internally; `include_warnings` currently controls logging verbosity.
    - `run_critical() -> Dict[str, ValidationResult]`: Runs only the critical subset of checks.
    - `print_summary() -> bool`: Prints a formatted report of accumulated results and returns ``True`` when all critical checks passed.
    - `export_results(output_path: str | Path) -> None`: Writes the accumulated results to CSV.

- `main() -> None`
  - **Description**: CLI entry point that parses command-line options, runs selected checks, optionally exports results, and exits with a status code reflecting critical outcomes.

## Utilities (`fha_data_manager.utils`)

### Configuration (`fha_data_manager.utils.config`)

- `class Config`
  - Centralises project directory configuration sourced from environment variables.
  - **Class attributes**: ``PROJECT_DIR``, ``DATA_DIR``, ``RAW_DIR``, ``CLEAN_DIR``, ``DATABASE_DIR``, ``BRONZE_DIR``, ``SILVER_DIR``, ``OUTPUT_DIR``.
  - **Methods**: `ensure_directories() -> None` – creates all configured directories when missing.
  - **Aliases**: Module-level constants (`PROJECT_DIR`, `DATA_DIR`, etc.) reference the corresponding class attributes.

### Data inventory (`fha_data_manager.utils.inventory`)

- `@dataclass class FileRecord`
  - **Fields**: Category, file name, relative path variants, parent directory, suffix, size metrics, and UTC timestamps.
  - `to_dict() -> dict[str, object]` converts the record into a CSV-friendly dictionary.

- `discover_data_files(base_dir: Path) -> list[Path]`
  - **Parameters**: `base_dir` – root directory to scan.
  - **Returns**: Sorted list of file paths underneath `base_dir`. Logs a warning if the directory is missing.

- `infer_category(path: Path) -> str`
  - **Parameters**: `path` – file path to classify.
  - **Returns**: Category label (``"raw"``, ``"clean"``, ``"data"``, or ``"outside-data"``).

- `relative_to(path: Path, base: Path) -> str`
  - **Parameters**: `path`, `base` – the target path and reference directory.
  - **Returns**: String representation of `path` relative to `base`, or the absolute path when not nested.

- `human_readable_size(size_bytes: int) -> str`
  - **Parameters**: `size_bytes` – file size in bytes.
  - **Returns**: Human-friendly size string (B/KB/MB/GB/TB).

- `format_timestamp(timestamp: float) -> str`
  - **Parameters**: `timestamp` – POSIX timestamp.
  - **Returns**: ISO-8601 UTC string.

- `build_records(files: Iterable[Path]) -> list[FileRecord]`
  - **Parameters**: `files` – iterable of paths discovered via `discover_data_files`.
  - **Returns**: List of populated ``FileRecord`` instances.

- `write_inventory(records: list[FileRecord], output_path: Path) -> Path`
  - **Parameters**: `records` – metadata to serialise; `output_path` – CSV destination.
  - **Returns**: `output_path` for chaining. Creates parent directories as needed.

- `main(log_level: str | int = "INFO") -> Path`
  - **Parameters**: `log_level` – logging verbosity string or numeric level.
  - **Returns**: Path to the generated ``data_inventory.csv`` file after logging configuration and inventory generation.

### Logging helpers (`fha_data_manager.utils.logging`)

- `resolve_log_level(level: str | int | None) -> int`
  - **Parameters**: `level` – textual or numeric log level (or ``None``).
  - **Returns**: Numeric logging level; raises ``ValueError`` for unknown strings.

- `configure_logging(level: str | int | None = logging.INFO, *, log_format: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s", datefmt: str | None = "%Y-%m-%d %H:%M:%S", force: bool = False) -> int`
  - **Parameters**
    - `level`: Desired logging level accepted by `resolve_log_level`.
    - `log_format`: Format string passed to ``logging.basicConfig`` when configuring handlers.
    - `datefmt`: Optional date format string for log records.
    - `force`: When ``True`` forces handler reconfiguration even if logging is already set up.
  - **Returns**: Resolved numeric logging level after applying configuration.

## Examples

See the [examples/](../../examples/) directory for end-to-end demonstrations of the download,
import, validation, and analysis workflows.
