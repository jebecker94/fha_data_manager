"""Example 8: Market concentration analysis with HHIs."""

from __future__ import annotations

from pathlib import Path

import plotly.express as px

from fha_data_manager.analysis.hhi import compute_lender_hhi


DATASET_PATH = Path("data/silver/single_family")
OUTPUT_DIR = Path("output") / "hhi_analysis"


def run_examples() -> None:
    """Compute HHIs across multiple configurations and save outputs."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("FHA MARKET CONCENTRATION ANALYSIS")
    print("=" * 80)
    print(f"Using dataset: {DATASET_PATH}")

    print("\nComputing national annual HHIs for originators...")
    national_year = compute_lender_hhi(
        DATASET_PATH,
        time_level="year",
        geography_level="all",
        lender_type="originator",
    )
    national_year.write_csv(OUTPUT_DIR / "national_year_originators.csv")
    print(national_year.tail())

    print("\nComputing state annual HHIs for sponsors...")
    state_year = compute_lender_hhi(
        DATASET_PATH,
        time_level="year",
        geography_level="state",
        lender_type="sponsor",
    )
    state_year.write_csv(OUTPUT_DIR / "state_year_sponsors.csv")
    print(state_year.tail())

    print("\nComputing county quarterly HHIs for sponsors (keeping missings)...")
    county_quarter = compute_lender_hhi(
        DATASET_PATH,
        time_level="quarter",
        geography_level="county",
        lender_type="sponsor",
        drop_missing=False,
    )
    county_quarter.write_parquet(OUTPUT_DIR / "county_quarter_sponsors.parquet")
    print(county_quarter.head())

    print("\nComputing overall HHIs for sponsors across all time and geographies...")
    overall = compute_lender_hhi(
        DATASET_PATH,
        time_level="all",
        geography_level="all",
        lender_type="sponsor",
    )
    overall.write_csv(OUTPUT_DIR / "overall_sponsor_hhi.csv")
    print(overall)

    print("\nCreating Plotly choropleth for state sponsor HHIs across all years...")
    state_all = compute_lender_hhi(
        DATASET_PATH,
        time_level="all",
        geography_level="state",
        lender_type="sponsor",
    )

    fig = px.choropleth(
        state_all.to_pandas(use_pyarrow_extension_array=True),
        locations="state",
        locationmode="USA-states",
        color="hhi",
        hover_data={
            "hhi": ':.0f',
            "hhi_normalized": ':.3f',
            "total_loans": ':,'
        },
        scope="usa",
        color_continuous_scale="Viridis",
        title="FHA Sponsor HHI by State (All Years)",
    )

    map_path = OUTPUT_DIR / "state_sponsor_hhi_all_years.html"
    fig.write_html(map_path)
    print(f"Saved map to {map_path}")

    print("\nAnalysis complete. Check the output directory for saved files.")


def main() -> None:
    run_examples()


if __name__ == "__main__":
    main()

