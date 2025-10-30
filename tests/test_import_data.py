"""Tests for import-related utilities."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

pytest.importorskip("addfips")

from fha_data_manager.import_data import build_county_fips_crosswalk


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_build_county_fips_crosswalk(tmp_path):
    """Crosswalk builder loads bronze data and persists results."""

    bronze_root = tmp_path / "bronze"
    sf_dir = bronze_root / "single_family"
    hecm_dir = bronze_root / "hecm"

    sf_df = pl.DataFrame(
        {
            "Property State": ["CA", "CA", "DC"],
            "Property County": [
                "Los Angeles County",
                "Orange",
                "Washington",
            ],
        }
    )
    _write_parquet(sf_df, sf_dir / "sf_snapshot.parquet")

    hecm_df = pl.DataFrame(
        {
            "Property State": ["PR", "ZZ"],
            "Property County": ["Bayam'n", "Unknown"],
        }
    )
    _write_parquet(hecm_df, hecm_dir / "hecm_snapshot.parquet")

    outputs_dir = tmp_path / "outputs"
    crosswalk_path = outputs_dir / "county_fips_crosswalk.parquet"
    problematic_path = outputs_dir / "county_fips_problematic.parquet"

    existing_crosswalk = pl.DataFrame(
        {
            "Property State": ["CA"],
            "Property County": ["ORANGE"],
            "FIPS": ["06059"],
        }
    )
    _write_parquet(existing_crosswalk, crosswalk_path)

    existing_problematic = pl.DataFrame(
        {
            "Property State": ["PR"],
            "Property County": ["BAYAMON"],
        }
    )
    _write_parquet(existing_problematic, problematic_path)

    manual_overrides = {("DC", "WASHINGTON"): "11001"}

    crosswalk_df, problematic_df = build_county_fips_crosswalk(
        bronze_root,
        crosswalk_path,
        problematic_path,
        manual_overrides=manual_overrides,
    )

    expected_crosswalk = {
        ("CA", "ORANGE"): "06059",
        ("CA", "LOS ANGELES"): "06037",
        ("PR", "BAYAMON"): "72021",
        ("DC", "WASHINGTON"): "11001",
    }

    crosswalk_map = {
        (state, county): fips
        for state, county, fips in crosswalk_df.select(
            ["Property State", "Property County", "FIPS"]
        ).iter_rows()
    }

    assert crosswalk_map == expected_crosswalk

    problematic_rows = list(
        problematic_df.select(["Property State", "Property County"]).iter_rows()
    )
    assert problematic_rows == [("ZZ", "UNKNOWN")]

    # Ensure files were written to disk
    assert crosswalk_path.exists()
    assert problematic_path.exists()
