"""Example script that imports FHA snapshots using the default configuration."""

from __future__ import annotations

from fha_data_manager.import_cli import import_hecm_snapshots, import_single_family_snapshots


def main() -> None:
    """Run the Single Family and HECM import pipelines with default settings."""

    import_single_family_snapshots()
    import_hecm_snapshots()


if __name__ == "__main__":  # pragma: no cover - manual invocation only
    main()
