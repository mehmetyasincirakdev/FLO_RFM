"""Compatibility entry point for running the FLO RFM analysis."""

from flo_rfm import (
    build_campaign_targets,
    create_rfm,
    data_prep,
    load_data,
    prepare_data,
    run_analysis,
)
from flo_rfm.cli import main

__all__ = [
    "build_campaign_targets",
    "create_rfm",
    "data_prep",
    "load_data",
    "prepare_data",
    "run_analysis",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
