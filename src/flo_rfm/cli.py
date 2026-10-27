"""Command-line interface for FLO RFM analysis."""

import argparse
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from flo_rfm.data import DATA_PATH
from flo_rfm.pipeline import OUTPUT_DIR, run_analysis


def _parse_analysis_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "Date must use the YYYY-MM-DD format."
        ) from error


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Calculate FLO customer RFM segments and campaign audiences."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DATA_PATH,
        help=f"Source customer CSV (default: {DATA_PATH}).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Directory for generated CSV files (default: {OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--analysis-date",
        type=_parse_analysis_date,
        help="RFM reference date in YYYY-MM-DD format (default: latest order + 1 day).",
    )
    args = parser.parse_args(argv)

    rfm = run_analysis(args.input, args.output_dir, args.analysis_date)
    segment_count = rfm["segment"].nunique()
    print(f"Analyzed {len(rfm):,} customers across {segment_count} segments.")
    print(f"CSV reports written to: {args.output_dir.resolve()}")
    return 0
