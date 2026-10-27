"""End-to-end analysis orchestration and CSV report generation."""

from datetime import date
from pathlib import Path

import pandas as pd

from flo_rfm.campaigns import build_campaign_targets
from flo_rfm.data import DATA_PATH, load_data, prepare_data
from flo_rfm.rfm import create_rfm

OUTPUT_DIR = Path("outputs")


def run_analysis(
    input_path: str | Path = DATA_PATH,
    output_dir: str | Path = OUTPUT_DIR,
    analysis_date: date | None = None,
) -> pd.DataFrame:
    """Run RFM analysis and write the analysis and campaign CSV files."""
    customers = prepare_data(load_data(input_path))
    rfm = create_rfm(customers, analysis_date=analysis_date)
    targets = build_campaign_targets(customers, rfm)
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    rfm.to_csv(destination / "rfm_analysis.csv", index=False)
    (
        rfm.groupby("segment", as_index=False)
        .agg(
            customer_count=("customer_id", "size"),
            average_recency=("recency", "mean"),
            average_frequency=("frequency", "mean"),
            average_monetary=("monetary", "mean"),
        )
        .sort_values("customer_count", ascending=False)
        .to_csv(destination / "rfm_segment_summary.csv", index=False)
    )
    for filename, audience in targets.items():
        audience.to_csv(destination / filename, index=False)

    return rfm
