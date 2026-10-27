"""Customer segmentation and campaign audience generation for the FLO dataset."""

from __future__ import annotations

import argparse
import math
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Sequence

import pandas as pd


DATA_PATH = Path("Datasets") / "flo_data_20k.csv"
OUTPUT_DIR = Path("outputs")
DATE_COLUMNS = (
    "first_order_date",
    "last_order_date",
    "last_order_date_online",
    "last_order_date_offline",
)
ORDER_COLUMNS = (
    "order_num_total_ever_online",
    "order_num_total_ever_offline",
)
SPENDING_COLUMNS = (
    "customer_value_total_ever_offline",
    "customer_value_total_ever_online",
)
REQUIRED_COLUMNS = {
    "master_id",
    "interested_in_categories_12",
    *DATE_COLUMNS,
    *ORDER_COLUMNS,
    *SPENDING_COLUMNS,
}
SEGMENT_RULES = {
    r"[1-2][1-2]": "hibernating",
    r"[1-2][3-4]": "at_Risk",
    r"[1-2]5": "cant_loose",
    r"3[1-2]": "about_to_sleep",
    r"33": "need_attention",
    r"[3-4][4-5]": "loyal_customers",
    r"41": "promising",
    r"51": "new_customers",
    r"[4-5][2-3]": "potential_loyalists",
    r"5[4-5]": "champions",
}


def prepare_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Validate customer records, parse dates, and add total order/value columns."""
    missing_columns = REQUIRED_COLUMNS.difference(dataframe.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns: {missing}")
    if dataframe.empty:
        raise ValueError("The input dataset is empty.")

    prepared = dataframe.copy()
    if prepared["master_id"].isna().any():
        raise ValueError("The dataset contains customers without a master_id.")
    if prepared["master_id"].duplicated().any():
        raise ValueError("The dataset contains duplicate master_id values.")

    for column in DATE_COLUMNS:
        prepared[column] = pd.to_datetime(prepared[column], errors="coerce")
        if prepared[column].isna().any():
            raise ValueError(f"The {column} column contains missing or invalid dates.")

    for column in (*ORDER_COLUMNS, *SPENDING_COLUMNS):
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
        if prepared[column].isna().any():
            raise ValueError(f"The {column} column contains missing or invalid numbers.")
        if prepared[column].lt(0).any():
            raise ValueError(f"The {column} column cannot contain negative values.")

    if prepared["interested_in_categories_12"].isna().any():
        raise ValueError("The interested_in_categories_12 column contains missing values.")

    prepared["order_num_total"] = prepared[list(ORDER_COLUMNS)].sum(axis="columns")
    prepared["customer_value_total"] = prepared[list(SPENDING_COLUMNS)].sum(
        axis="columns"
    )
    return prepared


def data_prep(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Keep the original data_prep entry point while preparing the provided data."""
    return prepare_data(dataframe)


def _quintile_scores(values: pd.Series, *, higher_is_better: bool) -> pd.Series:
    percentiles = values.rank(method="average", pct=True, ascending=higher_is_better)
    scores = percentiles.mul(5).map(math.ceil).clip(lower=1, upper=5)
    return scores.astype("int64")


def _segment_for_score(score: str) -> str:
    for pattern, segment in SEGMENT_RULES.items():
        if re.fullmatch(pattern, score):
            return segment
    raise ValueError(f"No customer segment is defined for RF score {score!r}.")


def create_rfm(
    dataframe: pd.DataFrame,
    analysis_date: date | None = None,
) -> pd.DataFrame:
    """Calculate RFM scores and segments without modifying the input dataframe."""
    prepared = prepare_data(dataframe)
    latest_order_date = prepared["last_order_date"].max()
    if analysis_date is None:
        reference_date = latest_order_date.date() + timedelta(days=1)
    else:
        reference_date = analysis_date

    analysis_timestamp = pd.Timestamp(reference_date)
    if analysis_timestamp < latest_order_date:
        raise ValueError("analysis_date cannot be earlier than the latest order date.")

    rfm = pd.DataFrame(
        {
            "customer_id": prepared["master_id"],
            "recency": (analysis_timestamp - prepared["last_order_date"]).dt.days,
            "frequency": prepared["order_num_total"],
            "monetary": prepared["customer_value_total"],
        }
    )
    rfm["recency_score"] = _quintile_scores(rfm["recency"], higher_is_better=False)
    rfm["frequency_score"] = _quintile_scores(rfm["frequency"], higher_is_better=True)
    rfm["monetary_score"] = _quintile_scores(rfm["monetary"], higher_is_better=True)
    rfm["RF_SCORE"] = (
        rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)
    )
    rfm["RFM_SCORE"] = (
        rfm["RF_SCORE"] + rfm["monetary_score"].astype(str)
    )
    rfm["segment"] = rfm["RF_SCORE"].map(_segment_for_score)

    return rfm[
        [
            "customer_id",
            "recency",
            "frequency",
            "monetary",
            "RF_SCORE",
            "RFM_SCORE",
            "segment",
        ]
    ]


def build_campaign_targets(
    customers: pd.DataFrame,
    rfm: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Build the existing women's-brand and discount customer ID audiences."""
    segments = rfm.set_index("customer_id")["segment"]
    categorized = customers[["master_id", "interested_in_categories_12"]].copy()
    categorized["segment"] = categorized["master_id"].map(segments)
    categories = categorized["interested_in_categories_12"].astype(str)

    new_brand_mask = categorized["segment"].isin(
        {"champions", "loyal_customers"}
    ) & categories.str.contains("KADIN", case=False, na=False)
    discount_mask = categorized["segment"].isin(
        {"cant_loose", "at_Risk", "new_customers"}
    ) & categories.str.contains("ERKEK|COCUK", case=False, na=False, regex=True)

    return {
        "yeni_marka_hedef_musteri_id.csv": categorized.loc[
            new_brand_mask, ["master_id"]
        ],
        "indirim_hedef_müşteri_ids.csv": categorized.loc[
            discount_mask, ["master_id"]
        ],
    }


def load_data(input_path: str | Path = DATA_PATH) -> pd.DataFrame:
    """Read the source customer CSV."""
    return pd.read_csv(input_path)


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
    print(f"Analyzed {len(rfm):,} customers across {rfm['segment'].nunique()} segments.")
    print(f"CSV reports written to: {args.output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
