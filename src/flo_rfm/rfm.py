"""RFM scoring and customer segmentation."""

import math
import re
from datetime import date, timedelta

import pandas as pd

from flo_rfm.data import prepare_data

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
    rfm["RF_SCORE"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(
        str
    )
    rfm["RFM_SCORE"] = rfm["RF_SCORE"] + rfm["monetary_score"].astype(str)
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
