"""Campaign audience selection."""

import pandas as pd


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
        "indirim_hedef_müşteri_ids.csv": categorized.loc[discount_mask, ["master_id"]],
    }
