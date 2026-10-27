"""Customer dataset loading, validation, and feature preparation."""

from pathlib import Path

import pandas as pd

DATA_PATH = Path("Datasets") / "flo_data_20k.csv"
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
            raise ValueError(
                f"The {column} column contains missing or invalid numbers."
            )
        if prepared[column].lt(0).any():
            raise ValueError(f"The {column} column cannot contain negative values.")

    if prepared["interested_in_categories_12"].isna().any():
        raise ValueError(
            "The interested_in_categories_12 column contains missing values."
        )

    prepared["order_num_total"] = prepared[list(ORDER_COLUMNS)].sum(axis="columns")
    prepared["customer_value_total"] = prepared[list(SPENDING_COLUMNS)].sum(
        axis="columns"
    )
    return prepared


def data_prep(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Keep the original data_prep entry point while preparing the provided data."""
    return prepare_data(dataframe)


def load_data(input_path: str | Path = DATA_PATH) -> pd.DataFrame:
    """Read the source customer CSV or explain how to supply the private dataset."""
    path = Path(input_path)
    if not path.is_file():
        raise FileNotFoundError(
            f"Dataset not found: {path}. Obtain flo_data_20k.csv from an "
            "authorized source and place it at Datasets/flo_data_20k.csv, "
            "or pass its location with --input."
        )
    return pd.read_csv(path)
