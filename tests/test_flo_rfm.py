import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from flo_rfm import (
    build_campaign_targets,
    create_rfm,
    prepare_data,
    run_analysis,
)


def make_customers(count: int = 10) -> pd.DataFrame:
    first_order = date(2021, 1, 1)
    last_order = date(2021, 5, 1)
    return pd.DataFrame(
        {
            "master_id": [f"customer-{index}" for index in range(count)],
            "order_channel": ["Android App"] * count,
            "last_order_channel": ["Android App"] * count,
            "first_order_date": [
                (first_order + timedelta(days=index)).isoformat()
                for index in range(count)
            ],
            "last_order_date": [
                (last_order + timedelta(days=index)).isoformat()
                for index in range(count)
            ],
            "last_order_date_online": [
                (last_order + timedelta(days=index)).isoformat()
                for index in range(count)
            ],
            "last_order_date_offline": [
                (last_order + timedelta(days=index)).isoformat()
                for index in range(count)
            ],
            "order_num_total_ever_online": list(range(1, count + 1)),
            "order_num_total_ever_offline": [0] * count,
            "customer_value_total_ever_offline": [0] * count,
            "customer_value_total_ever_online": [
                index * 10 for index in range(1, count + 1)
            ],
            "interested_in_categories_12": ["[KADIN, ERKEK]"] * count,
        }
    )


class PrepareDataTests(unittest.TestCase):
    def test_adds_totals_without_mutating_input(self) -> None:
        customers = make_customers()

        prepared = prepare_data(customers)

        self.assertNotIn("order_num_total", customers.columns)
        self.assertEqual(prepared.loc[0, "order_num_total"], 1)
        self.assertEqual(prepared.loc[0, "customer_value_total"], 10)
        self.assertTrue(
            pd.api.types.is_datetime64_any_dtype(prepared["last_order_date"])
        )

    def test_rejects_missing_required_columns(self) -> None:
        with self.assertRaisesRegex(ValueError, "Missing required columns"):
            prepare_data(pd.DataFrame({"master_id": ["customer-1"]}))

    def test_rejects_duplicate_customer_ids(self) -> None:
        customers = make_customers(2)
        customers.loc[1, "master_id"] = customers.loc[0, "master_id"]

        with self.assertRaisesRegex(ValueError, "duplicate master_id"):
            prepare_data(customers)


class RfmTests(unittest.TestCase):
    def test_scores_customers_and_keeps_recency_direction_correct(self) -> None:
        customers = make_customers()
        original = customers.copy(deep=True)

        rfm = create_rfm(customers, analysis_date=date(2021, 6, 1))

        self.assertEqual(len(rfm), len(customers))
        self.assertEqual(
            list(rfm.columns),
            [
                "customer_id",
                "recency",
                "frequency",
                "monetary",
                "RF_SCORE",
                "RFM_SCORE",
                "segment",
            ],
        )
        self.assertEqual(rfm.loc[0, "recency"], 31)
        self.assertEqual(rfm.loc[len(rfm) - 1, "recency"], 22)
        first_customer_score = str(rfm.loc[0, "RF_SCORE"])[0]
        last_customer_score = str(rfm.loc[len(rfm) - 1, "RF_SCORE"])[0]
        self.assertLess(first_customer_score, last_customer_score)
        self.assertTrue(
            rfm["segment"]
            .isin(
                {
                    "hibernating",
                    "at_Risk",
                    "cant_loose",
                    "about_to_sleep",
                    "need_attention",
                    "loyal_customers",
                    "promising",
                    "new_customers",
                    "potential_loyalists",
                    "champions",
                }
            )
            .all()
        )
        pd.testing.assert_frame_equal(customers, original)

    def test_rejects_analysis_date_before_latest_order(self) -> None:
        with self.assertRaisesRegex(ValueError, "earlier than the latest order"):
            create_rfm(make_customers(), analysis_date=date(2021, 1, 1))


class CampaignTests(unittest.TestCase):
    def test_targets_include_at_risk_discount_audience(self) -> None:
        customers = pd.DataFrame(
            {
                "master_id": ["champion", "at-risk", "cant-lose", "new", "other"],
                "interested_in_categories_12": [
                    "[KADIN]",
                    "[ERKEK]",
                    "[COCUK]",
                    "[ERKEK]",
                    "[KADIN]",
                ],
            }
        )
        rfm = pd.DataFrame(
            {
                "customer_id": customers["master_id"],
                "segment": [
                    "champions",
                    "at_Risk",
                    "cant_loose",
                    "new_customers",
                    "hibernating",
                ],
            }
        )

        targets = build_campaign_targets(customers, rfm)

        self.assertEqual(
            targets["yeni_marka_hedef_musteri_id.csv"]["master_id"].tolist(),
            ["champion"],
        )
        self.assertEqual(
            targets["indirim_hedef_müşteri_ids.csv"]["master_id"].tolist(),
            ["at-risk", "cant-lose", "new"],
        )

    def test_run_analysis_writes_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            input_path = root / "customers.csv"
            output_dir = root / "reports"
            make_customers().to_csv(input_path, index=False)

            rfm = run_analysis(input_path, output_dir)

            self.assertEqual(len(rfm), 10)
            self.assertTrue((output_dir / "rfm_analysis.csv").is_file())
            self.assertTrue((output_dir / "rfm_segment_summary.csv").is_file())
            self.assertTrue((output_dir / "yeni_marka_hedef_musteri_id.csv").is_file())
            self.assertTrue((output_dir / "indirim_hedef_müşteri_ids.csv").is_file())
            self.assertEqual(
                pd.read_csv(output_dir / "rfm_analysis.csv").shape[0],
                10,
            )


if __name__ == "__main__":
    unittest.main()
