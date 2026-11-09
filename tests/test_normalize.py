from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from connector.normalize import normalize_record


class NormalizeTests(unittest.TestCase):
    def test_date_only_ingestion_date_is_metadata_not_update_timestamp(self):
        normalized = normalize_record(
            "billing_usage",
            {"ingestion_date": date(2026, 10, 4)},
        )
        self.assertIsNone(normalized["source_update_time"])
        self.assertEqual(normalized["additional_source_metadata"]["ingestion_date"], date(2026, 10, 4))

    def test_normalizes_usage_record_and_preserves_decimal_precision(self):
        record = {
            "account_id": "account-1",
            "workspace_id": "workspace-1",
            "record_id": "record-1",
            "sku_name": "STANDARD_COMPUTE",
            "usage_start_time": "2026-10-01T05:30:00+05:30",
            "usage_end_time": "2026-10-01T06:30:00+05:30",
            "usage_quantity": Decimal("2.121203333333333333"),
            "usage_unit": "DBU",
            "currency_code": "USD",
            "billing_origin_product": "JOBS",
            "usage_metadata": {"job_id": "job-1", "job_name": "daily-job"},
            "identity_metadata": {"run_as": "service-principal-1"},
            "custom_tags": {"team": "data"},
        }

        normalized = normalize_record(
            "billing_usage",
            record,
            collection_time=datetime(2026, 10, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(normalized["platform"], "Databricks")
        self.assertEqual(normalized["source_record_id"], "record-1")
        self.assertEqual(normalized["resource_or_workload_identifier"], "job-1")
        self.assertEqual(normalized["resource_or_workload_name"], "daily-job")
        self.assertEqual(normalized["consumed_quantity"], "2.121203333333333333")
        self.assertEqual(normalized["usage_start"], "2026-10-01T00:00:00Z")
        self.assertEqual(normalized["usage_end"], "2026-10-01T01:00:00Z")
        self.assertEqual(normalized["collection_time"], "2026-10-05T00:00:00Z")
        self.assertEqual(normalized["user_or_service_principal"], "service-principal-1")
        self.assertEqual(normalized["additional_source_metadata"], record)

    def test_missing_fields_are_null_and_not_invented(self):
        normalized = normalize_record(
            "compute_clusters",
            {"cluster_id": "cluster-1"},
            collection_time=datetime(2026, 10, 5, tzinfo=timezone.utc),
        )

        self.assertIsNone(normalized["usage_start"])
        self.assertIsNone(normalized["list_cost"])
        self.assertIsNone(normalized["contracted_cost"])
        self.assertIsNone(normalized["billed_cost"])
        self.assertIsNone(normalized["currency"])

    def test_pricing_effective_list_price_is_preserved_as_decimal_text(self):
        normalized = normalize_record(
            "billing_list_prices",
            {
                "account_id": "account-1",
                "sku_name": "SERVERLESS",
                "pricing": {
                    "default": Decimal("0.700000000000000000"),
                    "effective_list": {
                        "default": Decimal("0.650000000000000000")
                    },
                },
            },
            collection_time=datetime(2026, 10, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(normalized["list_unit_price"], "0.650000000000000000")

    def test_parses_databricks_json_encoded_structs(self):
        normalized = normalize_record(
            "billing_usage",
            {
                "usage_metadata": (
                    '{"warehouse_id":"warehouse-1",'
                    '"warehouse_name":"analytics"}'
                ),
                "identity_metadata": '{"run_as":"service-principal"}',
                "pricing": '{"effective_list":{"default":0.650000000000000000}}',
            },
            collection_time=datetime(2026, 10, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(normalized["resource_or_workload_identifier"], "warehouse-1")
        self.assertEqual(normalized["resource_or_workload_name"], "analytics")
        self.assertEqual(normalized["user_or_service_principal"], "service-principal")
        self.assertEqual(normalized["list_unit_price"], "0.650000000000000000")

    def test_malformed_record_raises_clear_error(self):
        with self.assertRaises(TypeError):
            normalize_record("billing_usage", ["not", "a", "mapping"])

    def test_naive_timestamp_from_utc_session_is_normalized_as_utc(self):
        normalized = normalize_record(
            "billing_usage",
            {"usage_start_time": datetime(2026, 10, 1)},
            collection_time=datetime(2026, 10, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(normalized["usage_start"], "2026-10-01T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
