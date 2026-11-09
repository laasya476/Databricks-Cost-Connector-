from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

import connector.coverage as coverage
from connector.collectors import PartialCollectionError


class CoverageTests(unittest.TestCase):
    @patch.object(coverage, "collect_source")
    def test_partial_fetch_is_failed_with_separate_partial_count(self, collect_source):
        collect_source.side_effect = PartialCollectionError(3, RuntimeError("query secret"))
        result = coverage.collect_with_status("billing_usage")
        row = coverage.build_source_coverage_report({"billing_usage": result})[0]
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["records"], [])
        self.assertEqual(row["partial_record_count"], 3)

    def test_allowed_statuses_match_assessment(self):
        self.assertEqual(
            coverage.ALLOWED_STATUSES,
            {
                "collected",
                "empty",
                "permission_denied",
                "unavailable_in_account",
                "unsupported",
                "failed",
            },
        )

    def test_error_classification_includes_authentication_failures(self):
        cases = {
            "permission denied": "permission_denied",
            "authentication failed": "permission_denied",
            "invalid token": "permission_denied",
            "SCHEMA_NOT_FOUND": "unavailable_in_account",
            "feature not supported": "unsupported",
            "network connection failed": "failed",
        }

        for message, expected in cases.items():
            with self.subTest(message=message):
                self.assertEqual(
                    coverage._classify_error(RuntimeError(message)),
                    expected,
                )

    @patch.object(coverage, "collect_source")
    def test_collect_with_status_marks_records_collected(self, collect_source):
        collect_source.return_value = [
            {
                "record_id": "record-1",
                "usage_start_time": "2026-10-01T00:00:00Z",
            }
        ]

        result = coverage.collect_with_status("billing_usage")

        self.assertEqual(result["status"], "collected")
        self.assertEqual(result["record_count"], 1)
        self.assertEqual(result["malformed_record_count"], 0)
        self.assertIsNone(result["error"])

    @patch.object(coverage, "collect_source")
    def test_empty_results_are_not_failures(self, collect_source):
        collect_source.return_value = []

        result = coverage.collect_with_status("storage_table_metrics")

        self.assertEqual(result["status"], "empty")
        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["records"], [])

    @patch.object(coverage, "collect_source")
    def test_permission_failure_is_reported_and_does_not_raise(self, collect_source):
        collect_source.side_effect = RuntimeError("permission denied")

        result = coverage.collect_with_status("billing_usage")

        self.assertEqual(result["status"], "permission_denied")
        self.assertEqual(result["record_count"], 0)
        self.assertIn("permission denied", result["error"])

    @patch.object(coverage, "collect_source")
    def test_missing_source_is_unavailable(self, collect_source):
        result = coverage.collect_with_status("missing_source")

        self.assertEqual(result["status"], "unavailable_in_account")
        collect_source.assert_not_called()

    @patch.object(coverage, "collect_source")
    def test_malformed_records_do_not_break_collection(self, collect_source):
        collect_source.return_value = [
            {"usage_start_time": "2026-10-01T00:00:00Z"},
            "malformed-record",
            {"usage_start_time": "not-a-timestamp"},
        ]

        result = coverage.collect_with_status("billing_usage")
        report = coverage.build_source_coverage_report(
            {"billing_usage": result}
        )

        self.assertEqual(result["status"], "collected")
        self.assertEqual(result["malformed_record_count"], 1)
        self.assertEqual(report[0]["earliest_timestamp"], "2026-10-01T00:00:00Z")
        self.assertEqual(report[0]["latest_timestamp"], "2026-10-01T00:00:00Z")
        self.assertIn("malformed", report[0]["explanation"])

    @patch.object(coverage, "collect_source")
    def test_collect_all_continues_after_a_source_failure(self, collect_source):
        original_config = coverage.SOURCE_CONFIG
        coverage.SOURCE_CONFIG = {
            "first_source": {"table": "system.example.first"},
            "second_source": {"table": "system.example.second"},
        }

        def fake_collect(source_name, **kwargs):
            if source_name == "first_source":
                raise RuntimeError("table_or_view_not_found")
            return [{"event_time": "2026-10-01T00:00:00Z"}]

        collect_source.side_effect = fake_collect

        try:
            results = coverage.collect_all_with_status()
        finally:
            coverage.SOURCE_CONFIG = original_config

        self.assertEqual(results["first_source"]["status"], "unavailable_in_account")
        self.assertEqual(results["second_source"]["status"], "collected")

    def test_timestamp_handling_normalizes_aware_values_to_utc(self):
        results = {
            "billing_usage": {
                "source_name": "billing_usage",
                "table": "system.billing.usage",
                "status": "collected",
                "record_count": 2,
                "records": [
                    {
                        "usage_start_time": datetime(
                            2026,
                            10,
                            1,
                            5,
                            tzinfo=timezone.utc,
                        )
                    },
                    {"usage_start_time": "2026-10-02T01:00:00+02:00"},
                ],
                "error": None,
            }
        }

        report = coverage.build_source_coverage_report(results)

        self.assertEqual(
            report[0]["earliest_timestamp"],
            "2026-10-01T05:00:00Z",
        )
        self.assertEqual(
            report[0]["latest_timestamp"],
            "2026-10-01T23:00:00Z",
        )

    def test_naive_databricks_timestamp_is_interpreted_in_utc_session(self):
        report = coverage.build_source_coverage_report(
            {
                "billing_usage": {
                    "table": "system.billing.usage",
                    "status": "collected",
                    "record_count": 1,
                    "records": [
                        {"usage_start_time": datetime(2026, 10, 1, 12)}
                    ],
                }
            }
        )
        self.assertEqual(
            report[0]["earliest_timestamp"],
            "2026-10-01T12:00:00Z",
        )

    def test_date_timestamp_is_preserved_as_a_date(self):
        results = {
            "storage_table_metrics": {
                "source_name": "storage_table_metrics",
                "table": "system.storage.table_metrics_history",
                "status": "collected",
                "record_count": 1,
                "records": [{"snapshot_date": date(2026, 10, 4)}],
                "error": None,
            }
        }

        report = coverage.build_source_coverage_report(results)

        self.assertEqual(report[0]["earliest_timestamp"], "2026-10-04")
        self.assertEqual(report[0]["latest_timestamp"], "2026-10-04")

    def test_category_report_has_all_seventeen_required_categories(self):
        results = {
            "billing_usage": {
                "source_name": "billing_usage",
                "table": "system.billing.usage",
                "status": "collected",
                "record_count": 1,
                "records": [
                    {"usage_start_time": "2026-10-01T00:00:00Z"}
                ],
                "error": None,
            }
        }

        report = coverage.build_coverage_report(results)

        self.assertEqual(len(report), 17)

        required_fields = {
            "category",
            "dataset",
            "purpose",
            "status",
            "record_count",
            "earliest_timestamp",
            "latest_timestamp",
            "required_permission",
            "explanation",
        }

        for row in report:
            self.assertTrue(required_fields.issubset(row))
            self.assertIn(row["status"], coverage.ALLOWED_STATUSES)

        billing_row = next(
            row for row in report if row["category_key"] == "billing_and_usage"
        )
        self.assertEqual(billing_row["status"], "collected")
        self.assertEqual(billing_row["record_count"], 1)

    def test_categories_require_category_specific_evidence(self):
        results = {
            "billing_usage": {
                "source_name": "billing_usage",
                "table": "system.billing.usage",
                "status": "collected",
                "record_count": 1,
                "records": [
                    {
                        "usage_start_time": "2026-10-01T00:00:00Z",
                        "sku_name": "GENERIC_SKU",
                        "record_type": "ORIGINAL",
                        "product_features": '{"serverless":false}',
                        "custom_tags": "{}",
                    }
                ],
                "error": None,
            }
        }

        rows = {
            row["category_key"]: row
            for row in coverage.build_coverage_report(results)
        }

        self.assertEqual(rows["billing_and_usage"]["status"], "collected")
        for category_key in (
            "cluster_and_compute_consumption",
            "sql_warehouse_consumption",
            "jobs_and_pipeline_consumption",
            "serverless_consumption",
            "storage_related_usage",
            "tags_and_allocation_metadata",
            "resource_and_workload_attribution",
            "user_and_service_principal_attribution",
            "corrections_retractions_and_restatements",
        ):
            with self.subTest(category=category_key):
                self.assertEqual(rows[category_key]["status"], "empty")
                self.assertEqual(rows[category_key]["record_count"], 0)

    def test_category_metadata_is_attributed_and_explained(self):
        results = {
            "billing_usage": {
                "source_name": "billing_usage",
                "table": "system.billing.usage",
                "status": "collected",
                "record_count": 1,
                "records": [
                    {
                        "usage_start_time": "2026-10-01T00:00:00Z",
                        "sku_name": "SERVERLESS_SQL",
                        "record_type": "RETRACTION",
                        "usage_metadata": {
                            "job_id": "job-1",
                            "job_run_id": "run-1",
                            "warehouse_id": "warehouse-1",
                            "cluster_id": "cluster-1",
                            "custom_tags": {"team": "analytics"},
                            "budget_policy_id": "budget-1",
                        },
                        "identity_metadata": {"run_as": "service-principal"},
                    }
                ],
                "error": None,
            }
        }

        rows = {
            row["category_key"]: row
            for row in coverage.build_coverage_report(results)
        }

        for category_key in (
            "cluster_and_compute_consumption",
            "sql_warehouse_consumption",
            "jobs_and_pipeline_consumption",
            "serverless_consumption",
            "resource_and_workload_attribution",
            "user_and_service_principal_attribution",
            "tags_and_allocation_metadata",
            "budgets_alerts_and_cost_controls",
            "corrections_retractions_and_restatements",
        ):
            with self.subTest(category=category_key):
                self.assertEqual(rows[category_key]["status"], "collected")

    def test_budget_policy_is_found_in_json_encoded_usage_metadata(self):
        results = {
            "billing_usage": {
                "source_name": "billing_usage",
                "table": "system.billing.usage",
                "status": "collected",
                "record_count": 1,
                "records": [
                    {
                        "usage_start_time": "2026-10-01T00:00:00Z",
                        "usage_metadata": '{"budget_policy_id":"budget-1"}',
                    }
                ],
                "error": None,
            }
        }

        row = next(
            row
            for row in coverage.build_coverage_report(results)
            if row["category_key"] == "budgets_alerts_and_cost_controls"
        )

        self.assertEqual(row["status"], "collected")
        self.assertIn("budget_policy_id", row["explanation"])

    def test_contract_category_uses_discovery_not_inferred_from_prices(self):
        results = {
            "billing_list_prices": {
                "source_name": "billing_list_prices",
                "table": "system.billing.list_prices",
                "status": "collected",
                "record_count": 1,
                "records": [{"sku_name": "SKU"}],
                "error": None,
            },
            "billing_account_prices": {
                "source_name": "billing_account_prices",
                "table": "system.billing.account_prices",
                "status": "collected",
                "record_count": 1,
                "records": [{"sku_name": "SKU"}],
                "error": None,
            },
        }

        row = next(
            row
            for row in coverage.build_coverage_report(
                results, commercial_discovery_status="successful"
            )
            if row["category_key"] == "contract_commitment_and_discount"
        )

        self.assertEqual(row["status"], "unsupported")
        self.assertEqual(row["record_count"], 0)
        self.assertIsNone(row["earliest_timestamp"])
        self.assertIsNone(row["latest_timestamp"])
        self.assertIn("no validated commercial-terms source", row["explanation"])

        failed_row = next(
            row for row in coverage.build_coverage_report(
                results, commercial_discovery_status="failed"
            ) if row["category_key"] == "contract_commitment_and_discount"
        )
        self.assertEqual(failed_row["status"], "failed")

    def test_list_cost_dates_come_from_calculated_usage_not_price_validity(self):
        results = {
            "billing_usage": {
                "source_name": "billing_usage",
                "table": "system.billing.usage",
                "status": "collected",
                "record_count": 1,
                "priced_record_count": 1,
                "records": [{"usage_start_time": "2026-10-05T05:00:00Z"}],
                "list_cost_records": [
                    {"usage_start_time": "2026-10-05T05:00:00Z", "list_cost": "1.25"}
                ],
                "error": None,
            },
            "billing_list_prices": {
                "source_name": "billing_list_prices",
                "table": "system.billing.list_prices",
                "status": "collected",
                "record_count": 1,
                "records": [{"price_start_time": "2013-01-01T00:00:00Z"}],
                "error": None,
            },
            "billing_account_prices": {
                "source_name": "billing_account_prices",
                "table": "system.billing.account_prices",
                "status": "empty",
                "record_count": 0,
                "records": [],
                "error": None,
            },
        }

        row = next(
            row
            for row in coverage.build_coverage_report(results)
            if row["category_key"] == "list_cost_calculations"
        )

        self.assertEqual(row["record_count"], 1)
        self.assertEqual(row["earliest_timestamp"], "2026-10-05T05:00:00Z")
        self.assertEqual(row["latest_timestamp"], "2026-10-05T05:00:00Z")

    def test_optimization_category_names_collected_operation_source(self):
        results = {
            "storage_predictive_optimization": {
                "source_name": "storage_predictive_optimization",
                "table": "system.storage.predictive_optimization_operations_history",
                "status": "collected",
                "record_count": 1,
                "records": [{"start_time": "2026-10-01T00:00:00Z"}],
                "error": None,
            }
        }

        row = next(
            row
            for row in coverage.build_coverage_report(results)
            if row["category_key"] == "optimization_and_cost_saving"
        )

        self.assertEqual(row["status"], "collected")
        self.assertIn("predictive_optimization_operations_history", row["explanation"])

    def test_budget_category_reports_broad_alert_discovery_result(self):
        results = {
            "billing_usage": {
                "source_name": "billing_usage",
                "table": "system.billing.usage",
                "status": "collected",
                "record_count": 1,
                "records": [{"usage_start_time": "2026-10-01T00:00:00Z"}],
                "error": None,
            }
        }
        row = next(
            row for row in coverage.build_coverage_report(
                results, alert_schema_available=False
            )
            if row["category_key"] == "budgets_alerts_and_cost_controls"
        )
        self.assertEqual(row["status"], "empty")
        self.assertIn("no system.alert schema was visible", row["explanation"])


if __name__ == "__main__":
    unittest.main()
