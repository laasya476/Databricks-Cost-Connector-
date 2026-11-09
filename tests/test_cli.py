from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from connector import cli


class CliTests(unittest.TestCase):
    
    def test_redact_masks_nested_record_identifiers(self):
        redacted = cli._redact({
            "source_record_id": "source-123",
            "additional_source_metadata": {
                "record_id": "record-456",
                "consumed_quantity": "0.007609000000000000",
            },
        })

        self.assertEqual(redacted["source_record_id"], "[REDACTED]")
        self.assertEqual(
            redacted["additional_source_metadata"]["record_id"],
            "[REDACTED]",
        )
        self.assertEqual(
            redacted["additional_source_metadata"]["consumed_quantity"],
            "0.007609000000000000",
        )

    
    def test_redact_preserves_decimal_precision(self):
        sample = {
            "consumed_quantity": "0.007609000000000000",
            "list_unit_price": "0.023000000000000000",
            "list_cost": "0.0001750070000000000000000000000",
        }

        redacted = cli._redact(sample)

        self.assertEqual(
            redacted["consumed_quantity"], "0.007609000000000000"
        )
        self.assertEqual(
            redacted["list_unit_price"], "0.023000000000000000"
        )
        self.assertEqual(
            redacted["list_cost"], "0.0001750070000000000000000000000"
        )

    def test_parse_datetime_requires_timezone_and_normalizes_utc(self):
        parsed = cli.parse_datetime("2026-10-01T03:00:00+03:00")
        self.assertEqual(parsed, datetime(2026, 10, 1, tzinfo=timezone.utc))
        with self.assertRaises(Exception):
            cli.parse_datetime("2026-10-01T03:00:00")

    def test_json_safe_keeps_decimal_as_exact_text(self):
        converted = cli._json_safe(
            {
                "quantity": Decimal("0.100000000000000001"),
                "event_date": datetime(2026, 10, 1, tzinfo=timezone.utc),
            }
        )
        self.assertEqual(converted["quantity"], "0.100000000000000001")
        self.assertEqual(converted["event_date"], "2026-10-01T00:00:00Z")

    def test_json_safe_serializes_utc_session_timestamp_as_utc(self):
        converted = cli._json_safe(datetime(2026, 10, 1, 12, 30))
        self.assertEqual(converted, "2026-10-01T12:30:00Z")

    def test_redact_masks_nested_identity_account_ids_and_emails(self):
        redacted = cli._redact(
            {
                "account_id": "123456789012",
                "identity_metadata": {"run_as": "person@example.com"},
                "note": "contact person@example.com",
            }
        )
        self.assertEqual(redacted["account_id"], "[REDACTED]")
        self.assertEqual(redacted["identity_metadata"], "[REDACTED]")
        self.assertNotIn("person@example.com", json.dumps(redacted))

    def test_redact_parses_json_encoded_structs(self):
        redacted = cli._redact(
            '{"account_id":"123456789012",'
            '"identity_metadata":{"run_as":"person@example.com"}}'
        )
        self.assertNotIn("123456789012", json.dumps(redacted))
        self.assertNotIn("person@example.com", json.dumps(redacted))

    def test_safe_error_redacts_credentials_email_and_numeric_id(self):
        safe = cli._safe_error(
            "failed SELECT secret FROM private_table token=topsecret for person@example.com account 123456789012"
        )
        self.assertNotIn("topsecret", safe)
        self.assertNotIn("person@example.com", safe)
        self.assertNotIn("123456789012", safe)
        self.assertNotIn("SELECT", safe)
        self.assertNotIn("private_table", safe)

    def test_redacted_sample_removes_query_and_embedded_token(self):
        redacted = cli._redact({
            "message": "SELECT secret FROM private_table",
            "token_note": "token=topsecret",
            "api_secret": "hidden-secret",
            "workspaceId": "workspace-private",
        })
        text = json.dumps(redacted)
        self.assertNotIn("private_table", text)
        self.assertNotIn("topsecret", text)
        self.assertNotIn("hidden-secret", text)
        self.assertNotIn("workspace-private", text)

    @patch("connector.auth.test_connection", return_value=True)
    @patch("connector.cli._discover_and_collect")
    def test_pipeline_writes_raw_normalized_coverage_and_redacted_evidence(
        self,
        discover_collect,
        _test_connection,
    ):
        start = datetime(2026, 10, 1, tzinfo=timezone.utc)
        end = datetime(2026, 10, 3, tzinfo=timezone.utc)
        usage = {
            "account_id": "123456789012",
            "workspace_id": "workspace-123",
            "record_id": "record-1",
            "sku_name": "STANDARD_COMPUTE",
            "cloud": "AWS",
            "usage_start_time": start,
            "usage_end_time": end,
            "usage_quantity": Decimal("2.5"),
            "usage_unit": "DBU",
            "identity_metadata": {"run_as": "person@example.com"},
        }
        list_price = {
            "account_id": "123456789012",
            "sku_name": "STANDARD_COMPUTE",
            "cloud": "AWS",
            "usage_unit": "DBU",
            "price_start_time": start,
            "price_end_time": None,
            "currency_code": "USD",
            "pricing": {"effective_list": {"default": Decimal("0.70")}},
        }
        results = {
            "billing_usage": {
                "source_name": "billing_usage",
                "table": "system.billing.usage",
                "status": "collected",
                "record_count": 1,
                "records": [usage],
                "error": None,
            },
            "billing_list_prices": {
                "source_name": "billing_list_prices",
                "table": "system.billing.list_prices",
                "status": "collected",
                "record_count": 1,
                "records": [list_price],
                "error": None,
            },
            "network_inbound": {
                "source_name": "network_inbound",
                "table": "system.access.inbound_network",
                "status": "failed",
                "record_count": 0,
                "records": [],
                "error": "SELECT secret FROM private_table token=topsecret while contacting person@example.com account 123456789012",
            },
        }
        discover_collect.return_value = ({}, results)

        written_json = {}
        written_jsonl = {}
        written_csv = {}

        def capture_json(path, value):
            written_json[path.name] = value

        def capture_jsonl(path, records):
            written_jsonl[path.name] = records

        def capture_csv(path, rows):
            written_csv[path.name] = rows

        output_dir = Path("assessment-test-output")
        with (
            patch.object(Path, "mkdir"),
            patch("builtins.print"),
            patch.object(cli, "_write_json", side_effect=capture_json),
            patch.object(cli, "_write_jsonl", side_effect=capture_jsonl),
            patch.object(cli, "_write_csv", side_effect=capture_csv),
        ):
            summary = cli.run_pipeline(start, end, output_dir, batch_size=7)

            self.assertEqual(summary["priced_record_count"], 1)
            self.assertEqual(summary["normalized_record_count"], 1)
            self.assertIn("billing_usage.jsonl", written_jsonl)
            self.assertIn("normalized.jsonl", written_jsonl)
            self.assertIn("coverage_report.csv", written_csv)
            self.assertIn("discovery_reconciliation.json", written_json)

            category_rows = written_json["coverage_report.json"]
            self.assertEqual(len(category_rows), 17)

            evidence_text = json.dumps(
                written_json["live_execution_evidence_redacted.json"]
            )
            self.assertNotIn("topsecret", evidence_text)
            self.assertNotIn("person@example.com", evidence_text)
            self.assertNotIn("123456789012", evidence_text)
            self.assertNotIn("private_table", evidence_text)

            raw_sample_text = json.dumps(
                written_json["raw_sample_redacted.json"]
            )
            self.assertNotIn("person@example.com", raw_sample_text)
            self.assertNotIn("123456789012", raw_sample_text)

    


if __name__ == "__main__":
    unittest.main()
