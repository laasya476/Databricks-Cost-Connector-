from __future__ import annotations

import unittest
from unittest.mock import patch

from connector import discovery


class DiscoveryTests(unittest.TestCase):
    @patch.object(discovery, "discover_tables", side_effect=RuntimeError("SELECT secret"))
    @patch.object(discovery, "discover_system_schemas", return_value=[{"databaseName": "billing"}])
    def test_schema_listing_failure_is_recorded(self, _schemas, _tables):
        available = discovery.discover_available_system_tables()
        self.assertIn("billing", available["_schema_errors"])
        rows = discovery.reconcile_system_tables(available, {})
        self.assertEqual(rows[0]["collection_status"], "discovery_failed")

    def test_visible_unconfigured_cost_table_has_explanation(self):
        available = {
            "billing": [
                {"tableName": "usage"},
                {"tableName": "new_cost_table"},
            ],
            "misc": [{"tableName": "unrelated_table"}],
        }
        results = {"billing_usage": {"status": "collected"}}
        rows = discovery.reconcile_system_tables(available, results)
        by_table = {row["table"]: row for row in rows}
        self.assertEqual(by_table["usage"]["collection_status"], "collected")
        self.assertEqual(by_table["new_cost_table"]["collection_status"], "not_configured")
        self.assertIn("no collector", by_table["new_cost_table"]["explanation"])
        self.assertEqual(by_table["unrelated_table"]["collection_status"], "outside_cost_scope")


if __name__ == "__main__":
    unittest.main()
