from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from connector import collectors


class FakeCursor:
    def __init__(self, rows, columns):
        self.rows = list(rows)
        self.columns = columns
        self.description = [(column,) for column in columns]
        self.position = 0
        self.executed_query = None
        self.executed_parameters = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, parameters=None):
        self.executed_query = query
        self.executed_parameters = parameters

    def fetchmany(self, batch_size):
        start = self.position
        end = start + batch_size
        rows = self.rows[start:end]
        self.position = end
        return rows


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_object = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def cursor(self):
        return self.cursor_object


class CollectorTests(unittest.TestCase):
    def test_storage_filter_uses_snapshot_date_and_date_parameters(self):
        start = datetime(2026, 9, 1, tzinfo=timezone.utc)
        end = datetime(2026, 10, 5, tzinfo=timezone.utc)

        query, parameters = collectors._build_time_filter(
            collectors.SOURCE_CONFIG["storage_table_metrics"],
            start,
            end,
        )

        self.assertIn("snapshot_date", query)
        self.assertNotIn("snapshot_time", query)
        self.assertIn("CAST(? AS DATE)", query)
        self.assertEqual(parameters, [date(2026, 9, 1), date(2026, 10, 5)])

    @patch.object(collectors, "get_connection")
    def test_collection_retrieves_all_batches_and_preserves_decimal(self, get_connection):
        rows = [
            (date(2026, 10, 1), Decimal("0.100000000000000001")),
            (date(2026, 10, 2), Decimal("2.121203333333333333")),
        ]
        cursor = FakeCursor(rows, ["snapshot_date", "usage_quantity"])
        get_connection.return_value = FakeConnection(cursor)

        result = collectors.collect_source(
            "storage_table_metrics",
            start_time=datetime(2026, 9, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 10, 5, tzinfo=timezone.utc),
            batch_size=1,
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0]["usage_quantity"],
            Decimal("0.100000000000000001"),
        )
        self.assertIsInstance(result[0]["usage_quantity"], Decimal)
        self.assertIn("snapshot_date", cursor.executed_query)
        self.assertEqual(
            cursor.executed_parameters,
            [date(2026, 9, 1), date(2026, 10, 5)],
        )

    def test_partial_final_date_is_included(self):
        _, parameters = collectors._build_time_filter(
            collectors.SOURCE_CONFIG["storage_table_metrics"],
            datetime(2026, 10, 1, 12, tzinfo=timezone.utc),
            datetime(2026, 10, 5, 12, tzinfo=timezone.utc),
        )
        self.assertEqual(parameters, [date(2026, 10, 1), date(2026, 10, 6)])

    @patch.object(collectors, "get_connection")
    def test_billing_collection_preserves_extra_raw_field(self, get_connection):
        cursor = FakeCursor([("record-1", "source-value")], ["record_id", "new_field"])
        get_connection.return_value = FakeConnection(cursor)

        records = collectors.collect_source("billing_usage")

        self.assertIn("SELECT\n            *", cursor.executed_query)
        self.assertEqual(records, [{"record_id": "record-1", "new_field": "source-value"}])

    @patch.object(collectors, "get_connection")
    def test_fetch_failure_preserves_partial_count_without_returning_rows(self, get_connection):
        class FailingCursor(FakeCursor):
            def fetchmany(self, batch_size):
                if self.position:
                    raise RuntimeError("SELECT secret FROM private_table")
                return super().fetchmany(batch_size)

        cursor = FailingCursor([("record-1",)], ["record_id"])
        get_connection.return_value = FakeConnection(cursor)

        with self.assertRaises(collectors.PartialCollectionError) as raised:
            collectors.collect_source("billing_usage", batch_size=1)

        self.assertEqual(raised.exception.retrieved_count, 1)

    def test_interval_filter_includes_records_overlapping_window(self):
        query, parameters = collectors._build_time_filter(
            collectors.SOURCE_CONFIG["billing_usage"],
            datetime(2026, 10, 1, tzinfo=timezone.utc),
            datetime(2026, 10, 5, tzinfo=timezone.utc),
        )

        self.assertIn("usage_end_time > ?", query)
        self.assertIn("usage_start_time < ?", query)
        self.assertEqual(
            parameters,
            [
                datetime(2026, 10, 1, tzinfo=timezone.utc),
                datetime(2026, 10, 5, tzinfo=timezone.utc),
            ],
        )

    def test_point_source_end_boundary_is_exclusive(self):
        query, parameters = collectors._build_time_filter(
            collectors.SOURCE_CONFIG["compute_warehouse_events"],
            datetime(2026, 10, 1, tzinfo=timezone.utc),
            datetime(2026, 10, 5, tzinfo=timezone.utc),
        )
        self.assertIn("event_time < ?", query)
        self.assertNotIn("event_time <= ?", query)
        self.assertEqual(
            parameters,
            [
                datetime(2026, 10, 1, tzinfo=timezone.utc),
                datetime(2026, 10, 5, tzinfo=timezone.utc),
            ],
        )

    def test_serving_endpoint_usage_filters_on_request_time(self):
        query, parameters = collectors._build_time_filter(
            collectors.SOURCE_CONFIG["serving_endpoint_usage"],
            datetime(2026, 10, 1, tzinfo=timezone.utc),
            datetime(2026, 10, 5, tzinfo=timezone.utc),
        )

        self.assertIn("request_time", query)
        self.assertNotIn("usage_end_time", query)
        self.assertEqual(
            parameters,
            [
                datetime(2026, 10, 1, tzinfo=timezone.utc),
                datetime(2026, 10, 5, tzinfo=timezone.utc),
            ],
        )

    @patch.object(collectors, "get_connection")
    def test_collection_without_time_filter_uses_no_empty_parameter_list(
        self,
        get_connection,
    ):
        cursor = FakeCursor([], ["cluster_id"])
        get_connection.return_value = FakeConnection(cursor)

        result = collectors.collect_source("compute_clusters")

        self.assertEqual(result, [])
        self.assertIsNone(cursor.executed_parameters)


if __name__ == "__main__":
    unittest.main()
