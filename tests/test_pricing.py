from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from connector.pricing import calculate_list_cost, enrich_with_list_cost


def price(
    amount: str,
    *,
    start: str = "2026-10-01T00:00:00Z",
    end: str | None = None,
    sku: str = "STANDARD_COMPUTE",
) -> dict:
    return {
        "account_id": "account-1",
        "sku_name": sku,
        "cloud": "AWS",
        "usage_unit": "DBU",
        "price_start_time": start,
        "price_end_time": end,
        "currency_code": "USD",
        "pricing": {"effective_list": {"default": Decimal(amount)}},
    }


class PricingTests(unittest.TestCase):
    def usage(self, **overrides):
        record = {
            "account_id": "account-1",
            "sku_name": "STANDARD_COMPUTE",
            "cloud": "AWS",
            "usage_unit": "DBU",
            "usage_start_time": "2026-10-02T00:00:00Z",
            "usage_end_time": "2026-10-02T01:00:00Z",
            "usage_quantity": Decimal("2.121203333333333333"),
        }
        record.update(overrides)
        return record

    def test_matches_dimensions_and_calculates_decimal_cost(self):
        result = calculate_list_cost(
            self.usage(),
            [price("0.70")],
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["list_unit_price"], Decimal("0.70"))
        self.assertEqual(
            result["list_cost"],
            Decimal("1.4848423333333333331"),
        )
        self.assertEqual(result["currency_code"], "USD")

    def test_uses_the_price_active_at_the_usage_time(self):
        result = calculate_list_cost(
            self.usage(),
            [
                price("0.50", end="2026-10-02T00:00:00Z"),
                price("0.70", start="2026-10-02T00:00:00Z"),
            ],
        )

        self.assertEqual(result["list_unit_price"], Decimal("0.70"))
        self.assertEqual(result["list_cost"], Decimal("1.4848423333333333331"))

    def test_does_not_allocate_quantity_across_a_price_change(self):
        usage = self.usage(
            usage_start_time="2026-10-01T23:00:00Z",
            usage_end_time="2026-10-02T01:00:00Z",
            usage_quantity=Decimal("2"),
        )

        result = calculate_list_cost(
            usage,
            [
                price("0.50", end="2026-10-02T00:00:00Z"),
                price("0.70", start="2026-10-02T00:00:00Z"),
            ],
        )

        self.assertIsNone(result)

    def test_constant_price_across_intervals_uses_full_quantity(self):
        usage = self.usage(
            usage_start_time="2026-10-01T23:00:00Z",
            usage_end_time="2026-10-02T01:00:00Z",
            usage_quantity=Decimal("2"),
        )
        result = calculate_list_cost(
            usage,
            [
                price("0.50", end="2026-10-02T00:00:00Z"),
                price("0.50", start="2026-10-02T00:00:00Z"),
            ],
        )
        self.assertEqual(result["list_cost"], Decimal("1.00"))
        self.assertTrue(all(segment["quantity"] is None for segment in result["pricing_segments"]))

    def test_mismatched_sku_returns_no_cost(self):
        result = calculate_list_cost(
            self.usage(),
            [price("0.70", sku="OTHER_SKU")],
        )

        self.assertIsNone(result)

    
    def test_mismatched_account_returns_no_cost(self):
        mismatched_price = price("0.70")
        mismatched_price["account_id"] = "different-account"
        self.assertIsNone(calculate_list_cost(self.usage(), [mismatched_price]))

    def test_mismatched_cloud_returns_no_cost(self):
        mismatched_price = price("0.70")
        mismatched_price["cloud"] = "AZURE"
        self.assertIsNone(calculate_list_cost(self.usage(), [mismatched_price]))

    def test_mismatched_usage_unit_returns_no_cost(self):
        mismatched_price = price("0.70")
        mismatched_price["usage_unit"] = "HOUR"
        self.assertIsNone(calculate_list_cost(self.usage(), [mismatched_price]))


    def test_missing_price_returns_no_cost(self):
        result = calculate_list_cost(self.usage(), [])
        self.assertIsNone(result)

    def test_missing_required_dimension_fails_closed(self):
        incomplete_price = price("0.70")
        incomplete_price.pop("cloud")

        self.assertIsNone(calculate_list_cost(self.usage(), [incomplete_price]))

    def test_parses_databricks_json_encoded_pricing_struct(self):
        json_encoded_price = price("0.70")
        json_encoded_price["pricing"] = (
            '{"effective_list":{"default":0.700000000000000000},'
            '"default":0.70}'
        )

        result = calculate_list_cost(self.usage(), [json_encoded_price])

        self.assertIsNotNone(result)
        self.assertEqual(result["list_unit_price"], Decimal("0.700000000000000000"))

    def test_usage_gap_in_price_coverage_returns_no_cost(self):
        usage = self.usage(
            usage_start_time="2026-10-01T23:00:00Z",
            usage_end_time="2026-10-02T01:00:00Z",
        )
        prices = [
            price("0.50", end="2026-10-02T00:00:00Z"),
            price("0.70", start="2026-10-02T00:30:00Z"),
        ]

        self.assertIsNone(calculate_list_cost(usage, prices))

    def test_naive_timestamps_from_the_utc_session_are_interpreted_as_utc(self):
        usage = self.usage(
            usage_start_time=datetime(2026, 10, 2),
            usage_end_time=datetime(2026, 10, 2, 1),
        )

        result = calculate_list_cost(usage, [price("0.70")])
        self.assertIsNotNone(result)
        self.assertEqual(result["list_cost"], Decimal("1.4848423333333333331"))

    def test_enrichment_does_not_invent_other_cost_fields(self):
        enriched = enrich_with_list_cost(self.usage(), [price("0.70")])

        self.assertEqual(enriched["list_cost"], Decimal("1.4848423333333333331"))
        self.assertNotIn("contracted_cost", enriched)
        self.assertNotIn("effective_cost", enriched)
        self.assertNotIn("billed_cost", enriched)


if __name__ == "__main__":
    unittest.main()
