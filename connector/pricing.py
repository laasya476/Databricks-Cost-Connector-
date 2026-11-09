"""Time-aware Databricks list-price matching and cost calculation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from typing import Any


def _get(record: Any, *names: str) -> Any:
    if isinstance(record, Mapping):
        for name in names:
            if name in record and record[name] is not None:
                return record[name]

    if hasattr(record, "asDict"):
        return _get(record.asDict(recursive=True), *names)

    if isinstance(record, str) and record.lstrip().startswith("{"):
        try:
            parsed = json.loads(record, parse_float=Decimal)
        except (json.JSONDecodeError, TypeError):
            parsed = None
        if isinstance(parsed, Mapping):
            return _get(parsed, *names)

    for name in names:
        try:
            value = getattr(record, name)
        except AttributeError:
            continue
        if value is not None:
            return value

    return None


def _as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:

        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _pricing_value(price_record: Any) -> Decimal | None:
    pricing = _get(price_record, "pricing")
    if pricing is None:
        return _as_decimal(_get(price_record, "list_unit_price"))

    effective_list = _get(pricing, "effective_list")
    effective_default = _get(effective_list, "default")
    if effective_default is not None:
        return _as_decimal(effective_default)

    default = _get(pricing, "default")
    if default is not None:
        return _as_decimal(default)

    promotional = _get(pricing, "promotional")
    return _as_decimal(_get(promotional, "default"))


def _matches_dimensions(usage: Any, price: Any) -> bool:
    for usage_name, price_name in (
        ("account_id", "account_id"),
        ("sku_name", "sku_name"),
        ("cloud", "cloud"),
        ("usage_unit", "usage_unit"),
    ):
        usage_value = _get(usage, usage_name)
        price_value = _get(price, price_name)
        if usage_value is None or price_value is None:
            return False
        if str(usage_value) != str(price_value):
            return False
    return True


def _covers(price: Any, instant: datetime) -> bool:
    start = _as_datetime(_get(price, "price_start_time"))
    end = _as_datetime(_get(price, "price_end_time"))
    if start is None or instant < start:
        return False
    return end is None or instant < end


def _matching_prices(usage: Any, price_records: Sequence[Any]) -> list[Any]:
    prices = [
        price
        for price in price_records
        if _matches_dimensions(usage, price)
        and _pricing_value(price) is not None
        and _as_datetime(_get(price, "price_start_time")) is not None
    ]
    return sorted(
        prices,
        key=lambda price: _as_datetime(_get(price, "price_start_time")),
    )


def _price_at(prices: Sequence[Any], instant: datetime) -> Any | None:
    matches = [price for price in prices if _covers(price, instant)]
    return matches[-1] if matches else None


def calculate_list_cost(
    usage_record: Mapping[str, Any],
    price_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Calculate list cost using the applicable time/SKU/cloud/unit price.

    Returns ``None`` when the usage is missing a quantity or no complete price
    coverage exists. No contracted, effective, or billed cost is inferred.
    A usage interval with differing prices is not costed because this source
    record has no quantity split by price period.
    """
    quantity = _as_decimal(_get(usage_record, "usage_quantity", "consumed_quantity"))
    start = _as_datetime(
        _get(usage_record, "usage_start_time", "usage_start", "start_time")
    )
    end = _as_datetime(
        _get(usage_record, "usage_end_time", "usage_end", "end_time")
    ) or start

    if quantity is None or start is None or end is None or end < start:
        return None

    prices = _matching_prices(usage_record, price_records)
    if not prices:
        return None

    if end == start:
        price = _price_at(prices, start)
        if price is None:
            return None
        unit_price = _pricing_value(price)
        return {
            "list_unit_price": unit_price,
            "list_cost": quantity * unit_price,
            "currency_code": _get(price, "currency_code"),
            "pricing_segments": [
                {
                    "price_start_time": _get(price, "price_start_time"),
                    "price_end_time": _get(price, "price_end_time"),
                    "quantity": quantity,
                    "unit_price": unit_price,
                }
            ],
        }

    boundaries = {start, end}
    for price in prices:
        price_start = _as_datetime(_get(price, "price_start_time"))
        price_end = _as_datetime(_get(price, "price_end_time"))
        if price_start and start < price_start < end:
            boundaries.add(price_start)
        if price_end and start < price_end < end:
            boundaries.add(price_end)

    ordered_boundaries = sorted(boundaries)
    segments = []
    currencies = set()

    for segment_start, segment_end in zip(
        ordered_boundaries,
        ordered_boundaries[1:],
    ):
        midpoint = segment_start + (segment_end - segment_start) / 2
        price = _price_at(prices, midpoint)
        if price is None:
            return None

        unit_price = _pricing_value(price)
        currencies.add(_get(price, "currency_code"))
        segments.append(
            {
                "price_start_time": _get(price, "price_start_time"),
                "price_end_time": _get(price, "price_end_time"),
                "quantity": None,
                "unit_price": unit_price,
            }
        )

    unit_prices = {segment["unit_price"] for segment in segments}
    if len(unit_prices) != 1 or len(currencies) != 1 or None in currencies:
        return None
    return {
        "list_unit_price": next(iter(unit_prices)),
        "list_cost": quantity * next(iter(unit_prices)),
        "currency_code": next(iter(currencies)),
        "pricing_segments": segments,
    }


def enrich_with_list_cost(
    usage_record: Mapping[str, Any],
    price_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return a copy of a usage record enriched with calculated list cost."""
    enriched = dict(usage_record)
    result = calculate_list_cost(usage_record, price_records)
    if result is None:
        enriched["list_unit_price"] = None
        enriched["list_cost"] = None
        return enriched

    enriched.update(
        {
            "list_unit_price": result["list_unit_price"],
            "list_cost": result["list_cost"],
            "currency_code": result["currency_code"],
            "pricing_segments": result["pricing_segments"],
        }
    )
    return enriched
