"""Normalize Databricks source records without losing source information."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from typing import Any


def _value(record: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in record and record[name] is not None:
            return record[name]
    return None


def _nested(value: Any, *names: str) -> Any:
    if value is None:
        return None

    if isinstance(value, str) and value.lstrip().startswith("{"):
        try:
            value = json.loads(value, parse_float=Decimal)
        except (json.JSONDecodeError, TypeError):
            return None

    if isinstance(value, Mapping):
        return _value(value, *names)

    if hasattr(value, "asDict"):
        return _nested(value.asDict(recursive=True), *names)

    for name in names:
        try:
            result = getattr(value, name)
        except AttributeError:
            continue
        if result is not None:
            return result

    return None


def _utc_timestamp(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError:
            try:
                return date.fromisoformat(text).isoformat()
            except ValueError as exc:
                raise ValueError(f"Invalid timestamp value: {value!r}") from exc

    if not isinstance(value, datetime):
        raise TypeError(f"Unsupported timestamp value: {value!r}")

    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return format(Decimal(str(value)), "f")


def _pricing_default(pricing: Any) -> Any:
    if not isinstance(pricing, Mapping):
        if hasattr(pricing, "asDict"):
            pricing = pricing.asDict(recursive=True)
        elif isinstance(pricing, str) and pricing.lstrip().startswith("{"):
            try:
                pricing = json.loads(pricing, parse_float=Decimal)
            except (json.JSONDecodeError, TypeError):
                return None
        else:
            return None

    effective = pricing.get("effective_list")
    if isinstance(effective, Mapping) and effective.get("default") is not None:
        return effective["default"]

    default = pricing.get("default")
    if default is not None:
        return default

    return None


def normalize_record(
    source_category: str,
    record: Mapping[str, Any],
    *,
    collection_time: datetime | None = None,
) -> dict[str, Any]:
    """Map one source record to the assessment's normalized structure."""
    if not isinstance(record, Mapping):
        raise TypeError("Source record must be a mapping")

    usage_metadata = record.get("usage_metadata")
    identity_metadata = record.get("identity_metadata")
    pricing = record.get("pricing")

    if collection_time is None:
        collection_time = datetime.now(timezone.utc)

    normalized_collection_time = _utc_timestamp(collection_time)

    resource_id = _value(record, "resource_id", "workload_id", "table_id")
    if resource_id is None:
        resource_id = _nested(
            usage_metadata,
            "cluster_id",
            "warehouse_id",
            "job_id",
            "dlt_pipeline_id",
            "endpoint_id",
            "table_id",
        )

    resource_name = _value(record, "resource_name", "workload_name", "table_name")
    if resource_name is None:
        resource_name = _nested(
            usage_metadata,
            "cluster_name",
            "warehouse_name",
            "job_name",
            "run_name",
            "table_name",
        )

    return {
        "platform": "Databricks",
        "billing_account_id": _value(record, "billing_account_id", "account_id"),
        "workspace_id": _value(record, "workspace_id"),
        "source_category": source_category,
        "source_record_id": _value(record, "source_record_id", "record_id"),
        "resource_or_workload_identifier": resource_id,
        "resource_or_workload_name": resource_name,
        "resource_or_workload_type": _value(
            record,
            "resource_type",
            "workload_type",
            "billing_origin_product",
        ),
        "service_or_product": _value(
            record,
            "service_or_product",
            "billing_origin_product",
        ),
        "sku": _value(record, "sku", "sku_name"),
        "usage_start": _utc_timestamp(
            _value(
                record,
                "usage_start",
                "usage_start_time",
                "start_time",
                "period_start_time",
            )
        ),
        "usage_end": _utc_timestamp(
            _value(
                record,
                "usage_end",
                "usage_end_time",
                "end_time",
                "period_end_time",
            )
        ),
        "billing_period_start": _utc_timestamp(
            _value(record, "billing_period_start", "billing_period_start_time")
        ),
        "billing_period_end": _utc_timestamp(
            _value(record, "billing_period_end", "billing_period_end_time")
        ),
        "consumed_quantity": _decimal_text(
            _value(record, "consumed_quantity", "usage_quantity", "active_usage_quantity")
        ),
        "consumed_unit": _value(record, "consumed_unit", "usage_unit"),
        "pricing_quantity": _decimal_text(_value(record, "pricing_quantity")),
        "pricing_unit": _value(record, "pricing_unit"),
        "list_unit_price": _decimal_text(
            _value(record, "list_unit_price")
            if _value(record, "list_unit_price") is not None
            else _pricing_default(pricing)
        ),
        "list_cost": _decimal_text(_value(record, "list_cost")),
        "contracted_cost": _decimal_text(_value(record, "contracted_cost")),
        "effective_cost": _decimal_text(_value(record, "effective_cost")),
        "billed_cost": _decimal_text(_value(record, "billed_cost")),
        "currency": _value(record, "currency", "currency_code"),
        "user_or_service_principal": _value(
            record,
            "user_or_service_principal",
        )
        or _nested(identity_metadata, "run_as", "executed_by", "created_by"),
        "tags_or_allocation_metadata": _value(
            record,
            "tags_or_allocation_metadata",
            "custom_tags",
            "granular_tags",
        ),
        "source_update_time": _utc_timestamp(
            _value(
                record,
                "source_update_time",
                "record_generation_time",
            )
        ),
        "collection_time": normalized_collection_time,
        "additional_source_metadata": dict(record),
    }
