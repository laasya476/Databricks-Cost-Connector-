"""Command-line entry point for the Databricks cost connector."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from connector import discovery
from connector.collectors import SOURCE_CONFIG
from connector.coverage import (
    ALLOWED_STATUSES,
    build_coverage_report,
    build_source_coverage_report,
    collect_with_status,
)
from connector.normalize import normalize_record
from connector.pricing import calculate_list_cost


NORMALIZED_SOURCES = {
    "billing_usage",
    "billing_attributed_usage",
    "billing_cloud_infra_cost",
    "storage_predictive_optimization",
    "serving_endpoint_usage",
}

_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_SENSITIVE_KEYS = {
    "access_token",
    "token",
    "password",
    "email",
    "user_email",
    "user_name",
    "username",
    "user_identity",
    "user_or_service_principal",
    "identity_metadata",
    "run_as",
    "executed_by",
    "created_by",
    "principal",
    "request_params",
    "query_text",
    "statement_text",
    "sql_text",
    "query_string",
    "http_path",
    "user",
    "owner",
    "email_address",
    "principal_id",
    "service_principal_id",
    "service_principal_name",
    "created_by_user",
    "run_by",
    "owned_by",
}

_SENSITIVE_ID_KEYS = {
    "account_id",
    "billing_account_id",
    "cloud_account_id",
    "workspace_id",
    "metastore_id",
    "record_id",
    "source_record_id",
}



def parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and return an aware UTC datetime."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "timestamp must be ISO-8601, for example 2026-10-01T00:00:00Z"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_safe(value: Any) -> Any:
    """Convert connector values to JSON-safe values without float coercion."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=timezone.utc)
        return _utc_text(value)
    if hasattr(value, "isoformat") and value.__class__.__name__ == "date":
        return value.isoformat()
    if hasattr(value, "asDict"):
        return _json_safe(value.asDict(recursive=True))
    if hasattr(value, "_asdict"):
        return _json_safe(value._asdict())
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _redact(value: Any, key: str | None = None) -> Any:
    """Redact identity, account identifiers, tokens, and query contents."""
    normalized_key = (key or "").lower()
    if normalized_key in _SENSITIVE_KEYS or any(
        term in normalized_key
        for term in ("token", "password", "secret", "credential", "authorization", "query", "statement", "sql")
    ):
        return "[REDACTED]"
    if normalized_key in _SENSITIVE_ID_KEYS or re.search(r"(?:account|workspace)[_-]?id$", normalized_key):
        return "[REDACTED]" if value is not None else None
    if hasattr(value, "asDict"):
        return _redact(value.asDict(recursive=True), key)
    if hasattr(value, "_asdict"):
        return _redact(value._asdict(), key)
    if isinstance(value, str) and value.lstrip().startswith(("{", "[")):
        try:
            parsed = json.loads(value, parse_float=Decimal)
        except (json.JSONDecodeError, TypeError):
            parsed = None
        if isinstance(parsed, (dict, list)):
            return _redact(parsed, key)
    if isinstance(value, dict):
        return {str(k): _redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        if re.search(r"(?i)\b(select|with|insert|update|delete|merge|create|alter|drop)\b", value):
            return "[REDACTED_SQL]"
        value = _EMAIL.sub("[REDACTED_EMAIL]", value)
        value = re.sub(r"(?i)\b(?:dapi|eyJ)[A-Za-z0-9._-]{20,}\b", "[REDACTED_TOKEN]", value)
        value = re.sub(r"(?i)\b(?:bearer|token|password|authorization)\s*[:=]?\s*\S+", "[REDACTED_SECRET]", value)
        return re.sub(r"(?<![\w.])\d{8,20}(?![\w.])", "[REDACTED_ID]", value)
    return _json_safe(value)


def _safe_error(value: str | None) -> str | None:
    if not value:
        return None
    return "Operation failed; detailed error text was withheld from shared output."


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_safe(value), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, records: list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(
                json.dumps(_json_safe(record), ensure_ascii=False) + "\n"
            )


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(_json_safe(value), ensure_ascii=False)
    return _json_safe(value)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0]) if rows else []
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(value) for key, value in row.items()})


def _discover_and_collect(
    start_time: datetime,
    end_time: datetime,
    batch_size: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    discovery_results = discovery.discover_source_schemas()
    results: dict[str, dict[str, Any]] = {}
    total = len(SOURCE_CONFIG)

    for index, source_name in enumerate(SOURCE_CONFIG, start=1):
        discovered = discovery_results.get(source_name, {})
        if not discovered.get("schema_available", False):
            status = discovered.get("discovery_error_status") or "failed"
            if status not in ALLOWED_STATUSES:
                status = "failed"
            result = {
                "source_name": source_name,
                "table": SOURCE_CONFIG[source_name]["table"],
                "status": status,
                "record_count": 0,
                "records": [],
                "malformed_record_count": 0,
                "error": discovered.get("error") or "Schema discovery did not confirm this source.",
            }
        else:
            result = collect_with_status(
                source_name,
                start_time=start_time,
                end_time=end_time,
                batch_size=batch_size,
            )
        results[source_name] = result
        print(
            f"[{index:02}/{total}] {source_name:<38} "
            f"{result['status']:<24} {result['record_count']:>7} records"
        )

    return discovery_results, results


def _price_usage_records(
    results: dict[str, dict[str, Any]],
    collection_time: datetime,
) -> tuple[list[dict[str, Any]], int, int]:
    usage_result = results.get("billing_usage", {})
    usage_records = usage_result.get("records", [])
    prices = results.get("billing_list_prices", {}).get("records", [])

    enriched_records = []
    list_cost_records = []
    priced_count = 0
    unpriced_count = 0

    for source_record in usage_records:
        if not isinstance(source_record, dict):
            continue
        enriched = dict(source_record)
        calculated = calculate_list_cost(source_record, prices)
        if calculated is None:
            enriched["list_unit_price"] = None
            enriched["list_cost"] = None
            enriched["list_cost_unavailable_reason"] = (
                "No source-supported complete list-price calculation was available."
            )
            unpriced_count += 1
        else:
            enriched.update(
                {
                    "list_unit_price": calculated["list_unit_price"],
                    "list_cost": calculated["list_cost"],
                    "currency_code": calculated["currency_code"]
                    or source_record.get("currency_code"),
                    "pricing_segments": calculated["pricing_segments"],
                }
            )
            list_cost_records.append(enriched)
            priced_count += 1
        enriched_records.append(enriched)

    usage_result["priced_record_count"] = priced_count
    usage_result["unpriced_record_count"] = unpriced_count
    usage_result["priced_records"] = enriched_records
    usage_result["list_cost_records"] = list_cost_records
    return enriched_records, priced_count, unpriced_count


def _make_normalized_records(
    results: dict[str, dict[str, Any]],
    priced_usage: list[dict[str, Any]],
    collection_time: datetime,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    normalized = []
    malformed_counts: Counter[str] = Counter()

    for source_name in NORMALIZED_SOURCES:
        result = results.get(source_name)
        if not result or result["status"] not in {"collected", "empty"}:
            continue

        records = (
            priced_usage
            if source_name == "billing_usage"
            else result.get("records", [])
        )
        for record in records:
            if not isinstance(record, dict):
                malformed_counts[source_name] += 1
                continue
            try:
                normalized.append(
                    normalize_record(
                        source_name,
                        record,
                        collection_time=collection_time,
                    )
                )
            except (TypeError, ValueError, ArithmeticError):
                malformed_counts[source_name] += 1

    return normalized, dict(malformed_counts)


def _redacted_identity_summary(results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    observed_accounts: set[str] = set()
    observed_workspaces: set[str] = set()
    for result in results.values():
        for record in result.get("records", []):
            if not isinstance(record, dict):
                continue
            account = record.get("account_id")
            workspace = record.get("workspace_id")
            if account:
                observed_accounts.add(str(account))
            if workspace:
                observed_workspaces.add(str(workspace))

    return {
        "account_discovered": bool(observed_accounts),
        "account_ids": ["[REDACTED]"] * len(observed_accounts),
        "workspace_discovered": bool(observed_workspaces),
        "workspace_ids": ["[REDACTED]"] * len(observed_workspaces),
    }


def run_pipeline(
    start_time: datetime,
    end_time: datetime,
    output_dir: Path,
    batch_size: int = 1000,
) -> dict[str, Any]:
    """Run auth, discovery, collection, pricing, normalization, and reporting."""
    from connector.auth import test_connection

    auth_result = test_connection()
    if not auth_result:
        raise RuntimeError("Databricks authentication check returned no result")

    collection_time = datetime.now(timezone.utc)
    try:
        broad_discovery = discovery.discover_available_system_tables()
        alert_schema_available: bool | None = "alert" in broad_discovery
    except Exception as exc:
        broad_discovery = {"_discovery_error": str(exc)}
        alert_schema_available = None

    discovery_results, results = _discover_and_collect(
        start_time,
        end_time,
        batch_size,
    )
    schema_errors = broad_discovery.get("_schema_errors", {})
    discovery_status = (
        "failed" if "_discovery_error" in broad_discovery
        else "partial" if schema_errors
        else "successful"
    )
    reconciliation = discovery.reconcile_system_tables(broad_discovery, results)
    commercial_candidate_discovered = any(
        row["table"]
        and any(
            term in f"{row['schema']}.{row['table']}".lower()
            for term in ("contract", "commitment", "discount")
        )
        for row in reconciliation
    )

    priced_usage, priced_count, unpriced_count = _price_usage_records(
        results,
        collection_time,
    )
    normalized_records, malformed_normalized = _make_normalized_records(
        results,
        priced_usage,
        collection_time,
    )
    source_report = build_source_coverage_report(results)
    category_report = build_coverage_report(
        results,
        alert_schema_available=alert_schema_available,
        commercial_discovery_status=discovery_status,
        commercial_candidate_discovered=commercial_candidate_discovered,
    )
    safe_source_report = source_report
    safe_category_report = category_report

    raw_dir = output_dir / "raw"
    normalized_dir = output_dir / "normalized"
    samples_dir = output_dir / "samples"
    for directory in (raw_dir, normalized_dir, samples_dir):
        directory.mkdir(parents=True, exist_ok=True)

    for source_name, result in results.items():
        _write_jsonl(raw_dir / f"{source_name}.jsonl", result.get("records", []))

    _write_jsonl(normalized_dir / "normalized.jsonl", normalized_records)
    _write_json(output_dir / "coverage_report.json", safe_category_report)
    _write_csv(output_dir / "coverage_report.csv", safe_category_report)
    _write_json(output_dir / "source_coverage_report.json", safe_source_report)
    _write_csv(output_dir / "source_coverage_report.csv", safe_source_report)
    _write_json(output_dir / "discovery_reconciliation.json", reconciliation)

    sample_raw = {
        "billing_usage": results.get("billing_usage", {}).get("records", [])[:3],
        "billing_list_prices": results.get("billing_list_prices", {}).get("records", [])[:3],
        "storage_table_metrics": results.get("storage_table_metrics", {}).get("records", [])[:3],
    }
    _write_json(samples_dir / "raw_sample_redacted.json", _redact(sample_raw))
    _write_json(
        samples_dir / "normalized_sample_redacted.json",
        _redact(normalized_records[:5]),
    )

    source_evidence = safe_source_report
    category_evidence = safe_category_report

    evidence = {
        "connector": "Databricks Cost Connector",
        "execution_time_utc": _utc_text(collection_time),
        "authentication": {"status": "successful"},
        "account_workspace_discovery": _redacted_identity_summary(results),
        "collection_window": {
            "start_utc": _utc_text(start_time),
            "end_utc": _utc_text(end_time),
        },
        "source_count_investigated": len(source_report),
        "category_count_investigated": len(category_report),
        "broad_system_discovery": {
            "status": discovery_status,
            "schemas": sorted(
                key for key in broad_discovery if not key.startswith("_")
            ),
            "alert_schema_present": alert_schema_available,
            "tables_by_schema": {
                schema: [
                    row["table"] for row in reconciliation
                    if row["schema"] == schema and row["table"]
                ]
                for schema in broad_discovery if not schema.startswith("_")
            },
            "schema_listing_failures": sorted(schema_errors),
            "error": (
                _safe_error(broad_discovery.get("_discovery_error"))
                if "_discovery_error" in broad_discovery
                else None
            ),
        },
        "discovery_reconciliation": reconciliation,
        "pricing": {
            "list_cost_records_calculated": priced_count,
            "usage_records_without_applicable_price": unpriced_count,
        },
        "normalization": {
            "normalized_record_count": len(normalized_records),
            "records_skipped_by_source": malformed_normalized,
        },
        "source_results": source_evidence,
        "category_results": category_evidence,
        "redaction": "Account/workspace IDs, emails, user identities, tokens, and query contents are redacted in evidence and sample files. Raw JSONL exports are private and may contain identifying data.",
    }
    _write_json(output_dir / "live_execution_evidence_redacted.json", evidence)

    status_counts = Counter(result["status"] for result in results.values())
    print("\nCollection finished")
    print(f"Authentication: successful")
    print(f"Sources: {len(results)}; categories: {len(category_report)}")
    print(f"List costs calculated: {priced_count}; unmatched usage rows: {unpriced_count}")
    print(f"Normalized records: {len(normalized_records)}")
    print("Source statuses: " + ", ".join(f"{key}={status_counts[key]}" for key in sorted(status_counts)))
    print(f"Outputs: {output_dir.resolve()}")

    return {
        "results": results,
        "source_report": source_report,
        "category_report": category_report,
        "normalized_record_count": len(normalized_records),
        "priced_record_count": priced_count,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover and collect Databricks cost-management data."
    )
    parser.add_argument(
        "--start",
        required=True,
        type=parse_datetime,
        help="Inclusive start timestamp with timezone, e.g. 2026-10-01T00:00:00Z",
    )
    parser.add_argument(
        "--end",
        type=parse_datetime,
        default=datetime.now(timezone.utc),
        help="Exclusive end timestamp with timezone (default: now)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs"),
        help="Output directory (default: outputs)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows fetched per batch (default: 1000)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.start >= args.end:
        parser.error("--start must be earlier than --end")
    if args.batch_size < 1:
        parser.error("--batch-size must be a positive integer")

    try:
        run_pipeline(args.start, args.end, args.output, args.batch_size)
    except Exception as exc:
        print(f"Connector failed: {_safe_error(str(exc))}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
