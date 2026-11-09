"""Collection status and assessment coverage reporting."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from typing import Any

from connector.collectors import PartialCollectionError, SOURCE_CONFIG, collect_source


ALLOWED_STATUSES = frozenset(
    {
        "collected",
        "empty",
        "permission_denied",
        "unavailable_in_account",
        "unsupported",
        "failed",
    }
)


CATEGORY_CONFIG: tuple[dict[str, Any], ...] = (
    {
        "key": "billing_and_usage",
        "label": "Billing and usage",
        "sources": ("billing_usage",),
        "purpose": "Collect billable usage records and their source metadata.",
        "required_permission": "SELECT on system.billing.usage",
    },
    {
        "key": "account_and_workspace_consumption",
        "label": "Account and workspace consumption",
        "sources": ("billing_usage", "billing_cloud_infra_cost", "workspaces_latest"),
        "purpose": "Attribute usage and infrastructure cost to accounts and workspaces.",
        "required_permission": "Billing-table access and workspace metadata access",
    },
    {
        "key": "cluster_and_compute_consumption",
        "label": "Cluster and compute consumption",
        "sources": (
            "billing_usage",
            "compute_clusters",
            "compute_instance_events",
            "compute_instance_pools",
            "compute_node_timeline",
            "compute_node_types",
        ),
        "purpose": "Collect compute usage and cluster or instance context.",
        "required_permission": "SELECT on billing and compute system tables",
    },
    {
        "key": "sql_warehouse_consumption",
        "label": "SQL warehouse consumption",
        "sources": ("billing_usage", "compute_warehouses", "compute_warehouse_events"),
        "purpose": "Collect warehouse usage, configuration, and lifecycle events.",
        "required_permission": "SELECT on billing and compute system tables",
    },
    {
        "key": "jobs_and_pipeline_consumption",
        "label": "Jobs and pipeline consumption",
        "sources": (
            "billing_usage",
            "lakeflow_jobs",
            "lakeflow_job_tasks",
            "lakeflow_job_runs",
            "lakeflow_task_runs",
            "lakeflow_pipelines",
            "lakeflow_pipeline_updates",
        ),
        "purpose": "Attribute usage to jobs, tasks, pipelines, and their runs.",
        "required_permission": "SELECT on billing and Lakeflow system tables",
    },
    {
        "key": "serverless_consumption",
        "label": "Serverless consumption",
        "sources": ("billing_usage", "compute_warehouses", "lakeflow_pipelines"),
        "purpose": "Identify serverless usage and related workload metadata.",
        "required_permission": "SELECT on billing, compute, and Lakeflow system tables",
    },
    {
        "key": "storage_related_usage",
        "label": "Storage-related usage",
        "sources": (
            "billing_usage",
            "storage_table_metrics",
            "storage_predictive_optimization",
            "storage_auto_upgrade",
        ),
        "purpose": "Collect storage usage, table metrics, and storage optimization activity.",
        "required_permission": "SELECT on billing and storage system tables",
    },
    {
        "key": "data_transfer_and_network_usage",
        "label": "Data-transfer or network-related usage",
        "sources": ("billing_usage", "network_inbound", "network_outbound"),
        "purpose": "Collect network usage and transfer-related metadata.",
        "required_permission": "SELECT on billing and access/network system tables",
    },
    {
        "key": "sku_and_pricing",
        "label": "SKU and pricing information",
        "sources": ("billing_list_prices", "billing_account_prices"),
        "purpose": "Collect historical list and account-specific SKU prices.",
        "required_permission": "SELECT on billing pricing system tables",
    },
    {
        "key": "list_cost_calculations",
        "label": "List-cost calculations",
        "sources": ("billing_usage", "billing_list_prices", "billing_account_prices"),
        "purpose": "Join usage quantities to applicable prices using SKU, unit, cloud, and time.",
        "required_permission": "SELECT on usage and pricing system tables",
    },
    {
        "key": "resource_and_workload_attribution",
        "label": "Resource and workload attribution",
        "sources": (
            "billing_usage",
            "billing_attributed_usage",
            "query_history",
            "lakeflow_job_runs",
            "lakeflow_task_runs",
        ),
        "purpose": "Attribute usage to resources, queries, jobs, tasks, and workloads.",
        "required_permission": "SELECT on billing, query, and Lakeflow system tables",
    },
    {
        "key": "user_and_service_principal_attribution",
        "label": "User and service-principal attribution",
        "sources": (
            "billing_usage",
            "billing_attributed_usage",
            "query_history",
            "lakeflow_jobs",
            "lakeflow_job_runs",
        ),
        "purpose": "Collect identity metadata associated with usage and workloads.",
        "required_permission": "SELECT on identity-bearing billing and workload tables",
    },
    {
        "key": "tags_and_allocation_metadata",
        "label": "Tags and allocation metadata",
        "sources": (
            "billing_usage",
            "billing_attributed_usage",
            "compute_clusters",
            "compute_warehouses",
            "lakeflow_jobs",
            "lakeflow_pipelines",
        ),
        "purpose": "Collect custom tags, granular tags, and allocation metadata.",
        "required_permission": "SELECT on billing, compute, and Lakeflow system tables",
    },
    {
        "key": "budgets_alerts_and_cost_controls",
        "label": "Budgets, alerts, and cost controls",
        "sources": ("billing_usage",),
        "purpose": "Collect cost-control references such as budget policy identifiers exposed in usage metadata.",
        "required_permission": "Billing-table access; alert or budget permissions where configured",
    },
    {
        "key": "corrections_retractions_and_restatements",
        "label": "Corrections, retractions, or restatements",
        "sources": ("billing_usage",),
        "purpose": "Collect billing record types identifying original, retracted, or restated records.",
        "required_permission": "SELECT on system.billing.usage",
    },
    {
        "key": "optimization_and_cost_saving",
        "label": "Optimization and cost-saving information",
        "sources": (
            "compute_clusters",
            "compute_instance_pools",
            "compute_warehouses",
            "compute_warehouse_events",
            "compute_node_timeline",
            "storage_table_metrics",
            "storage_predictive_optimization",
            "storage_auto_upgrade",
        ),
        "purpose": "Collect configuration and activity that helps explain optimization opportunities.",
        "required_permission": "SELECT on compute and storage system tables",
    },
    {
        "key": "contract_commitment_and_discount",
        "label": "Contract, commitment, or discount information",
        "sources": ("billing_list_prices", "billing_account_prices"),
        "purpose": "Collect available account-specific or promotional pricing without inventing commercial contract terms.",
        "required_permission": "SELECT on billing pricing tables; contract capability if exposed",
    },
)


_TIMESTAMP_FIELDS = (
    "usage_start_time",
    "start_time",
    "event_time",
    "period_start_time",
    "price_start_time",
    "change_time",
    "create_time",
    "record_generation_time",
    "snapshot_date",
    "usage_date",
    "end_time",
    "usage_end_time",
    "period_end_time",
)


def _classify_error(exc: Exception) -> str:
    message = str(exc).lower()

    if any(
        marker in message
        for marker in (
            "permission",
            "not authorized",
            "unauthorized",
            "access denied",
            "insufficient privilege",
            "authentication",
            "invalid token",
            "credential",
            "401",
            "403",
        )
    ):
        return "permission_denied"

    if any(
        marker in message
        for marker in (
            "schema_not_found",
            "table_or_view_not_found",
            "table not found",
            "cannot be found",
            "does not exist",
        )
    ):
        return "unavailable_in_account"

    if any(
        marker in message
        for marker in ("unsupported", "not supported", "feature_not_available")
    ):
        return "unsupported"

    return "failed"


def _source_table(source_name: str) -> str:
    return str(SOURCE_CONFIG.get(source_name, {}).get("table", ""))


def collect_with_status(
    source_name: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    batch_size: int = 1000,
) -> dict[str, Any]:
    """Collect one source and return source-level coverage information."""
    if source_name not in SOURCE_CONFIG:
        return {
            "source_name": source_name,
            "table": "",
            "status": "unavailable_in_account",
            "record_count": 0,
            "records": [],
            "malformed_record_count": 0,
            "error": "Source is not configured.",
        }

    try:
        raw_records = collect_source(
            source_name,
            start_time=start_time,
            end_time=end_time,
            batch_size=batch_size,
        )

        if raw_records is None:
            raise TypeError("Collector returned None instead of records")

        records = list(raw_records)
        malformed_count = sum(
            1 for record in records if not isinstance(record, Mapping)
        )

        return {
            "source_name": source_name,
            "table": _source_table(source_name),
            "status": "collected" if records else "empty",
            "record_count": len(records),
            "records": records,
            "malformed_record_count": malformed_count,
            "error": None,
        }

    except PartialCollectionError as exc:
        return {
            "source_name": source_name,
            "table": _source_table(source_name),
            "status": "failed",
            "record_count": 0,
            "partial_record_count": exc.retrieved_count,
            "records": [],
            "malformed_record_count": 0,
            "error": f"Batch retrieval failed after {exc.retrieved_count} row(s): {exc.cause}",
        }
    except Exception as exc:
        return {
            "source_name": source_name,
            "table": _source_table(source_name),
            "status": _classify_error(exc),
            "record_count": 0,
            "records": [],
            "malformed_record_count": 0,
            "error": str(exc),
        }


def collect_all_with_status(
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    batch_size: int = 1000,
) -> dict[str, dict[str, Any]]:
    """Collect every configured source without stopping on failures."""
    return {
        source_name: collect_with_status(
            source_name=source_name,
            start_time=start_time,
            end_time=end_time,
            batch_size=batch_size,
        )
        for source_name in SOURCE_CONFIG
    }


def _timestamp_value(value: Any) -> tuple[str, str] | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        display = value.isoformat().replace("+00:00", "Z")
        return display, display

    if isinstance(value, date):
        display = value.isoformat()
        return display, display

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        candidate = text[:-1] + "+00:00" if text.endswith("Z") else text

        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            try:
                parsed_date = date.fromisoformat(text)
            except ValueError:
                return None
            display = parsed_date.isoformat()
            return display, display

        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        display = parsed.isoformat().replace("+00:00", "Z")

        return display, display

    return None


def _record_timestamps(
    records: list[Any],
) -> tuple[str | None, str | None, int, int]:
    values: list[tuple[str, str]] = []
    malformed_count = 0
    invalid_timestamp_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            malformed_count += 1
            continue

        for field in _TIMESTAMP_FIELDS:
            if field not in record or record[field] is None:
                continue

            parsed = _timestamp_value(record[field])
            if parsed is None:
                invalid_timestamp_count += 1
            else:
                values.append(parsed)
            break

    if not values:
        return None, None, malformed_count, invalid_timestamp_count

    values.sort(key=lambda item: item[0])
    return values[0][1], values[-1][1], malformed_count, invalid_timestamp_count


def _category_status(source_results: list[dict[str, Any]]) -> str:
    statuses = [result["status"] for result in source_results]

    if any(status == "collected" for status in statuses):
        return "collected"
    if statuses and all(status == "empty" for status in statuses):
        return "empty"

    for status in (
        "permission_denied",
        "unavailable_in_account",
        "unsupported",
        "failed",
        "empty",
    ):
        if status in statuses:
            return status

    return "unavailable_in_account"


def _named_values(value: Any):
    """Yield nested field names and values from mappings and Databricks Rows."""
    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)
    elif hasattr(value, "_asdict"):
        value = value._asdict()
    elif isinstance(value, str) and value.lstrip().startswith("{"):
        try:
            value = json.loads(value, parse_float=Decimal)
        except (json.JSONDecodeError, TypeError):
            return

    if isinstance(value, Mapping):
        for key, nested in value.items():
            yield str(key).lower(), nested
            yield from _named_values(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _named_values(nested)


def _nonempty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"null", "none", "false"}:
            return False
        if text.startswith(("{", "[")):
            try:
                return _nonempty(json.loads(text, parse_float=Decimal))
            except (json.JSONDecodeError, TypeError):
                pass
        return True
    if isinstance(value, (Mapping, list, tuple, set)):
        return bool(value)
    return True


def _has_any_field(record: Mapping[str, Any], names: set[str]) -> bool:
    return any(
        name in names and _nonempty(value)
        for name, value in _named_values(record)
    )


def _contains_term(value: Any, terms: tuple[str, ...]) -> bool:
    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)
    elif hasattr(value, "_asdict"):
        value = value._asdict()
    elif isinstance(value, str) and value.lstrip().startswith("{"):
        try:
            value = json.loads(value, parse_float=Decimal)
        except (json.JSONDecodeError, TypeError):
            pass

    if isinstance(value, Mapping):
        return any(
            any(term in str(key).lower() for term in terms)
            or _contains_term(nested, terms)
            for key, nested in value.items()
        )
    if isinstance(value, (list, tuple, set)):
        return any(_contains_term(nested, terms) for nested in value)
    return any(term in str(value).lower() for term in terms) if value is not None else False


def _fields_contain_terms(
    record: Mapping[str, Any],
    field_names: set[str],
    terms: tuple[str, ...],
) -> bool:
    return any(
        name in field_names and _contains_term(value, terms)
        for name, value in _named_values(record)
    )


_EVIDENCE_FILTERS = {
    "cluster_and_compute_consumption": "No cluster-specific usage or compute activity was present.",
    "sql_warehouse_consumption": "No warehouse-specific usage was present.",
    "jobs_and_pipeline_consumption": "No job, task, or pipeline run usage/activity was present.",
    "serverless_consumption": "No serverless-specific usage was identified.",
    "storage_related_usage": "No storage-specific usage, storage metrics, or storage operation records were present.",
    "resource_and_workload_attribution": "No resource or workload identifiers/attribution were present.",
    "user_and_service_principal_attribution": "No user or service-principal identity fields were populated.",
    "tags_and_allocation_metadata": "No non-empty tags or allocation metadata were present.",
    "corrections_retractions_and_restatements": "No retraction or restatement billing records were present.",
}


def _category_evidence_records(
    category_key: str,
    source_results: list[dict[str, Any]],
) -> list[Mapping[str, Any]]:
    """Return only records that substantiate the category, not broad usage rows."""
    relevant: list[Mapping[str, Any]] = []
    run_sources = {
        "lakeflow_job_runs",
        "lakeflow_task_runs",
        "lakeflow_pipeline_updates",
    }
    attribution_sources = {
        "billing_attributed_usage",
        "query_history",
        "lakeflow_job_runs",
        "lakeflow_task_runs",
    }

    for result in source_results:
        source_name = result.get("source_name")
        for record in result.get("records", []):
            if not isinstance(record, Mapping):
                continue

            matched = False
            if category_key == "cluster_and_compute_consumption":
                matched = source_name in {"compute_node_timeline", "compute_instance_events"} or (
                    source_name == "billing_usage"
                    and (
                        _has_any_field(record, {"cluster_id", "instance_pool_id"})
                        or _fields_contain_terms(
                            record,
                            {"sku_name", "usage_type", "billing_origin_product"},
                            ("compute",),
                        )
                    )
                )
            elif category_key == "sql_warehouse_consumption":
                matched = source_name == "billing_usage" and (
                    _has_any_field(record, {"warehouse_id"})
                    or _fields_contain_terms(
                        record,
                        {"sku_name", "usage_type", "billing_origin_product"},
                        ("sql", "warehouse"),
                    )
                )
            elif category_key == "jobs_and_pipeline_consumption":
                matched = source_name in run_sources or (
                    source_name == "billing_usage"
                    and _has_any_field(
                        record,
                        {
                            "job_id",
                            "job_run_id",
                            "dlt_pipeline_id",
                            "dlt_update_id",
                            "pipeline_id",
                        },
                    )
                )
            elif category_key == "serverless_consumption":
                matched = source_name == "billing_usage" and _fields_contain_terms(
                    record,
                    {"sku_name", "usage_type", "billing_origin_product"},
                    ("serverless",),
                )
            elif category_key == "storage_related_usage":
                matched = source_name in {
                    "storage_table_metrics",
                    "storage_predictive_optimization",
                    "storage_auto_upgrade",
                } or (
                    source_name == "billing_usage"
                    and (
                        _fields_contain_terms(
                            record,
                            {"sku_name", "usage_type", "billing_origin_product"},
                            ("storage",),
                        )
                        or _has_any_field(
                            record,
                            {"storage_api_type", "snapshot_id", "uc_table_name"},
                        )
                    )
                )
            elif category_key == "resource_and_workload_attribution":
                matched = source_name in attribution_sources or (
                    source_name == "billing_usage"
                    and _has_any_field(
                        record,
                        {
                            "cluster_id",
                            "warehouse_id",
                            "job_id",
                            "job_run_id",
                            "notebook_id",
                            "dlt_pipeline_id",
                            "endpoint_id",
                        },
                    )
                )
            elif category_key == "user_and_service_principal_attribution":
                matched = _has_any_field(
                    record,
                    {
                        "identity_metadata",
                        "executed_by_user_id",
                        "user_identity",
                        "run_as",
                        "run_by",
                        "created_by",
                        "owned_by",
                        "user_id",
                        "service_principal_id",
                    },
                )
            elif category_key == "tags_and_allocation_metadata":
                matched = _has_any_field(
                    record,
                    {"custom_tags", "granular_tags", "tags", "tagging", "allocation_metadata"},
                )
            elif category_key == "corrections_retractions_and_restatements":
                matched = any(
                    name == "record_type"
                    and str(value).strip().upper()
                    in {"RETRACTION", "RETRACTED", "RESTATEMENT", "RESTATED"}
                    for name, value in _named_values(record)
                )

            if matched:
                relevant.append(record)

    return relevant


def _category_explanation(
    category: dict[str, Any],
    source_results: list[dict[str, Any]],
    status: str,
    malformed_count: int,
    invalid_timestamp_count: int,
    alert_schema_available: bool | None = None,
) -> str | None:
    messages: list[str] = []

    if status == "empty":
        messages.append("No records were returned for the requested time range.")

    for result in source_results:
        if result.get("error"):
            messages.append(
                f"{result['source_name']}: {result['status']}; detailed error text withheld."
            )

    if malformed_count:
        messages.append(f"Ignored {malformed_count} malformed source record(s) when reading timestamps.")

    if invalid_timestamp_count:
        messages.append(f"Ignored {invalid_timestamp_count} invalid source timestamp value(s).")

    if category["key"] == "optimization_and_cost_saving" and status == "collected":
        populated_tables = [
            result.get("table")
            for result in source_results
            if result.get("record_count", 0) > 0 and result.get("table")
        ]
        if populated_tables:
            messages.append(
                "Optimization-related configuration or operational history was collected from: "
                + ", ".join(populated_tables)
                + "."
            )

    if category["key"] == "budgets_alerts_and_cost_controls":
        if status == "empty":
            if alert_schema_available is False:
                messages.append(
                    "No budget_policy_id was present in the collected usage records; no system.alert schema was visible to this connection."
                )
            elif alert_schema_available is True:
                messages.append(
                    "No budget_policy_id was present in the collected usage records; broad discovery found a system.alert schema, but no alert dataset is configured for collection."
                )
            else:
                messages.append(
                    "No budget_policy_id was present in the collected usage records; the presence of a dedicated system.alert schema was not established by this run."
                )

    if not messages and status == "collected":
        return "Records collected successfully."

    return " ".join(messages) if messages else None


def build_source_coverage_report(
    results: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build one diagnostic row per configured source."""
    report = []

    for source_name, result in results.items():
        records = result.get("records", [])
        if not isinstance(records, list):
            records = list(records) if records else []

        earliest, latest, malformed, invalid = _record_timestamps(records)
        status = result.get("status", "failed")
        if status not in ALLOWED_STATUSES:
            status = "failed"

        explanation_parts = []
        if result.get("error"):
            explanation_parts.append(f"{status}: detailed error text withheld.")
        if result.get("partial_record_count", 0):
            explanation_parts.append(
                f"Retrieved {result['partial_record_count']} row(s) before failure; no partial rows were exported."
            )
        if malformed:
            explanation_parts.append(f"Ignored {malformed} malformed record(s).")
        if invalid:
            explanation_parts.append(f"Ignored {invalid} invalid timestamp value(s).")
        if status == "empty":
            explanation_parts.append("No records were returned for the requested time range.")

        report.append(
            {
                "source_name": source_name,
                "dataset": result.get("table", _source_table(source_name)),
                "status": status,
                "record_count": int(result.get("record_count", len(records))),
                "partial_record_count": int(result.get("partial_record_count", 0)),
                "earliest_timestamp": earliest,
                "latest_timestamp": latest,
                "required_permission": "See source-specific Databricks system-table permissions.",
                "explanation": " ".join(explanation_parts) or None,
            }
        )

    return report


def build_coverage_report(
    results: dict[str, dict[str, Any]],
    *,
    alert_schema_available: bool | None = None,
    commercial_discovery_status: str | None = None,
    commercial_candidate_discovered: bool = False,
) -> list[dict[str, Any]]:
    """Build the 17 category rows required by the assessment."""
    report: list[dict[str, Any]] = []

    for category in CATEGORY_CONFIG:
        source_results = [
            results[source_name]
            for source_name in category["sources"]
            if source_name in results
        ]

        status = _category_status(source_results)
        records: list[Any] = []

        for result in source_results:
            source_records = result.get("records", [])
            if isinstance(source_records, list):
                records.extend(source_records)

        earliest, latest, malformed, invalid = _record_timestamps(records)
        record_count = sum(
            int(result.get("record_count", len(result.get("records", []))))
            for result in source_results
        )

        explanation = _category_explanation(
            category,
            source_results,
            status,
            malformed,
            invalid,
            alert_schema_available,
        )

        if category["key"] in _EVIDENCE_FILTERS:
            evidence_records = _category_evidence_records(
                category["key"],
                source_results,
            )
            if evidence_records:
                status = "collected"
                record_count = len(evidence_records)
                earliest, latest, malformed, invalid = _record_timestamps(
                    evidence_records
                )
                explanation = (
                    f"Collected category-specific evidence on {len(evidence_records)} record(s)."
                )
            else:
                unsuccessful = [
                    result["status"]
                    for result in source_results
                    if result["status"] not in {"collected", "empty"}
                ]
                if unsuccessful:
                    for candidate in (
                        "permission_denied",
                        "unavailable_in_account",
                        "unsupported",
                        "failed",
                    ):
                        if candidate in unsuccessful:
                            status = candidate
                            break
                else:
                    status = "empty"
                record_count = 0
                earliest = latest = None
                explanation = _EVIDENCE_FILTERS[category["key"]]

        elif category["key"] == "budgets_alerts_and_cost_controls":
            usage_records = [
                record
                for result in source_results
                for record in result.get("records", [])
                if isinstance(record, Mapping)
            ]
            policy_records = []
            for record in usage_records:
                metadata = record.get("usage_metadata")
                if hasattr(metadata, "asDict"):
                    metadata = metadata.asDict(recursive=True)
                elif isinstance(metadata, str) and metadata.lstrip().startswith("{"):
                    try:
                        metadata = json.loads(metadata, parse_float=Decimal)
                    except (json.JSONDecodeError, TypeError):
                        metadata = None
                if isinstance(metadata, Mapping) and metadata.get("budget_policy_id"):
                    policy_records.append(record)

            if policy_records:
                status = "collected"
                record_count = len(policy_records)
                earliest, latest, malformed, invalid = _record_timestamps(policy_records)
                explanation = (
                    f"Collected budget_policy_id references on {len(policy_records)} usage record(s); "
                    + (
                        "no system.alert schema was visible to this connection."
                        if alert_schema_available is False
                        else "broad discovery found a system.alert schema, but no alert dataset is configured for collection."
                        if alert_schema_available is True
                        else "the presence of a dedicated system.alert schema was not established by this run."
                    )
                )
            elif source_results and all(
                result["status"] in {"collected", "empty"}
                for result in source_results
            ):
                status = "empty"
                record_count = 0
                earliest = latest = None
                explanation = (
                    "No budget_policy_id was present in the collected usage records; "
                    + (
                        "no system.alert schema was visible to this connection."
                        if alert_schema_available is False
                        else "broad discovery found a system.alert schema, but no alert dataset is configured for collection."
                        if alert_schema_available is True
                        else "the presence of a dedicated system.alert schema was not established by this run."
                    )
                )

        elif category["key"] == "data_transfer_and_network_usage":
            network_records = []
            for result in source_results:
                for record in result.get("records", []):
                    if not isinstance(record, Mapping):
                        continue
                    usage_type = str(record.get("usage_type") or "").upper()
                    if usage_type.startswith("NETWORK_") or result["source_name"] in {
                        "network_inbound",
                        "network_outbound",
                    }:
                        network_records.append(record)

            if network_records:
                status = "collected"
                record_count = len(network_records)
                earliest, latest, malformed, invalid = _record_timestamps(network_records)
                explanation = "Collected network-related usage or network event records."
            elif source_results and all(
                result["status"] in {"collected", "empty"}
                for result in source_results
            ):
                status = "empty"
                record_count = 0
                earliest = latest = None
                explanation = (
                    "Sources were queried successfully, but no network-specific usage or events "
                    "were present in the requested time range."
                )

        elif category["key"] == "list_cost_calculations":

            calculation_records: list[Any] = []
            for result in source_results:
                if result.get("source_name") == "billing_usage":
                    calculation_records.extend(result.get("list_cost_records", []))

            priced_count = sum(
                int(result.get("priced_record_count", 0))
                for result in source_results
            )
            if priced_count:
                status = "collected"
                record_count = priced_count
                earliest, latest, malformed, invalid = _record_timestamps(
                    calculation_records
                )
                explanation = f"Calculated list cost for {priced_count} usage record(s) using time-matched list prices."
            elif source_results and all(
                result["status"] in {"empty", "collected"}
                for result in source_results
            ):
                has_usage = any(
                    result["source_name"] == "billing_usage"
                    and result.get("record_count", 0) > 0
                    for result in source_results
                )
                status = "failed" if has_usage else "empty"
                record_count = 0
                explanation = (
                    "Usage was collected, but no source-supported complete list-price calculation was available."
                    if has_usage
                    else "No usage records were available to calculate list cost."
                )

        elif category["key"] == "contract_commitment_and_discount":
            source_statuses = {result["status"] for result in source_results}
            if "permission_denied" in source_statuses:
                status = "permission_denied"
            elif commercial_discovery_status != "successful":
                status = "failed"
            else:
                status = "unsupported"
            record_count = 0
            earliest = latest = None
            if status == "permission_denied":
                explanation = "Access to account pricing was denied; commercial terms could not be assessed."
            elif status == "failed":
                explanation = "Broad commercial-source discovery did not complete."
            elif commercial_candidate_discovered:
                explanation = "A potential commercial-terms table was discovered, but no validated collector is configured."
            else:
                explanation = "Broad discovery found no validated commercial-terms source; account and list prices do not establish contracts or commitments."

        source_statuses = {
            result["source_name"]: result["status"]
            for result in source_results
        }

        report.append(
            {
                "category": category["label"],
                "category_key": category["key"],
                "source_name": category["key"],
                "dataset": ", ".join(
                    result.get("table", "") for result in source_results
                ),
                "sources": list(category["sources"]),
                "source_statuses": source_statuses,
                "purpose": category["purpose"],
                "status": status,
                "record_count": record_count,
                "earliest_timestamp": earliest,
                "latest_timestamp": latest,
                "required_permission": category["required_permission"],
                "explanation": explanation,
            }
        )

    return report
