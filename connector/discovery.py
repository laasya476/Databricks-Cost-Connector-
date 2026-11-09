"""Discover Databricks system tables used by the cost connector."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from typing import Any

from connector.auth import get_connection


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



_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


SOURCE_TABLES: dict[str, tuple[str, str]] = {

    "billing_usage": ("billing", "usage"),
    "billing_list_prices": ("billing", "list_prices"),
    "billing_account_prices": ("billing", "account_prices"),
    "billing_attributed_usage": ("billing", "attributed_usage"),
    "billing_cloud_infra_cost": ("billing", "cloud_infra_cost"),

    "compute_clusters": ("compute", "clusters"),
    "compute_instance_events": ("compute", "instance_events"),
    "compute_instance_pools": ("compute", "instance_pools"),
    "compute_node_timeline": ("compute", "node_timeline"),
    "compute_node_types": ("compute", "node_types"),
    "compute_warehouses": ("compute", "warehouses"),
    "compute_warehouse_events": ("compute", "warehouse_events"),

    "lakeflow_jobs": ("lakeflow", "jobs"),
    "lakeflow_job_tasks": ("lakeflow", "job_tasks"),
    "lakeflow_job_runs": ("lakeflow", "job_run_timeline"),
    "lakeflow_task_runs": ("lakeflow", "job_task_run_timeline"),
    "lakeflow_pipelines": ("lakeflow", "pipelines"),
    "lakeflow_pipeline_updates": (
        "lakeflow",
        "pipeline_update_timeline",
    ),

    "storage_table_metrics": (
        "storage",
        "table_metrics_history",
    ),
    "storage_predictive_optimization": (
        "storage",
        "predictive_optimization_operations_history",
    ),
    "storage_auto_upgrade": (
        "storage",
        "auto_upgrade_operations_history",
    ),

 
    "query_history": ("query", "history"),

    "workspaces_latest": ("access", "workspaces_latest"),
    "network_inbound": ("access", "inbound_network"),
    "network_outbound": ("access", "outbound_network"),


    "serving_endpoint_usage": (
        "serving",
        "endpoint_usage",
    ),
    "serving_entities": (
        "serving",
        "served_entities",
    ),
}


def _validate_identifier(value: str, name: str) -> str:
    """Validate a SQL identifier before using it in a query."""
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"Invalid SQL identifier for {name}: {value!r}"
        )

    return value


def _qualified_table(schema: str, table: str) -> str:
    """Return a validated fully qualified Databricks system table."""
    schema = _validate_identifier(schema, "schema")
    table = _validate_identifier(table, "table")

    return f"system.{schema}.{table}"


def _execute(
    cursor: Any,
    query: str,
    parameters: Iterable[Any] | None = None,
) -> None:
    """Execute a SQL query with optional parameters."""
    if parameters is None:
        cursor.execute(query)
    else:
        cursor.execute(query, list(parameters))


def run_query(
    query: str,
    parameters: Iterable[Any] | None = None,
    *,
    connection_factory: Callable[[], Any] = get_connection,
) -> list[Any]:
    """Execute a read-only query and return all rows."""
    with connection_factory() as connection:
        with connection.cursor() as cursor:
            _execute(cursor, query, parameters)
            return list(cursor.fetchall())


def discover_system_schemas(
    *,
    connection_factory: Callable[[], Any] = get_connection,
) -> list[Any]:
    """Return the system schemas visible to the account."""
    return run_query(
        "SHOW SCHEMAS IN system",
        connection_factory=connection_factory,
    )


def discover_tables(
    schema: str,
    *,
    connection_factory: Callable[[], Any] = get_connection,
) -> list[Any]:
    """Return the tables visible inside a system schema."""
    schema = _validate_identifier(schema, "schema")

    return run_query(
        f"SHOW TABLES IN system.{schema}",
        connection_factory=connection_factory,
    )


def describe_table(
    schema: str,
    table: str,
    *,
    connection_factory: Callable[[], Any] = get_connection,
) -> list[Any]:
    """Return the schema definition of a system table."""
    return run_query(
        f"DESCRIBE TABLE {_qualified_table(schema, table)}",
        connection_factory=connection_factory,
    )


def _row_value(row: Any, *names: str) -> Any:
    """Extract a named value from a Databricks row-like object."""
    if isinstance(row, dict):
        for name in names:
            if name in row:
                return row[name]

    for name in names:
        try:
            return getattr(row, name)
        except AttributeError:
            continue

    return None


def _describe_columns(rows: list[Any]) -> list[dict[str, Any]]:
    """Convert DESCRIBE TABLE rows into dictionaries."""
    columns: list[dict[str, Any]] = []

    for row in rows:
        if isinstance(row, dict):
            columns.append(dict(row))

        elif hasattr(row, "asDict"):
            columns.append(row.asDict(recursive=True))

        else:
            values = tuple(row)

            columns.append(
                {
                    "col_name": (
                        values[0] if len(values) > 0 else None
                    ),
                    "data_type": (
                        values[1] if len(values) > 1 else None
                    ),
                    "comment": (
                        values[2] if len(values) > 2 else None
                    ),
                }
            )

    return columns


def _status_for_error(error: Exception) -> str:
    """
    Map a discovery/collection error to an assessment status.

    These statuses are used when an actual operation fails or a source
    is unavailable. Successful schema discovery itself is represented
    by schema_available=True rather than by 'collected'.
    """
    message = str(error).lower()

    if any(
        value in message
        for value in (
            "permission denied",
            "not authorized",
            "unauthorized",
            "access denied",
            "insufficient privilege",
            "select privilege",
        )
    ):
        return "permission_denied"

    if any(
        value in message
        for value in (
            "schema_not_found",
            "table_or_view_not_found",
            "table not found",
            "cannot be found",
            "does not exist",
        )
    ):
        return "unavailable_in_account"

    if any(
        value in message
        for value in (
            "unsupported",
            "not supported",
            "feature_not_available",
        )
    ):
        return "unsupported"

    return "failed"


def discover_available_system_tables(
    *,
    connection_factory: Callable[[], Any] = get_connection,
) -> dict[str, Any]:
    """
    Return visible tables grouped by system schema.

    This is broad discovery only. It does not determine whether a
    source was successfully collected.
    """
    available: dict[str, Any] = {}
    schema_errors: dict[str, str] = {}

    for schema_row in discover_system_schemas(
        connection_factory=connection_factory,
    ):
        schema = _row_value(
            schema_row,
            "databaseName",
            "schema_name",
            "namespace",
        )

        if not schema:
            continue

        try:
            available[schema] = discover_tables(
                schema,
                connection_factory=connection_factory,
            )
        except Exception as exc:
            schema_errors[schema] = str(exc)

    if schema_errors:
        available["_schema_errors"] = schema_errors

    return available


def reconcile_system_tables(
    available: dict[str, Any],
    source_results: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    configured = {
        f"system.{schema}.{table}": source_name
        for source_name, (schema, table) in SOURCE_TABLES.items()
    }
    relevant_schemas = {schema for schema, _ in SOURCE_TABLES.values()} | {"alert"}
    reconciliation = []

    for schema, rows in available.items():
        if schema.startswith("_"):
            continue
        for row in rows:
            table = _row_value(row, "tableName", "table_name", "name")
            if table is None and not isinstance(row, (dict, str)):
                values = tuple(row)
                table = values[1] if len(values) > 1 else None
            if not table:
                reconciliation.append({
                    "schema": schema,
                    "table": None,
                    "source_name": None,
                    "collection_status": "discovery_failed",
                    "explanation": "A discovered table row could not be identified.",
                })
                continue
            qualified = f"system.{schema}.{table}"
            source_name = configured.get(qualified)
            if source_name:
                status = source_results.get(source_name, {}).get("status", "failed")
                explanation = (
                    "Collection attempted by the configured source."
                    if source_name in source_results
                    else "Configured source was not attempted."
                )
            elif schema in relevant_schemas or any(
                term in f"{schema}.{table}".lower()
                for term in ("billing", "cost", "price", "budget", "contract", "commitment", "discount", "usage")
            ):
                status = "not_configured"
                explanation = (
                    "Discovered in a cost-relevant schema; no collector is configured "
                    "because this table's record semantics have not been validated."
                )
            else:
                status = "outside_cost_scope"
                explanation = "Discovered outside the configured cost-management schemas."
            reconciliation.append({
                "schema": schema,
                "table": str(table),
                "source_name": source_name,
                "collection_status": status,
                "explanation": explanation,
            })

    for schema in available.get("_schema_errors", {}):
        reconciliation.append({
            "schema": schema,
            "table": None,
            "source_name": None,
            "collection_status": "discovery_failed",
            "explanation": "Table listing failed; tables in this schema could not be reconciled.",
        })

    return reconciliation


def discover_source_schemas(
    *,
    source_tables: dict[str, tuple[str, str]] | None = None,
    connection_factory: Callable[[], Any] = get_connection,
) -> dict[str, dict[str, Any]]:
    """
    Check whether each registered source table exists and inspect
    its schema.

    Important:
        schema_available=True means only that DESCRIBE TABLE succeeded.
        It does NOT mean that records have been collected.

    Actual collection status is determined later by collectors.py
    and reported by coverage.py.
    """
    registry = (
        SOURCE_TABLES
        if source_tables is None
        else source_tables
    )

    results: dict[str, dict[str, Any]] = {}

    with connection_factory() as connection:
        with connection.cursor() as cursor:

            for source_name, (schema, table) in registry.items():

                qualified_table = _qualified_table(
                    schema,
                    table,
                )

                result = {
                    "source_name": source_name,
                    "schema": schema,
                    "table": table,
                    "qualified_table": qualified_table,
                    "schema_available": False,
                    "columns": [],
                    "discovery_error_status": None,
                    "error": None,
                }

                try:
                    _execute(
                        cursor,
                        f"DESCRIBE TABLE {qualified_table}",
                    )

                    rows = list(cursor.fetchall())

                    result["columns"] = _describe_columns(rows)
                    result["schema_available"] = bool(rows)

                except Exception as error:
                    result["discovery_error_status"] = (
                        _status_for_error(error)
                    )
                    result["error"] = str(error)

                results[source_name] = result

    return results
