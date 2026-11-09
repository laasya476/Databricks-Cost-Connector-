"""Generic collection of Databricks system tables."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from connector.auth import get_connection


class PartialCollectionError(Exception):
    def __init__(self, retrieved_count: int, cause: Exception):
        super().__init__("Collection stopped after a batch retrieval failure")
        self.retrieved_count = retrieved_count
        self.cause = cause




SOURCE_CONFIG = {
 
    "billing_usage": {
        "table": "system.billing.usage",
        "columns": None,
        "time_column": "usage_start_time",
        "end_time_column": "usage_end_time",
        "time_filter_mode": "overlap",
    },
    "billing_list_prices": {
        "table": "system.billing.list_prices",
        "columns": None,
        "time_column": "price_start_time",
        "end_time_column": "price_end_time",
        "time_filter_mode": "overlap",
    },
    "billing_account_prices": {
        "table": "system.billing.account_prices",
        "columns": None,
        "time_column": "price_start_time",
        "end_time_column": "price_end_time",
        "time_filter_mode": "overlap",
    },
    "billing_attributed_usage": {
        "table": "system.billing.attributed_usage",
        "columns": None,
        "time_column": "start_time",
        "end_time_column": "end_time",
        "time_filter_mode": "overlap",
    },
    "billing_cloud_infra_cost": {
        "table": "system.billing.cloud_infra_cost",
        "columns": None,
        "time_column": "usage_start_time",
        "end_time_column": "usage_end_time",
        "time_filter_mode": "overlap",
    },

    "compute_clusters": {
        "table": "system.compute.clusters",
        "columns": None,
    },
    "compute_instance_events": {
        "table": "system.compute.instance_events",
        "columns": None,
        "time_column": "event_time",
    },
    "compute_instance_pools": {
        "table": "system.compute.instance_pools",
        "columns": None,
    },
    "compute_node_timeline": {
        "table": "system.compute.node_timeline",
        "columns": None,
        "time_column": "start_time",
        "end_time_column": "end_time",
        "time_filter_mode": "overlap",
    },
    "compute_node_types": {
        "table": "system.compute.node_types",
        "columns": None,
    },
    "compute_warehouses": {
        "table": "system.compute.warehouses",
        "columns": None,
    },
    "compute_warehouse_events": {
        "table": "system.compute.warehouse_events",
        "columns": None,
        "time_column": "event_time",
    },

    "lakeflow_jobs": {
        "table": "system.lakeflow.jobs",
        "columns": None,
        "time_column": "change_time",
    },
    "lakeflow_job_tasks": {
        "table": "system.lakeflow.job_tasks",
        "columns": None,
        "time_column": "change_time",
    },
    "lakeflow_job_runs": {
        "table": "system.lakeflow.job_run_timeline",
        "columns": None,
        "time_column": "period_start_time",
        "end_time_column": "period_end_time",
        "time_filter_mode": "overlap",
    },
    "lakeflow_task_runs": {
        "table": "system.lakeflow.job_task_run_timeline",
        "columns": None,
        "time_column": "period_start_time",
        "end_time_column": "period_end_time",
        "time_filter_mode": "overlap",
    },
    "lakeflow_pipelines": {
        "table": "system.lakeflow.pipelines",
        "columns": None,
        "time_column": "change_time",
    },
    "lakeflow_pipeline_updates": {
        "table": "system.lakeflow.pipeline_update_timeline",
        "columns": None,
        "time_column": "period_start_time",
        "end_time_column": "period_end_time",
        "time_filter_mode": "overlap",
    },

    "storage_table_metrics": {
        "table": "system.storage.table_metrics_history",
        "columns": None,
        "time_column": "snapshot_date",
        "time_type": "date",
    },
    "storage_predictive_optimization": {
        "table": "system.storage.predictive_optimization_operations_history",
        "columns": None,
        "time_column": "start_time",
        "end_time_column": "end_time",
        "time_filter_mode": "overlap",
    },
    "storage_auto_upgrade": {
        "table": "system.storage.auto_upgrade_operations_history",
        "columns": None,
        "time_column": "start_time",
        "end_time_column": "end_time",
        "time_filter_mode": "overlap",
    },

    "query_history": {
        "table": "system.query.history",
        "columns": None,
        "time_column": "start_time",
        "end_time_column": "end_time",
        "time_filter_mode": "overlap",
    },

    "workspaces_latest": {
        "table": "system.access.workspaces_latest",
        "columns": None,
    },
    "network_inbound": {
        "table": "system.access.inbound_network",
        "columns": None,
        "time_column": "event_time",
    },
    "network_outbound": {
        "table": "system.access.outbound_network",
        "columns": None,
        "time_column": "event_time",
    },

    "serving_endpoint_usage": {
        "table": "system.serving.endpoint_usage",
        "columns": None,
        "time_column": "request_time",
    },
    "serving_entities": {
        "table": "system.serving.served_entities",
        "columns": None,
    },
}




def _row_to_dict(cursor, row) -> dict[str, Any]:
    """Convert a SQL row to a dictionary without changing source values."""
    columns = [column[0] for column in cursor.description]
    return dict(zip(columns, row))


def _collect_query(
    query: str,
    params: list[Any] | None = None,
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    """Execute SQL and retrieve all rows in batches."""

    records: list[dict[str, Any]] = []

    with get_connection() as connection:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            while True:
                try:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    records.extend(_row_to_dict(cursor, row) for row in rows)
                except Exception as exc:
                    raise PartialCollectionError(len(records), exc) from exc

    return records


def _build_time_filter(
    config: dict[str, Any],
    start_time: datetime | None,
    end_time: datetime | None,
) -> tuple[str, list[Any]]:

    time_column = config.get("time_column")
    end_time_column = config.get("end_time_column")
    time_type = config.get("time_type", "timestamp")
    filter_mode = config.get("time_filter_mode", "point")

    if not time_column:
        return "", []

    conditions: list[str] = []
    params: list[Any] = []

    if time_type == "date":
        start_value = start_time.date() if start_time is not None else None
        if end_time is not None:
            end_value = end_time.date()
            if end_time.time() != datetime.min.time():
                end_value += timedelta(days=1)
        else:
            end_value = None
        start_placeholder = "CAST(? AS DATE)"
        end_placeholder = "CAST(? AS DATE)"
    else:
        start_value = start_time
        end_value = end_time
        start_placeholder = "?"
        end_placeholder = "?"

    if start_time is not None:
        if filter_mode == "overlap" and end_time_column:
            conditions.append(
                f"({end_time_column} IS NULL OR {end_time_column} > {start_placeholder})"
            )
        else:
            conditions.append(f"{time_column} >= {start_placeholder}")
        params.append(start_value)

    if end_time is not None:
        if filter_mode == "overlap" and end_time_column:
            conditions.append(f"{time_column} < {end_placeholder}")
        else:
            conditions.append(f"{time_column} < {end_placeholder}")

        params.append(end_value)

    if not conditions:
        return "", []

    return "\nWHERE " + "\n  AND ".join(conditions), params






def collect_source(
    source_name: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    """
    Collect a configured Databricks system table.

    The collector:
    - uses the source registry for table/column information
    - applies a time range when the source supports one
    - retrieves rows in batches
    - preserves Decimal, timestamps, NULLs and source metadata
    """

    if source_name not in SOURCE_CONFIG:
        raise ValueError(f"Unknown source: {source_name}")

    if start_time is not None and end_time is not None and start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")

    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 1:
        raise ValueError("batch_size must be a positive integer")

    config = SOURCE_CONFIG[source_name]

    table = config["table"]
    columns = config.get("columns")

    if columns:
        select_columns = ",\n            ".join(columns)
    else:
        select_columns = "*"

    time_filter, params = _build_time_filter(
        config,
        start_time,
        end_time,
    )

    order_column = config.get("time_column")

    query = f"""
        SELECT
            {select_columns}
        FROM {table}
        {time_filter}
    """

    if order_column:
        query += f"\nORDER BY {order_column}"

    return _collect_query(
        query=query,
        params=params,
        batch_size=batch_size,
    )



def collect_billing_usage(start_time=None, end_time=None, batch_size=1000):
    return collect_source(
        "billing_usage",
        start_time,
        end_time,
        batch_size,
    )


def collect_billing_list_prices(start_time=None, end_time=None, batch_size=1000):
    return collect_source(
        "billing_list_prices",
        start_time,
        end_time,
        batch_size,
    )


def collect_billing_account_prices(start_time=None, end_time=None, batch_size=1000):
    return collect_source(
        "billing_account_prices",
        start_time,
        end_time,
        batch_size,
    )


def collect_billing_attributed_usage(start_time=None, end_time=None, batch_size=1000):
    return collect_source(
        "billing_attributed_usage",
        start_time,
        end_time,
        batch_size,
    )


def collect_billing_cloud_infra_cost(start_time=None, end_time=None, batch_size=1000):
    return collect_source(
        "billing_cloud_infra_cost",
        start_time,
        end_time,
        batch_size,
    )


def collect_all_sources(
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    batch_size: int = 1000,
) -> dict[str, list[dict[str, Any]]]:
    """Collect every configured source."""

    results: dict[str, list[dict[str, Any]]] = {}

    for source_name in SOURCE_CONFIG:
        results[source_name] = collect_source(
            source_name,
            start_time,
            end_time,
            batch_size,
        )

    return results
