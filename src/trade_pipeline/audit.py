"""Small Delta-backed audit helpers for Databricks task attempts."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)
AUDIT_TABLE = "cibc.gold.pipeline_run_log"


def _merge_record(spark, record: dict[str, Any]) -> None:
    from pyspark.sql import Row
    from pyspark.sql.types import (
        DateType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("task_run_id", StringType(), False),
            StructField("task_name", StringType(), False),
            StructField("run_date", DateType(), False),
            StructField("started_at", TimestampType(), False),
            StructField("finished_at", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("source_table", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("requested_tickers", LongType(), True),
            StructField("input_rows", LongType(), True),
            StructField("output_rows", LongType(), True),
            StructField("rejected_rows", LongType(), True),
            StructField("duplicate_rows", LongType(), True),
            StructField("issue_counts", MapType(StringType(), LongType()), True),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True),
        ]
    )
    view = f"_pipeline_audit_{uuid.uuid4().hex}"
    spark.createDataFrame([Row(**record)], schema=schema).createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {AUDIT_TABLE} AS target
        USING {view} AS source
        ON target.run_id = source.run_id AND target.task_run_id = source.task_run_id
        WHEN MATCHED THEN UPDATE SET
            task_name = source.task_name,
            run_date = source.run_date,
            started_at = source.started_at,
            finished_at = source.finished_at,
            status = source.status,
            source_table = source.source_table,
            target_table = source.target_table,
            requested_tickers = source.requested_tickers,
            input_rows = source.input_rows,
            output_rows = source.output_rows,
            rejected_rows = source.rejected_rows,
            duplicate_rows = source.duplicate_rows,
            issue_counts = source.issue_counts,
            error_type = source.error_type,
            error_message = source.error_message
        WHEN NOT MATCHED THEN INSERT (
            run_id, task_run_id, task_name, run_date, started_at, finished_at,
            status, source_table, target_table, requested_tickers, input_rows,
            output_rows, rejected_rows, duplicate_rows, issue_counts, error_type,
            error_message
        ) VALUES (
            source.run_id, source.task_run_id, source.task_name, source.run_date,
            source.started_at, source.finished_at, source.status, source.source_table,
            source.target_table, source.requested_tickers, source.input_rows,
            source.output_rows, source.rejected_rows, source.duplicate_rows,
            source.issue_counts, source.error_type, source.error_message
        )
        """
    )
    spark.catalog.dropTempView(view)


def _record(
    attempt: dict[str, Any],
    status: str,
    metrics: Mapping[str, Any] | None = None,
    *,
    finished_at: datetime | None = None,
    error: Exception | None = None,
) -> dict[str, Any]:
    metrics = metrics or {}
    return {
        "run_id": attempt["run_id"],
        "task_run_id": attempt["task_run_id"],
        "task_name": attempt["task_name"],
        "run_date": attempt["started_at"].astimezone(timezone.utc).date(),
        "started_at": attempt["started_at"],
        "finished_at": finished_at,
        "status": status,
        "source_table": attempt.get("source_table"),
        "target_table": attempt.get("target_table"),
        "requested_tickers": int(metrics.get("requested_tickers", 0)),
        "input_rows": int(metrics.get("input_rows", 0)),
        "output_rows": int(metrics.get("output_rows", 0)),
        "rejected_rows": int(metrics.get("rejected_rows", 0)),
        "duplicate_rows": int(metrics.get("duplicate_rows", 0)),
        "issue_counts": {str(key): int(value) for key, value in metrics.get("issue_counts", {}).items()},
        "error_type": type(error).__name__ if error else None,
        "error_message": str(error) if error else None,
    }


def start_task(
    spark,
    run_id: str,
    task_name: str,
    source_table: str | None,
    target_table: str | None,
    task_run_id: str | None = None,
) -> dict[str, Any]:
    """Create/update the STARTED record for one task attempt and return its context."""
    started_at = datetime.now(timezone.utc)
    attempt = {
        "run_id": run_id,
        "task_run_id": task_run_id or str(uuid.uuid4()),
        "task_name": task_name,
        "source_table": source_table,
        "target_table": target_table,
        "started_at": started_at,
    }
    _merge_record(spark, _record(attempt, "STARTED"))
    return attempt


def record_metrics(spark, attempt: dict[str, Any], metrics: Mapping[str, Any]) -> None:
    """Persist processing counts and issue-reason counts for this task attempt."""
    _merge_record(spark, _record(attempt, "STARTED", metrics))


def complete_task(spark, attempt: dict[str, Any], metrics: Mapping[str, Any] | None = None) -> None:
    """Mark the current task attempt successful."""
    _merge_record(spark, _record(attempt, "SUCCESS", metrics, finished_at=datetime.now(timezone.utc)))


def fail_task(
    spark,
    attempt: dict[str, Any],
    error: Exception,
    metrics: Mapping[str, Any] | None = None,
) -> None:
    """Record a failed attempt and its exception details."""
    _merge_record(
        spark,
        _record(
            attempt,
            "FAILED",
            metrics,
            finished_at=datetime.now(timezone.utc),
            error=error,
        ),
    )


def run_audited_task(
    spark,
    run_id: str,
    task_run_id: str | None,
    task_name: str,
    source_table: str | None,
    target_table: str | None,
    operation: Callable[[], Mapping[str, Any]],
) -> Mapping[str, Any]:
    """Run one operation with STARTED/SUCCESS/FAILED audit updates."""
    attempt = start_task(spark, run_id, task_name, source_table, target_table, task_run_id)
    metrics: Mapping[str, Any] = {}
    try:
        metrics = operation()
    except Exception as exc:
        logger.exception(
            "Task operation failed task=%s run_id=%s task_run_id=%s",
            task_name,
            run_id,
            attempt["task_run_id"],
        )
        try:
            fail_task(spark, attempt, exc, metrics)
        except Exception:
            logger.exception(
                "Failed to persist FAILED audit status task_run_id=%s",
                attempt["task_run_id"],
            )
        raise

    try:
        record_metrics(spark, attempt, metrics)
        complete_task(spark, attempt, metrics)
    except Exception as exc:
        logger.exception(
            "Business operation succeeded but mandatory audit persistence failed; failing task "
            "task=%s run_id=%s task_run_id=%s",
            task_name,
            run_id,
            attempt["task_run_id"],
        )
        try:
            fail_task(spark, attempt, exc, metrics)
        except Exception:
            logger.exception(
                "Could not persist FAILED audit status after audit-write failure task_run_id=%s",
                attempt["task_run_id"],
            )
        raise
    return metrics
