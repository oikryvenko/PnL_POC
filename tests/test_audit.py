from __future__ import annotations

import sys
import types
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from trade_pipeline import audit
from trade_pipeline.gold import break_report


def test_successful_audit_records_metrics_and_status_transitions() -> None:
    records = []
    metrics = {
        "input_rows": 12,
        "output_rows": 8,
        "requested_tickers": 3,
        "rejected_rows": 4,
        "duplicate_rows": 2,
        "issue_counts": {"INVALID_CLOSE": 3, "INVALID_CURRENCY": 1},
    }
    with patch.object(audit, "_merge_record", side_effect=lambda _spark, record: records.append(record)):
        result = audit.run_audited_task(
            MagicMock(),
            "job-run-1",
            "task-run-1",
            "process_prices",
            "cibc.bronze.market_prices",
            "cibc.silver.market_prices",
            lambda: metrics,
        )

    assert result is metrics
    assert [record["status"] for record in records] == ["STARTED", "STARTED", "SUCCESS"]
    assert all(record["run_id"] == "job-run-1" for record in records)
    assert all(record["task_run_id"] == "task-run-1" for record in records)
    assert records[-1]["input_rows"] == 12
    assert records[-1]["output_rows"] == 8
    assert records[-1]["requested_tickers"] == 3
    assert records[-1]["rejected_rows"] == 4
    assert records[-1]["duplicate_rows"] == 2
    assert records[-1]["issue_counts"] == {"INVALID_CLOSE": 3, "INVALID_CURRENCY": 1}
    assert records[-1]["finished_at"].tzinfo == timezone.utc


def test_failed_audit_records_exception_details() -> None:
    records = []
    original = ValueError("bad source snapshot")

    def fail_operation():
        raise original

    with patch.object(audit, "_merge_record", side_effect=lambda _spark, record: records.append(record)):
        with pytest.raises(ValueError) as raised:
            audit.run_audited_task(
                MagicMock(), "job-run-2", "task-run-2", "process_prices", "bronze", "silver", fail_operation
            )

    assert raised.value is original
    assert records[-1]["status"] == "FAILED"
    assert records[-1]["error_type"] == "ValueError"
    assert records[-1]["error_message"] == "bad source snapshot"
    assert records[-1]["finished_at"] is not None


def test_audit_failure_does_not_hide_processing_exception() -> None:
    def fail_audit(_spark, record):
        if record["status"] == "FAILED":
            raise OSError("audit unavailable")

    original = RuntimeError("processing failed")
    with patch.object(audit, "_merge_record", side_effect=fail_audit):
        with pytest.raises(RuntimeError, match="processing failed"):
            audit.run_audited_task(
                MagicMock(), "job-run-3", "task-run-3", "ingest_prices", None, "bronze",
                lambda: (_ for _ in ()).throw(original),
            )


def test_audit_failure_after_successful_operation_fails_task_explicitly() -> None:
    seen = []

    def fail_success_update(_spark, record):
        seen.append(record["status"])
        if record["status"] == "SUCCESS":
            raise OSError("audit table unavailable")

    with patch.object(audit, "_merge_record", side_effect=fail_success_update):
        with pytest.raises(OSError, match="audit table unavailable"):
            audit.run_audited_task(
                MagicMock(), "job-run-audit-fail", "task-audit-fail", "process_prices",
                "bronze", "silver", lambda: {"output_rows": 4},
            )
    assert "STARTED" in seen
    assert "SUCCESS" in seen
    assert "FAILED" in seen


def test_retry_attempts_share_pipeline_id_but_have_distinct_task_ids() -> None:
    records = []
    with patch.object(audit, "_merge_record", side_effect=lambda _spark, record: records.append(record)):
        for attempt_id in ("task-attempt-1", "task-attempt-2"):
            audit.run_audited_task(
                MagicMock(),
                "shared-job-run",
                attempt_id,
                "ingest_prices",
                None,
                "cibc.bronze.market_prices",
                lambda: {"input_rows": 2, "output_rows": 40},
            )

    terminal_records = [record for record in records if record["status"] == "SUCCESS"]
    assert {record["run_id"] for record in terminal_records} == {"shared-job-run"}
    assert {record["task_run_id"] for record in terminal_records} == {
        "task-attempt-1",
        "task-attempt-2",
    }


def test_break_writer_merges_by_run_and_business_identity() -> None:
    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.Row = lambda **kwargs: kwargs
    fake_sql.Window = MagicMock()
    fake_sql.functions = MagicMock()
    fake_sql.functions.col.return_value.asc_nulls_last.return_value = MagicMock()
    fake_sql.functions.row_number.return_value.over.return_value = MagicMock()
    fake_types = types.ModuleType("pyspark.sql.types")
    for type_name in ("DateType", "StringType", "StructField", "StructType", "TimestampType"):
        setattr(fake_types, type_name, MagicMock(name=type_name))

    spark = MagicMock()
    source_df = spark.createDataFrame.return_value
    source_df.withColumn.return_value.filter.return_value.drop.return_value.count.return_value = 1
    detected_at = datetime(2026, 10, 11, tzinfo=timezone.utc)
    break_row = {
        "trade_id": "TRD000001",
        "instrument_id": "INS001",
        "valuation_date": date(2026, 9, 1),
        "break_type": "MISSING_MARKET_PRICE",
        "break_description": "No eligible close was found",
        "detected_at": detected_at,
    }

    with patch.dict(sys.modules, {"pyspark.sql": fake_sql, "pyspark.sql.types": fake_types}):
        written = break_report.write_valuation_breaks(spark, "job-run-4", [break_row, break_row])

    assert written == 1
    source_rows = spark.createDataFrame.call_args.args[0]
    assert len(source_rows) == 2
    assert source_rows[0]["run_id"] == "job-run-4"
    assert source_rows[0]["status"] == "OPEN"
    source_df.withColumn.assert_called_once()
    fake_sql.Window.partitionBy.assert_called_once_with(
        "run_id", "trade_id", "instrument_id", "valuation_date", "break_type"
    )
    merge_sql = spark.sql.call_args.args[0]
    assert "MERGE INTO cibc.gold.valuation_breaks" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql


def test_break_writer_accepts_spark_dataframe_without_collecting() -> None:
    fake_sql = types.ModuleType("pyspark.sql")
    fake_functions = MagicMock()
    fake_functions.col.return_value.cast.return_value.alias.return_value = MagicMock()
    fake_functions.lit.return_value.cast.return_value.alias.return_value = MagicMock()
    fake_functions.current_timestamp.return_value = MagicMock()
    fake_functions.row_number.return_value.over.return_value = MagicMock()
    fake_window = MagicMock()
    fake_window.partitionBy.return_value.orderBy.return_value = MagicMock()
    fake_sql.functions = fake_functions
    fake_sql.Window = fake_window
    fake_types = types.ModuleType("pyspark.sql.types")
    for type_name in ("DateType", "StringType", "StructField", "StructType", "TimestampType"):
        setattr(fake_types, type_name, MagicMock(name=type_name))

    spark = MagicMock()
    incoming = MagicMock()
    incoming.columns = [
        "trade_id", "instrument_id", "valuation_date", "break_type", "break_description"
    ]
    with patch.dict(sys.modules, {"pyspark.sql": fake_sql, "pyspark.sql.types": fake_types}):
        break_report.write_valuation_breaks(spark, "run-5", incoming)

    spark.createDataFrame.assert_not_called()
    assert spark.sql.call_args.args[0].startswith("\n        MERGE INTO cibc.gold.valuation_breaks")
