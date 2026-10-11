from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trade_pipeline.bronze import ingest_trades

HEADER = ",".join(ingest_trades.TRADE_COLUMNS)
ROW = "T1,CP1,INS001,AAPL,BUY,10,150.5,USD,2026-09-01T15:00:00Z,2026-09-01,2026-09-02,BOOK_TECH,1,2026-09-01T16:00:00Z"
ROW_V2 = ROW.replace(",BUY,10,", ",BUY,12,").replace(",1,2026-09-01T16:00:00Z", ",2,2026-09-01T17:00:00Z")
TARGET = "cibc_test.bronze_trades"


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    session = pyspark.sql.SparkSession.builder.master("local[1]").appName("ingest-trades").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def bronze(spark):
    """Empty Bronze table with the production column layout."""
    spark.sql("CREATE DATABASE IF NOT EXISTS cibc_test")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
    columns = ", ".join(f"{c} STRING" for c in ingest_trades.TRADE_COLUMNS)
    spark.sql(f"CREATE TABLE {TARGET} ({columns}, source_file STRING, ingestion_timestamp TIMESTAMP, run_id STRING)")
    return TARGET


def _csv(tmp_path, text: str, name: str = "trades.csv") -> str:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


def _ingest(spark, path: str, run_id: str) -> dict:
    with patch.object(ingest_trades, "TARGET_TABLE", TARGET), patch.object(
        ingest_trades.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ):
        return ingest_trades.run_ingestion(path, spark=spark, pipeline_run_id=run_id)


def _run_with_mocks(input_rows: int, distinct_rows: int):
    source_df, batch_df = MagicMock(), MagicMock()
    source_df.count.return_value = input_rows
    batch_df.count.return_value = distinct_rows
    with patch.object(
        ingest_trades.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(ingest_trades, "read_trade_csv", return_value=source_df), patch.object(
        ingest_trades, "prepare_bronze_batch", return_value=batch_df
    ), patch.object(ingest_trades, "overwrite_bronze_trades") as overwrite:
        summary = ingest_trades.run_ingestion("/tmp/trades.csv", spark=MagicMock(), pipeline_run_id="r")
    return summary, overwrite


def test_audit_metrics_count_in_batch_duplicates() -> None:
    summary, overwrite = _run_with_mocks(input_rows=7, distinct_rows=5)
    overwrite.assert_called_once()
    assert summary["input_rows"] == 7
    assert summary["output_rows"] == summary["rows_written"] == summary["inserted_rows"] == 5
    assert summary["duplicate_rows"] == 2
    assert summary["rejected_rows"] == 0
    assert summary["issue_counts"] == {"DUPLICATE_IN_BATCH": 2}


def test_audit_metrics_without_duplicates_report_no_issues() -> None:
    summary, _ = _run_with_mocks(input_rows=4, distinct_rows=4)
    assert summary["duplicate_rows"] == 0
    assert summary["issue_counts"] == {}


def test_overwrite_uses_insert_overwrite_with_explicit_columns() -> None:
    spark = MagicMock()
    ingest_trades.overwrite_bronze_trades(spark, MagicMock())
    (sql,) = [call.args[0] for call in spark.sql.call_args_list]
    columns = ", ".join(ingest_trades.BRONZE_COLUMNS)
    assert sql.startswith(f"INSERT OVERWRITE TABLE {ingest_trades.TARGET_TABLE} ({columns}) SELECT {columns}")
    for forbidden in ("MERGE", "DROP", "CREATE", "record_hash"):
        assert forbidden not in sql
    assert ingest_trades.BRONZE_COLUMNS == (
        *ingest_trades.TRADE_COLUMNS, "source_file", "ingestion_timestamp", "run_id"
    )
    assert len(ingest_trades.TRADE_COLUMNS) == 14


def test_failed_csv_validation_does_not_overwrite() -> None:
    with patch.object(
        ingest_trades.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(
        ingest_trades, "read_trade_csv", side_effect=ValueError("Trade CSV header mismatch")
    ), patch.object(ingest_trades, "overwrite_bronze_trades") as overwrite:
        with pytest.raises(ValueError, match="header mismatch"):
            ingest_trades.run_ingestion("/tmp/trades.csv", spark=MagicMock(), pipeline_run_id="r")
    overwrite.assert_not_called()


def test_header_mismatch_is_rejected(spark, tmp_path) -> None:
    bad_header = HEADER.replace("counterparty_id", "client_id")
    with pytest.raises(ValueError, match="header mismatch"):
        ingest_trades.read_trade_csv(spark, _csv(tmp_path, f"{bad_header}\n{ROW}\n"))


def test_header_with_missing_column_is_rejected(spark, tmp_path) -> None:
    short = ",".join(ingest_trades.TRADE_COLUMNS[:-1])
    with pytest.raises(ValueError, match="header mismatch"):
        ingest_trades.read_trade_csv(spark, _csv(tmp_path, f"{short}\n{ROW}\n"))


def test_header_only_csv_is_rejected_as_empty(spark, tmp_path) -> None:
    with pytest.raises(ValueError, match="empty"):
        ingest_trades.read_trade_csv(spark, _csv(tmp_path, f"{HEADER}\n"))


def test_structurally_malformed_csv_is_rejected(spark, tmp_path) -> None:
    with pytest.raises(ValueError, match="malformed"):
        ingest_trades.read_trade_csv(spark, _csv(tmp_path, f"{HEADER}\n{ROW},EXTRA\n"))


def test_values_are_read_as_strings(spark, tmp_path) -> None:
    df = ingest_trades.read_trade_csv(spark, _csv(tmp_path, f"{HEADER}\n{ROW}\n"))
    assert {field.dataType.simpleString() for field in df.schema.fields} == {"string"}
    assert df.first()["price"] == "150.5"


def test_first_ingestion_creates_snapshot(spark, bronze, tmp_path) -> None:
    summary = _ingest(spark, _csv(tmp_path, f"{HEADER}\n{ROW}\n{ROW.replace('T1', 'T2')}\n"), "run-1")
    rows = spark.table(bronze).collect()
    assert sorted(r["trade_id"] for r in rows) == ["T1", "T2"]
    assert {r["run_id"] for r in rows} == {"run-1"}
    assert all(r["source_file"].endswith("trades.csv") and r["ingestion_timestamp"] for r in rows)
    assert (summary["input_rows"], summary["rows_written"], summary["duplicate_rows"]) == (2, 2, 0)


def test_rerun_with_same_csv_does_not_add_rows(spark, bronze, tmp_path) -> None:
    path = _csv(tmp_path, f"{HEADER}\n{ROW}\n{ROW.replace('T1', 'T2')}\n")
    _ingest(spark, path, "run-1")
    _ingest(spark, path, "run-2")
    rows = spark.table(bronze).collect()
    assert len(rows) == 2
    assert {r["run_id"] for r in rows} == {"run-2"}


def test_identical_rows_are_deduplicated_and_versions_kept(spark, bronze, tmp_path) -> None:
    summary = _ingest(spark, _csv(tmp_path, f"{HEADER}\n{ROW}\n{ROW}\n{ROW_V2}\n"), "run-1")
    rows = spark.table(bronze).collect()
    assert sorted((r["trade_id"], r["version"]) for r in rows) == [("T1", "1"), ("T1", "2")]
    assert (summary["input_rows"], summary["output_rows"], summary["duplicate_rows"]) == (3, 2, 1)
    assert summary["issue_counts"] == {"DUPLICATE_IN_BATCH": 1}


def test_new_snapshot_replaces_previous_snapshot(spark, bronze, tmp_path) -> None:
    _ingest(spark, _csv(tmp_path, f"{HEADER}\n{ROW}\n", "a.csv"), "run-1")
    _ingest(spark, _csv(tmp_path, f"{HEADER}\n{ROW.replace('T1', 'T2')}\n", "b.csv"), "run-2")
    rows = spark.table(bronze).collect()
    assert [(r["trade_id"], r["run_id"]) for r in rows] == [("T2", "run-2")]
    assert rows[0]["source_file"].endswith("b.csv")


def test_invalid_csv_leaves_existing_bronze_unchanged(spark, bronze, tmp_path) -> None:
    _ingest(spark, _csv(tmp_path, f"{HEADER}\n{ROW}\n", "good.csv"), "run-1")
    before = spark.table(bronze).collect()
    bad_header = HEADER.replace("counterparty_id", "client_id")
    with pytest.raises(ValueError, match="header mismatch"):
        _ingest(spark, _csv(tmp_path, f"{bad_header}\n{ROW}\n", "bad.csv"), "run-2")
    with pytest.raises(ValueError, match="empty"):
        _ingest(spark, _csv(tmp_path, f"{HEADER}\n", "empty.csv"), "run-3")
    assert spark.table(bronze).collect() == before
