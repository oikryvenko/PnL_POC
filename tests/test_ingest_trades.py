from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from trade_pipeline.bronze import ingest_trades

HEADER = ",".join(ingest_trades.TRADE_COLUMNS)
ROW = "T1,CP1,INS001,AAPL,BUY,10,150.5,USD,2026-09-01T15:00:00Z,2026-09-01,2026-09-02,BOOK_TECH,1,2026-09-01T16:00:00Z"


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    session = pyspark.sql.SparkSession.builder.master("local[1]").appName("ingest-trades").getOrCreate()
    yield session
    session.stop()


def _csv(tmp_path, text: str) -> str:
    path = tmp_path / "trades.csv"
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_run_ingestion_reports_new_rows_and_duplicates_separately() -> None:
    source_df, batch_df = MagicMock(), MagicMock()
    source_df.count.return_value = 7  # CSV rows read
    batch_df.count.return_value = 5  # 2 exact copies inside the CSV
    with patch.object(
        ingest_trades.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(ingest_trades, "read_trade_csv", return_value=source_df), patch.object(
        ingest_trades, "prepare_bronze_batch", return_value=batch_df
    ), patch.object(ingest_trades, "merge_bronze_trades", return_value=3) as merge:
        summary = ingest_trades.run_ingestion("/tmp/trades.csv", spark=MagicMock(), pipeline_run_id="r")
    merge.assert_called_once()
    source_df.count.assert_called_once()
    assert summary["input_rows"] == 7
    assert summary["output_rows"] == 5
    assert summary["rows_written"] == summary["inserted_rows"] == 3
    assert summary["duplicate_rows"] == 4 == summary["input_rows"] - summary["rows_written"]
    assert summary["issue_counts"] == {"DUPLICATE_IN_BATCH": 2, "ALREADY_INGESTED": 2}


def test_rerun_metrics_do_not_claim_rows_were_loaded() -> None:
    source_df, batch_df = MagicMock(), MagicMock()
    source_df.count.return_value = batch_df.count.return_value = 4
    with patch.object(
        ingest_trades.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(ingest_trades, "read_trade_csv", return_value=source_df), patch.object(
        ingest_trades, "prepare_bronze_batch", return_value=batch_df
    ), patch.object(ingest_trades, "merge_bronze_trades", return_value=0):
        summary = ingest_trades.run_ingestion("/tmp/trades.csv", spark=MagicMock(), pipeline_run_id="r")
    assert summary["rows_written"] == 0
    assert summary["duplicate_rows"] == 4
    assert summary["issue_counts"] == {"ALREADY_INGESTED": 4}


def _merge_spark(null_hash_row=None):
    spark = MagicMock()
    merge_result = {"num_inserted_rows": 2, "num_updated_rows": 0, "num_deleted_rows": 0}
    spark.sql.return_value.first.side_effect = [null_hash_row, merge_result]
    return spark


def test_merge_is_insert_only_on_record_hash() -> None:
    spark = _merge_spark()
    assert ingest_trades.merge_bronze_trades(spark, MagicMock()) == 2
    merge_sql = spark.sql.call_args_list[1].args[0]
    assert "INSERT OVERWRITE" not in merge_sql
    assert "ON target.record_hash = source.record_hash" in merge_sql
    assert "WHEN MATCHED" not in merge_sql and "UPDATE" not in merge_sql and "DELETE" not in merge_sql
    for column in ingest_trades.BRONZE_COLUMNS:
        assert f"source.{column}" in merge_sql


def test_merge_refuses_to_run_before_hash_backfill() -> None:
    spark = _merge_spark(null_hash_row={"1": 1})
    with pytest.raises(ValueError, match="backfill"):
        ingest_trades.merge_bronze_trades(spark, MagicMock())
    assert spark.sql.call_count == 1  # MERGE not attempted


def test_record_hash_covers_all_raw_columns_and_matches_migration() -> None:
    expected = f"sha2(to_json(array({', '.join(ingest_trades.TRADE_COLUMNS)})), 256)"
    assert ingest_trades.RECORD_HASH_SQL == expected
    assert len(ingest_trades.TRADE_COLUMNS) == 14
    root = Path(__file__).parents[1]
    migration = (root / "database" / "migrations" / "add_bronze_trades_record_hash.sql").read_text(encoding="utf-8")
    ddl = (root / "database" / "tables" / "bronze" / "trades.sql").read_text(encoding="utf-8")
    assert f"SET record_hash = {expected}" in migration
    assert "WHERE record_hash IS NULL" in migration
    assert "ALTER TABLE cibc.bronze.trades ADD COLUMNS (record_hash STRING)" in migration
    assert "record_hash STRING" in ddl


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


def test_values_are_read_as_strings(spark, tmp_path) -> None:
    df = ingest_trades.read_trade_csv(spark, _csv(tmp_path, f"{HEADER}\n{ROW}\n"))
    assert {field.dataType.simpleString() for field in df.schema.fields} == {"string"}
    assert df.first()["price"] == "150.5"


def test_record_hash_is_deterministic_and_null_safe(spark) -> None:
    from pyspark.sql import functions as F

    values = ROW.split(",")
    with_null = [*values[:6], None, *values[7:]]
    with_empty = [*values[:6], "", *values[7:]]
    df = spark.createDataFrame([values, values, with_null, with_empty], list(ingest_trades.TRADE_COLUMNS))
    hashes = [row[0] for row in df.select(F.expr(ingest_trades.RECORD_HASH_SQL)).collect()]
    assert hashes[0] == hashes[1]
    assert len(set(hashes)) == 3  # NULL and "" are different raw records
    assert all(len(h) == 64 for h in hashes)

