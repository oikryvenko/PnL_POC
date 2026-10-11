from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trade_pipeline import config

ROOT = Path(__file__).parents[1]
CSV_PATH = ROOT / "data" / "dirty_trades.csv"
SOURCE_COLUMNS = (
    "trade_id",
    "counterparty_id",
    "instrument_id",
    "symbol",
    "side",
    "quantity",
    "price",
    "currency",
    "trade_timestamp",
    "trade_date",
    "settlement_date",
    "book_id",
    "version",
    "modified_timestamp",
)


def test_existing_trade_csv_is_available_and_has_source_schema() -> None:
    with CSV_PATH.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        rows = list(reader)
    assert tuple(reader.fieldnames or ()) == SOURCE_COLUMNS
    assert len(rows) == 1200
    assert all("counterparty_id" in row for row in rows)
    assert any(row["side"] == "SELL" for row in rows)


def test_trade_volume_path_and_sql_identity_schemas() -> None:
    assert config.TRADE_CSV_PATH == "/Volumes/cibc/bronze/trade_files/dirty_trades.csv"
    trades_sql = (ROOT / "database" / "tables" / "silver" / "trades.sql").read_text(encoding="utf-8")
    prices_sql = (ROOT / "database" / "tables" / "silver" / "market_prices.sql").read_text(encoding="utf-8")
    rejects_sql = (ROOT / "database" / "tables" / "silver" / "trades_rejects.sql").read_text(encoding="utf-8")
    assert "id BIGINT GENERATED ALWAYS AS IDENTITY" in trades_sql
    assert "counterparty_id STRING NOT NULL" in trades_sql
    assert "symbol STRING NOT NULL" in trades_sql
    assert "id BIGINT GENERATED ALWAYS AS IDENTITY" in prices_sql
    assert "rejection_reason STRING" in rejects_sql
    assert "source_file STRING" in rejects_sql
    trade_constraints = (
        ROOT / "database" / "tables" / "silver" / "trades_constraints.sql"
    ).read_text(encoding="utf-8")
    assert "CHECK (quantity <> 0)" in trade_constraints
    assert "CHECK (price > 0)" in trade_constraints


def test_silver_writers_omit_identity_columns_and_use_business_keys() -> None:
    trades_source = (ROOT / "src" / "trade_pipeline" / "silver" / "process_trades.py").read_text(
        encoding="utf-8"
    )
    prices_source = (ROOT / "src" / "trade_pipeline" / "silver" / "process_prices.py").read_text(
        encoding="utf-8"
    )
    silver_columns = re.search(r"SILVER_COLUMNS\s*=\s*\((.*?)\)", trades_source, re.DOTALL)
    assert silver_columns is not None
    assert "trade_row_id" not in silver_columns.group(1)
    assert '"symbol"' in silver_columns.group(1)
    assert "MERGE INTO {SILVER_TABLE}" in trades_source
    assert "ON target.trade_id = source.trade_id" in trades_source
    assert "price_id," not in prices_source
    assert "source.price_id" not in prices_source
    assert "ON target.symbol = source.symbol AND target.price_date = source.price_date" in prices_source


def test_job_config_has_independent_parallel_branches() -> None:
    job_file = ROOT / "jobs" / "resources" / "trade_pipeline.job.yml"
    content = job_file.read_text(encoding="utf-8")
    task_keys = re.findall(r"^        - task_key: (\w+)", content, flags=re.MULTILINE)
    assert task_keys == [
        "ingest_prices", "process_prices", "ingest_trades", "process_trades",
        "build_positions", "calculate_daily_pnl",
    ]
    assert "task_key: process_prices\n          depends_on:\n            - task_key: ingest_prices" in content
    assert "task_key: process_trades\n          depends_on:\n            - task_key: ingest_trades" in content
    assert (
        "task_key: build_positions\n          depends_on:\n            - task_key: process_prices\n"
        "            - task_key: process_trades"
    ) in content
    assert "task_key: calculate_daily_pnl\n          depends_on:\n            - task_key: build_positions" in content
    assert content.count("{{job.run_id}}") == 6
    assert content.count("{{task.run_id}}") == 6
    assert content.count("{{task.execution_count}}") == 6


def _spark_or_skip():
    pyspark = pytest.importorskip("pyspark")
    return pyspark.sql.SparkSession.builder.master("local[1]").appName("trade-tests").getOrCreate()


@pytest.fixture(scope="module")
def spark():
    session = _spark_or_skip()
    yield session
    session.stop()


def _bronze_schema(include_metadata: bool = True):
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    fields = [StructField(name, StringType(), True) for name in SOURCE_COLUMNS]
    if include_metadata:
        fields.extend(
            [
                StructField("source_file", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), True),
                StructField("run_id", StringType(), True),
            ]
        )
    return StructType(fields)


def test_generated_csv_produces_silver_accepts_and_rejects(spark) -> None:
    from trade_pipeline.silver.process_trades import prepare_trades

    source = (
        spark.read.option("header", "true")
        .schema(_bronze_schema(include_metadata=False))
        .csv(str(CSV_PATH))
    )
    from pyspark.sql import functions as F

    bronze = (
        source.withColumn("source_file", F.lit(str(CSV_PATH)))
        .withColumn("ingestion_timestamp", F.lit(datetime(2026, 10, 11, tzinfo=timezone.utc)))
        .withColumn("run_id", F.lit("local-trade-test"))
    )
    accepted, rejects, duplicate_count = prepare_trades(bronze)
    assert accepted.count() > 0
    assert rejects.count() > 0
    assert duplicate_count > 0
    assert accepted.select("trade_id").distinct().count() == accepted.count()
    assert accepted.filter(F.col("quantity") == 0).count() == 0
    assert accepted.filter(F.col("price") <= 0).count() == 0


def test_sell_quantity_is_signed_negative(spark) -> None:
    from trade_pipeline.silver.process_trades import prepare_trades

    timestamp = datetime(2026, 9, 1, 15, tzinfo=timezone.utc)
    record = (
        "SELL-1", "CP001", "INS001", "AAPL", "SELL", "25", "150.25", "USD",
        "2026-09-01T15:00:00Z", "2026-09-01", "2026-09-02", "BOOK_TECH", "1",
        "2026-09-01T16:00:00Z", "local.csv", timestamp, "test-run",
    )
    accepted, rejects, _ = prepare_trades(spark.createDataFrame([record], _bronze_schema()))
    assert accepted.count() == 1
    assert accepted.first()["quantity"] < 0
    assert rejects.count() == 0


def test_invalid_latest_version_does_not_fall_back_to_older_valid(spark) -> None:
    from trade_pipeline.silver.process_trades import prepare_trades

    timestamp = datetime(2026, 9, 1, 15, tzinfo=timezone.utc)
    older = (
        "REV-1", "CP001", "INS001", "AAPL", "BUY", "10", "150", "USD",
        "2026-09-01T15:00:00Z", "2026-09-01", "2026-09-02", "BOOK_TECH", "1",
        "2026-09-01T16:00:00Z", "local.csv", timestamp, "test-run",
    )
    invalid_latest = (*older[:12], "2", older[13], *older[14:])
    invalid_latest = (*invalid_latest[:6], "bad-price", *invalid_latest[7:])
    accepted, rejects, _ = prepare_trades(spark.createDataFrame([older, invalid_latest], _bronze_schema()))
    assert accepted.count() == 0
    assert {row["rejection_reason"] for row in rejects.collect()} >= {
        "INVALID_PRICE",
        "SUPERSEDED_BY_INVALID_LATEST_VERSION",
    }


def _trade(trade_id="T-1", side="BUY", quantity="10", price="150", trade_date="2026-09-01",
           settlement_date="2026-09-02", version="1", modified="2026-09-01T16:00:00Z",
           trade_timestamp=None):
    timestamp = datetime(2026, 9, 1, 15, tzinfo=timezone.utc)
    return (
        trade_id, "CP001", "INS001", "AAPL", side, quantity, price, "USD",
        trade_timestamp or f"{trade_date}T15:00:00Z", trade_date, settlement_date, "BOOK_TECH",
        version, modified, "local.csv", timestamp, "test-run",
    )


def _prepare(spark, *records):
    from trade_pipeline.silver.process_trades import prepare_trades

    return prepare_trades(spark.createDataFrame(list(records), _bronze_schema()))


def _reasons(rejects) -> list[str]:
    return [reason for row in rejects.collect() for reason in row["rejection_reason"].split("|")]


def test_historical_trade_outside_valuation_window_is_accepted(spark) -> None:
    accepted, rejects, _ = _prepare(
        spark, _trade("OLD-1", trade_date="2026-06-01", settlement_date="2026-06-02")
    )
    assert accepted.count() == 1 and rejects.count() == 0


def test_future_trade_date_is_rejected_once(spark) -> None:
    future = _trade("FUT", trade_date="2999-01-04", settlement_date="2999-01-05")
    accepted, rejects, _ = _prepare(spark, future)
    assert accepted.count() == 0
    assert _reasons(rejects) == ["FUTURE_TRADE_DATE"]  # not also NON_TRADING_TRADE_DATE


def test_weekend_and_holiday_trade_dates_are_rejected_once(spark) -> None:
    saturday = _trade("WKND", trade_date="2026-09-05", settlement_date="2026-09-08")
    labor_day = _trade("HOL", trade_date="2026-09-07", settlement_date="2026-09-08")
    accepted, rejects, _ = _prepare(spark, saturday, labor_day)
    assert accepted.count() == 0
    assert _reasons(rejects) == ["NON_TRADING_TRADE_DATE", "NON_TRADING_TRADE_DATE"]


def test_settlement_skips_weekend_and_holiday(spark) -> None:
    accepted, rejects, _ = _prepare(
        spark, _trade("FRI", trade_date="2026-09-04", settlement_date="2026-09-08")
    )
    assert accepted.count() == 1 and rejects.count() == 0


def test_settlement_reasons_are_not_duplicated(spark) -> None:
    _, rejects, _ = _prepare(
        spark, _trade("EARLY", settlement_date="2026-08-31"), _trade("WRONG", settlement_date="2026-09-03")
    )
    assert sorted(_reasons(rejects)) == ["INVALID_SETTLEMENT_CONVENTION", "INVALID_SETTLEMENT_SEQUENCE"]


def test_invalid_timestamp_reason_appears_once(spark) -> None:
    _, rejects, _ = _prepare(spark, _trade(trade_timestamp="not-a-time"))
    assert _reasons(rejects).count("INVALID_TRADE_TIMESTAMP") == 1


def test_buy_negative_rejected_and_sell_sign_is_normalised(spark) -> None:
    accepted, rejects, _ = _prepare(
        spark,
        _trade("B-NEG", side="BUY", quantity="-5"),
        _trade("S-POS", side="SELL", quantity="5"),
        _trade("S-NEG", side="SELL", quantity="-7"),
    )
    assert {row["trade_id"]: row["quantity"] for row in accepted.collect()} == {"S-POS": -5, "S-NEG": -7}
    assert _reasons(rejects) == ["QUANTITY_SIDE_MISMATCH"]


def test_accepted_trade_keeps_lineage_of_selected_latest_version(spark) -> None:
    older = _trade("LIN", quantity="10", version="1")
    newer = _trade("LIN", quantity="12", version="2", modified="2026-09-01T17:00:00Z")
    older = (*older[:14], "old.csv", datetime(2026, 9, 1, tzinfo=timezone.utc), "run-old")
    newer = (*newer[:14], "new.csv", datetime(2026, 9, 2, tzinfo=timezone.utc), "run-new")
    accepted, _, _ = _prepare(spark, older, newer)
    row = accepted.first()
    assert row["version"] == 2
    assert (row["bronze_run_id"], row["source_file"]) == ("run-new", "new.csv")
    assert row["bronze_ingestion_timestamp"] is not None


def test_reprocessing_same_bronze_gives_identical_accepted_trades(spark) -> None:
    first, _, _ = _prepare(spark, _trade("SAME"))
    second, _, _ = _prepare(spark, _trade("SAME"))
    assert first.collect() == second.collect()


def test_trade_merge_writes_lineage_but_ignores_it_in_unchanged_check(stats_spark) -> None:
    from conftest import statements
    from trade_pipeline.silver import process_trades

    process_trades.write_silver_trades(stats_spark, MagicMock(), MagicMock())
    merge_sql = statements(stats_spark)[0]
    unchanged_check = merge_sql[merge_sql.index("WHEN MATCHED AND NOT (") : merge_sql.index("THEN UPDATE SET")]
    assert "bronze" not in unchanged_check and "source_file" not in unchanged_check
    for column in ("bronze_run_id", "bronze_ingestion_timestamp", "source_file"):
        assert f"{column} = source.{column}" in merge_sql  # UPDATE
        assert f"source.{column}" in merge_sql.split("THEN INSERT")[1]  # INSERT
    assert "(id" not in merge_sql and " id =" not in merge_sql  # identity id is never written


def test_lineage_columns_are_nullable_in_trades_ddl_and_migration() -> None:
    ddl = (ROOT / "database" / "tables" / "silver" / "trades.sql").read_text(encoding="utf-8")
    migration = (ROOT / "database" / "migrations" / "add_silver_lineage_columns.sql").read_text(encoding="utf-8")
    for line in ("bronze_run_id STRING,", "bronze_ingestion_timestamp TIMESTAMP,", "source_file STRING"):
        assert line in ddl
    assert "NOT NULL" not in ddl.split("modified_timestamp TIMESTAMP NOT NULL")[1]  # old rows with NULL lineage stay valid
    assert "ALTER TABLE cibc.silver.trades ADD COLUMNS IF NOT EXISTS" in migration


def test_selected_latest_version_wins_and_older_is_a_duplicate_reject(spark) -> None:
    v1 = _trade("DUP", quantity="10", version="1")
    v2 = _trade("DUP", quantity="12", version="2", modified="2026-09-01T17:00:00Z")
    accepted, rejects, duplicate_count = _prepare(spark, v1, v2)
    assert [(r["version"], r["quantity"]) for r in accepted.collect()] == [(2, 12)]
    assert _reasons(rejects) == ["DUPLICATE_TRADE_VERSION"]
    assert duplicate_count == 1


def test_conflicting_latest_versions_are_ambiguous(spark) -> None:
    accepted, rejects, _ = _prepare(spark, _trade("AMB", quantity="10", version="2"), _trade("AMB", quantity="11", version="2"))
    assert accepted.count() == 0
    assert set(_reasons(rejects)) == {"AMBIGUOUS_TRADE_VERSION"}


def test_unparseable_version_blocks_older_valid_version(spark) -> None:
    accepted, rejects, _ = _prepare(spark, _trade("VER", version="1"), _trade("VER", version="two", quantity="99"))
    assert accepted.count() == 0
    assert set(_reasons(rejects)) == {"INVALID_VERSION", "SUPERSEDED_BY_INVALID_LATEST_VERSION"}


def test_selection_is_repeatable_regardless_of_input_order(spark) -> None:
    v1 = _trade("ORD", quantity="10", version="1")
    v2 = _trade("ORD", quantity="12", version="2", modified="2026-09-01T17:00:00Z")
    first, _, _ = _prepare(spark, v1, v2)
    second, _, _ = _prepare(spark, v2, v1)
    assert first.collect()[0]["quantity"] == second.collect()[0]["quantity"] == 12


def test_trade_silver_writer_keeps_history_and_omits_identity(stats_spark) -> None:
    from conftest import statements
    from trade_pipeline.silver import process_trades

    counts = process_trades.write_silver_trades(stats_spark, MagicMock(), MagicMock())
    merge_sql, delete_sql = statements(stats_spark)
    assert "NOT MATCHED BY SOURCE" not in merge_sql
    assert "WHEN MATCHED AND NOT (" in merge_sql
    assert "target.quantity <=> source.quantity" in merge_sql  # unchanged rows are not rewritten
    assert "trade_row_id" not in merge_sql and "(id" not in merge_sql
    # Only ids Bronze still lists with an unusable latest version are deleted.
    assert "WHERE trade_id IN (SELECT trade_id FROM" in delete_sql
    assert counts == {"inserted_rows": 2, "updated_rows": 1, "deleted_rows": 3}


def test_trade_rejects_are_idempotent_history(stats_spark) -> None:
    from conftest import statements
    from trade_pipeline.silver import process_trades

    new_rows = process_trades.write_trade_rejects_history(stats_spark, MagicMock())
    (sql,) = statements(stats_spark)
    assert "INSERT OVERWRITE" not in sql
    assert "MERGE INTO cibc.silver.trades_rejects" in sql
    assert "WHEN MATCHED" not in sql
    assert "run_id <=>" not in sql  # Bronze run metadata is not part of the reject identity
    assert "(id" not in sql
    assert new_rows == 2


def _fake_pyspark():
    modules = {name: MagicMock() for name in ("pyspark", "pyspark.sql", "pyspark.sql.functions")}
    modules["pyspark.sql"].functions.length.return_value.__gt__.return_value = MagicMock()
    return modules


def test_trade_run_metrics_do_not_count_unchanged_rows_as_written(stats_spark) -> None:
    import sys
    from unittest.mock import patch

    from trade_pipeline.silver import process_trades

    accepted, rejects = MagicMock(), MagicMock()
    accepted.count.return_value = 9
    rejects.count.return_value = 4
    counts = {"inserted_rows": 1, "updated_rows": 2, "deleted_rows": 1}
    with patch.dict(sys.modules, _fake_pyspark()), patch.object(
        process_trades.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(process_trades, "prepare_trades", return_value=(accepted, rejects, 2)), patch.object(
        process_trades, "write_trade_rejects_history", return_value=3
    ), patch.object(process_trades, "write_silver_trades", return_value=counts):
        summary = process_trades.run_processing(spark=stats_spark)
    assert summary["output_rows"] == 9 and summary["rows_written"] == 3  # 9 valid, only 3 changed
    assert summary["accepted_rows"] == 9 and summary["deleted_rows"] == 1
    assert summary["rejected_rows"] == 4 and summary["rejects_written"] == 3


def test_all_invalid_snapshot_does_not_touch_silver(stats_spark) -> None:
    import sys
    from unittest.mock import patch

    from trade_pipeline.silver import process_trades

    accepted = MagicMock()
    accepted.count.return_value = 0
    with patch.dict(sys.modules, _fake_pyspark()), patch.object(
        process_trades.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(process_trades, "prepare_trades", return_value=(accepted, MagicMock(), 0)), patch.object(
        process_trades, "write_silver_trades"
    ) as write:
        with pytest.raises(ValueError, match="Silver trades not changed"):
            process_trades.run_processing(spark=stats_spark)
    write.assert_not_called()
