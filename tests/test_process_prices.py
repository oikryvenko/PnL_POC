from __future__ import annotations

import sys
import types
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from trade_pipeline import config
from conftest import statements
from trade_pipeline.silver import process_prices


def test_parameter_defaults_and_overrides() -> None:
    tickers, start, end = process_prices.resolve_parameters(None, None, None)
    assert tickers == config.DEFAULT_TICKERS
    assert (start, end) == (date.today() - timedelta(days=config.LOOKBACK_DAYS), date.today())  # rolling default

    assert process_prices.resolve_parameters(" aapl,MSFT,aapl ", "2026-09-04", "2026-09-10") == (
        ["AAPL", "MSFT"],
        date(2026, 9, 4),
        date(2026, 9, 10),
    )


def test_parameter_validation_rejects_empty_tickers_and_invalid_date_range() -> None:
    with pytest.raises(ValueError, match="Ticker list must not be empty"):
        process_prices.resolve_parameters(" , ", None, None)
    with pytest.raises(ValueError, match="must not be later than"):
        process_prices.resolve_parameters("AAPL", "2026-09-11", "2026-09-10")
    with pytest.raises(ValueError, match="Invalid start_date"):
        process_prices.resolve_parameters("AAPL", "09/01/2026", "2026-09-03")


def test_single_day_range_is_allowed() -> None:
    assert process_prices.resolve_parameters("AAPL", "2026-09-10", "2026-09-10") == (
        ["AAPL"],
        date(2026, 9, 10),
        date(2026, 9, 10),
    )
    assert process_prices.requested_sessions(date(2026, 9, 10), date(2026, 9, 10), date(2026, 10, 1)) == [
        date(2026, 9, 10)
    ]


def test_requested_sessions_are_inclusive_and_skip_weekends_holidays_and_future() -> None:
    as_of = date(2026, 10, 1)
    # 2026-09-07 is Labor Day; 09-05/09-06 is a weekend.
    assert process_prices.requested_sessions(date(2026, 9, 4), date(2026, 9, 8), as_of) == [
        date(2026, 9, 4),
        date(2026, 9, 8),
    ]
    assert process_prices.requested_sessions(date(2026, 9, 5), date(2026, 9, 7), as_of) == []
    assert process_prices.requested_sessions(date(2026, 9, 29), date(2026, 10, 5), as_of) == [
        date(2026, 9, 29),
        date(2026, 9, 30),
        date(2026, 10, 1),
    ]


def test_expected_trading_sessions_excludes_weekends_and_labor_day() -> None:
    assert process_prices.expected_trading_sessions(date(2026, 9, 4), date(2026, 9, 9)) == [
        date(2026, 9, 4),
        date(2026, 9, 8),
    ]
    assert process_prices.expected_trading_sessions(date(2026, 9, 5), date(2026, 9, 8)) == []


def test_job_resource_has_one_job_and_ordered_task_dependency() -> None:
    job_config = Path(__file__).parents[1] / "jobs" / "resources" / "trade_pipeline.job.yml"
    content = job_config.read_text(encoding="utf-8")
    assert "name: trade_pipeline" in content
    assert len(re.findall(r"^        - task_key:", content, flags=re.MULTILINE)) == 6
    assert "task_key: ingest_prices" in content
    assert "task_key: process_prices" in content
    assert "depends_on:\n            - task_key: ingest_prices" in content
    assert "task_key: ingest_trades\n          environment_key:" in content
    assert "task_key: process_trades\n          depends_on:\n            - task_key: ingest_trades" in content
    assert "task_key: process_trades\n          depends_on:\n            - task_key: ingest_prices" not in content
    assert "task_key: build_positions\n          depends_on:\n            - task_key: process_prices" in content
    assert "            - task_key: process_trades" in content
    assert "task_key: calculate_daily_pnl\n          depends_on:\n            - task_key: build_positions" in content
    assert "{{job.parameters.tickers}}" in content
    assert "{{job.parameters.start_date}}" in content
    assert "{{job.parameters.end_date}}" in content


def test_silver_sql_definitions_match_contract() -> None:
    root = Path(__file__).parents[1]
    accepted_sql = (root / "database" / "tables" / "silver" / "market_prices.sql").read_text(
        encoding="utf-8"
    )
    rejects_sql = (
        root / "database" / "tables" / "silver" / "market_prices_rejects.sql"
    ).read_text(encoding="utf-8")

    assert "symbol STRING NOT NULL" in accepted_sql
    assert "price_date DATE NOT NULL" in accepted_sql
    assert "close DECIMAL(18,6) NOT NULL" in accepted_sql
    assert "currency STRING NOT NULL" in accepted_sql
    accepted_constraints = (
        root / "database" / "tables" / "silver" / "market_prices_constraints.sql"
    ).read_text(encoding="utf-8")
    assert "id BIGINT GENERATED ALWAYS AS IDENTITY" in accepted_sql
    assert "CHECK (close > 0)" in accepted_constraints
    assert "CHECK (currency = 'USD')" in accepted_constraints
    assert "rejection_reason STRING" in rejects_sql
    assert "rejected_at TIMESTAMP" in rejects_sql


def _local_spark():
    pyspark = pytest.importorskip("pyspark")
    return pyspark.sql.SparkSession.builder.master("local[1]").appName("silver-price-tests").getOrCreate()


def _bronze_rows(rows: list[tuple]):
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("symbol", StringType(), True),
            StructField("price_date", StringType(), True),
            StructField("close", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("source_api", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("run_id", StringType(), True),
        ]
    )
    default_timestamp = datetime(2026, 9, 1, tzinfo=timezone.utc)
    bronze_rows = [
        (*row, "yfinance.download", default_timestamp, "run-1") if len(row) == 4 else row
        for row in rows
    ]
    return bronze_rows, schema


def _complete_session_rows() -> list[tuple[str, str, str, str]]:
    return [
        ("AAPL", session.isoformat(), "150.0", "USD")
        for session in process_prices.expected_trading_sessions(
            date(2026, 9, 1), date(2026, 9, 9)
        )
    ]


@pytest.fixture(scope="module")
def spark():
    session = _local_spark()
    yield session
    session.stop()


def test_valid_prices_are_cast_to_silver_types(spark) -> None:
    rows, schema = _bronze_rows(
        [("AAPL", "2026-09-01", "150.123456", "USD"), ("AAPL", "2026-09-02", "151.0", "USD")]
    )
    bronze_df = spark.createDataFrame(rows, schema)

    silver_df, rejects_df = process_prices.prepare_and_validate_prices(
        bronze_df, spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 3)
    )
    result = silver_df.orderBy("price_date").collect()

    assert result[0]["price_date"] == date(2026, 9, 1)
    assert result[0]["close"] == Decimal("150.123456")
    assert result[1]["close"] == Decimal("151.000000")
    assert rejects_df.count() == 0


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([(None, "2026-09-01", "150", "USD"), ("AAPL", "2026-09-02", "151", "USD")], "NULL_SYMBOL"),
        ([(" ", "2026-09-01", "150", "USD"), ("AAPL", "2026-09-02", "151", "USD")], "EMPTY_SYMBOL"),
        ([("AAPL", "2026-09-01", None, "USD"), ("AAPL", "2026-09-02", "151", "USD")], "INVALID_CLOSE"),
        ([("AAPL", "2026-09-01", "-1", "USD"), ("AAPL", "2026-09-02", "151", "USD")], "NONPOSITIVE_CLOSE"),
        ([("AAPL", "2026-09-01", "0", "USD"), ("AAPL", "2026-09-02", "151", "USD")], "NONPOSITIVE_CLOSE"),
        ([("AAPL", "2026-09-01", "bad", "USD"), ("AAPL", "2026-09-02", "151", "USD")], "INVALID_CLOSE"),
        ([("AAPL", "not-a-date", "150", "USD"), ("AAPL", "2026-09-02", "151", "USD")], "INVALID_PRICE_DATE"),
        ([("AAPL", "2026-09-01", "150", "EUR"), ("AAPL", "2026-09-02", "151", "USD")], "INVALID_CURRENCY"),
        ([("AAPL", "2026-09-01", "150", "USD"), ("AAPL", "2026-09-05", "151", "USD")], "UNEXPECTED_TRADING_DATE"),
    ],
)
def test_invalid_price_rows_are_quarantined(spark, rows, message) -> None:
    bronze_rows, schema = _bronze_rows(_complete_session_rows() + rows)
    bronze_df = spark.createDataFrame(bronze_rows, schema)
    accepted, rejects = process_prices.prepare_and_validate_prices(
        bronze_df, spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 9)
    )
    assert accepted.count() > 0
    assert any(message in row["rejection_reason"] for row in rejects.collect())


def test_accepted_price_keeps_lineage_of_selected_bronze_row(spark) -> None:
    rows, schema = _bronze_rows(
        [
            ("AAPL", "2026-09-01", "150", "USD", "yfinance.download", datetime(2026, 9, 1, tzinfo=timezone.utc), "run-a"),
            ("AAPL", "2026-09-01", "151", "USD", "yfinance.download", datetime(2026, 9, 2, tzinfo=timezone.utc), "run-b"),
        ]
    )
    accepted, _ = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 1)
    )
    row = accepted.first()
    assert row["close"] == Decimal("151.000000")
    assert row["bronze_run_id"] == "run-b"  # the selected row, not the older one
    assert row["bronze_ingestion_timestamp"] is not None


def test_reprocessing_same_bronze_gives_identical_accepted_rows(spark) -> None:
    rows, schema = _bronze_rows([("AAPL", "2026-09-01", "150", "USD")])
    bronze = spark.createDataFrame(rows, schema)
    args = (bronze, spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 1))
    first, _ = process_prices.prepare_and_validate_prices(*args)
    second, _ = process_prices.prepare_and_validate_prices(*args)
    assert first.collect() == second.collect()  # same values and lineage, so the MERGE sees no change


def test_lineage_columns_are_nullable_in_ddl_and_migration() -> None:
    root = Path(__file__).parents[1] / "database"
    ddl = (root / "tables" / "silver" / "market_prices.sql").read_text(encoding="utf-8")
    migration = (root / "migrations" / "add_silver_lineage_columns.sql").read_text(encoding="utf-8")
    assert "bronze_run_id STRING," in ddl and "bronze_run_id STRING NOT NULL" not in ddl
    assert "bronze_ingestion_timestamp TIMESTAMP" in ddl
    assert "ALTER TABLE cibc.silver.market_prices ADD COLUMNS IF NOT EXISTS" in migration
    assert "DROP TABLE" not in migration and "UPDATE" not in migration  # no recreate, no backfill


def test_duplicate_business_key_keeps_latest_ingestion(spark) -> None:
    rows, schema = _bronze_rows(
        [
            ("AAPL", "2026-09-01", "150", "USD", "yfinance.download", datetime(2026, 9, 1, tzinfo=timezone.utc), "run-a"),
            ("AAPL", "2026-09-01", "151", "USD", "yfinance.download", datetime(2026, 9, 2, tzinfo=timezone.utc), "run-b"),
        ]
    )
    accepted, rejects = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 2)
    )
    assert accepted.count() == 1
    assert accepted.first()["close"] == Decimal("151.000000")
    assert rejects.first()["rejection_reason"] == "DUPLICATE_BUSINESS_KEY"


def test_tied_conflicting_duplicate_key_is_quarantined(spark) -> None:
    duplicate_time = datetime(2026, 9, 1, tzinfo=timezone.utc)
    rows, schema = _bronze_rows(
        [
            ("AAPL", "2026-09-01", "150", "USD", "yfinance.download", duplicate_time, "run-a"),
            ("AAPL", "2026-09-01", "151", "USD", "yfinance.download", duplicate_time, "run-a"),
        ]
    )
    accepted, rejects = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 2)
    )
    assert accepted.count() == 0
    assert rejects.count() == 2
    assert {row["rejection_reason"] for row in rejects.collect()} == {"AMBIGUOUS_DUPLICATE_KEY"}


def test_tied_identical_duplicates_keep_one_equivalent_row(spark) -> None:
    duplicate_time = datetime(2026, 9, 1, tzinfo=timezone.utc)
    duplicate = ("AAPL", "2026-09-01", "150", "USD", "yfinance.download", duplicate_time, "run-a")
    rows, schema = _bronze_rows([duplicate, duplicate])
    accepted, rejects = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 2)
    )

    assert accepted.count() == 1
    assert accepted.first()["close"] == Decimal("150.000000")
    assert rejects.count() == 1
    assert rejects.first()["rejection_reason"] == "DUPLICATE_BUSINESS_KEY"


def test_invalid_only_expected_session_is_quarantined_before_completeness_failure(spark) -> None:
    complete_rows = [
        row for row in _complete_session_rows() if row[1] != "2026-09-08"
    ]
    complete_rows.append(("AAPL", "2026-09-08", "not-a-price", "USD"))
    rows, schema = _bronze_rows(complete_rows)
    accepted, rejects = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 9)
    )
    assert any(row["rejection_reason"] == "INVALID_CLOSE" for row in rejects.collect())
    assert process_prices.find_missing_accepted_sessions(
        spark, accepted, ["AAPL"], date(2026, 9, 1), date(2026, 9, 9)
    )


def test_missing_expected_trading_session_fails(spark) -> None:
    rows, schema = _bronze_rows([("AAPL", "2026-09-01", "150", "USD")])
    accepted, _ = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 3)
    )
    assert process_prices.find_missing_accepted_sessions(
        spark, accepted, ["AAPL"], date(2026, 9, 1), date(2026, 9, 3)
    )


def test_run_processing_records_rejects_but_skips_silver_when_session_missing() -> None:
    spark = MagicMock()
    accepted = MagicMock()
    rejects = MagicMock()
    missing = [{"symbol": "AAPL", "price_date": date(2026, 9, 2)}]
    with patch.object(
        process_prices.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(process_prices, "scope_prices"), patch.object(
        process_prices, "prepare_and_validate_prices", return_value=(accepted, rejects)
    ), patch.object(
        process_prices, "find_missing_accepted_sessions", return_value=missing
    ), patch.object(process_prices, "write_rejects_history") as write_rejects, patch.object(
        process_prices, "write_silver_prices"
    ) as write_silver:
        with pytest.raises(ValueError, match=r"Silver not updated: AAPL 2026-09-02"):
            process_prices.run_processing("AAPL", "2026-09-01", "2026-09-03", spark=spark)
    write_rejects.assert_called_once_with(spark, rejects)
    write_silver.assert_not_called()


def test_empty_bronze_snapshot_fails(spark) -> None:
    rows, schema = _bronze_rows([])
    with pytest.raises(ValueError, match="Bronze market-price snapshot is empty"):
        process_prices.prepare_and_validate_prices(
            spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 1), date(2026, 9, 2)
        )


def test_validation_failure_writes_nothing() -> None:
    spark = MagicMock()
    with patch.object(
        process_prices.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(process_prices, "scope_prices"), patch.object(
        process_prices, "prepare_and_validate_prices", side_effect=ValueError("invalid")
    ), patch.object(
        process_prices, "write_silver_prices"
    ) as write_silver, patch.object(process_prices, "write_rejects_history") as write_rejects:
        with pytest.raises(ValueError, match="invalid"):
            process_prices.run_processing("AAPL", "2026-09-01", "2026-09-02", spark=spark)
    write_silver.assert_not_called()
    write_rejects.assert_not_called()


def test_silver_price_merge_preserves_history_and_skips_unchanged_rows(stats_spark) -> None:
    prices_df = MagicMock()
    counts = process_prices.write_silver_prices(stats_spark, prices_df)
    prices_df.createOrReplaceTempView.assert_called_once_with(
        "_trade_pipeline_silver_market_prices_staging"
    )
    sql = statements(stats_spark)[-1]
    assert "MERGE INTO cibc.silver.market_prices" in sql
    assert "ON target.symbol = source.symbol AND target.price_date = source.price_date" in sql
    assert "NOT MATCHED BY SOURCE" not in sql
    assert "DELETE" not in sql
    assert "WHEN MATCHED AND NOT (" in sql
    assert "target.close <=> source.close" in sql
    insert = sql[sql.index("WHEN NOT MATCHED THEN INSERT") :]
    assert "ingestion_timestamp, run_id," in insert
    assert "bronze_ingestion_timestamp, bronze_run_id)" in insert
    assert "bronze_run_id = source.bronze_run_id" in sql  # lineage is updated with the changed row
    unchanged_check = sql[sql.index("WHEN MATCHED AND NOT (") : sql.index("THEN UPDATE SET")]
    assert "bronze" not in unchanged_check  # lineage alone must not trigger a rewrite
    assert "(id" not in insert and " id," not in insert
    assert counts == {"inserted_rows": 2, "updated_rows": 1, "deleted_rows": 0}


def test_reject_history_is_idempotent_merge_without_identity_column(stats_spark) -> None:
    rejects_df = MagicMock()
    new_rows = process_prices.write_rejects_history(stats_spark, rejects_df)
    rejects_df.dropDuplicates.assert_called_once_with(list(process_prices.REJECT_KEY_COLUMNS))
    sql = statements(stats_spark)[-1]
    assert "MERGE INTO cibc.silver.market_prices_rejects" in sql
    assert "INSERT OVERWRITE" not in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "WHEN MATCHED" not in sql
    assert "target.symbol <=> source.symbol" in sql
    assert "target.rejection_reason <=> source.rejection_reason" in sql
    assert " id" not in sql.split("INSERT (")[1].split(")")[0]
    assert new_rows == 2  # inserted rows reported by the MERGE


def _fake_pyspark():
    return {name: MagicMock() for name in ("pyspark", "pyspark.sql", "pyspark.sql.functions")}


def test_run_processing_metrics_do_not_count_unchanged_rows_as_written(stats_spark) -> None:
    scoped, prices, rejects = MagicMock(), MagicMock(), MagicMock()
    scoped.count.return_value = 7
    prices.count.return_value = 7
    rejects.count.return_value = 3
    rejects.filter.return_value.count.return_value = 1
    with patch.dict(sys.modules, _fake_pyspark()), patch.object(
        process_prices.audit, "run_audited_task", side_effect=lambda *args: args[-1]()
    ), patch.object(process_prices, "scope_prices", return_value=scoped), patch.object(
        process_prices, "prepare_and_validate_prices", return_value=(prices, rejects)
    ), patch.object(process_prices, "find_missing_accepted_sessions", return_value=[]), patch.object(
        process_prices, "write_rejects_history", return_value=2
    ), patch.object(
        process_prices,
        "write_silver_prices",
        return_value={"inserted_rows": 2, "updated_rows": 1, "deleted_rows": 0},
    ):
        summary = process_prices.run_processing("AAPL", "2026-09-01", "2026-09-02", spark=stats_spark)
    assert summary["input_rows"] == 7  # scoped Bronze rows only, not all history
    assert summary["accepted_rows"] == 7
    assert summary["output_rows"] == 7  # valid rows produced, including unchanged ones
    assert summary["rows_written"] == 3  # inserted + updated only
    assert summary["rejected_rows"] == 3
    assert summary["rejects_written"] == 2  # new reject-history rows only
    assert summary["duplicate_rows"] == 1

def test_out_of_scope_valid_history_is_not_rejected(spark) -> None:
    rows, schema = _bronze_rows(
        [
            ("AAPL", "2026-08-03", "100", "USD"),  # earlier history
            ("MSFT", "2026-09-02", "300", "USD"),  # other ticker
            ("AAPL", "2026-09-02", "151", "USD"),
        ]
    )
    accepted, rejects = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 2), date(2026, 9, 2)
    )
    assert [(r["symbol"], r["price_date"]) for r in accepted.collect()] == [("AAPL", date(2026, 9, 2))]
    assert rejects.count() == 0


def test_unparseable_date_is_quarantined_only_for_requested_tickers(spark) -> None:
    rows, schema = _bronze_rows(
        [
            ("AAPL", "garbage", "150", "USD"),
            ("MSFT", "garbage", "300", "USD"),  # unrelated malformed history is not this run's concern
            ("AAPL", "2026-09-02", "151", "USD"),
        ]
    )
    accepted, rejects = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 2), date(2026, 9, 2)
    )
    assert accepted.count() == 1
    assert [(r["symbol"], r["rejection_reason"]) for r in rejects.collect()] == [
        ("AAPL", "INVALID_PRICE_DATE")
    ]


def test_future_price_is_rejected_and_not_expected(spark) -> None:
    rows, schema = _bronze_rows(
        [("AAPL", "2026-09-02", "151", "USD"), ("AAPL", "2026-09-03", "152", "USD")]
    )
    as_of = date(2026, 9, 2)
    accepted, rejects = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 2), date(2026, 9, 3), as_of
    )
    assert [r["price_date"] for r in accepted.collect()] == [date(2026, 9, 2)]
    assert [r["rejection_reason"] for r in rejects.collect()] == ["FUTURE_PRICE_DATE"]
    assert not process_prices.find_missing_accepted_sessions(
        spark, accepted, ["AAPL"], date(2026, 9, 2), date(2026, 9, 3), as_of
    )


def test_weekend_only_range_expects_nothing(spark) -> None:
    rows, schema = _bronze_rows([("AAPL", "2026-09-02", "151", "USD")])
    accepted, rejects = process_prices.prepare_and_validate_prices(
        spark.createDataFrame(rows, schema), spark, ["AAPL"], date(2026, 9, 5), date(2026, 9, 6)
    )
    assert accepted.count() == 0 and rejects.count() == 0
    assert not process_prices.find_missing_accepted_sessions(
        spark, accepted, ["AAPL"], date(2026, 9, 5), date(2026, 9, 6)
    )
