from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trade_pipeline.gold import build_positions

ROOT = Path(__file__).parents[1]


def test_positions_schema_and_identity_ddl() -> None:
    sql = (ROOT / "database" / "tables" / "gold" / "positions.sql").read_text(encoding="utf-8")
    assert "id BIGINT GENERATED ALWAYS AS IDENTITY" in sql
    for required in (
        "valuation_date DATE NOT NULL",
        "book_id STRING NOT NULL",
        "counterparty_id STRING NOT NULL",
        "instrument_id STRING NOT NULL",
        "currency STRING NOT NULL",
        "daily_buy_quantity DECIMAL(28,6) NOT NULL",
        "daily_sell_quantity DECIMAL(28,6) NOT NULL",
        "daily_net_quantity DECIMAL(28,6) NOT NULL",
        "closing_position_quantity DECIMAL(28,6) NOT NULL",
        "closing_price DECIMAL(18,6)",
        "market_value DECIMAL(38,6)",
    ):
        assert required in sql


def test_every_fresh_silver_and_gold_table_uses_exact_identity_name() -> None:
    ddl_paths = [
        ROOT / "database" / "tables" / "silver" / name
        for name in (
            "market_prices.sql",
            "market_prices_rejects.sql",
            "trades.sql",
            "trades_rejects.sql",
        )
    ] + [
        ROOT / "database" / "tables" / "gold" / name
        for name in ("positions.sql", "valuation_breaks.sql", "pipeline_run_log.sql")
    ]
    for ddl_path in ddl_paths:
        ddl = ddl_path.read_text(encoding="utf-8")
        assert "id BIGINT GENERATED ALWAYS AS IDENTITY" in ddl_path.read_text(encoding="utf-8"), ddl_path
        assert "price_id BIGINT" not in ddl
        assert "trade_row_id BIGINT" not in ddl


def test_position_writer_merges_by_business_key_and_omits_id(stats_spark) -> None:
    spark = stats_spark
    positions_df = MagicMock()
    build_positions.write_positions_snapshot(spark, positions_df)
    positions_df.createOrReplaceTempView.assert_called_once_with(
        "_trade_pipeline_gold_positions_source"
    )
    sql = spark.sql.call_args.args[0]
    assert "MERGE INTO cibc.gold.positions" in sql
    assert "target.valuation_date = source.valuation_date" in sql
    assert "target.book_id = source.book_id" in sql
    assert "target.counterparty_id = source.counterparty_id" in sql
    assert "target.instrument_id = source.instrument_id" in sql
    assert "target.currency = source.currency" in sql
    inserted_columns = [
        column.strip()
        for column in re.search(r"WHEN NOT MATCHED THEN INSERT \((.*?)\) VALUES", sql, re.S)
        .group(1)
        .split(",")
    ]
    assert "id" not in inserted_columns
    assert "THEN DELETE" in sql


def test_price_uniqueness_validation_uses_business_key_symbol_and_date() -> None:
    source = (ROOT / "src" / "trade_pipeline" / "gold" / "build_positions.py").read_text(
        encoding="utf-8"
    )
    assert 'groupBy("symbol", "price_date")' in source
    assert 'groupBy("symbol", "price_date", "currency")' not in source


def test_build_positions_depends_on_both_silver_tasks() -> None:
    yaml = (ROOT / "jobs" / "resources" / "trade_pipeline.job.yml").read_text(encoding="utf-8")
    assert re.search(
        r"task_key: build_positions\s+depends_on:\s+- task_key: process_prices\s+- task_key: process_trades",
        yaml,
    )
    assert yaml.count("{{job.run_id}}") == 6


def test_position_transform_carries_forward_and_joins_by_instrument_date_currency() -> None:
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql.types import (
        DateType,
        DecimalType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    spark = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("gold-position-tests")
        .getOrCreate()
    )
    try:
        trade_schema = StructType(
            [
                StructField("id", LongType(), False),
                StructField("trade_id", StringType(), False),
                StructField("counterparty_id", StringType(), False),
                StructField("instrument_id", StringType(), False),
                StructField("quantity", DecimalType(18, 6), False),
                StructField("price", DecimalType(18, 6), False),
                StructField("currency", StringType(), False),
                StructField("trade_date", DateType(), False),
                StructField("settlement_date", DateType(), False),
                StructField("book_id", StringType(), False),
                StructField("version", pyspark.sql.types.IntegerType(), False),
                StructField("modified_timestamp", TimestampType(), False),
            ]
        )
        trades = spark.createDataFrame(
            [
                (901, "T1", "CP1", "INS001", Decimal("100"), Decimal("10"), "USD", date(2026, 9, 1), date(2026, 9, 2), "BOOK_A", 1, datetime(2026, 9, 1)),
                (902, "T2", "CP1", "INS001", Decimal("-30"), Decimal("11"), "USD", date(2026, 9, 2), date(2026, 9, 3), "BOOK_A", 1, datetime(2026, 9, 2)),
            ],
            trade_schema,
        )
        price_schema = StructType(
            [
                StructField("id", LongType(), False),
                StructField("symbol", StringType(), False),
                StructField("price_date", DateType(), False),
                StructField("close", DecimalType(18, 6), False),
                StructField("currency", StringType(), False),
                StructField("source_api", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), True),
                StructField("run_id", StringType(), True),
            ]
        )
        prices = spark.createDataFrame(
            [
                (45, "AAPL", date(2026, 9, 1), Decimal("10"), "USD", "test", None, "run"),
                (46, "AAPL", date(2026, 9, 2), Decimal("11"), "USD", "test", None, "run"),
                (47, "MSFT", date(2026, 9, 3), Decimal("20"), "USD", "test", None, "run"),
            ],
            price_schema,
        )
        positions, breaks = build_positions.prepare_positions(trades, prices, "run-1")
        rows = positions.orderBy("valuation_date").collect()
        assert len(rows) == 3
        assert [row["closing_position_quantity"] for row in rows] == [
            Decimal("100.000000"), Decimal("70.000000"), Decimal("70.000000")
        ]
        assert [row["daily_buy_quantity"] for row in rows] == [
            Decimal("100.000000"), Decimal("0.000000"), Decimal("0.000000")
        ]
        assert [row["daily_sell_quantity"] for row in rows] == [
            Decimal("0.000000"), Decimal("30.000000"), Decimal("0.000000")
        ]
        assert [row["market_value"] for row in rows[:2]] == [
            Decimal("1000.000000"), Decimal("770.000000")
        ]
        assert rows[2]["closing_price"] is None
        assert rows[2]["market_value"] is None
        assert breaks.count() == 1
    finally:
        spark.stop()


TRADE_DDL = (
    "trade_id string, counterparty_id string, instrument_id string, quantity decimal(18,6), "
    "price decimal(18,6), currency string, trade_date date, settlement_date date, book_id string, "
    "version int, modified_timestamp timestamp"
)
PRICE_DDL = "symbol string, price_date date, close decimal(18,6), currency string"


def _trade(trade_id, day, quantity, price="10", book="BOOK_A", cp="CP1", instrument="INS001"):
    return (
        trade_id, cp, instrument, Decimal(str(quantity)), Decimal(price), "USD",
        day, day, book, 1, datetime(2026, 9, 1),
    )


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    session = pyspark.sql.SparkSession.builder.master("local[1]").appName("positions").getOrCreate()
    yield session
    session.stop()


def _positions(spark, trades, prices):
    trades_df = spark.createDataFrame(trades, TRADE_DDL)
    prices_df = spark.createDataFrame(prices, PRICE_DDL)
    positions, breaks = build_positions.prepare_positions(trades_df, prices_df, "run-1")
    return {row["valuation_date"]: row for row in positions.collect()}, breaks


MON, TUE, WED = date(2026, 9, 14), date(2026, 9, 15), date(2026, 9, 16)


def test_trade_on_day_without_price_still_counts_in_closing_position(spark) -> None:
    # BUY 100 Monday, SELL 30 Tuesday (no Tuesday price), price again on Wednesday.
    rows, breaks = _positions(
        spark,
        [_trade("T1", MON, 100), _trade("T2", TUE, -30)],
        [("AAPL", MON, Decimal("10"), "USD"), ("AAPL", WED, Decimal("12"), "USD")],
    )
    assert rows[MON]["closing_position_quantity"] == Decimal("100")
    assert rows[TUE]["closing_position_quantity"] == Decimal("70")
    assert rows[TUE]["daily_sell_quantity"] == Decimal("30")
    assert rows[TUE]["closing_price"] is None and rows[TUE]["market_value"] is None
    assert rows[WED]["closing_position_quantity"] == Decimal("70")
    assert rows[WED]["market_value"] == Decimal("840")
    assert [(b["valuation_date"], b["break_type"]) for b in breaks.collect()] == [
        (TUE, "MISSING_MARKET_PRICE")
    ]


def test_short_position_and_no_trade_day(spark) -> None:
    rows, _ = _positions(
        spark,
        [_trade("S1", MON, -40)],
        [("AAPL", MON, Decimal("10"), "USD"), ("AAPL", TUE, Decimal("11"), "USD")],
    )
    assert rows[MON]["closing_position_quantity"] == Decimal("-40")
    assert rows[TUE]["daily_net_quantity"] == Decimal("0")  # no trades, position carried forward
    assert rows[TUE]["closing_position_quantity"] == Decimal("-40")
    assert rows[TUE]["market_value"] == Decimal("-440")


def test_position_writer_updates_only_changed_values(stats_spark) -> None:
    from conftest import statements

    counts = build_positions.write_positions_snapshot(stats_spark, MagicMock())
    sql = statements(stats_spark)[-1]
    assert "WHEN MATCHED AND NOT (" in sql
    assert "target.market_value <=> source.market_value" in sql
    assert "run_id <=>" not in sql  # run metadata alone must not trigger a rewrite
    assert counts == {"inserted_rows": 2, "updated_rows": 1, "deleted_rows": 0}


def test_empty_trades_are_rejected_before_any_write(spark) -> None:
    trades_df = spark.createDataFrame([], TRADE_DDL)
    prices_df = spark.createDataFrame([("AAPL", MON, Decimal("10"), "USD")], PRICE_DDL)
    with pytest.raises(ValueError, match="trades is empty"):
        build_positions.prepare_positions(trades_df, prices_df, "run-1")


def test_multiple_trades_on_one_day_are_summed(spark) -> None:
    rows, _ = _positions(
        spark,
        [_trade("A", MON, 50), _trade("B", MON, 30), _trade("C", MON, -20)],
        [("AAPL", MON, Decimal("10"), "USD")],
    )
    assert rows[MON]["daily_buy_quantity"] == Decimal("80")
    assert rows[MON]["daily_sell_quantity"] == Decimal("20")
    assert rows[MON]["daily_net_quantity"] == Decimal("60")


def test_books_and_counterparties_keep_separate_positions(spark) -> None:
    trades_df = spark.createDataFrame(
        [
            _trade("A", MON, 10, book="BOOK_A", cp="CP1"),
            _trade("B", MON, 5, book="BOOK_B", cp="CP1"),
            _trade("C", MON, 7, book="BOOK_A", cp="CP2"),
            _trade("D", TUE, -3, book="BOOK_A", cp="CP1"),
        ],
        TRADE_DDL,
    )
    prices_df = spark.createDataFrame([("AAPL", MON, Decimal("10"), "USD")], PRICE_DDL)
    positions, _ = build_positions.prepare_positions(trades_df, prices_df, "run-1")
    closing = {
        (r["valuation_date"], r["book_id"], r["counterparty_id"]): r["closing_position_quantity"]
        for r in positions.collect()
    }
    assert closing[(TUE, "BOOK_A", "CP1")] == Decimal("7")
    assert closing[(TUE, "BOOK_B", "CP1")] == Decimal("5")
    assert closing[(TUE, "BOOK_A", "CP2")] == Decimal("7")


def test_rerun_on_unchanged_input_gives_identical_positions(spark) -> None:
    trades_df = spark.createDataFrame([_trade("T1", MON, 100), _trade("T2", TUE, -30)], TRADE_DDL)
    prices_df = spark.createDataFrame([("AAPL", MON, Decimal("10"), "USD")], PRICE_DDL)
    first, _ = build_positions.prepare_positions(trades_df, prices_df, "run-1")
    second, _ = build_positions.prepare_positions(trades_df, prices_df, "run-2")
    columns = [c for c in first.columns if c not in ("run_id", "calculated_at")]
    assert first.select(columns).orderBy("valuation_date").collect() == second.select(columns).orderBy("valuation_date").collect()


def test_position_delete_is_limited_to_source_instruments_and_dates(stats_spark) -> None:
    from conftest import statements

    build_positions.write_positions_snapshot(stats_spark, MagicMock())
    sql = statements(stats_spark)[-1]
    assert "NOT MATCHED BY SOURCE THEN DELETE" not in sql  # an unscoped delete could erase history
    assert "target.instrument_id IN ('INS001', 'INS002')" in sql
    assert "target.valuation_date >= DATE'2026-09-01'" in sql
    assert "THEN DELETE" in sql


def test_position_writer_refuses_empty_source_without_merging(stats_spark) -> None:
    from conftest import statements

    stats_spark.sql.return_value.first.return_value = {"first_date": None, "instruments": []}
    with pytest.raises(ValueError, match="empty"):
        build_positions.write_positions_snapshot(stats_spark, MagicMock())
    assert not any("MERGE INTO" in sql for sql in statements(stats_spark))