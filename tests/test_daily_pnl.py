from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trade_pipeline import config
from trade_pipeline.gold import calculate_daily_pnl

ROOT = Path(__file__).parents[1]


def test_daily_pnl_schema_uses_identity_id_and_business_key_merge() -> None:
    ddl = (ROOT / "database" / "tables" / "gold" / "daily_pnl.sql").read_text(encoding="utf-8")
    source = (ROOT / "src" / "trade_pipeline" / "gold" / "calculate_daily_pnl.py").read_text(
        encoding="utf-8"
    )
    assert "id BIGINT GENERATED ALWAYS AS IDENTITY" in ddl
    assert "valuation_date DATE NOT NULL" in ddl
    assert "book_id STRING NOT NULL" in ddl
    assert "symbol STRING NOT NULL" in ddl
    assert "currency STRING NOT NULL" in ddl
    assert "MERGE INTO {PNL_TABLE}" in source
    assert "THEN DELETE" in source


def test_daily_pnl_task_depends_on_positions() -> None:
    job = (ROOT / "jobs" / "resources" / "trade_pipeline.job.yml").read_text(encoding="utf-8")
    assert "task_key: calculate_daily_pnl" in job
    assert (
        "task_key: calculate_daily_pnl\n          depends_on:\n            - task_key: build_positions"
    ) in job


def test_trade_price_match_uses_symbol_and_trade_date_not_surrogate_ids() -> None:
    source = (ROOT / "src" / "trade_pipeline" / "gold" / "calculate_daily_pnl.py").read_text(
        encoding="utf-8"
    )
    assert 'F.col("trade._trade_symbol") == F.col("price._price_symbol")' in source
    assert 'F.col("price._price_date") <= F.col("trade.trade_date")' in source
    assert 'groupBy("trade_date").' not in source
    assert "trade.id" not in source


def _spark_or_skip():
    pyspark = pytest.importorskip("pyspark")
    return pyspark.sql.SparkSession.builder.master("local[1]").appName("daily-pnl-tests").getOrCreate()


@pytest.fixture(scope="module")
def spark():
    session = _spark_or_skip()
    yield session
    session.stop()


def _source_frames(spark, position_rows, trade_rows, price_rows):
    from pyspark.sql.types import (
        DateType,
        DecimalType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    position_schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("valuation_date", DateType(), False),
            StructField("book_id", StringType(), False),
            StructField("counterparty_id", StringType(), False),
            StructField("instrument_id", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("daily_buy_quantity", DecimalType(28, 6), False),
            StructField("daily_sell_quantity", DecimalType(28, 6), False),
            StructField("daily_net_quantity", DecimalType(28, 6), False),
            StructField("closing_position_quantity", DecimalType(28, 6), False),
            StructField("closing_price", DecimalType(18, 6), True),
            StructField("market_value", DecimalType(38, 6), True),
            StructField("run_id", StringType(), True),
            StructField("calculated_at", TimestampType(), True),
        ]
    )
    trade_schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("trade_id", StringType(), False),
            StructField("counterparty_id", StringType(), False),
            StructField("instrument_id", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("quantity", DecimalType(18, 6), False),
            StructField("price", DecimalType(18, 6), False),
            StructField("currency", StringType(), False),
            StructField("trade_date", DateType(), False),
            StructField("settlement_date", DateType(), False),
            StructField("book_id", StringType(), False),
            StructField("version", IntegerType(), False),
            StructField("modified_timestamp", TimestampType(), False),
        ]
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
    return (
        spark.createDataFrame(position_rows, position_schema),
        spark.createDataFrame(trade_rows, trade_schema),
        spark.createDataFrame(price_rows, price_schema),
    )


def _position_row(row_id, day, buy, sell, net, closing, book="BOOK_TECH", cp="CP001", instrument="INS001", close=None):
    return (
        row_id, day, book, cp, instrument, "USD", Decimal(str(buy)), Decimal(str(sell)),
        Decimal(str(net)), Decimal(str(closing)), Decimal(str(close)) if close is not None else None,
        None, "run", None,
    )


def _trade_row(row_id, trade_id, day, quantity, price, book="BOOK_TECH", cp="CP001", instrument="INS001"):
    return (
        row_id, trade_id, cp, instrument, config.INSTRUMENT_ID_TO_SYMBOL[instrument],
        Decimal(str(quantity)), Decimal(str(price)), "USD",
        day, day, book, 1, datetime.combine(day, datetime.min.time()),
    )


def _price_row(row_id, symbol, day, close, currency="USD"):
    return row_id, symbol, day, Decimal(str(close)), currency, "test", None, "run"


def test_example_daily_pnl_50_then_40_and_cumulative_proxy_90(spark) -> None:
    day1, day2 = date(2026, 9, 10), date(2026, 9, 11)
    positions, trades, prices = _source_frames(
        spark,
        [
            _position_row(1, day1, 10, 0, 10, 10, close=105),
            _position_row(2, day2, 5, 0, 5, 15, close=108),
        ],
        [_trade_row(1, "T1", day1, 10, 100), _trade_row(2, "T2", day2, 5, 106)],
        [_price_row(1, "AAPL", day1, 105), _price_row(2, "AAPL", day2, 108)],
    )

    result, breaks = calculate_daily_pnl(positions, trades, prices, "run-1")
    rows = result.orderBy("valuation_date").collect()

    assert [row["daily_pnl"] for row in rows] == [Decimal("50.000000"), Decimal("40.000000")]
    assert rows[1]["previous_close"] == Decimal("105.000000")
    assert rows[1]["unrealized_pnl_proxy"] == Decimal("90.000000")
    assert rows[1]["closing_quantity"] == Decimal("15.000000")
    assert rows[1]["market_value"] == Decimal("1620.000000")
    assert breaks.count() == 0


def test_sell_flat_multiple_books_and_symbols_aggregate_to_business_grain(spark) -> None:
    day = date(2026, 9, 10)
    positions, trades, prices = _source_frames(
        spark,
        [
            _position_row(1, day, 0, 4, -4, -4, book="BOOK_TECH", cp="CP1", close=90),
            _position_row(2, day, 4, 4, 0, 0, book="BOOK_TECH", cp="CP2", close=90),
            _position_row(4, day, 1, 1, 0, 0, book="BOOK_HEDGE", cp="CP4", close=90),
            _position_row(3, day, 2, 0, 2, 2, book="BOOK_GROWTH", cp="CP3", instrument="INS002", close=50),
        ],
        [
            _trade_row(1, "SELL", day, -4, 100, cp="CP1"),
            _trade_row(2, "BUY", day, 4, 100, cp="CP2"),
            _trade_row(4, "SELL-FLAT", day, -4, 100, cp="CP2"),
            _trade_row(5, "BUY-FLAT", day, 1, 100, book="BOOK_HEDGE", cp="CP4"),
            _trade_row(6, "SELL-FLAT-2", day, -1, 100, book="BOOK_HEDGE", cp="CP4"),
            _trade_row(3, "TSLA", day, 2, 45, book="BOOK_GROWTH", cp="CP3", instrument="INS002"),
        ],
        [_price_row(1, "AAPL", day, 90), _price_row(2, "TSLA", day, 50)],
    )
    result, _ = calculate_daily_pnl(positions, trades, prices, "run-1")
    rows = result.collect()
    assert len(rows) == 3
    by_book_symbol = {(row["book_id"], row["symbol"]): row for row in rows}
    assert by_book_symbol[("BOOK_TECH", "AAPL")]["closing_quantity"] == Decimal("-4.000000")
    assert by_book_symbol[("BOOK_TECH", "AAPL")]["daily_pnl"] == Decimal("40.000000")
    assert by_book_symbol[("BOOK_HEDGE", "AAPL")]["closing_quantity"] == Decimal("0.000000")
    assert by_book_symbol[("BOOK_GROWTH", "TSLA")]["closing_quantity"] == Decimal("2.000000")


def test_previous_close_uses_latest_prior_and_never_future_close(spark) -> None:
    day = date(2026, 9, 11)
    positions, trades, prices = _source_frames(
        spark,
        [_position_row(1, day, 5, 0, 5, 15, close=108)],
        [_trade_row(1, "T2", day, 5, 106)],
        [_price_row(1, "AAPL", date(2026, 9, 8), 104), _price_row(2, "AAPL", day, 108)],
    )
    result, _ = calculate_daily_pnl(positions, trades, prices, "run-1")
    row = result.first()
    assert row["previous_close"] == Decimal("104.000000")
    assert row["daily_pnl"] == Decimal("50.000000")


def test_trade_price_matching_uses_exact_or_latest_prior_never_future(spark) -> None:
    from trade_pipeline.gold.calculate_daily_pnl import match_trades_to_prices_asof

    trade_date = date(2026, 9, 10)
    _, trade_schema, prices = _source_frames(
        spark,
        [],
        [_trade_row(3, "schema", trade_date, 1, 1)],
        [_price_row(1, "AAPL", date(2026, 9, 8), 104), _price_row(2, "TSLA", date(2026, 9, 11), 108)],
    )
    trades = spark.createDataFrame(
        [
            _trade_row(1, "PRIOR", trade_date, 1, 100),
            _trade_row(2, "FUTURE_ONLY", trade_date, 1, 100, instrument="INS002"),
        ],
        trade_schema.schema,
    )
    matches = {
        row["trade_id"]: row
        for row in match_trades_to_prices_asof(trades, prices).collect()
    }
    assert matches["PRIOR"]["price_match_status"] == "PRIOR_CLOSE"
    assert matches["PRIOR"]["matched_price_date"] == date(2026, 9, 8)
    assert matches["FUTURE_ONLY"]["price_match_status"] == "MISSING_PRICE"
    assert matches["FUTURE_ONLY"]["matched_close"] is None


def test_missing_current_price_and_missing_baseline_produce_null_daily_pnl(spark) -> None:
    day = date(2026, 9, 11)
    positions, trades, prices = _source_frames(
        spark,
        [_position_row(1, day, 0, 0, 0, 5, close=None)],
        [],
        [_price_row(1, "AAPL", date(2026, 9, 12), 110)],
    )
    result, breaks = calculate_daily_pnl(positions, trades, prices, "run-1")
    row = result.first()
    assert row["current_close"] is None
    assert row["market_value"] is None
    assert row["daily_pnl"] is None
    assert row["calculation_status"] == "MISSING_CURRENT_PRICE"
    assert breaks.first()["break_type"] == "MISSING_MARKET_PRICE"
    assert breaks.first()["instrument_id"] == "INS001"


def test_nonzero_opening_position_without_prior_close_has_null_daily_pnl(spark) -> None:
    day = date(2026, 9, 11)
    positions, trades, prices = _source_frames(
        spark,
        [_position_row(1, day, 0, 0, 0, 5, close=108)],
        [],
        [_price_row(1, "AAPL", day, 108)],
    )
    result, breaks = calculate_daily_pnl(positions, trades, prices, "run-1")
    row = result.first()
    assert row["opening_quantity"] == Decimal("5.000000")
    assert row["previous_close"] is None
    assert row["daily_pnl"] is None
    assert row["calculation_status"] == "MISSING_BASELINE"
    assert breaks.first()["break_type"] == "MISSING_PNL_BASELINE"


def test_pnl_delete_is_limited_to_source_symbols_and_dates(stats_spark) -> None:
    from conftest import statements

    counts = calculate_daily_pnl.write_daily_pnl_snapshot(stats_spark, MagicMock())
    sql = statements(stats_spark)[-1]
    assert "NOT MATCHED BY SOURCE THEN DELETE" not in sql  # an unscoped delete could erase history
    assert "target.symbol IN ('AAPL', 'TSLA')" in sql
    assert "target.valuation_date >= DATE'2026-09-01'" in sql
    assert "WHEN MATCHED AND NOT (" in sql and "run_id <=>" not in sql
    assert counts["inserted_rows"] == 2


def test_pnl_writer_refuses_empty_source_without_merging(stats_spark) -> None:
    from conftest import statements

    stats_spark.sql.return_value.first.return_value = {"first_date": None, "symbols": []}
    with pytest.raises(ValueError, match="empty"):
        calculate_daily_pnl.write_daily_pnl_snapshot(stats_spark, MagicMock())
    assert not any("MERGE INTO" in sql for sql in statements(stats_spark))


def test_unrealized_pnl_proxy_is_documented_as_cumulative_mark_to_market_proxy() -> None:
    source = (ROOT / "src" / "trade_pipeline" / "gold" / "calculate_daily_pnl.py").read_text(encoding="utf-8")
    assert "Cumulative mark-to-market proxy" in source
    assert "not\n    # true unrealized P&L" in source.replace("\r\n", "\n")


D0, D1, D2, D3 = date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 11), date(2026, 9, 12)


def _pnl_row(spark, positions, trades, prices):
    frames = _source_frames(
        spark,
        [_position_row(i, *p) for i, p in enumerate(positions, start=1)],
        [_trade_row(i, f"T{i}", *t) for i, t in enumerate(trades, start=1)],
        [_price_row(i, "AAPL", *p) for i, p in enumerate(prices, start=1)],
    )
    result, breaks = calculate_daily_pnl(*frames, "run-1")
    return {row["valuation_date"]: row for row in result.collect()}, breaks


def test_same_day_buy_and_sell_pnl(spark) -> None:
    # BUY 30 at 102, SELL 10 at 104, close 105: 30*(105-102) + (-10)*(105-104) = 80
    rows, _ = _pnl_row(
        spark,
        [(D2, 30, 10, 20, 20)],
        [(D2, 30, 102), (D2, -10, 104)],
        [(D1, 100), (D2, 105)],
    )
    assert rows[D2]["daily_pnl"] == Decimal("80")
    assert rows[D2]["closing_quantity"] == Decimal("20")


def test_fully_closed_position_and_realized_effect_in_proxy(spark) -> None:
    # Day 1: BUY 10 at 100, close 105 -> 50. Day 2: SELL 10 at 110, close 108 -> 10*(108-105) + (-10)*(108-110) = 50.
    rows, _ = _pnl_row(
        spark,
        [(D1, 10, 0, 10, 10), (D2, 0, 10, -10, 0)],
        [(D1, 10, 100), (D2, -10, 110)],
        [(D1, 105), (D2, 108)],
    )
    assert rows[D1]["daily_pnl"] == Decimal("50")
    assert rows[D2]["daily_pnl"] == Decimal("50")
    assert rows[D2]["closing_quantity"] == Decimal("0")
    assert rows[D2]["market_value"] == Decimal("0")
    assert rows[D2]["unrealized_pnl_proxy"] == Decimal("100")  # realized: (110 - 100) * 10


def test_previous_close_is_latest_strictly_earlier_and_future_is_ignored(spark) -> None:
    rows, _ = _pnl_row(
        spark,
        [(D2, 0, 0, 0, 100)],
        [],
        [(D0, 90), (D1, 100), (D2, 105), (D3, 200)],
    )
    assert rows[D2]["previous_close"] == Decimal("100")  # not D0, not the same day, not the future D3
    assert rows[D2]["current_close"] == Decimal("105")
    assert rows[D2]["daily_pnl"] == Decimal("500")


def test_decimal_precision_is_preserved(spark) -> None:
    rows, _ = _pnl_row(
        spark,
        [(D2, 0, 0, 0, 3)],
        [],
        [(D1, Decimal("100.123456")), (D2, Decimal("105.654321"))],
    )
    assert rows[D2]["daily_pnl"] == Decimal("16.592595")  # 3 * 5.530865


def test_run_id_and_calculated_at_are_populated(spark) -> None:
    rows, _ = _pnl_row(spark, [(D2, 0, 0, 0, 100)], [], [(D1, 100), (D2, 105)])
    assert rows[D2]["run_id"] == "run-1"
    assert rows[D2]["calculated_at"] is not None


def test_missing_prices_create_breaks(spark) -> None:
    _, missing_current = _pnl_row(spark, [(D2, 0, 0, 0, 100)], [], [(D1, 100)])
    assert [b["break_type"] for b in missing_current.collect()] == ["MISSING_MARKET_PRICE"]
    _, missing_previous = _pnl_row(spark, [(D2, 0, 0, 0, 100)], [], [(D2, 105)])
    assert [b["break_type"] for b in missing_previous.collect()] == ["MISSING_PNL_BASELINE"]



D1, D2 = date(2026, 9, 10), date(2026, 9, 11)


@pytest.mark.parametrize(
    ("position", "trades", "prices", "expected_pnl", "expected_status"),
    [
        # Hold 100 shares, close 100 -> 105: 100 * (105 - 100)
        ((0, 0, 0, 100), [], [(D1, 100), (D2, 105)], "500", "COMPLETE"),
        # Buy 20 at 103, close 105, no opening position: 20 * (105 - 103)
        ((20, 0, 20, 20), [(20, 103)], [(D1, 100), (D2, 105)], "40", "COMPLETE"),
        # Hold 100 (+500), buy 20 at 103 (+40)
        ((20, 0, 20, 120), [(20, 103)], [(D1, 100), (D2, 105)], "540", "COMPLETE"),
        # Short 100 shares, price rises 100 -> 105: -100 * 5
        ((0, 0, 0, -100), [], [(D1, 100), (D2, 105)], "-500", "COMPLETE"),
        # No close on the valuation date
        ((0, 0, 0, 100), [], [(D1, 100)], None, "MISSING_CURRENT_PRICE"),
        # Opening position but no earlier close
        ((0, 0, 0, 100), [], [(D2, 105)], None, "MISSING_BASELINE"),
    ],
    ids=["hold", "buy_only", "hold_and_buy", "short", "missing_current", "missing_previous"],
)
def test_daily_pnl_formula_examples(spark, position, trades, prices, expected_pnl, expected_status) -> None:
    buy, sell, net, closing = position
    positions, trade_df, price_df = _source_frames(
        spark,
        [_position_row(1, D2, buy, sell, net, closing)],
        [_trade_row(i, f"T{i}", D2, qty, px) for i, (qty, px) in enumerate(trades, start=1)],
        [_price_row(i, "AAPL", day, close) for i, (day, close) in enumerate(prices, start=1)],
    )
    result, _ = calculate_daily_pnl(positions, trade_df, price_df, "run-1")
    row = result.first()
    assert row["calculation_status"] == expected_status
    if expected_pnl is None:
        assert row["daily_pnl"] is None
    else:
        assert row["daily_pnl"] == Decimal(expected_pnl)