"""Calculate daily mark-to-market P&L and a clearly labeled trade-price proxy."""

from __future__ import annotations

import argparse
import logging
import uuid
from typing import Sequence

from trade_pipeline import audit, config
from trade_pipeline.common.delta_stats import read_merge_counts
from trade_pipeline.gold.break_report import write_valuation_breaks

logger = logging.getLogger(__name__)
POSITIONS_TABLE = "cibc.gold.positions"
TRADES_TABLE = "cibc.silver.trades"
PRICES_TABLE = "cibc.silver.market_prices"
PNL_TABLE = "cibc.gold.daily_pnl"
PNL_BUSINESS_KEY = ["valuation_date", "book_id", "symbol", "currency"]


def _require_columns(dataframe, table_name: str, columns: set[str]) -> None:
    missing = sorted(columns - set(dataframe.columns))
    if missing:
        raise ValueError(f"{table_name} is missing required columns: {missing}")


def _validate_trade_keys(trades_df) -> None:
    from pyspark.sql import functions as F

    duplicates = (
        trades_df.groupBy("trade_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if duplicates:
        raise ValueError(
            "Silver trades contains duplicate current trade_id values: "
            + "; ".join(str(row.asDict()) for row in duplicates)
        )


def _validate_price_keys(prices_df) -> None:
    from pyspark.sql import functions as F

    duplicates = (
        prices_df.groupBy("symbol", "price_date")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if duplicates:
        raise ValueError(
            "Silver market prices contains duplicate (symbol, price_date) keys: "
            + "; ".join(str(row.asDict()) for row in duplicates)
        )


def match_trades_to_prices_asof(trades_df, prices_df):
    """Match each Silver trade to an exact or latest prior close, never a future close."""
    from pyspark.sql import Window, functions as F

    trade_rows = trades_df.withColumn("_trade_symbol", F.upper(F.trim(F.col("symbol"))))
    candidates = trade_rows.alias("trade").join(
        prices_df.select(
            F.col("symbol").alias("_price_symbol"),
            F.col("currency").alias("_price_currency"),
            F.col("price_date").alias("_price_date"),
            F.col("close").alias("_matched_close"),
        ).alias("price"),
        (F.col("trade._trade_symbol") == F.col("price._price_symbol"))
        & (F.col("trade.currency") == F.col("price._price_currency"))
        & (F.col("price._price_date") <= F.col("trade.trade_date")),
        "left",
    )
    match_window = Window.partitionBy(F.col("trade.trade_id")).orderBy(
        F.col("price._price_date").desc_nulls_last()
    )
    return (
        candidates.withColumn("_price_match_rank", F.row_number().over(match_window))
        .filter(F.col("_price_match_rank") == 1)
        .select(
            F.col("trade.trade_id").alias("trade_id"),
            F.col("trade.instrument_id").alias("instrument_id"),
            F.col("trade.trade_date").alias("trade_date"),
            F.col("price._price_date").alias("matched_price_date"),
            F.col("price._matched_close").alias("matched_close"),
            F.when(F.col("price._price_date").isNull(), "MISSING_PRICE")
            .when(F.col("price._price_date") == F.col("trade.trade_date"), "EXACT")
            .otherwise("PRIOR_CLOSE")
            .alias("price_match_status"),
        )
    )


def calculate_daily_pnl(positions_df, trades_df, prices_df, run_id):
    """Return one daily P&L row per book/symbol/currency and unmatched-price breaks."""
    from pyspark.sql import Window, functions as F
    from pyspark.sql.types import DecimalType

    _require_columns(
        positions_df,
        POSITIONS_TABLE,
        {
            "valuation_date",
            "book_id",
            "counterparty_id",
            "instrument_id",
            "currency",
            "daily_buy_quantity",
            "daily_sell_quantity",
            "daily_net_quantity",
            "closing_position_quantity",
            "closing_price",
        },
    )
    _require_columns(
        trades_df,
        TRADES_TABLE,
        {"trade_id", "book_id", "instrument_id", "symbol", "quantity", "price", "currency", "trade_date", "version"},
    )
    _require_columns(prices_df, PRICES_TABLE, {"symbol", "price_date", "close", "currency"})
    _validate_trade_keys(trades_df)
    _validate_price_keys(prices_df)
    if not positions_df.take(1):
        raise ValueError("Gold positions is empty; daily P&L not changed")

    symbol_to_instrument = F.create_map(
        *[
            item
            for instrument_id, symbol in config.INSTRUMENT_ID_TO_SYMBOL.items()
            for item in (F.lit(symbol), F.lit(instrument_id))
        ]
    )

    instrument_to_symbol = F.create_map(
        *[
            item
            for instrument_id, symbol in config.INSTRUMENT_ID_TO_SYMBOL.items()
            for item in (F.lit(instrument_id), F.lit(symbol))
        ]
    )
    positions = (
        positions_df.groupBy("valuation_date", "book_id", "instrument_id", "currency")
        .agg(
            F.sum("daily_buy_quantity").cast(DecimalType(28, 6)).alias("buy_quantity"),
            F.sum("daily_sell_quantity").cast(DecimalType(28, 6)).alias("sell_quantity"),
            F.sum("daily_net_quantity").cast(DecimalType(28, 6)).alias("daily_net_quantity"),
            F.sum("closing_position_quantity").cast(DecimalType(28, 6)).alias("closing_quantity"),
        )
        .withColumn("symbol", instrument_to_symbol[F.col("instrument_id")])
    )
    if positions.filter(F.col("symbol").isNull()).limit(1).count():
        raise ValueError("Gold Positions contains an instrument_id outside the configured symbol universe")

    positions = positions.groupBy("valuation_date", "book_id", "symbol", "currency").agg(
        F.sum("buy_quantity").cast(DecimalType(28, 6)).alias("buy_quantity"),
        F.sum("sell_quantity").cast(DecimalType(28, 6)).alias("sell_quantity"),
        F.sum("daily_net_quantity").cast(DecimalType(28, 6)).alias("daily_net_quantity"),
        F.sum("closing_quantity").cast(DecimalType(28, 6)).alias("closing_quantity"),
    )

    daily_trades = (
        trades_df.groupBy(
            F.col("trade_date").alias("valuation_date"),
            "book_id",
            F.upper(F.trim(F.col("symbol"))).alias("symbol"),
            "currency",
        )
        .agg(
            F.sum(F.col("quantity").cast(DecimalType(18, 6))).alias("trade_quantity"),
            F.sum(
                F.col("quantity").cast(DecimalType(18, 6))
                * F.col("price").cast(DecimalType(18, 6))
            ).cast(DecimalType(38, 6)).alias("net_trade_cash_flow"),
        )
    )
    positions = positions.join(
        daily_trades,
        ["valuation_date", "book_id", "symbol", "currency"],
        "left",
    ).fillna(0, subset=["trade_quantity", "net_trade_cash_flow"])

    current_prices = prices_df.select(
        F.col("symbol").alias("_current_symbol"),
        F.col("price_date").alias("_current_date"),
        F.col("currency").alias("_current_currency"),
        F.col("close").cast(DecimalType(18, 6)).alias("current_close"),
    )
    positions = positions.join(
        current_prices,
        (positions["symbol"] == current_prices["_current_symbol"])
        & (positions["valuation_date"] == current_prices["_current_date"])
        & (positions["currency"] == current_prices["_current_currency"]),
        "left",
    ).drop("_current_symbol", "_current_date", "_current_currency")

    prior_prices = prices_df.select(
        F.col("symbol").alias("_prior_symbol"),
        F.col("price_date").alias("_prior_date"),
        F.col("currency").alias("_prior_currency"),
        F.col("close").cast(DecimalType(18, 6)).alias("previous_close"),
    )
    candidates = positions.alias("position").join(
        prior_prices.alias("prior"),
        (F.col("prior._prior_symbol") == F.col("position.symbol"))
        & (F.col("prior._prior_currency") == F.col("position.currency"))
        & (F.col("prior._prior_date") < F.col("position.valuation_date")),
        "left",
    )
    prior_window = Window.partitionBy(*[F.col(f"position.{key}") for key in PNL_BUSINESS_KEY]).orderBy(
        F.col("prior._prior_date").desc_nulls_last()
    )
    with_prior = candidates.withColumn("_prior_rank", F.row_number().over(prior_window)).filter(
        F.col("_prior_rank") == 1
    )
    opening_quantity = (
        F.col("position.closing_quantity") - F.col("position.daily_net_quantity")
    ).cast(DecimalType(28, 6))
    trade_day_pnl = (
        F.col("position.daily_net_quantity") * F.col("position.current_close")
        - F.col("position.net_trade_cash_flow")
    )
    price_move_pnl = opening_quantity * (
        F.col("position.current_close") - F.col("previous_close")
    )
    status = (
        F.when(F.col("position.current_close").isNull(), F.lit("MISSING_CURRENT_PRICE"))
        .when((opening_quantity != 0) & F.col("previous_close").isNull(), F.lit("MISSING_BASELINE"))
        .otherwise(F.lit("COMPLETE"))
    )
    daily_pnl = (
        F.when(F.col("position.current_close").isNull(), F.lit(None).cast(DecimalType(38, 6)))
        .when(
            (opening_quantity != 0) & F.col("previous_close").isNull(),
            F.lit(None).cast(DecimalType(38, 6)),
        )
        .when(opening_quantity == 0, trade_day_pnl)
        .otherwise(price_move_pnl + trade_day_pnl)
        .cast(DecimalType(38, 6))
    )
    market_value = (
        F.col("position.closing_quantity") * F.col("position.current_close")
    ).cast(DecimalType(38, 6))
    pnl_window = Window.partitionBy("position.book_id", "position.symbol", "position.currency").orderBy(
        F.col("position.valuation_date")
    ).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    cumulative_cash_flow = F.sum(F.col("position.net_trade_cash_flow")).over(pnl_window)
    # Cumulative mark-to-market proxy: market value minus cumulative signed trade consideration.
    # It includes realized effects (once a position is closed it equals realized P&L) and is not
    # true unrealized P&L; the unrealized_pnl_proxy column name is kept for schema compatibility.
    unrealized_proxy = (market_value - cumulative_cash_flow).cast(DecimalType(38, 6))
    instrument = symbol_to_instrument[F.col("position.symbol")]

    breaks = (
        with_prior.withColumn("_opening_quantity", opening_quantity)
        .withColumn(
            "_break_type",
            F.when(F.col("position.current_close").isNull(), F.lit("MISSING_MARKET_PRICE"))
            .when(
                (F.col("_opening_quantity") != 0) & F.col("previous_close").isNull(),
                F.lit("MISSING_PNL_BASELINE"),
            ),
        )
        .filter(F.col("_break_type").isNotNull())
        .select(
            F.lit(None).cast("string").alias("trade_id"),
            instrument.alias("instrument_id"),
            F.col("position.valuation_date").alias("valuation_date"),
            F.col("_break_type").alias("break_type"),
            F.concat(
                F.lit("Daily P&L unavailable for book "),
                F.col("position.book_id"),
                F.lit(" and instrument "),
                instrument,
                F.lit(": "),
                F.col("_break_type"),
            ).alias("break_description"),
            F.current_timestamp().alias("detected_at"),
            F.lit("OPEN").alias("status"),
        )
        .distinct()
    )

    result = with_prior.select(
        F.col("position.valuation_date").alias("valuation_date"),
        F.col("position.book_id").alias("book_id"),
        F.col("position.symbol").alias("symbol"),
        F.col("position.currency").alias("currency"),
        opening_quantity.alias("opening_quantity"),
        F.col("position.buy_quantity").cast(DecimalType(28, 6)).alias("buy_quantity"),
        F.col("position.sell_quantity").cast(DecimalType(28, 6)).alias("sell_quantity"),
        F.col("position.closing_quantity").alias("closing_quantity"),
        F.col("previous_close").cast(DecimalType(18, 6)).alias("previous_close"),
        F.col("position.current_close").cast(DecimalType(18, 6)).alias("current_close"),
        market_value.alias("market_value"),
        unrealized_proxy.alias("unrealized_pnl_proxy"),
        daily_pnl.alias("daily_pnl"),
        status.alias("calculation_status"),
    )
    result = result.withColumn("run_id", F.lit(run_id)).withColumn("calculated_at", F.current_timestamp())
    trade_matches = match_trades_to_prices_asof(trades_df, prices_df)
    unpriced_trade_breaks = (
        trade_matches.filter(F.col("price_match_status") == "MISSING_PRICE")
        .select(
            "trade_id",
            "instrument_id",
            F.col("trade_date").alias("valuation_date"),
            F.lit("MISSING_MARKET_PRICE").alias("break_type"),
            F.lit("No exact or prior market close exists on or before trade date").alias(
                "break_description"
            ),
            F.current_timestamp().alias("detected_at"),
            F.lit("OPEN").alias("status"),
        )
    )
    return result, breaks.unionByName(unpriced_trade_breaks)


def write_daily_pnl_snapshot(spark, pnl_df) -> dict[str, int]:
    """Reconcile P&L history by business key, leaving the identity id to Delta.

    Completeness of the inputs cannot be proven from the data, so the delete is limited to the
    source's own scope: only Gold rows for symbols present in the source, dated on or after the
    source's first valuation date. A partial source therefore cannot erase other symbols or
    earlier history. Corrections inside that scope are still reconciled.
    The limit is that history missing for a symbol that is still present, or a symbol that
    disappears entirely, is kept and needs manual cleanup.
    Rows are updated only when a financial value changed, so run_id/calculated_at show when a
    row last changed.
    """
    view = "_trade_pipeline_daily_pnl_source"
    pnl_df.createOrReplaceTempView(view)
    scope = spark.sql(
        f"SELECT MIN(valuation_date) AS first_date, COLLECT_SET(symbol) AS symbols FROM {view}"
    ).first()
    if not scope["symbols"]:
        raise ValueError("Source P&L is empty; Gold daily P&L not changed")
    symbol_list = ", ".join(f"'{symbol}'" for symbol in sorted(scope["symbols"]))
    business_key = ["valuation_date", "book_id", "symbol", "currency"]
    value_columns = [
        "opening_quantity", "buy_quantity", "sell_quantity", "closing_quantity",
        "previous_close", "current_close", "market_value", "unrealized_pnl_proxy",
        "daily_pnl", "calculation_status", "run_id", "calculated_at",
    ]
    financial_columns = value_columns[:-2]
    unchanged = " AND ".join(f"target.{column} <=> source.{column}" for column in financial_columns)
    update_set = ",\n            ".join(f"{column} = source.{column}" for column in value_columns)
    insert_columns = ", ".join([*business_key, *value_columns])
    insert_values = ", ".join(f"source.{column}" for column in [*business_key, *value_columns])
    result = spark.sql(
        f"""
        MERGE INTO {PNL_TABLE} AS target
        USING {view} AS source
        ON target.valuation_date = source.valuation_date
           AND target.book_id = source.book_id
           AND target.symbol = source.symbol
           AND target.currency = source.currency
        WHEN MATCHED AND NOT ({unchanged}) THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        WHEN NOT MATCHED BY SOURCE
             AND target.symbol IN ({symbol_list})
             AND target.valuation_date >= DATE'{scope["first_date"]}'
             THEN DELETE
        """
    ).first()
    return read_merge_counts(result)


def run_processing(
    spark=None,
    pipeline_run_id: str | None = None,
    task_run_id: str | None = None,
    task_execution_count: str | int | None = None,
) -> dict:
    from pyspark.sql import functions as F

    spark_session = spark if spark is not None else _get_spark_session()
    run_id = pipeline_run_id or str(uuid.uuid4())
    attempt_id = (
        f"{task_run_id}-{task_execution_count}"
        if task_run_id is not None and task_execution_count is not None
        else task_run_id or str(uuid.uuid4())
    )

    def calculate() -> dict:
        positions_df = spark_session.table(POSITIONS_TABLE)
        trades_df = spark_session.table(TRADES_TABLE)
        prices_df = spark_session.table(PRICES_TABLE)
        position_count = positions_df.count()
        trade_count = trades_df.count()
        price_count = prices_df.count()
        pnl_df, breaks_df = calculate_daily_pnl(positions_df, trades_df, prices_df, run_id)
        output_count = pnl_df.count()
        missing_price_count = pnl_df.filter(F.col("calculation_status") != "COMPLETE").count()
        merge_counts = write_daily_pnl_snapshot(spark_session, pnl_df)
        rows_written = merge_counts["inserted_rows"] + merge_counts["updated_rows"]
        breaks_detected = write_valuation_breaks(spark_session, run_id, breaks_df)
        return {
            "input_rows": position_count + trade_count + price_count,
            # output_rows: P&L rows produced; rows_written: only those inserted or updated.
            "output_rows": output_count,
            "rows_written": rows_written,
            "rejected_rows": 0,
            "duplicate_rows": 0,
            "issue_counts": {"PNL_INCOMPLETE": missing_price_count} if missing_price_count else {},
            "position_rows": position_count,
            "trade_rows": trade_count,
            "price_rows": price_count,
            "pnl_rows": output_count,
            **merge_counts,
            "missing_price_rows": missing_price_count,
            "breaks_detected": breaks_detected,
        }

    return audit.run_audited_task(
        spark_session,
        run_id,
        attempt_id,
        "calculate_daily_pnl",
        f"{POSITIONS_TABLE}, {TRADES_TABLE}, {PRICES_TABLE}",
        PNL_TABLE,
        calculate,
    )


def _get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Calculate Gold daily P&L from positions, trades, and prices")
    parser.add_argument("--pipeline_run_id", default=None)
    parser.add_argument("--task_run_id", default=None)
    parser.add_argument("--task_execution_count", default=None)
    args = parser.parse_args(argv)
    try:
        run_processing(
            pipeline_run_id=args.pipeline_run_id or None,
            task_run_id=args.task_run_id or None,
            task_execution_count=args.task_execution_count or None,
        )
    except Exception:
        logger.exception("Gold daily P&L calculation failed")
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
