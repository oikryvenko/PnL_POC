"""Build daily trade-date positions and join same-day Silver market prices."""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date
from typing import Sequence

from trade_pipeline import audit, config
from trade_pipeline.common.delta_stats import read_merge_counts
from trade_pipeline.common.market_calendar import requested_sessions
from trade_pipeline.gold.break_report import write_valuation_breaks

logger = logging.getLogger(__name__)
TRADES_TABLE = "cibc.silver.trades"
PRICES_TABLE = "cibc.silver.market_prices"
POSITIONS_TABLE = "cibc.gold.positions"

POSITION_KEY = ["book_id", "counterparty_id", "instrument_id", "currency"]
POSITION_BUSINESS_KEY = ["valuation_date", *POSITION_KEY]


def _require_columns(dataframe, table_name: str, required: set[str]) -> None:
    missing = sorted(required - set(dataframe.columns))
    if missing:
        raise ValueError(f"{table_name} is missing required columns: {missing}")


def _validate_unique_price_keys(prices_df) -> None:
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
            "Silver market prices has duplicate (symbol, price_date) business keys: "
            + "; ".join(str(row.asDict()) for row in duplicates)
        )


def _valuation_calendar(trades_df, prices_df, as_of: date | None):
    """NYSE sessions from the first trade to the latest trade/price date, never past `as_of`.

    Trade dates are always included, so no accepted trade can fall off the timeline.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, StructField, StructType

    cutoff = as_of or date.today()
    first_trade, last_trade = trades_df.agg(F.min("trade_date"), F.max("trade_date")).first()
    last_price = prices_df.agg(F.max("price_date")).first()[0]
    last = min(max(d for d in (last_trade, last_price) if d is not None), cutoff)
    sessions = requested_sessions(first_trade, last, cutoff) if first_trade <= last else []
    schema = StructType([StructField("valuation_date", DateType(), False)])
    calendar = trades_df.sparkSession.createDataFrame([(s,) for s in sessions], schema)
    return calendar.union(trades_df.select(F.col("trade_date").alias("valuation_date"))).distinct()


def prepare_positions(trades_df, prices_df, run_id: str, as_of: date | None = None):
    """Return the full position history and missing-price breaks using Spark transforms.

    Quantities come only from trades; prices are joined same-day afterwards, so a missing
    price leaves price and market value NULL but never changes a quantity.
    """
    from pyspark.sql import Window, functions as F
    from pyspark.sql.types import DecimalType

    trade_required = {
        "trade_id",
        "counterparty_id",
        "instrument_id",
        "quantity",
        "price",
        "currency",
        "trade_date",
        "settlement_date",
        "book_id",
        "version",
        "modified_timestamp",
    }
    price_required = {"symbol", "price_date", "close", "currency"}
    _require_columns(trades_df, TRADES_TABLE, trade_required)
    _require_columns(prices_df, PRICES_TABLE, price_required)
    _validate_unique_price_keys(prices_df)

    if not trades_df.take(1):
        raise ValueError("Silver trades is empty; Gold positions not changed")
    if not prices_df.take(1):
        raise ValueError("Silver market prices is empty; Gold positions not changed")
    price_calendar = _valuation_calendar(trades_df, prices_df, as_of)

    daily = (
        trades_df.groupBy(
            F.col("trade_date").alias("valuation_date"),
            *POSITION_KEY,
        )
        .agg(
            F.sum(F.when(F.col("quantity") > 0, F.col("quantity")).otherwise(F.lit(0))).alias(
                "daily_buy_quantity"
            ),
            F.sum(
                F.when(F.col("quantity") < 0, F.abs(F.col("quantity"))).otherwise(F.lit(0))
            ).alias("daily_sell_quantity"),
            F.sum(F.col("quantity")).alias("daily_net_quantity"),
        )
    )
    position_keys = trades_df.groupBy(*POSITION_KEY).agg(
        F.min("trade_date").alias("_first_trade_date")
    )
    timeline = position_keys.join(
        price_calendar,
        price_calendar["valuation_date"] >= position_keys["_first_trade_date"],
        "inner",
    ).drop("_first_trade_date")
    daily_timeline = timeline.join(daily, POSITION_BUSINESS_KEY, "left").fillna(
        0,
        subset=["daily_buy_quantity", "daily_sell_quantity", "daily_net_quantity"],
    )
    closing_window = (
        Window.partitionBy(*POSITION_KEY)
        .orderBy("valuation_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    positions = daily_timeline.withColumn(
        "closing_position_quantity",
        F.sum("daily_net_quantity").over(closing_window).cast(DecimalType(28, 6)),
    )

    instrument_to_symbol = F.create_map(
        *[
            item
            for instrument_id, symbol in config.INSTRUMENT_ID_TO_SYMBOL.items()
            for item in (F.lit(instrument_id), F.lit(symbol))
        ]
    )
    positions = positions.withColumn("_market_symbol", instrument_to_symbol[F.col("instrument_id")])
    prices = prices_df.select(
        F.col("symbol").alias("_price_symbol"),
        F.col("price_date").alias("_price_date"),
        F.col("currency").alias("_price_currency"),
        F.col("close").cast(DecimalType(18, 6)).alias("closing_price"),
    )
    valued = positions.join(
        prices,
        (positions["_market_symbol"] == prices["_price_symbol"])
        & (positions["valuation_date"] == prices["_price_date"])
        & (positions["currency"] == prices["_price_currency"]),
        "left",
    ).withColumn(
        "market_value",
        F.expr("try_cast(closing_position_quantity * closing_price AS DECIMAL(38,6))"),
    )
    calculated = valued.select(
        *POSITION_BUSINESS_KEY[:1],
        *POSITION_KEY,
        F.col("daily_buy_quantity").cast(DecimalType(28, 6)).alias("daily_buy_quantity"),
        F.col("daily_sell_quantity").cast(DecimalType(28, 6)).alias("daily_sell_quantity"),
        F.col("daily_net_quantity").cast(DecimalType(28, 6)).alias("daily_net_quantity"),
        F.col("closing_position_quantity").cast(DecimalType(28, 6)).alias("closing_position_quantity"),
        "closing_price",
        "market_value",
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("calculated_at"),
    )
    missing_price_breaks = (
        calculated.filter(F.col("closing_price").isNull())
        # A flat position with no activity needs no price.
        .filter(
            (F.col("closing_position_quantity") != 0)
            | (F.col("daily_buy_quantity") != 0)
            | (F.col("daily_sell_quantity") != 0)
        )
        .select("instrument_id", F.col("valuation_date"), F.lit("MISSING_MARKET_PRICE").alias("break_type"))
        .distinct()
        .withColumn("trade_id", F.lit(None).cast("string"))
        .withColumn(
            "break_description",
            F.concat(F.lit("No same-day Silver market price for instrument "), F.col("instrument_id")),
        )
        .withColumn("detected_at", F.current_timestamp())
        .withColumn("status", F.lit("OPEN"))
    )
    return calculated, missing_price_breaks


def write_positions_snapshot(spark, positions_df) -> dict[str, int]:
    """Reconcile position history by business key, omitting the Delta identity id.

    Completeness of Silver cannot be proven from the data, so the delete is limited to the
    source's own scope: only Gold rows for instruments present in the source, dated on or after
    the source's first valuation date. A partial source therefore cannot erase other
    instruments or earlier history. Corrections inside that scope are still reconciled.
    The limit is that history missing for an instrument that is still present, or an instrument
    that disappears entirely, is kept and needs manual cleanup.
    Rows are updated only when a financial value changed, so run_id/calculated_at show when
    a row last changed, not the latest run.
    """
    view = "_trade_pipeline_gold_positions_source"
    positions_df.createOrReplaceTempView(view)
    scope = spark.sql(
        f"SELECT MIN(valuation_date) AS first_date, COLLECT_SET(instrument_id) AS instruments FROM {view}"
    ).first()
    if not scope["instruments"]:
        raise ValueError("Source positions are empty; Gold positions not changed")
    instrument_list = ", ".join(f"'{instrument}'" for instrument in sorted(scope["instruments"]))
    value_columns = [
        "daily_buy_quantity",
        "daily_sell_quantity",
        "daily_net_quantity",
        "closing_position_quantity",
        "closing_price",
        "market_value",
        "run_id",
        "calculated_at",
    ]
    financial_columns = value_columns[:6]
    unchanged = " AND ".join(f"target.{column} <=> source.{column}" for column in financial_columns)
    update_set = ",\n            ".join(f"{column} = source.{column}" for column in value_columns)
    insert_columns = ", ".join([*POSITION_BUSINESS_KEY, *value_columns])
    insert_values = ", ".join(f"source.{column}" for column in [*POSITION_BUSINESS_KEY, *value_columns])
    result = spark.sql(
        f"""
        MERGE INTO {POSITIONS_TABLE} AS target
        USING {view} AS source
        ON target.valuation_date = source.valuation_date
           AND target.book_id = source.book_id
           AND target.counterparty_id = source.counterparty_id
           AND target.instrument_id = source.instrument_id
           AND target.currency = source.currency
        WHEN MATCHED AND NOT ({unchanged}) THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        WHEN NOT MATCHED BY SOURCE
             AND target.instrument_id IN ({instrument_list})
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
    spark_session = spark if spark is not None else _get_spark_session()
    run_id = pipeline_run_id or str(uuid.uuid4())
    attempt_id = (
        f"{task_run_id}-{task_execution_count}"
        if task_run_id is not None and task_execution_count is not None
        else task_run_id or str(uuid.uuid4())
    )

    def build() -> dict:
        trades_df = spark_session.table(TRADES_TABLE)
        prices_df = spark_session.table(PRICES_TABLE)
        trade_count = trades_df.count()
        price_count = prices_df.count()
        positions_df, breaks_df = prepare_positions(trades_df, prices_df, run_id)
        position_count = positions_df.count()
        missing_price_count = positions_df.filter("closing_price IS NULL").count()
        merge_counts = write_positions_snapshot(spark_session, positions_df)
        rows_written = merge_counts["inserted_rows"] + merge_counts["updated_rows"]
        breaks_detected = write_valuation_breaks(spark_session, run_id, breaks_df)
        logger.info(
            "Gold positions complete run_id=%s task_run_id=%s trades=%d prices=%d positions=%d written=%d deleted=%d missing_prices=%d",
            run_id,
            attempt_id,
            trade_count,
            price_count,
            position_count,
            rows_written,
            merge_counts["deleted_rows"],
            missing_price_count,
        )
        return {
            "input_rows": trade_count + price_count,
            # output_rows: position rows produced; rows_written: only those inserted or updated.
            "output_rows": position_count,
            "rows_written": rows_written,
            "rejected_rows": 0,
            "duplicate_rows": 0,
            "issue_counts": {"MISSING_MARKET_PRICE": missing_price_count} if missing_price_count else {},
            "trade_rows": trade_count,
            "price_rows": price_count,
            "position_rows": position_count,
            **merge_counts,
            "missing_price_rows": missing_price_count,
            "breaks_detected": breaks_detected,
        }

    return audit.run_audited_task(
        spark_session,
        run_id,
        attempt_id,
        "build_positions",
        f"{TRADES_TABLE}, {PRICES_TABLE}",
        POSITIONS_TABLE,
        build,
    )


def _get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Build daily Gold positions from Silver trades and prices")
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
        logger.exception("Gold position building failed")
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
