"""Validate Bronze market prices and incrementally merge them into Silver.
"""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, timedelta
from typing import Sequence

from trade_pipeline import config
from trade_pipeline import audit
from trade_pipeline.common.market_calendar import expected_trading_sessions, requested_sessions
from trade_pipeline.common.delta_stats import read_merge_counts
from trade_pipeline.common.rejects import merge_rejects

logger = logging.getLogger(__name__)
BRONZE_TABLE = "cibc.bronze.market_prices"
SILVER_TABLE = "cibc.silver.market_prices"
REJECTS_TABLE = "cibc.silver.market_prices_rejects"
# Bronze run metadata is excluded so an unchanged Bronze row is recorded only once.
REJECT_KEY_COLUMNS = ("symbol", "price_date", "close", "currency", "source_api", "rejection_reason")


def normalize_tickers(raw: str | Sequence[str] | None) -> list[str]:
    values = config.DEFAULT_TICKERS if raw is None else raw.split(",") if isinstance(raw, str) else raw
    tickers = list(dict.fromkeys(value.strip().upper() for value in values if value.strip()))
    if not tickers:
        raise ValueError("Ticker list must not be empty")
    return tickers


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name} '{value}'; expected YYYY-MM-DD") from exc


def resolve_parameters(
    tickers: str | Sequence[str] | None,
    start_date: str | None,
    end_date: str | None,
) -> tuple[list[str], date, date]:
    selected_tickers = normalize_tickers(tickers)
    # Same defaults as ingest_prices: the last LOOKBACK_DAYS days to today when no dates are given.
    end = _parse_date(end_date, "end_date") if end_date else date.today()
    start = _parse_date(start_date, "start_date") if start_date else end - timedelta(days=config.LOOKBACK_DAYS)
    if start > end:
        raise ValueError(f"start_date {start} must not be later than end_date {end}")
    return selected_tickers, start, end


def scope_prices(bronze_df, tickers: list[str], start: date, end: date):
    """Parse Bronze prices and keep rows relevant to this request.
    """
    from pyspark.sql import functions as F

    parsed = bronze_df.withColumn(
        "_parsed_price_date", F.expr("try_cast(price_date AS DATE)")
    ).withColumn("_parsed_close", F.expr("try_cast(close AS DECIMAL(18,6))"))
    symbol = F.col("symbol")
    price_date = F.col("_parsed_price_date")
    blank_symbol = symbol.isNull() | (F.length(F.trim(symbol)) == 0)
    return parsed.filter(
        blank_symbol
        | (symbol.isin(*tickers) & (price_date.isNull() | price_date.between(F.lit(start), F.lit(end))))
    )


def prepare_and_validate_prices(
    bronze_df, spark, tickers: list[str], start: date, end: date, as_of: date | None = None
):
    """Scope Bronze to the request, then return accepted and quarantined prices."""
    from pyspark.sql import functions as F

    if not bronze_df.take(1):
        raise ValueError("Bronze market-price snapshot is empty")

    cutoff = as_of or date.today()
    session_values = [session.isoformat() for session in requested_sessions(start, end, cutoff)]

    prepared = scope_prices(bronze_df, tickers, start, end)

    symbol = F.col("symbol")
    price_date = F.col("_parsed_price_date")
    close = F.col("_parsed_close")
    currency = F.col("currency")
    is_session = price_date.cast("string").isin(*session_values) if session_values else F.lit(False)
    checks = [
        ("NULL_SYMBOL", symbol.isNull()),
        ("EMPTY_SYMBOL", symbol.isNotNull() & (F.length(F.trim(symbol)) == 0)),
        ("INVALID_PRICE_DATE", price_date.isNull()),
        ("FUTURE_PRICE_DATE", price_date.isNotNull() & (price_date > F.lit(cutoff))),
        (
            "UNEXPECTED_TRADING_DATE",
            price_date.isNotNull() & (price_date <= F.lit(cutoff)) & ~is_session,
        ),
        ("INVALID_CLOSE", F.col("close").isNull() | close.isNull()),
        ("NONPOSITIVE_CLOSE", close.isNotNull() & (close <= F.lit(0))),
        ("INVALID_CURRENCY", currency.isNull() | (currency != F.lit(config.CURRENCY))),
    ]
    reason_columns = [F.when(condition, F.lit(reason)) for reason, condition in checks]
    prepared = prepared.withColumn("_validation_reason", F.concat_ws("|", *reason_columns))
    invalid_rows = prepared.filter(F.length(F.col("_validation_reason")) > 0)
    valid_rows = prepared.filter(F.length(F.col("_validation_reason")) == 0)

    from pyspark.sql import Window

    business_key = ["symbol", "_parsed_price_date"]
    precedence = Window.partitionBy(*business_key).orderBy(
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("run_id").desc_nulls_last(),
    )
    # Tied identical payloads are interchangeable because Bronze has no row identifier.
    ranked = valid_rows.withColumn("_row_number", F.row_number().over(precedence)).withColumn(
        "_preference_rank", F.dense_rank().over(precedence)
    )
    top_payloads = (
        ranked.filter(F.col("_preference_rank") == 1)
        .groupBy(*business_key)
        .agg(
            F.countDistinct(
                F.to_json(
                    F.struct("close", "currency", "source_api", "ingestion_timestamp", "run_id")
                )
            ).alias("_payload_versions")
        )
    )
    ambiguous_keys = top_payloads.filter(F.col("_payload_versions") > 1).select(*business_key)
    unambiguous = ranked.join(ambiguous_keys, business_key, "left_anti")
    accepted = unambiguous.filter(F.col("_row_number") == 1).select(
        "symbol",
        F.col("_parsed_price_date").alias("price_date"),
        F.col("_parsed_close").alias("close"),
        "currency",
        "source_api",
        "ingestion_timestamp",
        "run_id",
        # Lineage of the selected Bronze row (same values as ingestion_timestamp/run_id above).
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        F.col("run_id").alias("bronze_run_id"),
    )

    invalid_rejects = invalid_rows.withColumn(
        "rejection_reason", F.col("_validation_reason")
    )
    unambiguous_ranked = ranked.join(ambiguous_keys, business_key, "left_anti")
    duplicate_rejects = (
        unambiguous_ranked.filter(F.col("_row_number") > 1)
        .withColumn("rejection_reason", F.lit("DUPLICATE_BUSINESS_KEY"))
    )
    ambiguous_rejects = ranked.join(ambiguous_keys, business_key, "inner").withColumn(
        "rejection_reason", F.lit("AMBIGUOUS_DUPLICATE_KEY")
    )
    raw_columns = [
        "symbol",
        "price_date",
        "close",
        "currency",
        "source_api",
        "ingestion_timestamp",
        "run_id",
    ]

    def reject_projection(frame):
        return frame.select(
            *raw_columns,
            F.col("rejection_reason"),
            F.current_timestamp().alias("rejected_at"),
        )

    rejects = reject_projection(invalid_rejects).unionByName(
        reject_projection(duplicate_rejects)
    ).unionByName(reject_projection(ambiguous_rejects))
    return accepted, rejects


def find_missing_accepted_sessions(
    spark, accepted_df, tickers: list[str], start: date, end: date, as_of: date | None = None
):
    """Return a sample of expected ticker/session pairs absent from the accepted rows."""
    from pyspark.sql.types import DateType, StringType, StructField, StructType

    sessions = requested_sessions(start, end, as_of)
    if not sessions:
        return []
    expected_rows = [(ticker, session) for ticker in tickers for session in sessions]
    expected_schema = StructType(
        [StructField("symbol", StringType(), False), StructField("price_date", DateType(), False)]
    )
    expected_df = spark.createDataFrame(expected_rows, schema=expected_schema)
    accepted_keys = accepted_df.select("symbol", "price_date").distinct()
    return expected_df.join(accepted_keys, ["symbol", "price_date"], "left_anti").limit(10).collect()


def write_rejects_history(spark, rejects_df) -> int:
    """Append rejects not yet recorded and return how many were new; reruns add nothing."""
    return merge_rejects(
        spark,
        rejects_df,
        REJECTS_TABLE,
        columns=(*REJECT_KEY_COLUMNS, "ingestion_timestamp", "run_id", "rejected_at"),
        key_columns=REJECT_KEY_COLUMNS,
    )


def write_silver_prices(spark, prices_df) -> dict[str, int]:
    """Upsert by (symbol, price_date); history is kept and unchanged rows are not rewritten.
    """
    prices_view = "_trade_pipeline_silver_market_prices_staging"
    prices_df.createOrReplaceTempView(prices_view)
    result = spark.sql(
        f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING {prices_view} AS source
        ON target.symbol = source.symbol AND target.price_date = source.price_date
        WHEN MATCHED AND NOT (
            target.close <=> source.close
            AND target.currency <=> source.currency
            AND target.source_api <=> source.source_api
        ) THEN UPDATE SET
            close = source.close,
            currency = source.currency,
            source_api = source.source_api,
            ingestion_timestamp = source.ingestion_timestamp,
            run_id = source.run_id,
            bronze_ingestion_timestamp = source.bronze_ingestion_timestamp,
            bronze_run_id = source.bronze_run_id
        WHEN NOT MATCHED THEN INSERT
            (symbol, price_date, close, currency, source_api, ingestion_timestamp, run_id,
             bronze_ingestion_timestamp, bronze_run_id)
        VALUES
            (source.symbol, source.price_date, source.close, source.currency,
             source.source_api, source.ingestion_timestamp, source.run_id,
             source.bronze_ingestion_timestamp, source.bronze_run_id)
        """
    ).first()
    return read_merge_counts(result)


def run_processing(
    tickers: str | Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
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

    def process() -> dict:
        selected_tickers, start, end = resolve_parameters(tickers, start_date, end_date)
        logger.info(
            "Starting Silver market-price processing run_id=%s task_run_id=%s tickers=%s start_date=%s end_date=%s",
            run_id,
            attempt_id,
            selected_tickers,
            start,
            end,
        )
        bronze_df = spark_session.table(BRONZE_TABLE)
        # Only rows relevant to this request are input; unrelated history is not counted.
        input_count = scope_prices(bronze_df, selected_tickers, start, end).count()
        prices_df, rejects_df = prepare_and_validate_prices(
            bronze_df, spark_session, selected_tickers, start, end
        )
        rejects_written = write_rejects_history(spark_session, rejects_df)
        missing_sessions = find_missing_accepted_sessions(
            spark_session, prices_df, selected_tickers, start, end
        )
        if missing_sessions:
            raise ValueError(
                "Missing expected ticker/session prices in accepted data; Silver not updated: "
                + "; ".join(f"{row['symbol']} {row['price_date']}" for row in missing_sessions)
            )
        from pyspark.sql import functions as F

        accepted_count = prices_df.count()
        rejected_count = rejects_df.count()
        duplicate_count = rejects_df.filter(
            F.col("rejection_reason").isin("DUPLICATE_BUSINESS_KEY", "AMBIGUOUS_DUPLICATE_KEY")
        ).count()
        reason_counts = {
            row["reason"]: row["count"]
            for row in rejects_df.select(
                F.explode(F.split(F.col("rejection_reason"), r"\|")).alias("reason")
            )
            .groupBy("reason")
            .count()
            .collect()
        }
        merge_counts = write_silver_prices(spark_session, prices_df)
        rows_written = merge_counts["inserted_rows"] + merge_counts["updated_rows"]
        logger.info(
            "Completed Silver market-price processing run_id=%s task_run_id=%s input=%d accepted=%d written=%d rejected=%d new_rejects=%d duplicates=%d",
            run_id,
            attempt_id,
            input_count,
            accepted_count,
            rows_written,
            rejected_count,
            rejects_written,
            duplicate_count,
        )
        return {
            "run_id": run_id,
            "task_run_id": attempt_id,
            "tickers": selected_tickers,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "requested_tickers": len(selected_tickers),
            "input_rows": input_count,
            # output_rows: valid Silver rows produced; rows_written: only those inserted or updated.
            "output_rows": accepted_count,
            "rows_written": rows_written,
            "accepted_rows": accepted_count,
            **merge_counts,
            # rejected_rows: rejected in-scope Bronze rows; rejects_written: new reject-history rows.
            "rejected_rows": rejected_count,
            "duplicate_rows": duplicate_count,
            "rejects_written": rejects_written,
            "issue_counts": reason_counts,
        }

    return audit.run_audited_task(
        spark_session,
        run_id,
        attempt_id,
        "process_prices",
        BRONZE_TABLE,
        f"{SILVER_TABLE}, {REJECTS_TABLE}",
        process,
    )


def _get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Validate Bronze market prices into Silver")
    parser.add_argument("--tickers", default=None)
    parser.add_argument("--start_date", default=None)
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--pipeline_run_id", default=None)
    parser.add_argument("--task_run_id", default=None)
    parser.add_argument("--task_execution_count", default=None)
    args = parser.parse_args(argv)
    run_processing(
        args.tickers or None,
        args.start_date or None,
        args.end_date or None,
        pipeline_run_id=args.pipeline_run_id or None,
        task_run_id=args.task_run_id or None,
        task_execution_count=args.task_execution_count or None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
