"""Validate Bronze trades and maintain Silver trade history, one accepted row per trade_id.

Business rules for versions:
- The latest version is the highest version, then the latest modified_timestamp.
- If the latest version is invalid, or ties with a conflicting payload, the trade is
  unusable: it is quarantined and NOT replaced by an older valid version.
- A trade with any unparseable version cannot be ordered, so it is unusable too.
- An unusable trade is removed from Silver, so it never feeds positions or P&L. Bronze keeps every
  raw record ever ingested, so every trade_id ever seen is listed; accepted trades are never deleted.
- Trades are processed over their full history; the job's valuation dates do not filter them.
- Bronze is insert-only history: a record re-sent with changed attributes but the same version and
  modified_timestamp ties with the older record and is quarantined as AMBIGUOUS_TRADE_VERSION.
"""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, timedelta
from typing import Sequence

from trade_pipeline import audit, config
from trade_pipeline.common.delta_stats import read_delete_count, read_merge_counts
from trade_pipeline.common.market_calendar import expected_trading_sessions
from trade_pipeline.common.rejects import merge_rejects

logger = logging.getLogger(__name__)
BRONZE_TABLE = "cibc.bronze.trades"
SILVER_TABLE = "cibc.silver.trades"
REJECTS_TABLE = "cibc.silver.trades_rejects"
BRONZE_COLUMNS = (
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
    "source_file",
    "ingestion_timestamp",
    "run_id",
)
TRADE_SOURCE_BUSINESS_COLUMNS = BRONZE_COLUMNS[:14]
# Run metadata is excluded so a reject's identity does not depend on which run ingested it.
REJECT_KEY_COLUMNS = (*TRADE_SOURCE_BUSINESS_COLUMNS, "source_file", "rejection_reason")
SILVER_COLUMNS = (
    "trade_id",
    "counterparty_id",
    "instrument_id",
    "symbol",
    "quantity",
    "price",
    "currency",
    "trade_date",
    "settlement_date",
    "book_id",
    "version",
    "modified_timestamp",
)
# Bronze record that produced the Silver row (the run that first ingested it). Not part of the
# unchanged check, so a lineage-only difference never rewrites a Silver row.
LINEAGE_COLUMNS = ("bronze_run_id", "bronze_ingestion_timestamp", "source_file")
DUPLICATE_REASONS = (
    "DUPLICATE_TRADE_VERSION",
    "AMBIGUOUS_TRADE_VERSION",
    "SUPERSEDED_BY_INVALID_LATEST_VERSION",
    "SUPERSEDED_BY_AMBIGUOUS_LATEST_VERSION",
)


def _parse_date(value: str | None, field_name: str, default: str) -> date:
    try:
        return date.fromisoformat(value or default)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name} '{value}'; expected YYYY-MM-DD") from exc


def resolve_parameters(start_date: str | None, end_date: str | None) -> tuple[date, date]:
    """Validate the job's valuation window; trade history processing does not use it."""
    end = _parse_date(end_date, "end_date", date.today().isoformat())
    start = _parse_date(start_date, "start_date", (end - timedelta(days=config.LOOKBACK_DAYS)).isoformat())
    if start > end:
        raise ValueError(f"start_date {start} must not be later than end_date {end}")
    return start, end


def _session_calendar(bronze_df):
    """One row per NYSE session in the data's trade-date span with its T+1 settlement session."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, StructField, StructType

    bounds = (
        bronze_df.select(F.expr("try_cast(trade_date AS DATE)").alias("d"))
        .agg(F.min("d").alias("lo"), F.max("d").alias("hi"))
        .first()
    )
    rows = []
    if bounds["lo"] is not None:
        # Clamp so implausible dates cannot overflow the calendar; old ones fail as NON_TRADING_TRADE_DATE.
        low = max(bounds["lo"], date(1970, 1, 1))
        high = min(bounds["hi"], date.today())
        sessions = expected_trading_sessions(low, high + timedelta(days=10))
        rows = [(s, nxt) for s, nxt in zip(sessions, sessions[1:]) if s <= high]
    schema = StructType(
        [
            StructField("_session_date", DateType(), False),
            StructField("_expected_settlement", DateType(), False),
        ]
    )
    return bronze_df.sparkSession.createDataFrame(rows, schema)


def prepare_trades(bronze_df, as_of: date | None = None):
    """Return latest valid trades from the full Bronze history, rejects, and duplicate count.

    No valuation-window filter is applied: history before the job's dates is needed for positions.
    """
    from pyspark.sql import Window, functions as F

    if not bronze_df.take(1):
        raise ValueError("Bronze trade snapshot is empty")

    today = as_of or date.today()
    calendar = _session_calendar(bronze_df)
    raw = bronze_df
    exact_window = Window.partitionBy(*TRADE_SOURCE_BUSINESS_COLUMNS).orderBy(
        F.col("source_file").asc_nulls_last(),
        F.col("ingestion_timestamp").asc_nulls_last(),
        F.col("run_id").asc_nulls_last(),
    )
    raw = raw.withColumn("_source_copy_rank", F.row_number().over(exact_window))
    exact_duplicates = raw.filter(F.col("_source_copy_rank") > 1).withColumn(
        "rejection_reason", F.lit("DUPLICATE_TRADE_VERSION")
    )
    unique = raw.filter(F.col("_source_copy_rank") == 1)

    parsed = (
        unique.withColumn("_quantity", F.expr("try_cast(quantity AS DECIMAL(18,6))"))
        .withColumn("_price", F.expr("try_cast(price AS DECIMAL(18,6))"))
        .withColumn("_trade_date", F.expr("try_cast(trade_date AS DATE)"))
        .withColumn("_settlement_date", F.expr("try_cast(settlement_date AS DATE)"))
        .withColumn("_version", F.expr("try_cast(version AS INT)"))
        .withColumn("_modified_timestamp", F.expr("try_cast(modified_timestamp AS TIMESTAMP)"))
        .withColumn("_trade_timestamp", F.expr("try_cast(trade_timestamp AS TIMESTAMP)"))
    )
    parsed = parsed.join(
        F.broadcast(calendar), parsed["_trade_date"] == calendar["_session_date"], "left"
    )
    side = F.upper(F.trim(F.col("side")))
    instrument_ids = tuple(config.INSTRUMENT_ID_TO_SYMBOL)
    instrument_mapping = F.create_map(
        *[
            item
            for instrument_id, symbol_name in config.INSTRUMENT_ID_TO_SYMBOL.items()
            for item in (F.lit(instrument_id), F.lit(symbol_name))
        ]
    )
    checks = [
        ("NULL_TRADE_ID", F.col("trade_id").isNull() | (F.length(F.trim(F.col("trade_id"))) == 0)),
        ("NULL_CLIENT_ID", F.col("counterparty_id").isNull() | (F.length(F.trim(F.col("counterparty_id"))) == 0)),
        ("NULL_INSTRUMENT_ID", F.col("instrument_id").isNull() | (F.length(F.trim(F.col("instrument_id"))) == 0)),
        ("UNKNOWN_INSTRUMENT", F.col("instrument_id").isNotNull() & ~F.col("instrument_id").isin(*instrument_ids)),
        ("INVALID_SYMBOL", F.col("symbol").isNull() | (F.length(F.trim(F.col("symbol"))) == 0)),
        (
            "INSTRUMENT_SYMBOL_MISMATCH",
            F.col("instrument_id").isin(*instrument_ids)
            & F.col("symbol").isNotNull()
            & (F.col("symbol") != instrument_mapping[F.col("instrument_id")]),
        ),
        ("NULL_BOOK_ID", F.col("book_id").isNull() | (F.length(F.trim(F.col("book_id"))) == 0)),
        ("INVALID_SIDE", side.isNull() | ~side.isin("BUY", "SELL")),
        ("INVALID_QUANTITY", F.col("_quantity").isNull()),
        ("ZERO_QUANTITY", F.col("_quantity").isNotNull() & (F.col("_quantity") == 0)),
        (
            "QUANTITY_SIDE_MISMATCH",
            F.col("_quantity").isNotNull() & (side == "BUY") & (F.col("_quantity") < 0),
        ),
        ("INVALID_PRICE", F.col("_price").isNull()),
        ("NONPOSITIVE_PRICE", F.col("_price").isNotNull() & (F.col("_price") <= 0)),
        ("INVALID_CURRENCY", F.col("currency").isNull() | (F.col("currency") != config.CURRENCY)),
        ("INVALID_TRADE_DATE", F.col("_trade_date").isNull()),
        # Future-dated trades are not supported, and their T+1 settlement cannot be checked.
        ("FUTURE_TRADE_DATE", F.col("_trade_date") > F.lit(today)),
        (
            "NON_TRADING_TRADE_DATE",
            F.col("_trade_date").isNotNull() & (F.col("_trade_date") <= F.lit(today)) & F.col("_session_date").isNull(),
        ),
        (
            "TRADE_DATE_TIMESTAMP_MISMATCH",
            F.col("_trade_date").isNotNull()
            & F.col("_trade_timestamp").isNotNull()
            & (F.col("_trade_date") != F.to_date(F.col("_trade_timestamp"))),
        ),
        ("INVALID_SETTLEMENT_DATE", F.col("_settlement_date").isNull()),
        (
            "INVALID_SETTLEMENT_SEQUENCE",
            F.col("_trade_date").isNotNull()
            & F.col("_settlement_date").isNotNull()
            & (F.col("_settlement_date") < F.col("_trade_date")),
        ),
        (
            "INVALID_SETTLEMENT_CONVENTION",
            F.col("_trade_date").isNotNull()
            & F.col("_settlement_date").isNotNull()
            & (F.col("_settlement_date") >= F.col("_trade_date"))
            & F.col("_expected_settlement").isNotNull()
            & (F.col("_settlement_date") != F.col("_expected_settlement")),
        ),
        ("INVALID_VERSION", F.col("_version").isNull() | (F.col("_version") <= 0)),
        ("INVALID_MODIFIED_TIMESTAMP", F.col("_modified_timestamp").isNull()),
        ("INVALID_TRADE_TIMESTAMP", F.col("_trade_timestamp").isNull()),
        (
            "MODIFIED_BEFORE_TRADE_TIMESTAMP",
            F.col("_modified_timestamp").isNotNull()
            & F.col("_trade_timestamp").isNotNull()
            & (F.col("_modified_timestamp") < F.col("_trade_timestamp")),
        ),
    ]
    reasons = [F.when(condition, F.lit(reason)) for reason, condition in checks]
    parsed = parsed.withColumn("_rejection_reason", F.concat_ws("|", *reasons))
    invalid = parsed.filter(F.length(F.col("_rejection_reason")) > 0)
    valid = parsed.filter(F.length(F.col("_rejection_reason")) == 0)

    orderable = parsed.filter(
        F.col("trade_id").isNotNull() & F.col("_version").isNotNull()
    )
    version_window = Window.partitionBy("trade_id").orderBy(
        F.col("_version").desc_nulls_last(),
        F.col("_modified_timestamp").desc_nulls_last(),
    )
    ranked = orderable.withColumn("_version_rank", F.dense_rank().over(version_window)).withColumn(
        "_row_number", F.row_number().over(version_window)
    )
    version_payload = [F.col(column) for column in TRADE_SOURCE_BUSINESS_COLUMNS]
    top_payloads = (
        ranked.filter(F.col("_version_rank") == 1)
        .groupBy("trade_id")
        .agg(F.countDistinct(F.to_json(F.struct(*version_payload))).alias("_payload_count"))
    )
    ambiguous_ids = top_payloads.filter(F.col("_payload_count") > 1).select("trade_id")
    latest_invalid_ids = ranked.filter(
        (F.col("_version_rank") == 1) & (F.length(F.col("_rejection_reason")) > 0)
    ).select("trade_id").distinct()
    unorderable_version_ids = parsed.filter(
        F.col("trade_id").isNotNull() & F.col("_version").isNull()
    ).select("trade_id").distinct()
    latest_valid = ranked.filter(
        (F.col("_version_rank") == 1)
        & (F.col("_row_number") == 1)
        & (F.length(F.col("_rejection_reason")) == 0)
    ).join(ambiguous_ids, "trade_id", "left_anti").join(
        unorderable_version_ids, "trade_id", "left_anti"
    )
    accepted = latest_valid.select(
        "trade_id",
        "counterparty_id",
        "instrument_id",
        "symbol",
        F.when((side == "SELL") & (F.col("_quantity") > 0), -F.col("_quantity"))
        .otherwise(F.col("_quantity"))
        .alias("quantity"),
        F.col("_price").alias("price"),
        "currency",
        F.col("_trade_date").alias("trade_date"),
        F.col("_settlement_date").alias("settlement_date"),
        "book_id",
        F.col("_version").alias("version"),
        F.col("_modified_timestamp").alias("modified_timestamp"),
        # Lineage comes from the selected latest valid Bronze row.
        F.col("run_id").alias("bronze_run_id"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        "source_file",
    )

    invalid_rejects = invalid.withColumn("rejection_reason", F.col("_rejection_reason"))
    top_valid = valid.join(
        ranked.filter(F.col("_version_rank") == 1).select("trade_id"), "trade_id", "inner"
    )
    ambiguous_rejects = top_valid.join(ambiguous_ids, "trade_id", "inner").withColumn(
        "rejection_reason", F.lit("AMBIGUOUS_TRADE_VERSION")
    )
    lower_valid = ranked.filter(
        (F.col("_version_rank") > 1) & (F.length(F.col("_rejection_reason")) == 0)
    ).join(
        latest_invalid_ids.withColumn("_has_invalid_latest", F.lit(True)), "trade_id", "left"
    ).join(
        ambiguous_ids.withColumn("_has_ambiguous_latest", F.lit(True)), "trade_id", "left"
    ).join(
        unorderable_version_ids.withColumn("_has_unorderable_version", F.lit(True)),
        "trade_id",
        "left",
    )
    lower_valid = lower_valid.withColumn(
        "rejection_reason",
        F.when(
            F.col("_has_invalid_latest").isNotNull()
            | F.col("_has_unorderable_version").isNotNull(),
            F.lit("SUPERSEDED_BY_INVALID_LATEST_VERSION"),
        ).when(
            F.col("_has_ambiguous_latest").isNotNull(),
            F.lit("SUPERSEDED_BY_AMBIGUOUS_LATEST_VERSION"),
        ).otherwise(F.lit("DUPLICATE_TRADE_VERSION")),
    )
    base_rejects = reject_projection(invalid_rejects)
    duplicate_rejects = reject_projection(exact_duplicates.withColumnRenamed("rejection_reason", "rejection_reason"))
    version_rejects = reject_projection(ambiguous_rejects).unionByName(reject_projection(lower_valid))
    rejects = base_rejects.unionByName(duplicate_rejects).unionByName(version_rejects)
    duplicate_count = exact_duplicates.count() + lower_valid.count() + ambiguous_rejects.count()
    return accepted, rejects, duplicate_count


def reject_projection(frame):
    from pyspark.sql import functions as F

    return frame.select(
        *BRONZE_COLUMNS,
        F.col("rejection_reason"),
        F.current_timestamp().alias("rejected_at"),
    )


def write_silver_trades(spark, trades_df, unusable_ids_df) -> dict[str, int]:
    """Upsert accepted trades, then delete trades whose latest version is unusable.

    The two statements are not atomic, but both are idempotent: if the DELETE fails, the task
    fails and a rerun finishes it. Delta generates the identity id.
    """
    view = "_trade_pipeline_silver_trades_source"
    unusable_view = "_trade_pipeline_silver_trades_unusable_ids"
    trades_df.createOrReplaceTempView(view)
    unusable_ids_df.createOrReplaceTempView(unusable_view)
    update_columns = [column for column in SILVER_COLUMNS if column != "trade_id"]
    update_set = ", ".join(f"{column} = source.{column}" for column in [*update_columns, *LINEAGE_COLUMNS])
    unchanged = " AND ".join(f"target.{column} <=> source.{column}" for column in update_columns)
    insert_columns = ", ".join([*SILVER_COLUMNS, *LINEAGE_COLUMNS])
    insert_values = ", ".join(f"source.{column}" for column in [*SILVER_COLUMNS, *LINEAGE_COLUMNS])
    merge_result = spark.sql(
        f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING {view} AS source
        ON target.trade_id = source.trade_id
        WHEN MATCHED AND NOT ({unchanged}) THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    ).first()
    delete_result = spark.sql(
        f"DELETE FROM {SILVER_TABLE} WHERE trade_id IN (SELECT trade_id FROM {unusable_view})"
    ).first()
    counts = read_merge_counts(merge_result)
    counts["deleted_rows"] = read_delete_count(delete_result)
    return counts

def write_trade_rejects_history(spark, rejects_df) -> int:
    """Append rejects not yet recorded and return how many were new."""
    return merge_rejects(
        spark,
        rejects_df,
        REJECTS_TABLE,
        columns=(*BRONZE_COLUMNS, "rejection_reason", "rejected_at"),
        key_columns=REJECT_KEY_COLUMNS,
    )


def run_processing(
    start_date: str | None = None,
    end_date: str | None = None,
    spark=None,
    pipeline_run_id: str | None = None,
    task_run_id: str | None = None,
    task_execution_count: str | int | None = None,
) -> dict:
    from pyspark.sql import functions as F

    spark_session = spark if spark is not None else _get_spark_session()
    start, end = resolve_parameters(start_date, end_date)
    run_id = pipeline_run_id or str(uuid.uuid4())
    attempt_id = (
        f"{task_run_id}-{task_execution_count}"
        if task_run_id is not None and task_execution_count is not None
        else task_run_id or str(uuid.uuid4())
    )

    def process() -> dict:
        logger.info("Processing full trade history; valuation window %s..%s is not applied", start, end)
        bronze_df = spark_session.table(BRONZE_TABLE)
        input_count = bronze_df.count()
        accepted_df, rejects_df, duplicate_count = prepare_trades(bronze_df)
        accepted_count = accepted_df.count()
        if accepted_count == 0:
            raise ValueError("No valid trades in Bronze snapshot; Silver trades not changed")
        rejected_count = rejects_df.count()
        reason_counts = {
            row["reason"]: row["count"]
            for row in rejects_df.select(
                F.explode(F.split(F.col("rejection_reason"), r"\|")).alias("reason")
            )
            .groupBy("reason")
            .count()
            .collect()
        }
        rejects_written = write_trade_rejects_history(spark_session, rejects_df)
        listed_ids = bronze_df.filter(
            F.col("trade_id").isNotNull() & (F.length(F.trim(F.col("trade_id"))) > 0)
        ).select("trade_id").distinct()
        unusable_ids_df = listed_ids.join(accepted_df.select("trade_id"), "trade_id", "left_anti")
        merge_counts = write_silver_trades(spark_session, accepted_df, unusable_ids_df)
        rows_written = merge_counts["inserted_rows"] + merge_counts["updated_rows"]
        return {
            "run_id": run_id,
            "task_run_id": attempt_id,
            "input_rows": input_count,
            "requested_tickers": 0,
            # output_rows: valid Silver rows produced; rows_written: only those inserted or updated.
            "output_rows": accepted_count,
            "rows_written": rows_written,
            "accepted_rows": accepted_count,
            **merge_counts,
            # rejected_rows: rejected Bronze rows; rejects_written: new reject-history rows.
            "rejected_rows": rejected_count,
            "rejects_written": rejects_written,
            "duplicate_rows": duplicate_count,
            "issue_counts": reason_counts,
        }

    return audit.run_audited_task(
        spark_session,
        run_id,
        attempt_id,
        "process_trades",
        BRONZE_TABLE,
        f"{SILVER_TABLE}, {REJECTS_TABLE}",
        process,
    )


def _get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Validate Bronze trades into Silver")
    parser.add_argument("--start_date", default=None)
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--pipeline_run_id", default=None)
    parser.add_argument("--task_run_id", default=None)
    parser.add_argument("--task_execution_count", default=None)
    args = parser.parse_args(argv)
    try:
        run_processing(
            start_date=args.start_date or None,
            end_date=args.end_date or None,
            pipeline_run_id=args.pipeline_run_id or None,
            task_run_id=args.task_run_id or None,
            task_execution_count=args.task_execution_count or None,
        )
    except Exception:
        logger.exception("Trade Silver processing failed")
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
