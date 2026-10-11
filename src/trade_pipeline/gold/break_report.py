"""Retry-safe writer for valuation business breaks."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

BREAKS_TABLE = "cibc.gold.valuation_breaks"


def write_valuation_breaks(
    spark,
    run_id: str,
    breaks,
) -> int:
    """Insert this run's breaks; rerunning the same run_id inserts nothing new.

    Breaks are run-level history: each run_id keeps its own rows and older runs are never
    updated or deleted, so consumers should filter on the latest run_id for current breaks.
    Returns the number of distinct breaks detected in this run (not the number newly inserted).
    A Spark DataFrame is merged directly without driver collection. If
    duplicate identities differ in details, the lexically first description is
    chosen deterministically, followed by earliest detection time and status.
    """
    from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType
    from pyspark.sql import Window, functions as F

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("trade_id", StringType(), True),
            StructField("instrument_id", StringType(), True),
            StructField("valuation_date", DateType(), True),
            StructField("break_type", StringType(), False),
            StructField("break_description", StringType(), False),
            StructField("detected_at", TimestampType(), False),
            StructField("status", StringType(), False),
        ]
    )
    if hasattr(breaks, "select") and hasattr(breaks, "columns"):
        source = breaks
        if "run_id" in source.columns:
            source = source.drop("run_id")
        if "detected_at" not in source.columns:
            source = source.withColumn("detected_at", F.current_timestamp())
        if "status" not in source.columns:
            source = source.withColumn("status", F.lit("OPEN"))
        source = source.select(
            F.lit(run_id).cast("string").alias("run_id"),
            F.col("trade_id").cast("string").alias("trade_id"),
            F.col("instrument_id").cast("string").alias("instrument_id"),
            F.col("valuation_date").cast("date").alias("valuation_date"),
            F.col("break_type").cast("string").alias("break_type"),
            F.col("break_description").cast("string").alias("break_description"),
            F.col("detected_at").cast("timestamp").alias("detected_at"),
            F.col("status").cast("string").alias("status"),
        )
    else:
        from pyspark.sql import Row

        records = [
            {
                "run_id": run_id,
                "trade_id": item.get("trade_id"),
                "instrument_id": item.get("instrument_id"),
                "valuation_date": item.get("valuation_date"),
                "break_type": item["break_type"],
                "break_description": item["break_description"],
                "detected_at": item.get("detected_at") or datetime.now(timezone.utc),
                "status": item.get("status", "OPEN"),
            }
            for item in breaks
        ]
        if not records:
            return 0
        source = spark.createDataFrame([Row(**record) for record in records], schema=schema)

    key_columns = ["run_id", "trade_id", "instrument_id", "valuation_date", "break_type"]
    identity_order = Window.partitionBy(*key_columns).orderBy(
        F.col("break_description").asc_nulls_last(),
        F.col("detected_at").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
    source = source.withColumn("_identity_rank", F.row_number().over(identity_order)).filter(
        F.col("_identity_rank") == 1
    ).drop("_identity_rank")
    view = "_valuation_breaks_source"
    source.createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {BREAKS_TABLE} AS target
        USING {view} AS source
        ON target.run_id = source.run_id
           AND target.trade_id <=> source.trade_id
           AND target.instrument_id <=> source.instrument_id
           AND target.valuation_date <=> source.valuation_date
           AND target.break_type = source.break_type
        WHEN NOT MATCHED THEN INSERT (
            run_id, trade_id, instrument_id, valuation_date, break_type,
            break_description, detected_at, status
        ) VALUES (
            source.run_id, source.trade_id, source.instrument_id, source.valuation_date,
            source.break_type, source.break_description, source.detected_at, source.status
        )
        """
    )
    spark.catalog.dropTempView(view)
    return source.count()
