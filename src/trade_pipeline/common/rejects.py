"""Idempotent, history-preserving writes to Silver reject tables."""

from __future__ import annotations

from trade_pipeline.common.delta_stats import read_merge_counts


def merge_rejects(spark, rejects_df, table: str, columns: tuple[str, ...], key_columns: tuple[str, ...]) -> int:
    """Insert rejects not already recorded for the same key and return how many were new.

    <=> is null-safe equality, so rows with NULL values still match on rerun.
    The identity id column is never written.
    """
    view = "_trade_pipeline_rejects_source"
    rejects_df.dropDuplicates(list(key_columns)).createOrReplaceTempView(view)
    match = " AND ".join(f"target.{column} <=> source.{column}" for column in key_columns)
    column_list = ", ".join(columns)
    values = ", ".join(f"source.{column}" for column in columns)
    result = spark.sql(
        f"""
        MERGE INTO {table} AS target
        USING {view} AS source
        ON {match}
        WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({values})
        """
    ).first()
    return read_merge_counts(result)["inserted_rows"]