"""Ingest raw synthetic trades from the Unity Catalog Volume into Bronze.

The CSV is a complete trade snapshot: each run replaces cibc.bronze.trades with the distinct raw
records of the file (INSERT OVERWRITE), so rerunning the same file gives the same table.
"""

from __future__ import annotations

import argparse
import logging
import uuid
from collections.abc import Sequence

from trade_pipeline import audit, config

logger = logging.getLogger(__name__)
TARGET_TABLE = "cibc.bronze.trades"
TRADE_COLUMNS = (
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
BRONZE_COLUMNS = (*TRADE_COLUMNS, "source_file", "ingestion_timestamp", "run_id")


def read_trade_csv(spark, input_path: str):
    """Read raw CSV strings; fail on a wrong header, empty source or structurally corrupt records."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [*[StructField(name, StringType(), True) for name in TRADE_COLUMNS],
         StructField("_corrupt_record", StringType(), True)]
    )
    try:
        header = tuple(spark.read.option("header", "true").csv(input_path).columns)
        if header != TRADE_COLUMNS:
            raise ValueError(
                f"Trade CSV header mismatch at {input_path}: "
                f"expected {list(TRADE_COLUMNS)}, got {list(header)}"
            )
        source = (
            spark.read.schema(schema)
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("multiLine", "false")
            .csv(input_path)
        )
        if not source.take(1):
            raise ValueError(f"Trade CSV source is empty: {input_path}")
        corrupt = source.filter(F.col("_corrupt_record").isNotNull())
        corrupt_sample = corrupt.select("_corrupt_record").limit(5).collect()
        if corrupt_sample:
            raise ValueError(
                f"Trade CSV has structurally malformed records at {input_path}: "
                + "; ".join(str(row["_corrupt_record"]) for row in corrupt_sample)
            )
        return source.drop("_corrupt_record")
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Could not read trade CSV from Volume path '{input_path}': {exc}") from exc


def prepare_bronze_batch(source_df, input_path: str, run_id: str):
    """Drop identical raw rows (versions of a trade differ, so they are kept) and add run metadata."""
    from pyspark.sql import functions as F

    return (
        source_df.select(*TRADE_COLUMNS)
        .dropDuplicates()
        .withColumn("source_file", F.lit(input_path))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .select(*BRONZE_COLUMNS)
    )


def overwrite_bronze_trades(spark, batch_df) -> None:
    """Atomically replace the Bronze snapshot; the table, its schema and grants are kept."""
    view = "_trade_pipeline_bronze_trades_source"
    batch_df.createOrReplaceTempView(view)
    columns = ", ".join(BRONZE_COLUMNS)
    spark.sql(f"INSERT OVERWRITE TABLE {TARGET_TABLE} ({columns}) SELECT {columns} FROM {view}")


def _get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def run_ingestion(
    input_path: str | None = None,
    spark=None,
    pipeline_run_id: str | None = None,
    task_run_id: str | None = None,
    task_execution_count: str | int | None = None,
) -> dict:
    spark_session = spark if spark is not None else _get_spark_session()
    source_path = input_path or config.TRADE_CSV_PATH
    run_id = pipeline_run_id or str(uuid.uuid4())
    attempt_id = (
        f"{task_run_id}-{task_execution_count}"
        if task_run_id is not None and task_execution_count is not None
        else task_run_id or str(uuid.uuid4())
    )

    def ingest() -> dict:
        # Validation happens before the overwrite, so a bad file never replaces Bronze.
        source_df = read_trade_csv(spark_session, source_path)
        input_rows = source_df.count()
        batch_df = prepare_bronze_batch(source_df, source_path, run_id)
        rows_written = batch_df.count()
        overwrite_bronze_trades(spark_session, batch_df)
        duplicate_rows = input_rows - rows_written
        logger.info(
            "Bronze trades snapshot replaced run_id=%s task_run_id=%s input_rows=%d rows_written=%d "
            "duplicate_rows=%d source=%s",
            run_id,
            attempt_id,
            input_rows,
            rows_written,
            duplicate_rows,
            source_path,
        )
        return {
            "run_id": run_id,
            "task_run_id": attempt_id,
            "input_rows": input_rows,
            # The snapshot holds exactly the distinct CSV rows, so output = written.
            "output_rows": rows_written,
            "rows_written": rows_written,
            # Kept for compatibility: rows written into the replacement snapshot.
            "inserted_rows": rows_written,
            "requested_tickers": 0,
            "rejected_rows": 0,
            "duplicate_rows": duplicate_rows,
            "issue_counts": {"DUPLICATE_IN_BATCH": duplicate_rows} if duplicate_rows else {},
        }

    return audit.run_audited_task(
        spark_session,
        run_id,
        attempt_id,
        "ingest_trades",
        source_path,
        TARGET_TABLE,
        ingest,
    )


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest trade CSV from the configured UC Volume")
    parser.add_argument("--input_path", default=None)
    parser.add_argument("--pipeline_run_id", default=None)
    parser.add_argument("--task_run_id", default=None)
    parser.add_argument("--task_execution_count", default=None)
    args = parser.parse_args(argv)
    try:
        run_ingestion(
            input_path=args.input_path or None,
            pipeline_run_id=args.pipeline_run_id or None,
            task_run_id=args.task_run_id or None,
            task_execution_count=args.task_execution_count or None,
        )
    except Exception:
        logger.exception("Trade Bronze ingestion failed")
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
