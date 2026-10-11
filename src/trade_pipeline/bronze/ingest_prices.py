"""Download Yahoo Finance daily closing prices.
Rerunning is idempotent: existing history is kept and a row changes only when its price or
source changed. 
"""

from __future__ import annotations

import argparse
import logging
import math
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Sequence

import pandas as pd
import yfinance as yf

from trade_pipeline import config
from trade_pipeline import audit
from trade_pipeline.common.market_calendar import expected_trading_sessions, requested_sessions

logger = logging.getLogger(__name__)
MAX_DOWNLOAD_ATTEMPTS = 3
RETRY_BASE_SECONDS = 1


def normalize_tickers(raw: str | Sequence[str] | None) -> list[str]:
    """Parse, uppercase, and deduplicate the requested tickers, preserving order."""
    if raw is None:
        values: Iterable[str] = config.DEFAULT_TICKERS
    elif isinstance(raw, str):
        values = raw.split(",")
    else:
        values = raw

    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        ticker = value.strip().upper()
        if not ticker or ticker in seen:
            continue
        normalized.append(ticker)
        seen.add(ticker)

    if not normalized:
        raise ValueError("Ticker list must not be empty")
    return normalized


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name} '{value}'; expected YYYY-MM-DD") from exc


def resolve_date_range(start_param: str | None, end_param: str | None) -> tuple[date, date]:
    """Resolve start/end parameters (default: the last LOOKBACK_DAYS days to today) and validate the range."""
    end_date = _parse_date(end_param, "end_date") if end_param else date.today()
    default_start = end_date - timedelta(days=config.LOOKBACK_DAYS)
    start_date = _parse_date(start_param, "start_date") if start_param else default_start
    if start_date > end_date:
        raise ValueError(f"start_date {start_date} must not be later than end_date {end_date}")
    return start_date, end_date


def fetch_prices(tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Download daily unadjusted closes; public dates are inclusive, provider end is exclusive."""
    start = _parse_date(start_date, "start_date")
    end = _parse_date(end_date, "end_date")
    if start > end:
        raise ValueError(f"start_date {start} must not be later than end_date {end}")
    expected_sessions = expected_trading_sessions(start, end + timedelta(days=1))
    if not expected_sessions:
        logger.info(
            "No US equity sessions requested for [%s, %s]; treating empty response as expected",
            start,
            end,
        )
        return pd.DataFrame()

    provider_end = (end + timedelta(days=1)).isoformat()
    last_error: Exception | None = None
    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        try:
            raw_df = yf.download(
                tickers=tickers,
                start=start.isoformat(),
                end=provider_end,
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
                group_by="column",
            )
            if raw_df is not None and not raw_df.empty:
                return raw_df
            last_error = RuntimeError(
                f"Yahoo Finance returned no data for expected trading sessions "
                f"[{start}, {end}] and tickers {tickers}"
            )
        except (TypeError, ValueError):
            raise
        except Exception as exc:  # pragma: no cover - network/library failures
            last_error = exc

        if attempt < MAX_DOWNLOAD_ATTEMPTS:
            delay = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Yahoo Finance download attempt %d/%d failed for %s; retrying in %d seconds: %s",
                attempt,
                MAX_DOWNLOAD_ATTEMPTS,
                tickers,
                delay,
                last_error,
            )
            time.sleep(delay)

    raise RuntimeError(
        f"Yahoo Finance download failed after {MAX_DOWNLOAD_ATTEMPTS} attempts "
        f"for tickers {tickers} in inclusive range [{start}, {end}]: {last_error}"
    ) from last_error


def _extract_close_series(raw_df: pd.DataFrame, ticker: str, requested_tickers: list[str]) -> pd.Series:
    """Return the Close series for one ticker, handling both yfinance MultiIndex orientations."""
    if isinstance(raw_df.columns, pd.MultiIndex):
        if raw_df.columns.nlevels != 2:
            raise ValueError(
                f"Ambiguous response: expected 2-level MultiIndex columns, got {raw_df.columns.nlevels}"
            )
        level_0 = set(raw_df.columns.get_level_values(0))
        level_1 = set(raw_df.columns.get_level_values(1))
        if "Close" not in level_0 and "Close" not in level_1:
            raise ValueError(f"'Close' column missing for ticker '{ticker}'")
        ticker_in_level_0 = ticker in level_0
        ticker_in_level_1 = ticker in level_1
        if ticker_in_level_0 and ticker_in_level_1:
            raise ValueError(f"Ambiguous response: ticker '{ticker}' appears in both column levels")
        if ticker_in_level_1 and "Close" in level_0:
            close_series = raw_df["Close"][ticker]
            if not isinstance(close_series, pd.Series):
                raise ValueError(f"Ambiguous duplicate Close columns for ticker '{ticker}'")
            return close_series
        if ticker_in_level_0 and "Close" in level_1:
            close_series = raw_df[ticker]["Close"]
            if not isinstance(close_series, pd.Series):
                raise ValueError(f"Ambiguous duplicate Close columns for ticker '{ticker}'")
            return close_series
        raise ValueError(f"Ticker '{ticker}' not found in downloaded data")

    # Flat (non-MultiIndex) columns only unambiguously describe a single requested ticker.
    if len(requested_tickers) > 1:
        raise ValueError(
            "Ambiguous response: a flat (non-MultiIndex) column layout cannot be used to "
            f"populate multiple requested tickers {requested_tickers}"
        )
    if "Close" not in raw_df.columns:
        raise ValueError(f"'Close' column missing for ticker '{ticker}'")
    return raw_df["Close"]


def build_price_records(
    raw_df: pd.DataFrame,
    tickers: list[str],
    run_id: str,
    ingestion_timestamp: datetime,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    """Convert downloaded Close values into Bronze rows; NaN closes are skipped, never invented."""
    records: list[dict] = []
    for ticker in tickers:
        close_series = _extract_close_series(raw_df, ticker, tickers)

        if close_series.empty:
            raise ValueError(f"No closing prices available for requested ticker '{ticker}'")

        for price_date, close_value in close_series.items():
            price_date_text = pd.Timestamp(price_date).date().isoformat()
            price_day = date.fromisoformat(price_date_text)
            if start_date is not None and price_day < start_date:
                raise ValueError(f"Returned price date {price_day} precedes requested start date {start_date}")
            if end_date is not None and price_day > end_date:
                raise ValueError(f"Returned price date {price_day} exceeds requested end date {end_date}")
            if pd.isna(close_value):
                continue  # reported by find_missing_price_keys if the session was expected
            try:
                numeric_close = float(close_value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid closing price for '{ticker}' on {price_date_text}: {close_value!r}"
                ) from exc
            if not math.isfinite(numeric_close):
                raise ValueError(
                    f"Invalid closing price for '{ticker}' on {price_date_text}: {close_value!r}"
                )

            records.append(
                {
                    "symbol": ticker,
                    "price_date": price_date_text,
                    "close": str(close_value),
                    "currency": config.CURRENCY,
                    "source_api": config.SOURCE_API,
                    "ingestion_timestamp": ingestion_timestamp,
                    "run_id": run_id,
                }
            )
    return records


def validate_price_records(records: list[dict], tickers: list[str]) -> None:
    """Ensure every requested symbol appears and the source batch has unique keys."""
    represented = {record["symbol"] for record in records}
    missing = sorted(set(tickers) - represented)
    if missing:
        raise ValueError(f"Yahoo Finance response omitted requested ticker data: {missing}")
    keys = [(record["symbol"], record["price_date"]) for record in records]
    if len(keys) != len(set(keys)):
        raise ValueError("Yahoo Finance response contains duplicate (symbol, price_date) keys")


def find_missing_price_keys(
    records: list[dict], tickers: list[str], start: date, end: date, as_of: date | None = None
) -> list[tuple[str, str]]:
    """Return expected (symbol, price_date) pairs absent from the response.

    Weekends, NYSE holidays and future dates are not expected, matching Silver.
    """
    present = {(record["symbol"], record["price_date"]) for record in records}
    return [
        (ticker, session.isoformat())
        for ticker in tickers
        for session in requested_sessions(start, end, as_of)
        if (ticker, session.isoformat()) not in present
    ]


def write_bronze_snapshot(spark, records: list[dict]) -> dict[str, int]:
    """Incrementally merge prices, changing metadata only when price/source data changes.

    Returned counts are computed by a join before the MERGE, not read from Delta operation metrics.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Row
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
    rows = [Row(**record) for record in records]
    dataframe = spark.createDataFrame(rows, schema=schema)
    if not records:
        return {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0}

    target = spark.table(config.TARGET_TABLE)
    target_duplicates = (
        target.groupBy("symbol", "price_date")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if target_duplicates:
        raise ValueError(
            "Bronze market price target already has duplicate symbol/date keys: "
            + "; ".join(str(row.asDict()) for row in target_duplicates)
        )

    source_alias = dataframe.alias("source")
    target_alias = target.select("symbol", "price_date", "close", "currency", "source_api").alias("target")
    joined = source_alias.join(
        target_alias,
        (F.col("source.symbol") == F.col("target.symbol"))
        & (F.col("source.price_date") == F.col("target.price_date")),
        "left",
    )
    exists = F.col("target.symbol").isNotNull()
    unchanged = (
        F.col("source.close").eqNullSafe(F.col("target.close"))
        & F.col("source.currency").eqNullSafe(F.col("target.currency"))
        & F.col("source.source_api").eqNullSafe(F.col("target.source_api"))
    )
    inserted_count = joined.filter(~exists).count()
    updated_count = joined.filter(exists & ~unchanged).count()
    unchanged_count = joined.filter(exists & unchanged).count()

    staging_view = "_trade_pipeline_market_prices_staging"
    dataframe.createOrReplaceTempView(staging_view)
    spark.sql(
        f"""
        MERGE INTO {config.TARGET_TABLE} AS target
        USING {staging_view} AS source
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
            run_id = source.run_id
        WHEN NOT MATCHED THEN INSERT
            (symbol, price_date, close, currency, source_api, ingestion_timestamp, run_id)
        VALUES
            (source.symbol, source.price_date, source.close, source.currency,
             source.source_api, source.ingestion_timestamp, source.run_id)
        """
    )
    result = {
        "inserted_rows": inserted_count,
        "updated_rows": updated_count,
        "unchanged_rows": unchanged_count,
    }
    logger.info("Bronze market-price MERGE counts: %s", result)
    return result


def _get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def run_ingestion(
    tickers: str | Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    spark=None,
    pipeline_run_id: str | None = None,
    task_run_id: str | None = None,
    task_execution_count: str | int | None = None,
) -> dict:
    """Resolve parameters, download requested prices, and incrementally merge them into Bronze."""
    spark_session = spark if spark is not None else _get_spark_session()
    run_id = pipeline_run_id or str(uuid.uuid4())
    attempt_id = (
        f"{task_run_id}-{task_execution_count}"
        if task_run_id is not None and task_execution_count is not None
        else task_run_id or str(uuid.uuid4())
    )

    def ingest() -> dict:
        resolved_tickers = normalize_tickers(tickers)
        resolved_start, resolved_end = resolve_date_range(start_date, end_date)
        logger.info(
            "Starting market-price ingestion run_id=%s task_run_id=%s tickers=%s start_date=%s end_date=%s",
            run_id,
            attempt_id,
            resolved_tickers,
            resolved_start,
            resolved_end,
        )

        ingestion_timestamp = datetime.now(timezone.utc)
        raw_df = fetch_prices(resolved_tickers, resolved_start.isoformat(), resolved_end.isoformat())
        expected_sessions = expected_trading_sessions(resolved_start, resolved_end + timedelta(days=1))
        if raw_df.empty and not expected_sessions:
            logger.info("No trading sessions in inclusive range [%s, %s]; Bronze unchanged", resolved_start, resolved_end)
            records = []
            merge_counts = {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0}
        else:
            records = build_price_records(
                raw_df,
                resolved_tickers,
                run_id,
                ingestion_timestamp,
                resolved_start,
                resolved_end,
            )
            validate_price_records(records, resolved_tickers)
            missing = find_missing_price_keys(records, resolved_tickers, resolved_start, resolved_end)
            if missing:
                shown = "; ".join(f"{symbol} {day}" for symbol, day in missing[:20])
                raise ValueError(
                    f"Incomplete Yahoo Finance response: {len(missing)} expected "
                    f"(symbol, price_date) prices missing; Bronze not updated: {shown}"
                )
            merge_counts = write_bronze_snapshot(spark_session, records)
        logger.info(
            "Completed market-price ingestion run_id=%s task_run_id=%s downloaded_rows=%d requested_tickers=%d inserted=%d updated=%d unchanged=%d",
            run_id,
            attempt_id,
            len(records),
            len(resolved_tickers),
            merge_counts["inserted_rows"],
            merge_counts["updated_rows"],
            merge_counts["unchanged_rows"],
        )
        rows_written = merge_counts["inserted_rows"] + merge_counts["updated_rows"]
        return {
            "run_id": run_id,
            "task_run_id": attempt_id,
            "tickers": resolved_tickers,
            "start_date": resolved_start.isoformat(),
            "end_date": resolved_end.isoformat(),
            "requested_tickers": len(resolved_tickers),
            "input_rows": len(records),
            # output_rows: valid price rows produced; rows_written: only those inserted or updated.
            "output_rows": len(records),
            "rows_written": rows_written,
            **merge_counts,
            "rejected_rows": 0,
            "duplicate_rows": 0,
            "issue_counts": {},
        }

    return audit.run_audited_task(
        spark_session,
        run_id,
        attempt_id,
        "ingest_prices",
        config.SOURCE_API,
        config.TARGET_TABLE,
        ingest,
    )


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Ingest Yahoo Finance daily closing prices into cibc.bronze.market_prices"
    )
    parser.add_argument("--tickers", default=None)
    parser.add_argument("--start_date", default=None)
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--pipeline_run_id", default=None)
    parser.add_argument("--task_run_id", default=None)
    parser.add_argument("--task_execution_count", default=None)
    args = parser.parse_args(argv)

    try:
        # Databricks job parameters cannot be truly omitted; an empty string
        # means "use the config default", same as a CLI arg left unset.
        run_ingestion(
            tickers=args.tickers or None,
            start_date=args.start_date or None,
            end_date=args.end_date or None,
            pipeline_run_id=args.pipeline_run_id or None,
            task_run_id=args.task_run_id or None,
            task_execution_count=args.task_execution_count or None,
        )
    except Exception:
        logger.exception("Market-price ingestion failed")
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
