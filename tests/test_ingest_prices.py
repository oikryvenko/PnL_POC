"""Unit tests for trade_pipeline.bronze.ingest_prices.

No live Databricks connection, Yahoo Finance connection, or running Spark
cluster is required: yfinance is mocked and write_bronze_snapshot is replaced
with a recording stub (or a MagicMock spark session) so the Spark write path
is never exercised against a real cluster.
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trade_pipeline import config
from trade_pipeline.bronze import ingest_prices

def _price_first_frame() -> pd.DataFrame:
    """yf.download(group_by='column') layout: level 0 = price field, level 1 = ticker."""
    index = pd.to_datetime(["2026-09-01", "2026-09-02"])
    columns = pd.MultiIndex.from_tuples(
        [("Close", "AAPL"), ("Close", "MSFT")], names=["Price", "Ticker"]
    )
    return pd.DataFrame([[150.0, 300.0], [151.5, 301.5]], index=index, columns=columns)


def _ticker_first_frame() -> pd.DataFrame:
    """yf.download(group_by='ticker') layout: level 0 = ticker, level 1 = price field."""
    index = pd.to_datetime(["2026-09-01", "2026-09-02"])
    columns = pd.MultiIndex.from_tuples(
        [("AAPL", "Close"), ("MSFT", "Close")], names=["Ticker", "Price"]
    )
    return pd.DataFrame([[150.0, 300.0], [151.5, 301.5]], index=index, columns=columns)


def _single_ticker_frame() -> pd.DataFrame:
    index = pd.to_datetime(["2026-09-01", "2026-09-02"])
    return pd.DataFrame({"Close": [150.0, 151.5]}, index=index)


def _run_without_audit(*args):
    return args[-1]()


def _run_without_audit(*args):
    return args[-1]()


# 1. Default ticker configuration
def test_default_ticker_configuration() -> None:
    assert ingest_prices.normalize_tickers(None) == config.DEFAULT_TICKERS


# 2. Runtime ticker override
def test_runtime_ticker_override_parses_and_dedupes() -> None:
    assert ingest_prices.normalize_tickers("tsla, aapl, TSLA") == ["TSLA", "AAPL"]


# 3. Runtime date override
def test_runtime_date_override() -> None:
    start, end = ingest_prices.resolve_date_range("2026-09-05", "2026-09-10")
    assert (start, end) == (date(2026, 9, 5), date(2026, 9, 10))


# 4. Invalid date ranges
def test_missing_dates_default_to_rolling_window_for_scheduled_runs() -> None:
    start, end = ingest_prices.resolve_date_range(None, None)
    assert end == date.today()
    assert (end - start).days == config.LOOKBACK_DAYS


def test_start_defaults_relative_to_an_explicit_end_for_backfills() -> None:
    assert ingest_prices.resolve_date_range("", "2026-09-30") == (date(2026, 9, 23), date(2026, 9, 30))


def test_inclusive_date_range_allows_same_start_and_end_date() -> None:
    assert ingest_prices.resolve_date_range("2026-09-10", "2026-09-10") == (
        date(2026, 9, 10),
        date(2026, 9, 10),
    )


def test_invalid_date_range_start_after_end() -> None:
    with pytest.raises(ValueError, match="must not be later than"):
        ingest_prices.resolve_date_range("2026-09-11", "2026-09-10")


def test_invalid_date_range_malformed_date() -> None:
    with pytest.raises(ValueError, match="Invalid start_date"):
        ingest_prices.resolve_date_range("09/05/2026", "2026-09-10")


# 5. Empty ticker list
def test_empty_ticker_list_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        ingest_prices.normalize_tickers("")

    with pytest.raises(ValueError, match="must not be empty"):
        ingest_prices.normalize_tickers([])


# 1. Single-ticker response (flat columns)
def test_extract_close_series_single_ticker_flat_columns() -> None:
    series = ingest_prices._extract_close_series(_single_ticker_frame(), "AAPL", ["AAPL"])
    assert list(series) == [150.0, 151.5]


# 2. Multi-ticker response, both MultiIndex orientations
def test_extract_close_series_price_first_multiindex() -> None:
    series = ingest_prices._extract_close_series(_price_first_frame(), "MSFT", ["AAPL", "MSFT"])
    assert list(series) == [300.0, 301.5]


def test_extract_close_series_ticker_first_multiindex() -> None:
    series = ingest_prices._extract_close_series(_ticker_first_frame(), "MSFT", ["AAPL", "MSFT"])
    assert list(series) == [300.0, 301.5]


# 3. Rejection of ambiguous flat-column multi-ticker data
def test_extract_close_series_rejects_flat_columns_for_multiple_tickers() -> None:
    with pytest.raises(ValueError, match="Ambiguous response"):
        ingest_prices._extract_close_series(_single_ticker_frame(), "AAPL", ["AAPL", "MSFT"])


# 4. Missing ticker
def test_extract_close_series_missing_ticker_raises() -> None:
    with pytest.raises(ValueError, match="Ticker 'TSLA' not found"):
        ingest_prices._extract_close_series(_price_first_frame(), "TSLA", ["AAPL", "TSLA"])


# 5. Missing Close column
def test_extract_close_series_missing_close_column_raises() -> None:
    index = pd.to_datetime(["2026-09-01", "2026-09-02"])
    raw_df = pd.DataFrame({"Open": [150.0, 151.5]}, index=index)
    with pytest.raises(ValueError, match="'Close' column missing"):
        ingest_prices._extract_close_series(raw_df, "AAPL", ["AAPL"])


def test_extract_close_series_missing_multiindex_close_raises() -> None:
    index = pd.to_datetime(["2026-09-01"])
    columns = pd.MultiIndex.from_tuples([("Open", "AAPL")])
    raw_df = pd.DataFrame([[150.0]], index=index, columns=columns)
    with pytest.raises(ValueError, match="'Close' column missing"):
        ingest_prices._extract_close_series(raw_df, "AAPL", ["AAPL"])


# 6. Empty price series
def test_build_price_records_empty_series_raises() -> None:
    raw_df = pd.DataFrame({"Close": []})
    timestamp = datetime(2026, 9, 3, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="No closing prices available"):
        ingest_prices.build_price_records(raw_df, ["AAPL"], "run-1", timestamp)


@pytest.mark.parametrize("close", [float("inf"), float("-inf")])
def test_build_price_records_rejects_invalid_close_values(close: float) -> None:
    raw_df = pd.DataFrame({"Close": [close]}, index=pd.to_datetime(["2026-10-02"]))
    timestamp = datetime(2026, 10, 3, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="closing price"):
        ingest_prices.build_price_records(raw_df, ["AAPL"], "run-1", timestamp)


def test_nan_close_is_skipped_and_nonpositive_close_is_left_to_silver() -> None:
    raw_df = pd.DataFrame(
        {"Close": [float("nan"), 0.0]}, index=pd.to_datetime(["2026-09-01", "2026-09-02"])
    )
    timestamp = datetime(2026, 9, 3, tzinfo=timezone.utc)
    records = ingest_prices.build_price_records(raw_df, ["AAPL"], "run-1", timestamp)
    assert [(r["price_date"], r["close"]) for r in records] == [("2026-09-02", "0.0")]


def test_duplicate_source_keys_are_rejected_before_write() -> None:
    duplicate_date = pd.Timestamp("2026-09-01")
    raw_df = pd.DataFrame(
        {"Close": [150.0, 150.0]}, index=pd.DatetimeIndex([duplicate_date, duplicate_date])
    )
    timestamp = datetime(2026, 9, 3, tzinfo=timezone.utc)
    records = ingest_prices.build_price_records(raw_df, ["AAPL"], "run-1", timestamp)

    with pytest.raises(ValueError, match="duplicate"):
        ingest_prices.validate_price_records(records, ["AAPL"])


# 10. Price precision is not artificially rounded
def test_build_price_records_preserves_precision() -> None:
    index = pd.to_datetime(["2026-09-01"])
    raw_df = pd.DataFrame({"Close": [150.123456]}, index=index)
    timestamp = datetime(2026, 9, 3, tzinfo=timezone.utc)
    records = ingest_prices.build_price_records(raw_df, ["AAPL"], "run-1", timestamp)
    assert records[0]["close"] == "150.123456"


# Successful transformation / output fields (retained from the original suite)
def test_successful_api_response_transformation() -> None:
    raw_df = _price_first_frame()
    timestamp = datetime(2026, 9, 3, tzinfo=timezone.utc)
    records = ingest_prices.build_price_records(raw_df, ["AAPL", "MSFT"], "run-1", timestamp)

    assert len(records) == 4
    aapl_rows = [r for r in records if r["symbol"] == "AAPL"]
    assert {r["price_date"] for r in aapl_rows} == {"2026-09-01", "2026-09-02"}
    assert aapl_rows[0]["close"] == "150.0"


def test_output_fields_and_values() -> None:
    raw_df = _single_ticker_frame()
    timestamp = datetime(2026, 9, 3, tzinfo=timezone.utc)
    records = ingest_prices.build_price_records(raw_df, ["AAPL"], "run-xyz", timestamp)

    assert set(records[0]) == {
        "symbol",
        "price_date",
        "close",
        "currency",
        "source_api",
        "ingestion_timestamp",
        "run_id",
    }
    assert records[0]["currency"] == "USD"
    assert records[0]["source_api"] == "yfinance.download"
    assert records[0]["run_id"] == "run-xyz"
    assert records[0]["ingestion_timestamp"] == timestamp


# Empty API response
def test_empty_api_response_raises_after_bounded_retries() -> None:
    with patch.object(ingest_prices.yf, "download", return_value=pd.DataFrame()) as download, patch.object(
        ingest_prices.time, "sleep"
    ) as sleep:
        with pytest.raises(RuntimeError, match="no data"):
            ingest_prices.fetch_prices(["AAPL"], "2026-09-01", "2026-09-30")

    assert download.call_count == ingest_prices.MAX_DOWNLOAD_ATTEMPTS
    assert sleep.call_count == ingest_prices.MAX_DOWNLOAD_ATTEMPTS - 1


def test_fetch_treats_weekend_only_range_as_empty_without_download() -> None:
    with patch.object(ingest_prices.yf, "download") as download:
        result = ingest_prices.fetch_prices(["AAPL"], "2026-09-05", "2026-09-06")

    assert result.empty
    download.assert_not_called()


def test_fetch_requests_only_configured_tickers_and_unadjusted_daily_close() -> None:
    with patch.object(ingest_prices.yf, "download", return_value=_single_ticker_frame()) as download:
        ingest_prices.fetch_prices(["MSFT"], "2026-09-01", "2026-09-30")

    assert download.call_args.kwargs["tickers"] == ["MSFT"]
    assert download.call_args.kwargs["interval"] == "1d"
    assert download.call_args.kwargs["auto_adjust"] is False
    assert download.call_args.kwargs["start"] == "2026-09-01"
    assert download.call_args.kwargs["end"] == "2026-10-01"


# API exception handling
def test_api_exception_is_wrapped_after_bounded_retries() -> None:
    with patch.object(ingest_prices.yf, "download", side_effect=RuntimeError("boom")) as download, patch.object(
        ingest_prices.time, "sleep"
    ):
        with pytest.raises(RuntimeError, match="Yahoo Finance download failed"):
            ingest_prices.fetch_prices(["AAPL"], "2026-09-01", "2026-09-30")

    assert download.call_count == ingest_prices.MAX_DOWNLOAD_ATTEMPTS


# Unusable response structures fail before a Bronze write.
def test_no_write_when_validation_fails() -> None:
    with patch.object(
        ingest_prices.audit, "run_audited_task", side_effect=_run_without_audit
    ), patch.object(ingest_prices, "fetch_prices", return_value=_price_first_frame()), patch.object(
        ingest_prices, "write_bronze_snapshot"
    ) as mock_write:
        with pytest.raises(ValueError, match="Ticker 'TSLA' not found"):
            ingest_prices.run_ingestion(
                tickers="AAPL,TSLA", start_date="2026-09-01", end_date="2026-09-02", spark=object()
            )
        mock_write.assert_not_called()


def test_missing_trading_session_fails_before_any_bronze_write() -> None:
    index = pd.to_datetime(["2026-09-01"])
    raw_df = pd.DataFrame({"Close": [150.0]}, index=index)
    with patch.object(
        ingest_prices.audit, "run_audited_task", side_effect=_run_without_audit
    ), patch.object(ingest_prices, "fetch_prices", return_value=raw_df), patch.object(
        ingest_prices, "write_bronze_snapshot"
    ) as mock_write:
        with pytest.raises(ValueError, match=r"Bronze not updated: AAPL 2026-09-02"):
            ingest_prices.run_ingestion(
                tickers="AAPL", start_date="2026-09-01", end_date="2026-09-02", spark=object()
            )
        mock_write.assert_not_called()


def test_find_missing_price_keys_ignores_weekends_holidays_and_future_dates() -> None:
    def rec(symbol, day):
        return {"symbol": symbol, "price_date": day}

    # 2026-09-05/06 weekend, 09-07 Labor Day; 2026-09-09 is after as_of.
    records = [rec("AAPL", "2026-09-04"), rec("AAPL", "2026-09-08"), rec("MSFT", "2026-09-04")]
    missing = ingest_prices.find_missing_price_keys(
        records, ["AAPL", "MSFT"], date(2026, 9, 4), date(2026, 9, 9), as_of=date(2026, 9, 8)
    )
    assert missing == [("MSFT", "2026-09-08")]
    assert ingest_prices.find_missing_price_keys(
        [], ["AAPL"], date(2026, 9, 5), date(2026, 9, 7), as_of=date(2026, 10, 1)
    ) == []


def test_single_day_ingestion_includes_end_date() -> None:
    raw_df = pd.DataFrame({"Close": [150.0]}, index=pd.to_datetime(["2026-09-02"]))
    counts = {"inserted_rows": 1, "updated_rows": 0, "unchanged_rows": 0}
    with patch.object(
        ingest_prices.audit, "run_audited_task", side_effect=_run_without_audit
    ), patch.object(ingest_prices, "fetch_prices", return_value=raw_df), patch.object(
        ingest_prices, "write_bronze_snapshot", return_value=counts
    ) as mock_write:
        summary = ingest_prices.run_ingestion(
            tickers="AAPL", start_date="2026-09-02", end_date="2026-09-02", spark=object()
        )
    assert mock_write.call_args[0][1][0]["price_date"] == "2026-09-02"
    assert summary["end_date"] == "2026-09-02"


def test_rows_written_excludes_unchanged_rows() -> None:
    raw_df = _price_first_frame()
    counts = {"inserted_rows": 1, "updated_rows": 1, "unchanged_rows": 2}
    with patch.object(
        ingest_prices.audit, "run_audited_task", side_effect=_run_without_audit
    ), patch.object(ingest_prices, "fetch_prices", return_value=raw_df), patch.object(
        ingest_prices, "write_bronze_snapshot", return_value=counts
    ):
        summary = ingest_prices.run_ingestion(
            tickers="AAPL,MSFT", start_date="2026-09-01", end_date="2026-09-02", spark=object()
        )
    assert summary["input_rows"] == 4
    assert summary["output_rows"] == 4 and summary["rows_written"] == 2
    assert summary["unchanged_rows"] == 2


def test_run_ingestion_writes_once_on_success() -> None:
    raw_df = _price_first_frame()
    fake_spark = object()
    with patch.object(
        ingest_prices.audit, "run_audited_task", side_effect=_run_without_audit
    ) as mock_audit, patch.object(ingest_prices, "fetch_prices", return_value=raw_df), patch.object(
        ingest_prices, "write_bronze_snapshot"
    ) as mock_write:
        mock_write.return_value = {"inserted_rows": 4, "updated_rows": 0, "unchanged_rows": 0}
        summary = ingest_prices.run_ingestion(
            tickers="AAPL,MSFT",
            start_date="2026-09-01",
            end_date="2026-09-02",
            spark=fake_spark,
            pipeline_run_id="shared-job-run",
            task_run_id="ingest-attempt-1",
        )

    mock_write.assert_called_once()
    written_spark, written_records = mock_write.call_args[0]
    assert written_spark is fake_spark
    assert len(written_records) == 4
    assert summary["rows_written"] == 4
    assert summary["tickers"] == ["AAPL", "MSFT"]
    assert summary["input_rows"] == 4
    assert summary["requested_tickers"] == 2
    assert all(row["run_id"] == "shared-job-run" for row in written_records)
    assert mock_audit.call_args.args[1:3] == ("shared-job-run", "ingest-attempt-1")


# The MERGE preserves existing rows and never recreates or truncates the table.
def test_write_bronze_snapshot_merges_incrementally_on_existing_table() -> None:
    # pyspark is not installed locally; fake just enough of its API surface so
    # write_bronze_snapshot's lazy imports succeed without a real Spark cluster.
    import sys
    import types

    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.Row = lambda **kwargs: kwargs
    fake_functions_module = types.ModuleType("pyspark.sql.functions")
    fake_functions_module.col = MagicMock()
    fake_functions_module.col.return_value.__gt__.return_value = MagicMock()
    fake_sql_module.functions = fake_functions_module
    fake_types_module = types.ModuleType("pyspark.sql.types")
    for type_name in ("StringType", "StructField", "StructType", "TimestampType"):
        setattr(fake_types_module, type_name, MagicMock(name=type_name))

    mock_spark = MagicMock()
    mock_spark.table.return_value.groupBy.return_value.count.return_value.filter.return_value.limit.return_value.collect.return_value = []
    mock_spark.createDataFrame.return_value.alias.return_value.join.return_value.filter.return_value.count.side_effect = [1, 0, 0]
    records = [
        {
            "symbol": "AAPL",
            "price_date": "2026-09-01",
            "close": "150.0",
            "currency": "USD",
            "source_api": "yfinance.download",
            "ingestion_timestamp": datetime(2026, 9, 1, tzinfo=timezone.utc),
            "run_id": "run-1",
        }
    ]

    with patch.dict(
        sys.modules,
        {
            "pyspark.sql": fake_sql_module,
            "pyspark.sql.functions": fake_functions_module,
            "pyspark.sql.types": fake_types_module,
        },
    ):
        result = ingest_prices.write_bronze_snapshot(mock_spark, records)

    mock_spark.createDataFrame.assert_called_once()
    dataframe_mock = mock_spark.createDataFrame.return_value
    dataframe_mock.createOrReplaceTempView.assert_called_once()
    mock_spark.sql.assert_called_once()
    executed_sql = mock_spark.sql.call_args[0][0]
    assert "MERGE INTO cibc.bronze.market_prices" in executed_sql
    assert "WHEN MATCHED AND NOT" in executed_sql
    assert "INSERT OVERWRITE" not in executed_sql
    assert "NOT MATCHED BY SOURCE" not in executed_sql
    assert "CREATE TABLE" not in executed_sql
    assert "DROP TABLE" not in executed_sql
    assert "DELETE" not in executed_sql
    # Counts come from the pre-MERGE join (inserted, updated, unchanged).
    assert result == {"inserted_rows": 1, "updated_rows": 0, "unchanged_rows": 0}


# Databricks job parameters are always passed as strings and cannot be omitted;
# an empty string must fall back to config defaults, same as a true CLI omission.
def test_main_treats_empty_string_job_parameters_as_defaults() -> None:
    with patch.object(ingest_prices, "run_ingestion", return_value={}) as mock_run:
        exit_code = ingest_prices.main(["--tickers", "", "--start_date", "", "--end_date", ""])

    assert exit_code == 0
    mock_run.assert_called_once_with(
        tickers=None,
        start_date=None,
        end_date=None,
        pipeline_run_id=None,
        task_run_id=None,
        task_execution_count=None,
    )


def test_main_reraises_ingestion_failure_for_job_failure_status() -> None:
    with patch.object(ingest_prices, "run_ingestion", side_effect=RuntimeError("failed")):
        with pytest.raises(RuntimeError, match="failed"):
            ingest_prices.main([])
