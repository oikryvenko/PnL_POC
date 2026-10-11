# Data Contract

Summary of the current table behaviour. Source of truth: `database/tables/` (DDL) and `src/trade_pipeline/`.

## Dates

`start_date` and `end_date` are inclusive (`YYYY-MM-DD`); a single day is allowed. When omitted, both price tasks use the last 7 days up to today (UTC). Trade processing does not filter by these dates.

## Bronze

- `cibc.bronze.market_prices`: raw string values plus `source_api`, `ingestion_timestamp`, `run_id`. Written with a Delta MERGE on (`symbol`, `price_date`); a row is updated only when `close`, `currency` or `source_api` changed. History is kept. The task fails before writing if an expected (symbol, trading day) price is missing.
- `cibc.bronze.trades`: **historical raw-record store, not a snapshot.** The 14 CSV columns as strings plus `record_hash`, `source_file`, `ingestion_timestamp`, `run_id`. `record_hash = sha2(to_json(array(<14 columns in CSV order>)), 256)` (NULL-safe). Each run deduplicates the CSV on `record_hash` and runs an insert-only Delta MERGE on `record_hash` (`WHEN NOT MATCHED THEN INSERT` only): rows are never updated or deleted, metadata keeps the run that first ingested the record, rerunning the same CSV inserts 0 rows, and new trades, versions or changed attributes are inserted as new rows. Invalid business values are kept. The CSV header must match the 14 columns, and an empty or corrupt file fails the task. `record_hash` is unique for rows written by the MERGE; rows from the old `INSERT OVERWRITE` snapshot keep their exact duplicates after the backfill.

## Silver market prices

`cibc.silver.market_prices` holds validated, typed prices: `symbol`, `price_date` DATE, `close` DECIMAL(18,6), `currency`, `source_api`, `ingestion_timestamp`, `run_id`, plus lineage `bronze_run_id` and `bronze_ingestion_timestamp` (nullable). CHECK constraints: `close > 0`, `currency = 'USD'`. Business key: (`symbol`, `price_date`).

- **Scope:** only the requested tickers and dates are processed. Valid rows outside the request are ignored, not rejected.
- **Rejected** (to `cibc.silver.market_prices_rejects`, with pipe-separated `rejection_reason`): null or empty symbol, unparseable date, future date, non-trading day, unparseable or non-positive close, currency other than USD.
- **Deduplication:** for one key the latest `ingestion_timestamp` wins, then the greatest `run_id`. Other rows are `DUPLICATE_BUSINESS_KEY`; tied rows with different payloads are all `AMBIGUOUS_DUPLICATE_KEY`.
- **Completeness:** every expected NYSE session (not in the future) must have an accepted price for each ticker. If not, the task fails before Silver is written.
- **Write:** conditional MERGE on the business key; unchanged rows are not rewritten and nothing is deleted. Rejects are merged as history, so reruns add no duplicates.

## Silver trades

`cibc.silver.trades` holds one validated row per `trade_id`: the highest `version` wins, then the latest `modified_timestamp`. Quantity is signed (BUY positive, SELL negative). Lineage columns: `bronze_run_id`, `bronze_ingestion_timestamp`, `source_file` (nullable). Invalid, ambiguous and superseded rows go to `cibc.silver.trades_rejects`. An invalid or ambiguous latest version is not replaced by an older one, and that trade is removed from Silver. Silver reads the whole Bronze history every run, so a record re-sent with changed attributes but the same `version` and `modified_timestamp` makes the trade ambiguous (quarantined). Accepted trades are never deleted because they disappear from a later file. Write: conditional MERGE on `trade_id`.

## Gold

- `cibc.gold.positions`: key (`valuation_date`, `book_id`, `counterparty_id`, `instrument_id`, `currency`). Cumulative signed quantities over NYSE sessions and trade dates; valued at the exact-date close.
- `cibc.gold.daily_pnl`: key (`valuation_date`, `book_id`, `symbol`, `currency`). Also gives positions by book and symbol (`closing_quantity`, `market_value`).
- Both are written by MERGE on their key, updating a row only when a financial value changed. Rows are deleted only for the instruments or symbols present in the source and dates on or after the source's first date, so a partial input cannot erase other history.
- A missing current-day close gives NULL values and a `MISSING_MARKET_PRICE` break; an older price is never substituted. `cibc.gold.valuation_breaks` is run-level history.
- Gold is recomputed from the full Silver history each run, so a back-dated trade correction updates every affected valuation date from the trade date onwards. Gap: an instrument or symbol that disappears from Silver entirely is outside the delete scope and its old Gold rows remain.

## Audit metrics for `ingest_trades`

`input_rows` = CSV rows read; `output_rows` = distinct raw records in the CSV (all present in Bronze after the run); `duplicate_rows` = `input_rows` − rows inserted; `issue_counts` splits duplicates into `DUPLICATE_IN_BATCH` and `ALREADY_INGESTED`. Rows inserted (`rows_written`, from the MERGE `num_inserted_rows`) is logged, not stored, and equals `input_rows − duplicate_rows`.

## Verification status

Local tests cover parameters, calendar logic, DDL, SQL shape and packaging. Spark-based tests need PySpark and Java and are skipped without them. Table constraints, MERGE result metrics and actual results need a Databricks run.
