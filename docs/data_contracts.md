# Data Contract

Summary of the current table behaviour. Source of truth: `database/tables/` (DDL) and `src/trade_pipeline/`.

## Dates

`start_date` and `end_date` are inclusive (`YYYY-MM-DD`); a single day is allowed. When omitted, both price tasks use the last 7 days up to today (UTC). Trade processing does not filter by these dates.

## Bronze

- `cibc.bronze.market_prices`: raw string values plus `source_api`, `ingestion_timestamp`, `run_id`. Written with a Delta MERGE on (`symbol`, `price_date`); a row is updated only when `close`, `currency` or `source_api` changed. History is kept. The task fails before writing if an expected (symbol, trading day) price is missing.
- `cibc.bronze.trades`: **full snapshot of the latest trade CSV.** The 14 CSV columns as strings plus `source_file`, `ingestion_timestamp`, `run_id`. Each run removes identical rows within the CSV (different versions of a `trade_id` are different rows and are kept) and replaces the table with `INSERT OVERWRITE`, so rerunning the same CSV leaves the same rows. The table, its schema and grants are kept. Invalid business values are kept for Silver. The CSV header must match the 14 columns, and an empty or structurally corrupt file fails the task before anything is written.

## Silver market prices

`cibc.silver.market_prices` holds validated, typed prices: `symbol`, `price_date` DATE, `close` DECIMAL(18,6), `currency`, `source_api`, `ingestion_timestamp`, `run_id`, plus lineage `bronze_run_id` and `bronze_ingestion_timestamp` (nullable). CHECK constraint: `close > 0`. Business key: (`symbol`, `price_date`).

- **Scope:** only the requested tickers and dates are processed. Valid rows outside the request are ignored, not rejected.
- **Rejected** (to `cibc.silver.market_prices_rejects`, with pipe-separated `rejection_reason`): null or empty symbol, unparseable date, future date, non-trading day, unparseable or non-positive close, currency other than USD.
- **Deduplication:** for one key the latest `ingestion_timestamp` wins, then the greatest `run_id`. Other rows are `DUPLICATE_BUSINESS_KEY`; tied rows with different payloads are all `AMBIGUOUS_DUPLICATE_KEY`.
- **Completeness:** every expected NYSE session (not in the future) must have an accepted price for each ticker. If not, the task fails before Silver is written.
- **Write:** conditional MERGE on the business key; unchanged rows are not rewritten and nothing is deleted. Rejects are merged as history, so reruns add no duplicates.

## Silver trades

`cibc.silver.trades` holds one validated row per `trade_id`: the highest `version` wins, then the latest `modified_timestamp`. Quantity is signed (BUY positive, SELL negative). Lineage columns: `bronze_run_id`, `bronze_ingestion_timestamp`, `source_file` (nullable). Invalid, ambiguous and superseded rows go to `cibc.silver.trades_rejects`. An invalid or ambiguous latest version is not replaced by an older one, and that trade is removed from Silver (only trade_ids still present in the Bronze snapshot are removed). Trades missing from a later snapshot are not deleted from Silver. Write: conditional MERGE on `trade_id`.

## Gold

- `cibc.gold.positions`: key (`valuation_date`, `book_id`, `counterparty_id`, `instrument_id`, `currency`). Cumulative signed quantities over NYSE sessions and trade dates; valued at the exact-date close.
- `cibc.gold.daily_pnl`: key (`valuation_date`, `book_id`, `symbol`, `currency`). Also gives positions by book and symbol (`closing_quantity`, `market_value`).
- Both are written by MERGE on their key, updating a row only when a financial value changed. Rows are deleted only for the instruments or symbols present in the source and dates on or after the source's first date, so a partial input cannot erase other history.
- A missing current-day close gives NULL values and a `MISSING_MARKET_PRICE` break; an older price is never substituted. `cibc.gold.valuation_breaks` is run-level history.
- Gold is recomputed from the full Silver history each run, so a back-dated trade correction updates every affected valuation date from the trade date onwards. Gap: an instrument or symbol that disappears from Silver entirely is outside the delete scope and its old Gold rows remain.

## Audit metrics for `ingest_trades`

`input_rows` = CSV rows read; `output_rows` = distinct rows written to the Bronze snapshot; `duplicate_rows` = identical rows found within the CSV (`input_rows − output_rows`, also in `issue_counts` as `DUPLICATE_IN_BATCH`); `rejected_rows` = 0. `rows_written` (logged, not stored) equals `output_rows`; the returned `inserted_rows` is kept for compatibility and means rows written into the replacement snapshot.

## Verification status

Local tests cover parameters, calendar logic, DDL, SQL shape and packaging. Spark-based tests need PySpark and Java and are skipped without them. Table constraints, MERGE result metrics and actual results need a Databricks run.
