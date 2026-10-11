-- Demo queries for the trading ETL pipeline (Databricks SQL). Read-only: SELECT / DESCRIBE only.
-- Run after the six-task job has completed.

-- 1. Bronze market prices: raw string values with source and ingestion metadata.
SELECT symbol, price_date, close, currency, source_api, ingestion_timestamp, run_id
FROM cibc.bronze.market_prices
ORDER BY price_date DESC, symbol


-- 2. Bronze raw trades: insert-only history of distinct raw records (strings, including dirty values).
SELECT trade_id, instrument_id, symbol, side, quantity, price, trade_date, book_id, version,
       record_hash, source_file, ingestion_timestamp, run_id
FROM cibc.bronze.trades
ORDER BY trade_id, version, ingestion_timestamp;

-- 2b. Bronze trade history: rows first ingested by each run (a rerun of the same CSV adds none).
SELECT run_id, source_file, MIN(ingestion_timestamp) AS ingested_at, COUNT(*) AS new_records
FROM cibc.bronze.trades
GROUP BY run_id, source_file
ORDER BY ingested_at DESC;

-- 2c. Trades with more than one raw record (new versions or changed attributes kept side by side).
SELECT trade_id, COUNT(*) AS raw_records, COLLECT_SET(version) AS versions
FROM cibc.bronze.trades
WHERE trade_id IS NOT NULL
GROUP BY trade_id
HAVING COUNT(*) > 1
ORDER BY raw_records DESC, trade_id
LIMIT 20


-- 3. Silver validated prices: typed values with Bronze lineage.
SELECT symbol, price_date, close, currency, bronze_run_id, bronze_ingestion_timestamp
FROM cibc.silver.market_prices
ORDER BY price_date DESC, symbol


-- 4. Silver validated trades: one row per trade_id, signed quantity, Bronze lineage.
SELECT trade_id, book_id, symbol, quantity, price, trade_date, settlement_date, version,
       bronze_run_id, source_file
FROM cibc.silver.trades
ORDER BY trade_date, trade_id


-- 5. Rejected market prices: how often each reason occurred, then a few examples.
SELECT reason, COUNT(*) AS rejected_records
FROM cibc.silver.market_prices_rejects
LATERAL VIEW explode(split(rejection_reason, '[|]')) r AS reason
GROUP BY reason
ORDER BY rejected_records DESC;

SELECT symbol, price_date, close, currency, rejection_reason, rejected_at
FROM cibc.silver.market_prices_rejects
ORDER BY rejected_at DESC


-- 6. Rejected trades: reason counts, then raw rejected rows (values are the original strings).
SELECT reason, COUNT(*) AS rejected_records
FROM cibc.silver.trades_rejects
LATERAL VIEW explode(split(rejection_reason, '[|]')) r AS reason
GROUP BY reason
ORDER BY rejected_records DESC;

SELECT trade_id, side, quantity, price, trade_date, settlement_date, version, rejection_reason
FROM cibc.silver.trades_rejects
ORDER BY rejected_at DESC
LIMIT 10;

-- 7. Positions by book and symbol on the latest valuation date.
--    daily_pnl already has one row per date, book and symbol, so nothing is summed twice.
SELECT valuation_date, book_id, symbol, closing_quantity, current_close, market_value
FROM cibc.gold.daily_pnl
WHERE valuation_date = (SELECT MAX(valuation_date) FROM cibc.gold.daily_pnl)
ORDER BY book_id, symbol;

-- 7b. The same from the finer-grained positions table: sum the counterparty rows per book and instrument.
SELECT valuation_date, book_id, instrument_id, SUM(closing_position_quantity) AS closing_quantity
FROM cibc.gold.positions
WHERE valuation_date = (SELECT MAX(valuation_date) FROM cibc.gold.positions)
GROUP BY valuation_date, book_id, instrument_id
ORDER BY book_id, instrument_id;

-- 8. Daily P&L by date and book (rows with a missing price have NULL daily_pnl and are not summed).
SELECT valuation_date, book_id,
       SUM(daily_pnl) AS daily_pnl,
       SUM(CASE WHEN calculation_status <> 'COMPLETE' THEN 1 ELSE 0 END) AS incomplete_rows
FROM cibc.gold.daily_pnl
GROUP BY valuation_date, book_id
ORDER BY valuation_date DESC, book_id
LIMIT 20;

-- 8b. P&L detail for one symbol: opening position, previous and current close, result and status.
SELECT valuation_date, book_id, symbol, opening_quantity, previous_close, current_close,
       daily_pnl, unrealized_pnl_proxy, calculation_status
FROM cibc.gold.daily_pnl
WHERE symbol = 'AAPL'
ORDER BY valuation_date DESC, book_id
LIMIT 20;

-- 9. Valuation breaks of the most recent run (missing prices or baselines). Breaks are kept per run_id.
SELECT break_type, instrument_id, valuation_date, trade_id, break_description, status
FROM cibc.gold.valuation_breaks
WHERE run_id = (SELECT run_id FROM cibc.gold.valuation_breaks ORDER BY detected_at DESC LIMIT 1)
ORDER BY valuation_date, instrument_id
LIMIT 50;

-- 10. Pipeline audit: one row per task attempt with row counts and status.
SELECT task_name, status, started_at, finished_at, input_rows, output_rows, rejected_rows,
       duplicate_rows, issue_counts, error_message
FROM cibc.gold.pipeline_run_log
ORDER BY started_at DESC
LIMIT 12;

-- 11. Row counts across Bronze, Silver and Gold.
SELECT 'bronze.market_prices' AS table_name, COUNT(*) AS row_count FROM cibc.bronze.market_prices
UNION ALL SELECT 'bronze.trades', COUNT(*) FROM cibc.bronze.trades
UNION ALL SELECT 'silver.market_prices', COUNT(*) FROM cibc.silver.market_prices
UNION ALL SELECT 'silver.market_prices_rejects', COUNT(*) FROM cibc.silver.market_prices_rejects
UNION ALL SELECT 'silver.trades', COUNT(*) FROM cibc.silver.trades
UNION ALL SELECT 'silver.trades_rejects', COUNT(*) FROM cibc.silver.trades_rejects
UNION ALL SELECT 'gold.positions', COUNT(*) FROM cibc.gold.positions
UNION ALL SELECT 'gold.daily_pnl', COUNT(*) FROM cibc.gold.daily_pnl
UNION ALL SELECT 'gold.valuation_breaks', COUNT(*) FROM cibc.gold.valuation_breaks;

-- 12. Idempotency. Run the job twice with the same dates and compare query 11 before and after:
--     the counts must not change. Business keys must also never repeat (every duplicate_keys = 0).
SELECT 'silver.market_prices' AS table_name, COUNT(*) AS duplicate_keys FROM (
  SELECT symbol, price_date FROM cibc.silver.market_prices GROUP BY symbol, price_date HAVING COUNT(*) > 1)
UNION ALL SELECT 'silver.trades', COUNT(*) FROM (
  SELECT trade_id FROM cibc.silver.trades GROUP BY trade_id HAVING COUNT(*) > 1)
UNION ALL SELECT 'gold.positions', COUNT(*) FROM (
  SELECT valuation_date, book_id, counterparty_id, instrument_id, currency
  FROM cibc.gold.positions GROUP BY 1, 2, 3, 4, 5 HAVING COUNT(*) > 1)
UNION ALL SELECT 'gold.daily_pnl', COUNT(*) FROM (
  SELECT valuation_date, book_id, symbol, currency
  FROM cibc.gold.daily_pnl GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1);

-- 12b. The second run should not have changed Silver prices: look at the latest versions of the table
--      and compare operationMetrics (numTargetRowsInserted / numTargetRowsUpdated should be 0).
DESCRIBE HISTORY cibc.silver.market_prices LIMIT 5;

-- 12c. Bronze trades are insert-only: every recent operation is a MERGE (no WRITE/overwrite, no
--      UPDATE/DELETE), and a rerun of the same CSV shows operationMetrics.numTargetRowsInserted = 0.
DESCRIBE HISTORY cibc.bronze.trades LIMIT 5;
