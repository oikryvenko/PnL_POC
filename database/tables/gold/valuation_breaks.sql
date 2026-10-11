-- Breaks are instrument-level: one row per run, instrument, date and break type (or per trade for
-- unpriced trades). Books are consolidated, so the description may name only one book.
-- Rows are run-level history and are never updated or deleted; filter on the latest run_id.
CREATE TABLE IF NOT EXISTS cibc.gold.valuation_breaks (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id STRING,
    trade_id STRING,
    instrument_id STRING,
    valuation_date DATE,
    break_type STRING,
    break_description STRING,
    detected_at TIMESTAMP,
    status STRING
)
USING DELTA;
