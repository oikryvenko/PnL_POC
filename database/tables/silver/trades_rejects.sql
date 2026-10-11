CREATE TABLE IF NOT EXISTS cibc.silver.trades_rejects (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    trade_id STRING,
    counterparty_id STRING,
    instrument_id STRING,
    symbol STRING,
    side STRING,
    quantity STRING,
    price STRING,
    currency STRING,
    trade_timestamp STRING,
    trade_date STRING,
    settlement_date STRING,
    book_id STRING,
    version STRING,
    modified_timestamp STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    run_id STRING,
    rejection_reason STRING,
    rejected_at TIMESTAMP
)
USING DELTA;
