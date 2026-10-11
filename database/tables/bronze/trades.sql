-- Bronze trades: full snapshot of the latest trade CSV. The 14 source columns are kept as STRING
-- exactly as read (invalid values included); each run replaces the rows with INSERT OVERWRITE.
CREATE TABLE IF NOT EXISTS cibc.bronze.trades (
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
    run_id STRING
)
USING DELTA;
