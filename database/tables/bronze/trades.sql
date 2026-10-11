-- Bronze trades: insert-only history of distinct raw records. The 14 source columns are kept as
-- STRING exactly as read (invalid values included); rows are never updated or deleted.
-- record_hash = sha2(to_json(array(<14 source columns in order>)), 256) is the MERGE key.
-- Existing tables are NOT changed by this script: apply
-- database/migrations/add_bronze_trades_record_hash.sql instead.
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
    run_id STRING,
    record_hash STRING
)
USING DELTA;
