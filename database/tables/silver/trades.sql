CREATE TABLE IF NOT EXISTS cibc.silver.trades (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    trade_id STRING NOT NULL,
    counterparty_id STRING NOT NULL,
    instrument_id STRING NOT NULL,
    symbol STRING NOT NULL,
    quantity DECIMAL(18,6) NOT NULL,
    price DECIMAL(18,6) NOT NULL,
    currency STRING NOT NULL,
    trade_date DATE NOT NULL,
    settlement_date DATE NOT NULL,
    book_id STRING NOT NULL,
    version INT NOT NULL,
    modified_timestamp TIMESTAMP NOT NULL,
    bronze_run_id STRING,
    bronze_ingestion_timestamp TIMESTAMP,
    source_file STRING
)
USING DELTA;

-- Adding a CHECK validates existing rows, so the script fails if any current row violates it.
ALTER TABLE cibc.silver.trades DROP CONSTRAINT IF EXISTS nonzero_trade_quantity;
ALTER TABLE cibc.silver.trades ADD CONSTRAINT nonzero_trade_quantity CHECK (quantity <> 0);

ALTER TABLE cibc.silver.trades DROP CONSTRAINT IF EXISTS positive_trade_price;
ALTER TABLE cibc.silver.trades ADD CONSTRAINT positive_trade_price CHECK (price > 0);

ALTER TABLE cibc.silver.trades DROP CONSTRAINT IF EXISTS positive_trade_version;
ALTER TABLE cibc.silver.trades ADD CONSTRAINT positive_trade_version CHECK (version > 0);

ALTER TABLE cibc.silver.trades DROP CONSTRAINT IF EXISTS valid_trade_settlement;
ALTER TABLE cibc.silver.trades ADD CONSTRAINT valid_trade_settlement CHECK (settlement_date >= trade_date);