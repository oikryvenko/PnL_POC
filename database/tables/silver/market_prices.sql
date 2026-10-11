CREATE TABLE IF NOT EXISTS cibc.silver.market_prices (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    symbol STRING NOT NULL,
    price_date DATE NOT NULL,
    close DECIMAL(18,6) NOT NULL,
    currency STRING NOT NULL,
    source_api STRING,
    ingestion_timestamp TIMESTAMP,
    run_id STRING,
    bronze_run_id STRING,
    bronze_ingestion_timestamp TIMESTAMP
)
USING DELTA;

ALTER TABLE cibc.silver.market_prices DROP CONSTRAINT IF EXISTS positive_market_price;
ALTER TABLE cibc.silver.market_prices ADD CONSTRAINT positive_market_price CHECK (close > 0);

