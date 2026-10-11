-- Bronze market prices
CREATE TABLE IF NOT EXISTS cibc.bronze.market_prices (
    symbol STRING,
    price_date STRING,
    close STRING,
    currency STRING,
    source_api STRING,
    ingestion_timestamp TIMESTAMP,
    run_id STRING
)
USING DELTA;
