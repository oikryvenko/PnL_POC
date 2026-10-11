CREATE TABLE IF NOT EXISTS cibc.silver.market_prices_rejects (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    symbol STRING,
    price_date STRING,
    close STRING,
    currency STRING,
    source_api STRING,
    ingestion_timestamp TIMESTAMP,
    run_id STRING,
    rejection_reason STRING,
    rejected_at TIMESTAMP
)
USING DELTA;
