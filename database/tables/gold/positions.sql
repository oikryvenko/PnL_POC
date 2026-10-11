CREATE TABLE IF NOT EXISTS cibc.gold.positions (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    valuation_date DATE NOT NULL,
    book_id STRING NOT NULL,
    counterparty_id STRING NOT NULL,
    instrument_id STRING NOT NULL,
    currency STRING NOT NULL,
    daily_buy_quantity DECIMAL(28,6) NOT NULL,
    daily_sell_quantity DECIMAL(28,6) NOT NULL,
    daily_net_quantity DECIMAL(28,6) NOT NULL,
    closing_position_quantity DECIMAL(28,6) NOT NULL,
    closing_price DECIMAL(18,6),
    market_value DECIMAL(38,6),
    run_id STRING,
    calculated_at TIMESTAMP
)
USING DELTA;
