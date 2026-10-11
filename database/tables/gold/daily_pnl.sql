CREATE TABLE IF NOT EXISTS cibc.gold.daily_pnl (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    valuation_date DATE NOT NULL,
    book_id STRING NOT NULL,
    symbol STRING NOT NULL,
    currency STRING NOT NULL,
    opening_quantity DECIMAL(28,6) NOT NULL,
    buy_quantity DECIMAL(28,6) NOT NULL,
    sell_quantity DECIMAL(28,6) NOT NULL,
    closing_quantity DECIMAL(28,6) NOT NULL,
    previous_close DECIMAL(18,6),
    current_close DECIMAL(18,6),
    market_value DECIMAL(38,6),
    unrealized_pnl_proxy DECIMAL(38,6),
    daily_pnl DECIMAL(38,6),
    calculation_status STRING NOT NULL,
    run_id STRING,
    calculated_at TIMESTAMP
)
USING DELTA;
