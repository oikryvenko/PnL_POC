"""Configuration defaults for trade_pipeline market-price ingestion."""

DEFAULT_TICKERS = [
    "AAPL",
    "TSLA",
    "NFLX",
    "COIN",
    "DIS",
    "MSFT",
    "AMZN",
    "GOOG",
    "META",
    "PLTR",
]

# Scheduled runs pass no dates: the window is the last LOOKBACK_DAYS days ending today (UTC).
# Pass start_date/end_date explicitly for manual runs and historical backfills.
LOOKBACK_DAYS = 7

TARGET_TABLE = "cibc.bronze.market_prices"
SOURCE_API = "yfinance.download"
CURRENCY = "USD"

TRADE_CSV_PATH = "/Volumes/cibc/bronze/trade_files/dirty_trades.csv"

INSTRUMENT_ID_TO_SYMBOL = {
    "INS001": "AAPL",
    "INS002": "TSLA",
    "INS003": "NFLX",
    "INS004": "COIN",
    "INS005": "DIS",
    "INS006": "MSFT",
    "INS007": "AMZN",
    "INS008": "GOOG",
    "INS009": "META",
    "INS010": "PLTR",
}
