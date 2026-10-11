"""Live probe: can Yahoo Finance daily prices be downloaded? Needs network access."""

import yfinance as yf

TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]
START_DATE = "2026-09-01"
END_DATE = "2026-10-01"  # Exclusive


def test_yahoo_returns_daily_prices_for_each_ticker():
    for ticker in TICKERS:
        prices = yf.download(
            tickers=ticker,
            start=START_DATE,
            end=END_DATE,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
            timeout=15,
        )
        assert not prices.empty, f"{ticker}: NO DATA"
        print(
            f"{ticker}: SUCCESS | {len(prices)} rows | "
            f"{prices.index.min().date()} to {prices.index.max().date()}"
        )