"""Live probe: can Stooq daily prices be downloaded without an API key? Needs network access."""

import io

import pandas as pd
import requests

STOOQ_URL = "https://stooq.com/q/d/l/"


def fetch_stooq_prices(symbol, start_date, end_date):
    """Fetch daily historical closing prices from Stooq."""

    params = {
        "s": f"{symbol.lower()}.us",
        "d1": start_date.replace("-", ""),
        "d2": end_date.replace("-", ""),
        "i": "d",
    }

    response = requests.get(
        STOOQ_URL,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    if not response.text.startswith("Date,"):
        raise ValueError(
            f"Stooq returned unexpected data: {response.text[:200]}"
        )

    prices = pd.read_csv(io.StringIO(response.text))

    return prices[["Date", "Close"]]


def test_stooq_returns_daily_closes_for_aapl():
    prices = fetch_stooq_prices(
        symbol="AAPL",
        start_date="2026-09-01",
        end_date="2026-09-10",
    )

    print(prices)

    assert list(prices.columns) == ["Date", "Close"]
    assert not prices.empty
    assert (prices["Close"] > 0).all()


if __name__ == "__main__":
    print(fetch_stooq_prices(symbol="AAPL", start_date="2026-09-01", end_date="2026-09-10"))
