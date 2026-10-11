"""Generate a deterministic synthetic equity-trades CSV for pipeline exercises."""

from __future__ import annotations

import argparse
import csv
import random
from collections.abc import Iterable, Iterator, Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

FIELDNAMES = (
    "trade_id",
    "counterparty_id",
    "instrument_id",
    "symbol",
    "side",
    "quantity",
    "price",
    "currency",
    "trade_timestamp",
    "trade_date",
    "settlement_date",
    "book_id",
    "version",
    "modified_timestamp",
)

INSTRUMENTS = {
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
SYMBOL_TO_INSTRUMENT = {symbol: instrument_id for instrument_id, symbol in INSTRUMENTS.items()}
BOOKS = ("BOOK_TECH", "BOOK_GROWTH", "BOOK_HEDGE")
COUNTERPARTIES = tuple(f"CP{index:03d}" for index in range(1, 11))
HOLIDAYS = frozenset({date(2026, 9, 7)})
DATE_START = date(2026, 9, 1)
DATE_END = date(2026, 9, 30)
DIRTY_RECORDS_PER_CATEGORY = 5
DIRTY_CATEGORIES = (
    "missing_trade_id",
    "missing_counterparty_id",
    "missing_instrument_id",
    "missing_book_id",
    "missing_symbol",
    "missing_side",
    "missing_quantity",
    "missing_price",
    "missing_currency",
    "missing_trade_timestamp",
    "missing_trade_date",
    "missing_settlement_date",
    "missing_version",
    "missing_modified_timestamp",
    "invalid_side",
    "zero_quantity",
    "negative_quantity",
    "zero_price",
    "negative_price",
    "invalid_currency",
    "invalid_instrument_id",
    "incorrect_instrument_symbol_mapping",
    "malformed_trade_date",
    "malformed_trade_timestamp",
    "inconsistent_trade_date_timestamp",
    "invalid_settlement_date",
    "invalid_version",
    "weekend_trade",
    "unknown_ticker",
    "settlement_before_trade_date",
    "settlement_not_t_plus_one",
)
DIRTY_RECORD_COUNTS = {
    category: 10 if category in {
        "settlement_before_trade_date",
        "settlement_not_t_plus_one",
    } else DIRTY_RECORDS_PER_CATEGORY
    for category in DIRTY_CATEGORIES
}
TOTAL_DIRTY_RECORDS = sum(DIRTY_RECORD_COUNTS.values())
EXACT_DUPLICATE_ROWS = 10
CORRECTED_VERSIONS = 10
MINIMUM_ROWS = TOTAL_DIRTY_RECORDS + EXACT_DUPLICATE_ROWS + 2 * CORRECTED_VERSIONS
SETTLEMENT_REFERENCE_DATE = date(2026, 9, 30)
MISSING_SAME_DAY_PRICE_FIXTURE = ("AAPL", date(2026, 9, 15))
NO_EARLIER_PRICE_FIXTURE = ("PLTR", date(2026, 9, 1))

PRICE_RANGES = {
    "AAPL": (15_000, 30_000),
    "TSLA": (20_000, 50_000),
    "NFLX": (60_000, 140_000),
    "COIN": (15_000, 40_000),
    "DIS": (7_000, 15_000),
    "MSFT": (35_000, 60_000),
    "AMZN": (15_000, 30_000),
    "GOOG": (12_000, 25_000),
    "META": (50_000, 90_000),
    "PLTR": (2_000, 10_000),
}
CORE_TRADES = (
    ("BOOK_TECH", "AAPL", "BUY", 100, "2026-09-01"),
    ("BOOK_TECH", "AAPL", "SELL", 25, "2026-09-01"),
    ("BOOK_TECH", "AAPL", "BUY", 10, "2026-09-02"),
    ("BOOK_GROWTH", "TSLA", "SELL", 100, "2026-09-01"),
    ("BOOK_GROWTH", "TSLA", "BUY", 30, "2026-09-03"),
    ("BOOK_GROWTH", "TSLA", "SELL", 10, "2026-09-03"),
    ("BOOK_HEDGE", "MSFT", "BUY", 50, "2026-09-02"),
    ("BOOK_HEDGE", "MSFT", "SELL", 50, "2026-09-02"),
    ("BOOK_GROWTH", "TSLA", "BUY", 1, "2026-09-04"),
    ("BOOK_GROWTH", "TSLA", "SELL", 1, "2026-09-04"),
    ("BOOK_TECH", "AAPL", "BUY", 10, "2026-09-15"),
    ("BOOK_TECH", "AAPL", "SELL", 5, "2026-09-15"),
    ("BOOK_GROWTH", "AAPL", "BUY", 20, "2026-09-15"),
    ("BOOK_HEDGE", "AAPL", "SELL", 3, "2026-09-15"),
    ("BOOK_TECH", "AAPL", "BUY", 7, "2026-09-15"),
    ("BOOK_TECH", "PLTR", "BUY", 10, "2026-09-01"),
    ("BOOK_GROWTH", "PLTR", "SELL", 4, "2026-09-01"),
    ("BOOK_HEDGE", "PLTR", "BUY", 6, "2026-09-01"),
    ("BOOK_TECH", "PLTR", "SELL", 2, "2026-09-01"),
    ("BOOK_GROWTH", "PLTR", "BUY", 5, "2026-09-01"),
)
PROTECTED_BOOK_SYMBOLS = frozenset(
    {
        ("BOOK_TECH", "AAPL"),
        ("BOOK_GROWTH", "TSLA"),
        ("BOOK_HEDGE", "MSFT"),
        ("BOOK_TECH", "PLTR"),
        ("BOOK_GROWTH", "PLTR"),
        ("BOOK_HEDGE", "PLTR"),
    }
)

TradeRow = dict[str, str]


def trading_dates() -> tuple[date, ...]:
    current = DATE_START
    dates: list[date] = []
    while current <= DATE_END:
        if current.weekday() < 5 and current not in HOLIDAYS:
            dates.append(current)
        current += timedelta(days=1)
    return tuple(dates)


TRADING_DATES = trading_dates()


def calculate_settlement_date(trade_date: date) -> date:
    """Return the next US equity business day under this challenge's calendar."""
    settlement = trade_date + timedelta(days=1)
    while settlement.weekday() >= 5 or settlement in HOLIDAYS:
        settlement += timedelta(days=1)
    return settlement


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def generate_valid_trades(count: int, rng: random.Random) -> Iterator[TradeRow]:
    """Yield valid version-1 trades without retaining the generated dataset."""
    random_pairs = tuple(
        (book, symbol)
        for book in BOOKS
        for symbol in SYMBOL_TO_INSTRUMENT
        if (book, symbol) not in PROTECTED_BOOK_SYMBOLS
    )

    for index in range(1, count + 1):
        if index <= len(CORE_TRADES):
            book, symbol, side, quantity, trade_date_text = CORE_TRADES[index - 1]
            trade_date = date.fromisoformat(trade_date_text)
        else:
            book, symbol = rng.choice(random_pairs)
            side = rng.choice(("BUY", "SELL"))
            quantity = rng.randint(1, 5_000)
            trade_date = rng.choice(TRADING_DATES)

        timestamp = datetime.combine(trade_date, time.min, tzinfo=timezone.utc) + timedelta(
            minutes=rng.randint(810, 1_200)
        )
        modified_timestamp = timestamp + timedelta(seconds=rng.randint(0, 3_600))
        low_cents, high_cents = PRICE_RANGES[symbol]
        price = Decimal(rng.randint(low_cents, high_cents)) / Decimal(100)

        yield {
            "trade_id": f"TRD{index:06d}",
            "counterparty_id": rng.choice(COUNTERPARTIES),
            "instrument_id": SYMBOL_TO_INSTRUMENT[symbol],
            "symbol": symbol,
            "side": side,
            "quantity": str(quantity),
            "price": f"{price:.2f}",
            "currency": "USD",
            "trade_timestamp": format_timestamp(timestamp),
            "trade_date": trade_date.isoformat(),
            "settlement_date": calculate_settlement_date(trade_date).isoformat(),
            "book_id": book,
            "version": "1",
            "modified_timestamp": format_timestamp(modified_timestamp),
        }


def _set_scenario(row: TradeRow, category: str) -> None:
    match category:
        case "missing_trade_id":
            row["trade_id"] = ""
        case "missing_counterparty_id":
            row["counterparty_id"] = ""
        case "missing_instrument_id":
            row["instrument_id"] = ""
        case "missing_book_id":
            row["book_id"] = ""
        case "missing_symbol":
            row["symbol"] = ""
        case "missing_side":
            row["side"] = ""
        case "missing_quantity":
            row["quantity"] = ""
        case "missing_price":
            row["price"] = ""
        case "missing_currency":
            row["currency"] = ""
        case "missing_trade_timestamp":
            row["trade_timestamp"] = ""
        case "missing_trade_date":
            row["trade_date"] = ""
        case "missing_settlement_date":
            row["settlement_date"] = ""
        case "missing_version":
            row["version"] = ""
        case "missing_modified_timestamp":
            row["modified_timestamp"] = ""
        case "invalid_side":
            row["side"] = "HOLD"
        case "zero_quantity":
            row["quantity"] = "0"
        case "negative_quantity":
            row["quantity"] = "-100"
        case "zero_price":
            row["price"] = "0.00"
        case "negative_price":
            row["price"] = "-10.00"
        case "invalid_currency":
            row["currency"] = "EUR"
        case "invalid_instrument_id":
            row["instrument_id"] = "BAD001"
        case "incorrect_instrument_symbol_mapping":
            row["instrument_id"] = "INS001"
            row["symbol"] = "TSLA"
        case "malformed_trade_date":
            row["trade_date"] = "09/15/2026"
        case "malformed_trade_timestamp":
            row["trade_timestamp"] = "not-a-timestamp"
        case "inconsistent_trade_date_timestamp":
            original_date = date.fromisoformat(row["trade_date"])
            alternative_date = next(
                (candidate for candidate in TRADING_DATES if candidate != original_date),
                DATE_START,
            )
            row["trade_date"] = alternative_date.isoformat()
            row["settlement_date"] = calculate_settlement_date(alternative_date).isoformat()
        case "invalid_settlement_date":
            row["settlement_date"] = "2026-02-30"
        case "settlement_before_trade_date":
            trade_date = date.fromisoformat(row["trade_date"])
            row["settlement_date"] = (trade_date - timedelta(days=1)).isoformat()
        case "settlement_not_t_plus_one":
            trade_date = date.fromisoformat(row["trade_date"])
            row["settlement_date"] = (calculate_settlement_date(trade_date) + timedelta(days=1)).isoformat()
        case "invalid_version":
            row["version"] = "0"
        case "unknown_ticker":
            row["symbol"] = "UNKNOWN"
        case "weekend_trade":
            weekend_date = date(2026, 9, 5)
            original_timestamp = datetime.fromisoformat(
                row["trade_timestamp"].replace("Z", "+00:00")
            )
            weekend_timestamp = datetime.combine(
                weekend_date,
                original_timestamp.timetz().replace(tzinfo=None),
                tzinfo=timezone.utc,
            )
            row["trade_timestamp"] = format_timestamp(weekend_timestamp)
            row["trade_date"] = weekend_date.isoformat()
            row["settlement_date"] = calculate_settlement_date(weekend_date).isoformat()
            row["modified_timestamp"] = format_timestamp(weekend_timestamp + timedelta(hours=1))
        case _:
            raise ValueError(f"Unknown dirty-data category: {category}")


def inject_dirty_records(
    templates: Iterable[TradeRow], first_trade_number: int
) -> Iterator[TradeRow]:
    """Yield the fixed, documented count of isolated invalid records."""
    trade_number = first_trade_number
    template_iterator = iter(templates)
    for category in DIRTY_CATEGORIES:
        for _ in range(DIRTY_RECORD_COUNTS[category]):
            template = next(template_iterator)
            dirty_row = template.copy()
            dirty_row["trade_id"] = f"TRD{trade_number:06d}"
            dirty_row["version"] = "1"
            _set_scenario(dirty_row, category)
            yield dirty_row
            trade_number += 1


def _corrected_trade(original: TradeRow) -> TradeRow:
    corrected = original.copy()
    corrected["version"] = str(int(original["version"]) + 1)
    corrected["price"] = f"{Decimal(original['price']) + Decimal('0.01'):.2f}"
    modified_timestamp = datetime.fromisoformat(
        original["modified_timestamp"].replace("Z", "+00:00")
    )
    corrected["modified_timestamp"] = format_timestamp(modified_timestamp + timedelta(hours=1))
    return corrected


def generate_dataset(row_count: int, seed: int) -> Iterator[TradeRow]:
    """Yield base trades and a fixed number of dirty, duplicate, and corrected rows."""
    base_count = row_count - TOTAL_DIRTY_RECORDS - EXACT_DUPLICATE_ROWS - CORRECTED_VERSIONS
    originals: list[TradeRow] = []
    for trade in generate_valid_trades(base_count, random.Random(seed)):
        if len(originals) < CORRECTED_VERSIONS:
            originals.append(trade)
        yield trade

    dirty_templates = generate_valid_trades(TOTAL_DIRTY_RECORDS, random.Random(seed + 1))
    yield from inject_dirty_records(dirty_templates, base_count + 1)
    for original in originals:
        yield original.copy()
    for original in originals:
        yield _corrected_trade(original)


def write_csv(output: Path, rows: Iterable[TradeRow]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES, extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=_positive_integer, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/dirty_trades.csv"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.rows < MINIMUM_ROWS:
        parser.error(
            f"--rows must be at least {MINIMUM_ROWS} to include every required dirty-data scenario"
        )

    write_csv(args.output, generate_dataset(args.rows, args.seed))
    invalid_rows = TOTAL_DIRTY_RECORDS
    valid_rows = args.rows - invalid_rows
    print(
        f"Generated {args.rows} rows at {args.output} (seed={args.seed}; "
        f"valid={valid_rows}; invalid={invalid_rows}; "
        f"exact_duplicate_rows={EXACT_DUPLICATE_ROWS}; "
        f"corrected_versions={CORRECTED_VERSIONS}; missing_price_fixture_minimum=10)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())