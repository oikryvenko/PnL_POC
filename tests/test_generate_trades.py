import csv
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import tracemalloc

import pytest

from scripts.generate_trades import (
    BOOKS,
    DIRTY_CATEGORIES,
    DIRTY_RECORD_COUNTS,
    DIRTY_RECORDS_PER_CATEGORY,
    EXACT_DUPLICATE_ROWS,
    FIELDNAMES,
    INSTRUMENTS,
    MINIMUM_ROWS,
    MISSING_SAME_DAY_PRICE_FIXTURE,
    NO_EARLIER_PRICE_FIXTURE,
    SETTLEMENT_REFERENCE_DATE,
    TOTAL_DIRTY_RECORDS,
    CORRECTED_VERSIONS,
    calculate_settlement_date,
    main,
)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def is_valid_trade(row: dict[str, str]) -> bool:
    try:
        trade_date = date.fromisoformat(row["trade_date"])
        trade_timestamp = parse_timestamp(row["trade_timestamp"])
        modified_timestamp = parse_timestamp(row["modified_timestamp"])
        settlement_date = date.fromisoformat(row["settlement_date"])
        quantity = int(row["quantity"])
        price = Decimal(row["price"])
        version = int(row["version"])
    except (InvalidOperation, ValueError, KeyError):
        return False

    return all(
        (
            bool(row["trade_id"]),
            row["counterparty_id"] in {f"CP{number:03d}" for number in range(1, 11)},
            INSTRUMENTS.get(row["instrument_id"]) == row["symbol"],
            row["side"] in {"BUY", "SELL"},
            quantity > 0,
            price > 0,
            row["currency"] == "USD",
            trade_timestamp.tzinfo == timezone.utc,
            trade_date == trade_timestamp.date(),
            date(2026, 9, 1) <= trade_date <= date(2026, 9, 30),
            trade_date.weekday() < 5 and trade_date != date(2026, 9, 7),
            13 * 60 + 30 <= trade_timestamp.hour * 60 + trade_timestamp.minute <= 20 * 60,
            settlement_date == calculate_settlement_date(trade_date),
            row["book_id"] in BOOKS,
            version > 0,
            modified_timestamp.tzinfo == timezone.utc,
            modified_timestamp >= trade_timestamp,
        )
    )


def generate_default(path: Path, seed: int = 42) -> None:
    assert main(["--rows", "1200", "--seed", str(seed), "--output", str(path)]) == 0


@pytest.fixture
def generated_csv(tmp_path: Path) -> tuple[Path, list[dict[str, str]]]:
    output = tmp_path / "nested" / "dirty_trades.csv"
    generate_default(output)
    return output, read_rows(output)


def test_schema_and_exact_default_row_count(generated_csv) -> None:
    output, rows = generated_csv
    with output.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        assert next(reader) == list(FIELDNAMES)
        assert sum(1 for _ in reader) == 1_200
    assert len(rows) == 1_200
    assert all(tuple(row) == FIELDNAMES for row in rows)


def test_output_is_deterministic_for_seed_and_changes_with_seed(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    same_seed = tmp_path / "same_seed.csv"
    other_seed = tmp_path / "other_seed.csv"
    generate_default(first, 42)
    generate_default(same_seed, 42)
    generate_default(other_seed, 43)
    assert first.read_bytes() == same_seed.read_bytes()
    assert first.read_bytes() != other_seed.read_bytes()


def test_valid_records_obey_schema_reference_and_date_rules(generated_csv) -> None:
    _, rows = generated_csv
    valid_rows = [row for row in rows if is_valid_trade(row)]
    assert len(valid_rows) == 1_200 - TOTAL_DIRTY_RECORDS
    assert all(INSTRUMENTS[row["instrument_id"]] == row["symbol"] for row in valid_rows)
    assert all(Decimal(row["price"]) > 0 and int(row["quantity"]) > 0 for row in valid_rows)
    assert all(
        date.fromisoformat(row["settlement_date"])
        == calculate_settlement_date(date.fromisoformat(row["trade_date"]))
        for row in valid_rows
    )


def test_every_invalid_category_has_the_documented_count(generated_csv) -> None:
    _, rows = generated_csv
    counts = {
        "missing_trade_id": sum(not row["trade_id"] for row in rows),
        "missing_counterparty_id": sum(not row["counterparty_id"] for row in rows),
        "missing_instrument_id": sum(not row["instrument_id"] for row in rows),
        "missing_book_id": sum(not row["book_id"] for row in rows),
        "missing_symbol": sum(not row["symbol"] for row in rows),
        "missing_side": sum(not row["side"] for row in rows),
        "missing_quantity": sum(not row["quantity"] for row in rows),
        "missing_price": sum(not row["price"] for row in rows),
        "missing_currency": sum(not row["currency"] for row in rows),
        "missing_trade_timestamp": sum(not row["trade_timestamp"] for row in rows),
        "missing_trade_date": sum(not row["trade_date"] for row in rows),
        "missing_settlement_date": sum(not row["settlement_date"] for row in rows),
        "missing_version": sum(not row["version"] for row in rows),
        "missing_modified_timestamp": sum(not row["modified_timestamp"] for row in rows),
        "invalid_side": sum(row["side"] == "HOLD" for row in rows),
        "zero_quantity": sum(row["quantity"] == "0" for row in rows),
        "negative_quantity": sum(bool(row["quantity"]) and int(row["quantity"]) < 0 for row in rows),
        "zero_price": sum(bool(row["price"]) and Decimal(row["price"]) == 0 for row in rows),
        "negative_price": sum(bool(row["price"]) and Decimal(row["price"]) < 0 for row in rows),
        "invalid_currency": sum(bool(row["currency"]) and row["currency"] != "USD" for row in rows),
        "invalid_instrument_id": sum(row["instrument_id"] == "BAD001" for row in rows),
        "incorrect_instrument_symbol_mapping": sum(
            row["instrument_id"] == "INS001" and row["symbol"] == "TSLA" for row in rows
        ),
        "malformed_trade_date": sum(row["trade_date"] == "09/15/2026" for row in rows),
        "malformed_trade_timestamp": sum(
            row["trade_timestamp"] == "not-a-timestamp" for row in rows
        ),
        "inconsistent_trade_date_timestamp": sum(
            _dates_disagree(row) for row in rows
        ),
        "invalid_settlement_date": sum(
            row["settlement_date"] == "2026-02-30" for row in rows
        ),
        "invalid_version": sum(row["version"] == "0" for row in rows),
        "weekend_trade": sum(
            _is_weekend_trade(row) for row in rows
        ),
        "unknown_ticker": sum(row["symbol"] == "UNKNOWN" for row in rows),
        "settlement_before_trade_date": sum(_settles_before_trade(row) for row in rows),
        "settlement_not_t_plus_one": sum(
            _settlement_not_t_plus_one(row) for row in rows
        ),
    }
    assert tuple(counts) == DIRTY_CATEGORIES
    assert counts == DIRTY_RECORD_COUNTS
    blank_rows = [row for row in rows if any(not row[field] for field in FIELDNAMES)]
    assert len(blank_rows) == 14 * DIRTY_RECORDS_PER_CATEGORY
    assert all(
        sum(not row[field] for field in FIELDNAMES) == 1 for row in blank_rows
    )


def _dates_disagree(row: dict[str, str]) -> bool:
    try:
        return date.fromisoformat(row["trade_date"]) != parse_timestamp(
            row["trade_timestamp"]
        ).date()
    except ValueError:
        return False


def _settles_before_trade(row: dict[str, str]) -> bool:
    try:
        trade_date = date.fromisoformat(row["trade_date"])
        if trade_date != parse_timestamp(row["trade_timestamp"]).date():
            return False
        return date.fromisoformat(row["settlement_date"]) < trade_date
    except ValueError:
        return False


def _settlement_not_t_plus_one(row: dict[str, str]) -> bool:
    try:
        trade_date = date.fromisoformat(row["trade_date"])
        if trade_date != parse_timestamp(row["trade_timestamp"]).date():
            return False
        settlement_date = date.fromisoformat(row["settlement_date"])
        return settlement_date > trade_date and settlement_date != calculate_settlement_date(
            trade_date
        )
    except ValueError:
        return False


def _is_weekend_trade(row: dict[str, str]) -> bool:
    try:
        trade_date = date.fromisoformat(row["trade_date"])
        timestamp_date = parse_timestamp(row["trade_timestamp"]).date()
    except ValueError:
        return False
    return trade_date == timestamp_date and trade_date.weekday() >= 5


def test_exact_duplicates_and_corrected_versions_are_distinguishable(generated_csv) -> None:
    _, rows = generated_csv
    record_counts = Counter(tuple(row[field] for field in FIELDNAMES) for row in rows)
    assert sum(count - 1 for count in record_counts.values() if count > 1) == EXACT_DUPLICATE_ROWS

    versions: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row["trade_id"]:
            versions[row["trade_id"]].append(row)
    corrected = [
        versions_for_trade
        for versions_for_trade in versions.values()
        if any(row["version"] == "2" for row in versions_for_trade)
    ]
    assert len(corrected) == CORRECTED_VERSIONS
    for trade_versions in corrected:
        assert sorted(int(row["version"]) for row in trade_versions) == [1, 1, 2]
        version_one = [row for row in trade_versions if row["version"] == "1"]
        version_two = next(row for row in trade_versions if row["version"] == "2")
        assert all(
            parse_timestamp(version_two["modified_timestamp"])
            > parse_timestamp(row["modified_timestamp"])
            for row in version_one
        )
        assert all(is_valid_trade(row) for row in trade_versions)


def test_valid_logical_trades_include_long_short_and_flat_positions(generated_csv) -> None:
    _, rows = generated_csv
    latest_by_id: dict[str, dict[str, str]] = {}
    for row in rows:
        if not is_valid_trade(row):
            continue
        current = latest_by_id.get(row["trade_id"])
        if current is None or int(row["version"]) > int(current["version"]):
            latest_by_id[row["trade_id"]] = row

    positions: dict[tuple[str, str], int] = defaultdict(int)
    for row in latest_by_id.values():
        direction = 1 if row["side"] == "BUY" else -1
        positions[(row["book_id"], row["symbol"])] += direction * int(row["quantity"])

    assert positions[("BOOK_TECH", "AAPL")] > 0
    assert positions[("BOOK_GROWTH", "TSLA")] < 0
    assert positions[("BOOK_HEDGE", "MSFT")] == 0


def test_settlement_skips_weekends_and_labor_day() -> None:
    assert calculate_settlement_date(date(2026, 9, 4)) == date(2026, 9, 8)
    assert calculate_settlement_date(date(2026, 9, 3)) == date(2026, 9, 4)


def test_settlement_categories_and_fixed_reference_date(generated_csv) -> None:
    _, rows = generated_csv
    valid_settled = [
        row for row in rows
        if is_valid_trade(row)
        and date.fromisoformat(row["settlement_date"]) < SETTLEMENT_REFERENCE_DATE
    ]
    before_trade = [row for row in rows if _settles_before_trade(row)]
    not_t_plus_one = [row for row in rows if _settlement_not_t_plus_one(row)]

    assert len(valid_settled) >= 10
    assert len(before_trade) == 10
    assert len(not_t_plus_one) == 10
    assert all(is_valid_trade(row) for row in valid_settled)
    assert all(not is_valid_trade(row) for row in before_trade + not_t_plus_one)


def test_unknown_and_missing_market_price_fixtures(generated_csv) -> None:
    _, rows = generated_csv
    unknown_tickers = [row for row in rows if row["symbol"] == "UNKNOWN"]
    assert len(unknown_tickers) == 5
    assert all(not is_valid_trade(row) for row in unknown_tickers)

    same_day_symbol, same_day_date = MISSING_SAME_DAY_PRICE_FIXTURE
    no_prior_symbol, no_prior_date = NO_EARLIER_PRICE_FIXTURE
    missing_same_day_price = [
        row for row in rows
        if row["symbol"] == same_day_symbol and row["trade_date"] == same_day_date.isoformat()
        and is_valid_trade(row)
    ]
    no_earlier_price = [
        row for row in rows
        if row["symbol"] == no_prior_symbol and row["trade_date"] == no_prior_date.isoformat()
        and is_valid_trade(row)
    ]
    assert len(missing_same_day_price) >= 5
    assert len(no_earlier_price) >= 5


def test_cli_rejects_too_few_rows(tmp_path: Path, capsys) -> None:
    output = tmp_path / "not-created.csv"
    with pytest.raises(SystemExit) as error:
        main(["--rows", str(MINIMUM_ROWS - 1), "--output", str(output)])
    assert error.value.code == 2
    assert f"at least {MINIMUM_ROWS}" in capsys.readouterr().err
    assert not output.exists()


def test_large_generation_and_counting_are_streamed(tmp_path: Path, capsys) -> None:
    output = tmp_path / "large" / "dirty_trades_100k.csv"
    tracemalloc.start()
    assert main(["--rows", "100000", "--seed", "42", "--output", str(output)]) == 0
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    capsys.readouterr()

    assert peak_bytes < 8 * 1024 * 1024
    with output.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        assert next(reader) == list(FIELDNAMES)
        assert sum(1 for _ in reader) == 100_000