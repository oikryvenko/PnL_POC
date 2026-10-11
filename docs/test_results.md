# Pipeline Test Results

Databricks run of the deployed `trade_pipeline` Job (`dev` target), 2026-10-11 UTC, with
`start_date=2026-09-01`, `end_date=2026-09-30` and the default trade CSV.

## Run 1 (`937413025907399`, September 2026): all six tasks succeeded

| Task | Read | Accepted / written | Rejected | Duplicates |
|---|---|---|---|---|
| `ingest_trades` | 1200 | 1190 | 0 | 10 (identical rows in the CSV) |
| `process_trades` | 1190 | 992 | 198 (25 reason types) | 10 |
| `ingest_prices` / `process_prices` | 210 | 210 | 0 | 0 |
| `build_positions` | — | 5990 position rows | — | — |
| `calculate_daily_pnl` | — | 835 P&L rows | — | — |

## Positions and P&L are calculated correctly

Both were recalculated from Silver with independent SQL and compared with Gold:

- **Positions:** 5990 rows checked. Closing quantity, daily net quantity, market value and the closing price (same-day close only) had 0 mismatches. All 251 trade groupings (book/counterparty/instrument) have a row from their first trade date.
- **Daily P&L:** 835 rows checked. Opening and closing quantity, previous and current close, status and the P&L amount had 0 mismatches. Rows with a missing price never carry a value.
- **Positions agree with P&L:** summed by book and symbol, closing quantities match in all 835 rows.
- **No duplicates:** 0 duplicate keys in Bronze, Silver or Gold.
- **Hand check, BOOK_TECH / AAPL on 2026-10-09:** 97 × (336.640015 − 340.420013) = −366.66, which matches the stored value.

## Missing prices for 1–2 October

No ticker has a price for those two dates because they fall outside every loaded date range. As designed, this produced 20 valuation breaks, 502 position rows without a market value and 60 P&L rows left empty with `MISSING_CURRENT_PRICE`; the calculation is correct. A run for 2026-10-01 to 2026-10-02 would fill the gap.

## Run 2 (`849132887074038`, same parameters)

Submitted after run 1 to test idempotency; at the time of writing it was still queued waiting for serverless compute, so its results are not recorded here.
