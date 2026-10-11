# Trading ETL Pipeline (Databricks, Delta Lake)

A small Bronze → Silver → Gold pipeline for equity trades and daily closing prices. It ingests a dirty trades CSV and market prices, validates and quarantines bad records, builds daily positions by book, and calculates a daily mark-to-market P&L. It runs as a six-task Databricks job deployed with a Databricks Asset Bundle. Two Databricks notebooks (`notebooks/`) let users start the job and review its results. The full solution design and user guide is `docs/trading_pipeline_solution_design.html` (self-contained, opens offline).

## Architecture

```
ingest_prices ──> process_prices ──┐
                                   ├──> build_positions ──> calculate_daily_pnl
ingest_trades ──> process_trades ──┘
```

| Layer | Task | Table(s) | What it does |
|---|---|---|---|
| Bronze | `ingest_prices` | `cibc.bronze.market_prices` | Downloads daily closes for the tickers and date range and MERGEs them by (`symbol`, `price_date`). History is kept; reruns do not duplicate. |
| Bronze | `ingest_trades` | `cibc.bronze.trades` | Reads the 14-column trades CSV from a Unity Catalog Volume as raw strings and inserts only raw records not seen before (insert-only MERGE on `record_hash`). Bronze is a historical raw-record store, not a snapshot. |
| Silver | `process_prices` | `cibc.silver.market_prices`, `..._rejects` | Casts and validates prices, deduplicates, MERGEs into Silver, quarantines rejects. |
| Silver | `process_trades` | `cibc.silver.trades`, `..._rejects` | Casts and validates trades, keeps the latest valid version per `trade_id`, quarantines rejects. |
| Gold | `build_positions` | `cibc.gold.positions`, `cibc.gold.valuation_breaks` | Daily positions by date, book, counterparty and instrument, valued at the exact-date close. |
| Gold | `calculate_daily_pnl` | `cibc.gold.daily_pnl`, `cibc.gold.valuation_breaks` | Daily P&L by date, book and symbol. |

Every task writes a row to `cibc.gold.pipeline_run_log`. Silver and Gold tables and the audit log use a generated `id BIGINT GENERATED ALWAYS AS IDENTITY` (Bronze tables have none); writers never write `id`.

| Notebook | Purpose |
|---|---|
| `notebooks/01_run_pipeline.py` | Submits the existing job through the Jobs API with `start_date`, `end_date` and `tickers` widgets, only after `confirm_run = YES`; shows the run link and SUBMITTED / RUNNING / SUCCESS / FAILED. |
| `notebooks/02_pipeline_report.py` | Read-only report: run summary, rejections, positions, daily P&L, valuation breaks and daily execution monitoring (scheduled vs manual runs). |

## Data sources

**Market prices.** The exercise names Stooq. I implemented and tested a standalone Stooq CSV fetcher (`tests/live/test_stooq.py`). Requests to its historical-price endpoint returned HTTP 404 in the test environment; requests with other date ranges, and to the Stooq homepage, also returned HTTP 404. This suggests an environment-specific connectivity or access restriction, but the exact cause is not established. To keep a working end-to-end pipeline I used Yahoo Finance (`yfinance`, unadjusted daily closes). **This does not strictly satisfy the named-provider requirement.** The provider is isolated in `bronze/ingest_prices.py`; Silver and Gold read only `symbol`, `price_date`, `close`, `currency`, so changing provider does not affect them.

**Trades CSV.** `data/dirty_trades.csv` is generated deterministically by `scripts/generate_trades.py` (1,200 rows, seed 42): 165 deliberately invalid rows, 10 exact duplicate rows and 10 corrected versions. Schema: `trade_id, counterparty_id, instrument_id, symbol, side, quantity, price, currency, trade_timestamp, trade_date, settlement_date, book_id, version, modified_timestamp`. Instruments `INS001`–`INS010` map to the 10 default tickers (`src/trade_pipeline/config.py`).

## Bronze behaviour

- **Prices:** if any expected (symbol, trading day) pair is missing from the response, the task fails before writing. Weekends, NYSE holidays and future dates are not expected.
- **Trades:** the CSV header must match the 14 expected columns, an empty file is rejected, structurally corrupt rows fail the task, and all values stay strings (invalid business values are kept for Silver to quarantine). `cibc.bronze.trades` is a **historical, insert-only store of distinct raw records**, not a copy of the latest file: `record_hash = sha2(to_json(array(<14 raw columns>)), 256)`, the batch is deduplicated on it, and a Delta `MERGE ... WHEN NOT MATCHED THEN INSERT` on `record_hash` adds only new records. Existing rows (and their `source_file`, `ingestion_timestamp`, `run_id`) are never updated or deleted, so a rerun of the same CSV inserts nothing, and new trades, new versions and changed attributes become new rows. Trades that disappear from a later file stay in Bronze.
- **Upgrading from the earlier `INSERT OVERWRITE` version:** apply `database/migrations/add_bronze_trades_record_hash.sql` (adds `record_hash`, backfills it, then checks that no row is left without a hash) **before** the next run. Until then `ingest_trades` fails without writing, and the trade branch and Gold are skipped. Rows from the last overwrite load keep their exact duplicate rows; Silver still rejects them as duplicates.

## Silver rules

**Prices:** rows are scoped to the requested tickers and dates (other history is left alone). Rejected: null/empty symbol, unparseable date, future date, non-trading day, unparseable or non-positive close, currency other than USD, duplicate business keys (latest ingestion wins; conflicting ties are quarantined). If an expected session has no accepted price, the task fails before touching Silver.

**Trades:** rejected: null/empty trade, counterparty, instrument, symbol or book IDs; instrument not in the configured universe or not matching its symbol; side not BUY/SELL; zero or non-numeric quantity; BUY with negative quantity; non-positive price; currency other than USD; unparseable, future or non-trading trade date; trade date not matching the timestamp; settlement before trade date or not T+1 (NYSE calendar); invalid version or timestamps. Quantity is signed: BUY positive, SELL negative.

**Trade deduplication rule:** per `trade_id`, the highest `version` wins, then the latest `modified_timestamp`. Exact duplicates are removed at Bronze; older versions are rejected as `DUPLICATE_TRADE_VERSION`; conflicting rows tied at the latest version and `modified_timestamp` are rejected as `AMBIGUOUS_TRADE_VERSION` (because Bronze keeps history, this includes a record re-sent with changed attributes but the same version and `modified_timestamp`). If the latest version is invalid, unparseable or ambiguous, an older valid version is **not** used (`SUPERSEDED_BY_...`), and that trade is removed from Silver. Silver reads the full Bronze history each run; trades are not filtered by the job's dates.

**Rejects** are kept as history in `cibc.silver.*_rejects` (one row per distinct rejected record and reason, with pipe-separated reasons). Reruns do not add duplicates.

**Lineage:** each Silver row carries `bronze_run_id` and `bronze_ingestion_timestamp` (and `source_file` for trades) copied from the Bronze row that was selected after validation and deduplication. Unchanged rows are not rewritten, so lineage only changes when the row changes. For trades, lineage points to the Bronze row of the run that first ingested the selected record, and that row is kept.

## Gold calculations

**Positions.** Signed trades are summed per date, book, counterparty, instrument and currency (buys positive, sells as absolute sells). The timeline is every NYSE session from each position's first trade to the latest trade or price date (never in the future), plus every trade date. Closing position is the cumulative net quantity, so positions carry forward on days without trades and a missing price never removes a trade. Market value is closing quantity × the same-day close.

Positions by book and symbol are available in `daily_pnl` (`closing_quantity`, `market_value`), for example:

```sql
SELECT valuation_date, book_id, symbol, closing_quantity, market_value
FROM cibc.gold.daily_pnl ORDER BY valuation_date, book_id, symbol;
```

**Daily P&L** (`cibc.gold.daily_pnl`, one row per date, book, symbol, currency):

```text
daily_pnl = opening_quantity × (current_close − previous_close)
          + SUM(signed_trade_quantity × (current_close − execution_price))
```

`opening_quantity = closing_quantity − daily_net_quantity`. Example: buy 10 at 100, close 105 → +50. Next day, previous close 105, buy 5 at 106, close 108 → 10×(108−105) + 5×(108−106) = 40. Shorts give negative quantities and negative P&L when the price rises. `unrealized_pnl_proxy` is `market_value − cumulative signed trade cash flow`: a cumulative mark-to-market proxy that includes realized effects, not true unrealized P&L, and no cost-basis accounting is done.

**Market price matching and valuation policy.** The pipeline uses exact-date market prices for Gold position valuations and daily P&L. Trades are matched to market prices using the instrument symbol and valuation date. Daily P&L uses the latest available close strictly before the valuation date as its previous-close baseline; future prices are never used. When a current-date price is unavailable, the pipeline does not substitute an older price: it records a valuation break and leaves the affected valuation and P&L NULL (`calculation_status` is `MISSING_CURRENT_PRICE`; a nonzero opening position with no earlier close gives `MISSING_BASELINE`). A separate nearest-prior function, `match_trades_to_prices_asof`, is used for trade-price reconciliation and break detection. **Tradeoff:** transparent valuation exceptions are preferred over automatically filling missing prices.

**Breaks** (`cibc.gold.valuation_breaks`) are instrument-level: one row per run, instrument, date and break type (books are consolidated). They are history per `run_id` and are never updated or deleted, so filter on the latest `run_id`. Rerunning the same `run_id` adds nothing.

**Gold writes** are MERGEs on the business key that update a row only when a financial value changed. Obsolete rows are deleted only for the instruments/symbols present in the source and dates on or after the source's first date, so a partial upstream load cannot erase other history. Stale rows outside that scope need manual cleanup.

## Build, Deploy and Run

Commands are run from the repository root. `scripts/deploy.sh` wraps the Databricks CLI (`chmod +x scripts/deploy.sh` once; on Windows use Git Bash). The bundle name and job resource key are both `trade_pipeline`; the only target is `dev` (`databricks.yml`).

**1. Prerequisites.** Python 3.10+ with `python` and `pip` on PATH, bash, the Databricks CLI, and a Databricks workspace with Unity Catalog. The job uses serverless compute only (no cluster settings). Java and Spark are not needed to deploy.

**2. Install the Databricks CLI** (validated with v1.20.0): `winget install Databricks.DatabricksCLI` (Windows), `brew tap databricks/tap && brew install databricks` (macOS), or the install script in the Databricks CLI docs. Check with `databricks --version`.

**3. Authenticate with a CLI profile** (OAuth; no secrets are stored in the repository):

```
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile>
export DATABRICKS_CONFIG_PROFILE=<profile>
databricks current-user me
```

`databricks.yml` contains the workspace host (`workspace.host`); change it to your workspace when cloning. Select another target with `DATABRICKS_TARGET`.

**4. Unity Catalog setup.** The project uses catalog `cibc` (create it, or edit the names in `database/` and `config.py`). Run the SQL files in the Databricks SQL editor in this order: `database/catalog.sql`, `bronze.sql`, `silver.sql`, `gold.sql`, `volumes/trade_files.sql`, then `tables/bronze/*`, `tables/silver/*` (table scripts before `*_constraints.sql`), `tables/gold/*`. The constraint scripts are safe to rerun. If the job runs as a principal other than the catalog owner, run `database/grants.sql` after replacing `<job_run_as_principal>` (it grants what the job needs at run time; an owner already has these). The person doing the setup needs more: `CREATE SCHEMA` on the catalog (if the schemas do not exist), `CREATE TABLE` and `CREATE VOLUME` on the schemas, and `WRITE VOLUME` on `cibc.bronze.trade_files` to upload the CSV; a catalog owner has all of these. `CREATE TABLE IF NOT EXISTS` does not change existing tables: for an existing Silver deployment run `database/migrations/add_silver_lineage_columns.sql` once, and for an existing `cibc.bronze.trades` run `database/migrations/add_bronze_trades_record_hash.sql` (adds and backfills `record_hash`; `ingest_trades` refuses to run while any row has no hash). Databricks cannot add an identity column to an existing table, so a table created without `id` needs a reviewed copy-and-rename migration (none is included or run).

**5. Trade CSV.** The job reads `/Volumes/cibc/bronze/trade_files/dirty_trades.csv` (job parameter `trade_input_path`). Upload it once (regenerate with `python scripts/generate_trades.py` if needed):

```
databricks fs cp data/dirty_trades.csv dbfs:/Volumes/cibc/bronze/trade_files/dirty_trades.csv
```

**6. Build the wheel** (optional check; `deploy` also builds it): `./scripts/deploy.sh build` creates `dist/trade_pipeline-0.1.0-py3-none-any.whl` from `pyproject.toml` and lists its modules. The job references the bundle-managed wheel (`jobs/resources/trade_pipeline.job.yml`, `../../dist/*.whl`), never a manually uploaded file. The file name only changes with the version, so bump `version` in `pyproject.toml` for each release to make sure serverless installs the new code rather than a cached copy (not verified either way).

**7. Validate:** `./scripts/deploy.sh validate` (runs `databricks bundle validate -t dev`).

**8. Deploy:** `./scripts/deploy.sh deploy` validates, builds the wheel (`artifacts` in `databricks.yml`), uploads it and creates the job (named `[dev <user>] trade_pipeline` in development mode) under `/Workspace/Users/<you>/.bundle/trade_pipeline/dev`. The repository files are synced to `.../dev/files`, so the notebooks appear as `.../dev/files/notebooks/01_run_pipeline` and `02_pipeline_report`. Deploying does not start a run. Equivalent: `databricks bundle deploy -t dev`. Apply any pending migration (step 4) before the first run of new code.

**9. Run:** `./scripts/deploy.sh run` (default dates: the last 7 days to today), or with dates: `./scripts/deploy.sh run --params start_date=2026-09-01,end_date=2026-09-30`. Equivalent: `databricks bundle run trade_pipeline -t dev --params ...`. Job parameters: `tickers` (comma-separated; empty = the 10 defaults), `start_date`, `end_date` (inclusive, `YYYY-MM-DD`), `trade_input_path`. You can also start it from the Jobs UI (*Run now with different parameters*) or from notebook 01 (below).

**Run and review from notebooks.**

1. Open `.../dev/files/notebooks/01_run_pipeline` and attach serverless compute. Set `start_date`, `end_date` and optionally `tickers` (blank = job default).
2. **Run all** with `confirm_run = NO`: inputs are validated and the job is resolved by name (`trade_pipeline` or `[dev <user>] trade_pipeline`; `job_id` overrides), but nothing is submitted.
3. Set `confirm_run = YES` and run again. If a run is already active, the notebook monitors it instead of starting another. The widget is then removed, so re-running cannot submit twice. Wait for `SUCCESS` or `FAILED` and open the run link; submission alone does not mean success.
4. Open `02_pipeline_report` and **Run all**. Blank `run_id` means the latest run whose six tasks all succeeded; blank `processing_date` means the latest date in `daily_pnl`. Section A shows status and counts (metrics the audit log does not store show `NOT AVAILABLE`), B rejections, C positions, D daily P&L, E valuation breaks, F daily execution monitoring.
5. Idempotency check: run the same dates again. Section A should show Bronze trades newly inserted = 0 and duplicates = rows read.

**10. Check the run.** `databricks bundle summary -t dev` prints the job URL; open the run in the Jobs UI and confirm all six tasks succeeded. Task results are also in the audit table:

```sql
SELECT run_id, task_run_id, task_name, status, started_at, finished_at,
       input_rows, output_rows, rejected_rows, duplicate_rows, issue_counts, error_message
FROM cibc.gold.pipeline_run_log ORDER BY started_at DESC;
```

**11. Verify the tables.**

```sql
SELECT 'bronze.market_prices' AS tbl, COUNT(*) AS n FROM cibc.bronze.market_prices
UNION ALL SELECT 'bronze.trades', COUNT(*) FROM cibc.bronze.trades
UNION ALL SELECT 'silver.market_prices', COUNT(*) FROM cibc.silver.market_prices
UNION ALL SELECT 'silver.market_prices_rejects', COUNT(*) FROM cibc.silver.market_prices_rejects
UNION ALL SELECT 'silver.trades', COUNT(*) FROM cibc.silver.trades
UNION ALL SELECT 'silver.trades_rejects', COUNT(*) FROM cibc.silver.trades_rejects
UNION ALL SELECT 'gold.positions', COUNT(*) FROM cibc.gold.positions
UNION ALL SELECT 'gold.daily_pnl', COUNT(*) FROM cibc.gold.daily_pnl
UNION ALL SELECT 'gold.valuation_breaks', COUNT(*) FROM cibc.gold.valuation_breaks;
```

**12. Re-run the same dates.** This is safe: Bronze prices and Silver MERGE on their business keys and update only changed rows, Bronze trades insert only records not seen before (0 on a rerun), rejects are not re-inserted, Gold updates only changed values, and breaks are keyed by run. Row counts stay the same; `rows_written` in the task metrics should be 0 for unchanged data.

**13. Historical backfill.** Pass explicit dates, for example `./scripts/deploy.sh run --params start_date=2026-09-01,end_date=2026-09-30`. Run the September 2026 range once so Silver prices cover the sample trades. Older Silver history is never deleted.

**14. Common failures.**

| Symptom | Likely cause and fix |
|---|---|
| `databricks: command not found` or "cannot configure default credentials" | Install the CLI; set `DATABRICKS_CONFIG_PROFILE` or run `databricks auth login`. |
| Build fails or `python` not found | Put `python` and `pip` on PATH; run `./scripts/deploy.sh build` to see the error. |
| Validate warns about `dist/*.whl` | Run `./scripts/deploy.sh build` first, or deploy (which builds it). |
| `ingest_trades`: could not read trade CSV | Create the Volume (`volumes/trade_files.sql`) and upload the CSV to the path above. |
| Table or column not found, or MERGE schema errors | Run the `database/` SQL; apply the migrations to tables created by earlier versions. |
| `ingest_trades`: "rows without record_hash" or column `record_hash` not found | Apply `database/migrations/add_bronze_trades_record_hash.sql`, then repair the run. |
| Trade quarantined as `AMBIGUOUS_TRADE_VERSION` after a re-sent file | The record changed without a new `version` or `modified_timestamp`; Bronze keeps both copies. Send a new version. |
| `ingest_prices` or `process_prices`: missing expected prices | The range includes a trading day with no provider data (for example today before the close), or the provider is unreachable. Narrow the dates and rerun. |
| Schedule never fires | It is `PAUSED`, and development mode keeps schedules paused. |
| Notebook 01 cannot find the job | Several or no jobs match the name; set the `job_id` widget. |

**Daily run:** the job has a weekday 6 PM America/New_York schedule (`0 0 18 ? * MON-FRI`) and stays `PAUSED`; the repository has only the `dev` Bundle target, and development mode pauses schedules again on every deploy. Enable or pause it from the Databricks Jobs UI (job `[dev <user>] trade_pipeline` > Schedules & Triggers), or run it manually with notebook 01. No scheduled run has been verified yet. The cron also fires on NYSE holidays; the price tasks then find no new session and succeed without new prices. With empty `start_date`/`end_date`, price ingestion and processing use the last 7 days up to the runtime's current date (`date.today()`, UTC on serverless); both tasks resolve the same rule, and older Silver history is preserved.

**Monitoring daily execution:** notebook 02 section F lists one row per job run (not one per task) from the Jobs API and the audit log, grouped by New York business date: successful, failed and multiple runs, and the trigger type (`PERIODIC` = scheduled, `ONE_TIME` = manual). A manual run never counts as a scheduled one. Missing scheduled runs are reported only when the schedule is `UNPAUSED` and the `schedule_active_since` widget is set, because the Jobs API does not expose pause history.

**Operational metrics:** `cibc.gold.pipeline_run_log` has one row per task attempt (query above).

`input_rows` = source records examined; `output_rows` = valid records produced; `rejected_rows` = records rejected; `duplicate_rows` = records classified as duplicates. Tasks also return and log `rows_written` (inserted or updated only), `rejects_written` (new reject-history rows) and MERGE counts; these are not stored in the table. `output_rows` and `rows_written` differ on a rerun of unchanged data.

For `ingest_trades`: `input_rows` = CSV rows read; `output_rows` = distinct raw records in the CSV (all present in Bronze after the run); `duplicate_rows` = `input_rows` − newly inserted rows (exact copies inside the CSV plus records already in Bronze, split in `issue_counts` as `DUPLICATE_IN_BATCH` / `ALREADY_INGESTED`); `rows_written` = rows inserted (Delta MERGE `num_inserted_rows`), derivable as `input_rows − duplicate_rows`.

## Tests

For a five-minute demo, `docs/demo_queries.sql` has read-only queries for Bronze (including trade history), Silver, rejects, positions, P&L, breaks, audit metrics, row counts and idempotency checks. `docs/data_contracts.md` summarizes the table contracts, and `docs/trading_pipeline_solution_design.html` is the full design and user guide.

```
python -m pytest tests -q
```

The default suite is local and offline. It includes mocked ingestion tests, SQL-shape and packaging tests, and unit tests of the notebook helper cells (`tests/test_notebooks.py`). Spark-based tests (financial scenarios, version selection, lineage) need `pyspark` and Java, and the insert-only Bronze history tests (`tests/test_bronze_trades_history.py`) also need `delta-spark`; without them they are **skipped**, and skipped tests are not passes. Delta MERGE/DELETE result columns, identity columns, the notebooks and the six-task run need Databricks. Live-network probes (Stooq, Yahoo) are excluded by default: `python -m pytest -o addopts="" tests/live -s`.

## Assumptions and tradeoffs

- USD only; a small hard-coded instrument universe instead of a reference table. No trade status, so cancellations are not supported.
- Lightweight rule-based NYSE calendar (`common/market_calendar.py`): no early closes or exceptional closures; New Year's Day uses `nearest_workday`, which wrongly treats 31 December as a holiday when 1 January is a Saturday; "today" is the runtime's date, not explicitly America/New_York; today's session is expected even before the close.
- Daily mark-to-market P&L proxy; no cost basis, fees or FX.
- Yahoo Finance instead of Stooq (see above); free source, no service guarantees.
- ETL runs as Python wheel tasks (testable, versioned); notebooks are used only to start the job and report results.
- Bronze trades keep every distinct raw record, so storage grows and Silver re-reads the full history each run.
- Gold is rebuilt in full from Silver each run (scoped deletes) rather than incrementally.
- The audit table does not store inserted/updated/deleted counts (only logged), so the report shows them as `NOT AVAILABLE`.

## Production improvements

An approved market-data feed with managed credentials, retries and monitoring; incremental Silver trade processing (Silver currently re-reads the full Bronze history); reference tables for instruments, counterparties and books; trade lifecycle (status, cancellations); storing insert/update/delete counts in the audit table; a production bundle target with a service-principal `run_as` and an unpaused schedule; alerting on failed or missing runs and data-quality thresholds; permissions, retention and cost controls; CI running the Spark and Delta tests; and an authoritative exchange-calendar library. See section 33 of `docs/trading_pipeline_solution_design.html` for the prioritized list.
