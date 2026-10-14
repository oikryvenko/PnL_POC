"""Generate interview-ready SDLC deliverables from one structured planning source.

Run from the repository root with:
    python docs/project-management/generate_deliverables.py

The DDL-backed data dictionary is read from database/tables at generation time.
"""

from __future__ import annotations

import csv
import html as html_lib
import re
from collections import defaultdict
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.worksheet.datavalidation import DataValidation
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Inches, Pt


ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
AS_OF = "2026-10-14"

NAVY = "16324F"
TEAL = "007C78"
BLUE = "2C6E9B"
ORANGE = "C8752D"
RED = "A94442"
INK = "233746"
MUTED = "647786"
PALE = "EAF1F4"
PALE_TEAL = "E1F1EF"
PALE_ORANGE = "FAF0E5"
PALE_RED = "F8E8E6"
WHITE = "FFFFFF"
GRID = "D7E0E5"


# Authoritative business and delivery plan. Workbook, decks, and Jira CSV all
# consume these same records; status values deliberately distinguish evidence
# from target work.
PLAN = {
    "title": "Trading Platform Project Plan",
    "purpose": "Databricks trading data processing and daily P&L proof of concept",
    "status_legend": ["IMPLEMENTED", "PARTIALLY IMPLEMENTED", "PLANNED", "TO BE CONFIRMED"],
    "business_requirements": [
        {"id": "BR-001", "description": "Ingest trade transactions from an agreed source.", "why": "Provide complete, traceable trade activity for positions and P&L.", "priority": "Must", "acceptance": "A valid input snapshot is recorded in Bronze with source and run lineage; invalid file structure fails before replacing the prior snapshot.", "dependencies": "Trade source contract; Unity Catalog Volume", "status": "IMPLEMENTED", "fr": ["FR-001", "FR-003", "FR-004", "FR-005", "FR-013"]},
        {"id": "BR-002", "description": "Ingest daily market prices for supported instruments.", "why": "Provide valuation marks and P&L inputs.", "priority": "Must", "acceptance": "Requested daily prices are recorded with source/date/currency lineage; missing expected sessions are visible and block incomplete Silver publication.", "dependencies": "Provider availability and ticker universe", "status": "IMPLEMENTED", "fr": ["FR-002", "FR-003", "FR-005", "FR-013"]},
        {"id": "BR-003", "description": "Maintain accurate trade records, including corrections and duplicates.", "why": "Prevent stale or ambiguous executions from contaminating financial results.", "priority": "Must", "acceptance": "One latest valid version per trade_id is used; invalid/ambiguous latest versions are excluded and explained; source-history limitations are disclosed.", "dependencies": "Trade version contract; immutable source history decision", "status": "PARTIALLY IMPLEMENTED", "fr": ["FR-004", "FR-005", "FR-009", "FR-013"]},
        {"id": "BR-004", "description": "Calculate historical and daily positions by date, book, counterparty, instrument, and currency.", "why": "Give trading users a reproducible inventory view.", "priority": "Must", "acceptance": "Daily signed net quantities reconcile to accepted trades and carry forward on expected sessions; source corrections update affected dates.", "dependencies": "Validated trades; supported trading calendar", "status": "IMPLEMENTED", "fr": ["FR-006", "FR-007", "FR-009"]},
        {"id": "BR-005", "description": "Calculate daily position valuation and daily P&L.", "why": "Provide transparent daily equity valuation and trading P&L.", "priority": "Must", "acceptance": "Gold contains daily valuation and decomposed daily P&L; missing marks/baselines remain NULL with breaks; quantity changes are not misattributed to price movement.", "dependencies": "Positions; validated price history; approved methodology", "status": "IMPLEMENTED", "fr": ["FR-007", "FR-008", "FR-009"]},
        {"id": "BR-006", "description": "Provide trading, P&L, and operational reporting.", "why": "Make outcomes and exceptions accessible to users.", "priority": "Should", "acceptance": "Read-only report/query surfaces expose run status, rejects, positions, P&L, and breaks; a dashboard is delivered only after user acceptance criteria are agreed.", "dependencies": "Reporting requirements; Gold datasets", "status": "PARTIALLY IMPLEMENTED", "fr": ["FR-010", "FR-011"]},
        {"id": "BR-007", "description": "Support operational monitoring, auditability, and error investigation.", "why": "Make failed and incomplete processing diagnosable.", "priority": "Must", "acceptance": "Every task attempt records status, timestamps, row counts, issue counts, and error details; valuation breaks and rejects retain lineage.", "dependencies": "Audit and reject tables; operations ownership", "status": "PARTIALLY IMPLEMENTED", "fr": ["FR-005", "FR-011", "FR-012", "FR-013"]},
        {"id": "BR-008", "description": "Automate repeatable daily processing with controlled reruns.", "why": "Reduce manual handling and support predictable delivery.", "priority": "Must", "acceptance": "A parameterized workflow executes the dependency graph; retry/rerun behavior is tested; production schedule remains disabled until readiness sign-off.", "dependencies": "Production environment, source SLA, support model", "status": "PARTIALLY IMPLEMENTED", "fr": ["FR-012", "FR-013"]},
    ],
    "functional_requirements": [
        {"id": "FR-001", "br": "BR-001", "description": "Read the 14-column trade CSV from the configured Unity Catalog Volume as raw strings.", "input": "CSV snapshot at configured Volume path", "logic": "Validate exact header, non-empty content, and structural integrity before write; remove identical in-batch rows.", "output": "cibc.bronze.trades snapshot with source_file, ingestion_timestamp, run_id", "rules": "All source columns remain raw strings in Bronze; different trade versions remain distinct.", "errors": "Fail before overwrite for bad header, empty file, corrupt row, or unreadable source; audit failure.", "acceptance": "A valid fixture replaces Bronze with distinct raw rows and correct lineage; invalid fixtures leave the previous snapshot unchanged.", "components": "scripts/generate_trades.py; bronze/ingest_trades.py; UC Volume", "status": "IMPLEMENTED", "stories": ["TMP-011"]},
        {"id": "FR-002", "br": "BR-002", "description": "Download daily unadjusted closing prices for requested tickers and inclusive date range.", "input": "Ticker/date parameters; yfinance public endpoint", "logic": "Retry transient provider exceptions up to 3 attempts; normalize response shapes; record close, USD, source API, ingestion timestamp, run id.", "output": "cibc.bronze.market_prices", "rules": "Default 10 configured symbols and seven-day lookback; no credentials currently used.", "errors": "Fail on empty expected response, missing ticker, malformed or ambiguous response; no synthetic price is invented.", "acceptance": "Fixture tests cover flat and MultiIndex payloads; missing expected sessions are surfaced.", "components": "bronze/ingest_prices.py; yfinance", "status": "IMPLEMENTED", "stories": ["TMP-013", "TMP-014"]},
        {"id": "FR-003", "br": "BR-001", "description": "Validate source schemas and convert raw values into typed Silver fields.", "input": "Bronze trades and prices", "logic": "Use try_cast into DATE, TIMESTAMP, INT, DECIMAL(18,6), and preserve raw values in rejects.", "output": "Typed Silver trades/prices and reject tables", "rules": "Financial amounts and quantities use DECIMAL; source IDs remain STRING.", "errors": "Uncastable/invalid records are quarantined; incomplete expected price sessions prevent Silver merge.", "acceptance": "Schema and cast tests verify typed values and preserve reason codes.", "components": "silver/process_trades.py; silver/process_prices.py", "status": "IMPLEMENTED", "stories": ["TMP-015", "TMP-016"]},
        {"id": "FR-004", "br": "BR-003", "description": "Select the highest trade version, then latest modified_timestamp, per trade_id.", "input": "Bronze trade snapshot", "logic": "Reject exact duplicates; reject conflicting ties; do not fall back when the latest version is invalid or ambiguous.", "output": "At most one accepted Silver row per trade_id", "rules": "Only trade IDs still in the new snapshot are removed when latest is unusable; absent IDs are not deleted from Silver.", "errors": "Ambiguity and unorderable version are quarantined with reason codes.", "acceptance": "Fixtures cover valid correction, exact duplicate, conflicting tie, invalid latest version, and lower version.", "components": "silver/process_trades.py; silver/trades_rejects", "status": "IMPLEMENTED", "stories": ["TMP-015", "TMP-012"]},
        {"id": "FR-005", "br": "BR-007", "description": "Apply trade and price data-quality rules and retain actionable rejects.", "input": "Raw trade and price records", "logic": "Validate identifiers, mapping, side/quantity, positive prices, currency, dates/calendar, settlement, version, and price key uniqueness.", "output": "Silver accepted datasets, rejects with reason(s), valuation breaks", "rules": "BUY is positive and SELL is negative; only USD and configured instruments are supported.", "errors": "Reject invalid rows; fail task where missing expected prices make the requested output incomplete.", "acceptance": "Each configured invalid fixture maps to expected reason(s); reject merge is idempotent.", "components": "Silver processors; common/rejects.py; gold/break_report.py", "status": "IMPLEMENTED", "stories": ["TMP-015", "TMP-016"]},
        {"id": "FR-006", "br": "BR-004", "description": "Calculate daily and cumulative signed positions at the defined Gold grain.", "input": "Silver accepted trades", "logic": "Aggregate daily buy/sell/net then cumulative SUM(quantity) over trading sessions by book/counterparty/instrument/currency.", "output": "cibc.gold.positions", "rules": "Position key is valuation_date + book_id + counterparty_id + instrument_id + currency; short quantities are allowed.", "errors": "Empty sources or duplicate price keys fail without publishing; source-scoped merge reconciles history.", "acceptance": "Daily/cumulative quantities match independent aggregation including no-trade days and short/flat positions.", "components": "gold/build_positions.py; gold/positions", "status": "IMPLEMENTED", "stories": ["TMP-017"]},
        {"id": "FR-007", "br": "BR-005", "description": "Value positions using same-day close in the same currency.", "input": "Gold positions; Silver prices", "logic": "market_value = closing_position_quantity * closing_price at exact valuation date and currency.", "output": "Gold position closing_price and market_value; missing-price break", "rules": "Missing close remains NULL; quantities are unaffected; a flat/no-activity position does not require a mark.", "errors": "Missing non-flat position marks create OPEN valuation breaks.", "acceptance": "Valuation matches independent Decimal calculation; missing close produces NULL and a break.", "components": "gold/build_positions.py; gold/valuation_breaks", "status": "IMPLEMENTED", "stories": ["TMP-017"]},
        {"id": "FR-008", "br": "BR-005", "description": "Calculate daily equity P&L separating opening-position price movement from trade-day economics.", "input": "Gold positions; Silver trades; Silver price history", "logic": "OpeningQty*(CurrentClose-PreviousClose) + (DailyNetQty*CurrentClose - SignedTradeConsideration); new positions with zero opening quantity use trade-day term only.", "output": "cibc.gold.daily_pnl with status and valuation inputs", "rules": "Latest prior close is the previous baseline; current close is exact date. Missing required current/baseline price leaves daily_pnl NULL.", "errors": "Create missing-current-price or missing-baseline break; never treat NULL as zero.", "acceptance": "Independent test examples match; activity quantity changes do not create false opening-position P&L.", "components": "gold/calculate_daily_pnl.py; gold/daily_pnl", "status": "IMPLEMENTED", "stories": ["TMP-018"]},
        {"id": "FR-009", "br": "BR-003", "description": "Retain historical accepted prices, reject history, audit/break history, and recompute affected Gold history after corrections.", "input": "Full Silver history and correction snapshot", "logic": "Prices/rejects MERGE by business key; Gold recomputes full Silver history; changed business values update scoped Gold rows.", "output": "Historical Silver/Gold datasets and lineage", "rules": "Bronze trades is only the latest snapshot; disappeared IDs are not purged from Silver; Gold delete scope cannot remove absent instruments/symbols.", "errors": "Require reconciliation and manual remediation for missing historical scope; expose limitations.", "acceptance": "Backdated correction updates affected dates; retention and deletion semantics are documented before production.", "components": "Bronze/Silver/Gold Delta tables; Delta history", "status": "PARTIALLY IMPLEMENTED", "stories": ["TMP-012", "TMP-017", "TMP-018"]},
        {"id": "FR-010", "br": "BR-006", "description": "Provide read-only operational and business reporting over run logs, rejects, positions, P&L, and breaks.", "input": "Gold/Silver Delta tables and Jobs API", "logic": "Existing notebook report and demo SQL show status, metrics, rejects, positions, P&L, breaks; proposed dashboard consumes curated Gold.", "output": "Notebook/SQL reporting now; dashboard dataset/view and dashboard proposed", "rules": "Do not claim Databricks SQL dashboard exists; current report is notebook-based.", "errors": "Report shows NOT AVAILABLE when audit/API data is absent rather than inventing metrics.", "acceptance": "Users reconcile report totals to tables; dashboard acceptance criteria and audience approved before build.", "components": "notebooks/02_pipeline_report.py; docs/demo_queries.sql; proposed dashboard", "status": "PARTIALLY IMPLEMENTED", "stories": ["TMP-019", "TMP-020"]},
        {"id": "FR-011", "br": "BR-007", "description": "Audit each task attempt with run/task identity, timing, status, counts, issues, and error details.", "input": "Databricks job/task context and module metrics", "logic": "MERGE STARTED, metrics, SUCCESS/FAILED by run_id + task_run_id.", "output": "cibc.gold.pipeline_run_log; valuation_breaks; Silver reject tables", "rules": "Audit persistence failure fails the task; audit currently omits insert/update/delete/rejects_written counters.", "errors": "Capture exception type/message; if audit write itself fails log secondary exception.", "acceptance": "Success and failure paths produce expected audit record; sensitive exception content reviewed before prod.", "components": "trade_pipeline/audit.py; break_report.py; report notebook", "status": "PARTIALLY IMPLEMENTED", "stories": ["TMP-019", "TMP-024"]},
        {"id": "FR-012", "br": "BR-008", "description": "Orchestrate six parameterized tasks with parallel source branches and dependency joins.", "input": "Databricks Job parameters", "logic": "ingest_prices→process_prices and ingest_trades→process_trades run independently; build_positions joins both; calculate_daily_pnl follows.", "output": "Databricks Workflow run", "rules": "max_concurrent_runs=1; weekday 18:00 America/New_York schedule is PAUSED; dev target only.", "errors": "Workflow task failure blocks downstream; rerun through Jobs/CLI/notebook after investigation.", "acceptance": "Bundle validation and a successful manual run confirm all six task dependencies and parameters.", "components": "jobs/resources/trade_pipeline.job.yml; databricks.yml; notebooks/01_run_pipeline.py", "status": "PARTIALLY IMPLEMENTED", "stories": ["TMP-009", "TMP-010", "TMP-023"]},
        {"id": "FR-013", "br": "BR-008", "description": "Make reruns deterministic and recoverable for a fixed source snapshot and date range.", "input": "Same trade snapshot, ticker/date request, run parameters", "logic": "Trade Bronze overwrite; price/Silver/Gold conditional MERGEs; reject/break writes are idempotent per identity/run.", "output": "Stable business keys/values and separate run audit history", "rules": "A retry gets task attempt identity; full-history recompute is used; no partial input may delete outside source scope.", "errors": "Fail-fast at bad input; inspect audit/reject/breaks, repair source, rerun; exact operational recovery time TBD.", "acceptance": "Two complete identical end-to-end runs preserve business output and business keys; audit history records both.", "components": "Ingestion/processing modules; Databricks Workflow; audit", "status": "PARTIALLY IMPLEMENTED", "stories": ["TMP-016", "TMP-022", "TMP-024"]},
    ],
    "nfrs": [
        {"id": "NFR-01", "name": "Performance", "target": "PROPOSED: process the documented 1,200-trade / 10-symbol demonstration workload within 15 minutes end-to-end, excluding provider outages.", "status": "TO BE CONFIRMED", "evidence": "No measured run duration/SLA recorded in docs/test_results.md."},
        {"id": "NFR-02", "name": "Reliability", "target": "PROPOSED: >=99% successful scheduled runs per calendar month after production support and alerting are established.", "status": "TO BE CONFIRMED", "evidence": "Only a dev job is configured; schedule paused."},
        {"id": "NFR-03", "name": "Idempotency", "target": "PROPOSED: two runs over identical inputs produce identical business keys and financial values; only run-level audit/break history may grow.", "status": "PARTIALLY IMPLEMENTED", "evidence": "Unit tests cover scoped writes; documented second Databricks run was queued, not verified."},
        {"id": "NFR-04", "name": "Security", "target": "PROPOSED: job service principal has least privilege on cibc schemas and input Volume; secrets never stored in source control; access reviewed before non-dev deploy.", "status": "TO BE CONFIRMED", "evidence": "Grant template exists; principal/environment are placeholders."},
        {"id": "NFR-05", "name": "Maintainability", "target": "PROPOSED: source modules remain separated by Bronze/Silver/Gold; new business rules have unit tests and documented owners.", "status": "PARTIALLY IMPLEMENTED", "evidence": "Layered Python package and tests exist; CI/ownership not evidenced."},
        {"id": "NFR-06", "name": "Testability", "target": "PROPOSED: deterministic unit tests for rules; Spark integration tests for MERGE/Delta behavior; UAT evidence for financial outputs.", "status": "PARTIALLY IMPLEMENTED", "evidence": "pytest tests exist; PySpark/Java/Delta runtime availability is not confirmed locally."},
        {"id": "NFR-07", "name": "Scalability", "target": "PROPOSED: benchmark 100,000 trades and 50 symbols; confirm runtime and compute cost before adopting an incremental design.", "status": "TO BE CONFIRMED", "evidence": "Full-history recompute is documented as a prototype limitation; no benchmark."},
        {"id": "NFR-08", "name": "Observability", "target": "PROPOSED: detect failed run and notify support within 5 minutes; audit every task attempt; reconcile expected task count.", "status": "PARTIALLY IMPLEMENTED", "evidence": "Delta audit/reject/break data and report notebook; no automated alerting."},
        {"id": "NFR-09", "name": "Deployability", "target": "PROPOSED: repeatable Bundle validate/deploy in isolated dev/test/prod targets with reviewable configuration and smoke test.", "status": "PARTIALLY IMPLEMENTED", "evidence": "Databricks Asset Bundle has dev target and deploy script; no CI or non-dev target."},
        {"id": "NFR-10", "name": "Retention and recovery", "target": "TBD by data owner: approve retention, RTO, RPO, and source replay window before production. No numeric policy is assumed.", "status": "TO BE CONFIRMED", "evidence": "Not specified in repository."},
    ],
    "sources": [
        {"id": "SRC-TRADE", "name": "Trade transactions", "status": "IMPLEMENTED (synthetic prototype source)", "system": "scripts/generate_trades.py; no upstream trade system integration evidenced", "format": "CSV, 14 named columns; raw values landed as STRING", "location": "Local data/dirty_trades.csv then /Volumes/cibc/bronze/trade_files/dirty_trades.csv", "frequency": "Full snapshot per Job run; weekday schedule configured but paused; file delivery SLA TBD", "volume": "Checked-in sample has 1,200 generated rows per docs/test_results.md; production volumes TBD", "keys": "trade_id is logical entity; version + modified_timestamp select the latest; Bronze has no enforced PK", "versioning": "Highest version, then latest modified_timestamp; invalid/ambiguous latest is not replaced by older version", "quality": "Nonblank IDs; configured instrument/symbol match; BUY/SELL; nonzero quantity; positive price; USD; valid timestamps/dates; supported NYSE session; settlement next session; version > 0", "ingestion": "Validate exact header/nonempty/structure; dedupe identical in-batch rows; INSERT OVERWRITE Bronze snapshot", "failure": "Fail before overwrite; audit failure; Silver quarantines business-invalid rows", "open_items": "Real source owner, legal entity/system, delivery format contract, completeness watermark, file naming, correction/deletion semantics, volume/SLA, retention and access classification."},
        {"id": "SRC-PRICE", "name": "Daily market prices", "status": "IMPLEMENTED (prototype provider; not approved for banking production)", "system": "Yahoo Finance via yfinance.download; provider rights/approval unverified", "format": "Python provider response normalized to symbol, price_date, close, currency", "location": "Provider response to cibc.bronze.market_prices Delta", "frequency": "Daily interval; seven-day default lookback; schedule is paused", "volume": "10 hard-coded default tickers; volume/limits and broader universe TBD", "keys": "symbol + price_date (logical business key)", "versioning": "Latest ingestion_timestamp then run_id; conflicting tied payloads rejected", "quality": "Expected NYSE sessions; non-future date; finite positive close; USD; unique symbol/date; completeness required", "ingestion": "Retry up to three attempts; unadjusted Close; Bronze conditional MERGE", "failure": "No data/omitted symbol/expected missing session fails; no stale close substituted for position valuation", "open_items": "Market-data owner/provider approval and license, adjustment/close definition, correction policy, availability SLA, symbol mapping, market calendar and missing-data escalation."},
        {"id": "SRC-REF", "name": "Minimal reference data", "status": "PARTIALLY IMPLEMENTED (in-code mappings only)", "system": "trade_pipeline/config.py INSTRUMENT_ID_TO_SYMBOL; books/counterparties arrive as input strings", "format": "Python dictionary and input attributes; no reference Delta tables", "location": "Code configuration; no independent reference-data store", "frequency": "Release-time mapping update; business owner/update cadence TBD", "volume": "10 configured instrument-symbol pairs; book/counterparty cardinalities unknown", "keys": "instrument_id; symbol mapping; book_id and counterparty_id are opaque strings", "versioning": "No effective-dated reference history", "quality": "Configured instrument/symbol match; book/counterparty nonblank; USD-only currency", "ingestion": "No separate pipeline; Silver validates against configured mapping", "failure": "Unknown/mismatched instrument is rejected", "open_items": "Reference-data ownership, effective dating, books/counterparties validation, and need for persisted mappings must be confirmed."},
    ],
    "epics": [
        {"id": "TMP-EP01", "name": "Project Initiation and Business Analysis", "priority": "High"},
        {"id": "TMP-EP02", "name": "Requirements and Acceptance Criteria", "priority": "High"},
        {"id": "TMP-EP03", "name": "Solution Architecture and Technical Design", "priority": "High"},
        {"id": "TMP-EP04", "name": "Data Sourcing and Data Modelling", "priority": "High"},
        {"id": "TMP-EP05", "name": "Platform Setup and CI/CD", "priority": "High"},
        {"id": "TMP-EP06", "name": "Trade Data Ingestion", "priority": "High"},
        {"id": "TMP-EP07", "name": "Market Price Ingestion", "priority": "High"},
        {"id": "TMP-EP08", "name": "Silver Data Processing", "priority": "High"},
        {"id": "TMP-EP09", "name": "Position Calculation", "priority": "High"},
        {"id": "TMP-EP10", "name": "P&L Calculation", "priority": "High"},
        {"id": "TMP-EP11", "name": "Reporting and Dashboard", "priority": "Medium"},
        {"id": "TMP-EP12", "name": "Testing and Reconciliation", "priority": "High"},
        {"id": "TMP-EP13", "name": "Deployment, Monitoring and Handover", "priority": "High"},
    ],
    "stories": [
        {"id": "TMP-001", "epic": "TMP-EP01", "title": "Baseline current implementation and confirm scope", "objective": "Record verified capabilities, limitations, stakeholders, assumptions, and prototype-versus-production boundaries.", "description": "Use repository modules, DDL, tests, Bundle, and dated run evidence to approve the first scope baseline. Do not describe proposed controls as deployed.", "criteria": ["Inventory covers all six workflow tasks, 10 physical tables, two notebooks, Bundle targets, and test evidence.", "Every capability is labelled implemented, partial, planned, or to be confirmed with a repository evidence reference.", "Business owner confirms prototype scope and named decision owners before Sprint 2 exit."], "points": 2, "priority": "High", "status": "PARTIALLY IMPLEMENTED", "sprint": "Sprint 1", "br": ["BR-001", "BR-008"], "fr": ["FR-012"], "depends": [], "inputs": "Repository; product/operations contacts", "outputs": "Approved scope and implementation inventory", "subtasks": ["Inventory code, DDL, tests, docs, and deployment assets", "Review prototype boundaries and obtain scope decisions"]},
        {"id": "TMP-002", "epic": "TMP-EP02", "title": "Approve business and functional requirements", "objective": "Turn current capabilities and business needs into testable BR/FR acceptance criteria.", "description": "Confirm user roles, daily business outputs, timing, correction semantics, report needs, and sign-off owners. Link BR-001..008 to FR-001..013.", "criteria": ["All eight business requirements have an owner, priority, acceptance evidence, and dependency.", "All 13 functional requirements identify input, processing, output, business rule, and failure behavior.", "Product owner records decisions or explicit TBDs; no unsupported policy is silently assumed."], "points": 3, "priority": "High", "status": "PLANNED", "sprint": "Sprint 1", "br": ["BR-001", "BR-002", "BR-003", "BR-004", "BR-005", "BR-006", "BR-007", "BR-008"], "fr": ["FR-001", "FR-002", "FR-003", "FR-004", "FR-005", "FR-006", "FR-007", "FR-008", "FR-009", "FR-010", "FR-011", "FR-012", "FR-013"], "depends": ["TMP-001"], "inputs": "Implementation inventory; business/operations interviews", "outputs": "Signed requirements and acceptance criteria", "subtasks": ["Interview trading, operations, and technology stakeholders", "Review and approve BR/FR traceability and decisions"]},
        {"id": "TMP-003", "epic": "TMP-EP02", "title": "Set measurable non-functional and operational targets", "objective": "Agree performance, availability, support, security, retention, RTO/RPO, and recovery targets.", "description": "Use proposed thresholds as discussion points only. Confirm owner, measurement window, and evidence for each target.", "criteria": ["NFR-01..10 each has a confirmed target or named owner and due date for TBD.", "Performance baseline includes representative row counts, symbols, runtime, and cost.", "Retention, access classification, RTO/RPO, support hours, and alert response are approved or explicitly excluded from prototype."], "points": 2, "priority": "High", "status": "PLANNED", "sprint": "Sprint 1", "br": ["BR-007", "BR-008"], "fr": ["FR-011", "FR-012", "FR-013"], "depends": ["TMP-002"], "inputs": "Business/technology policy owners; proposed NFRs", "outputs": "Approved NFR baseline", "subtasks": ["Measure demo workload and propose service thresholds", "Confirm security, retention, recovery, and support policies"]},
        {"id": "TMP-004", "epic": "TMP-EP03", "title": "Document and validate existing solution architecture", "objective": "Establish an evidence-backed architecture baseline and component responsibilities.", "description": "Document Unity Catalog, Delta layers, Python wheel, Job, notebooks, audit/rejects, and current deployment path.", "criteria": ["Current architecture matches databricks.yml, job YAML, source modules, and DDL.", "Trade/price parallel branches and join dependencies are accurate.", "Current provider, paused schedule, dev-only target, and notebook reporting are labelled as implemented/current limitations."], "points": 3, "priority": "High", "status": "IMPLEMENTED", "sprint": "", "br": ["BR-001", "BR-002", "BR-004", "BR-005", "BR-007", "BR-008"], "fr": ["FR-006", "FR-008", "FR-011", "FR-012"], "depends": [], "inputs": "Repository implementation", "outputs": "Current-state architecture", "subtasks": ["Trace workflow and component ownership from code/config", "Review target-state gap and architecture decisions"]},
        {"id": "TMP-005", "epic": "TMP-EP03", "title": "Approve target security and environment architecture", "objective": "Define deployable dev/test/prod boundaries and least-privilege runtime identity.", "description": "Design only what is supported by actual workspace controls; confirm environment availability, principals, grants, networking, and secrets approach.", "criteria": ["Environment promotion and job identity are approved by platform owner.", "Unity Catalog grants cover required schemas and Volume only; no secrets are committed.", "Architecture labels all unprovisioned targets and controls as proposed."], "points": 3, "priority": "High", "status": "PLANNED", "sprint": "Sprint 1", "br": ["BR-007", "BR-008"], "fr": ["FR-011", "FR-012"], "depends": ["TMP-002", "TMP-003", "TMP-004"], "inputs": "Workspace/platform/security owner decisions", "outputs": "Approved environment and access design", "subtasks": ["Confirm target workspaces, principals, and data access", "Document secret, grant, and deployment configuration controls"]},
        {"id": "TMP-006", "epic": "TMP-EP04", "title": "Confirm trade source contract and snapshot semantics", "objective": "Make source ownership, completeness, corrections, and deletion semantics explicit.", "description": "Current input is synthetic; define the future producer contract and safe transition from full snapshot to an authoritative feed.", "criteria": ["Trade source owner, file schema, delivery cutoff, completeness marker, and correction/deletion semantics are recorded.", "A missing trade in a later snapshot is not treated as a cancellation without explicit business rule.", "Sample volume, retention, privacy classification, and replay window are approved or tracked as TBD."], "points": 2, "priority": "High", "status": "PARTIALLY IMPLEMENTED", "sprint": "", "br": ["BR-001", "BR-003"], "fr": ["FR-001", "FR-004", "FR-009"], "depends": [], "inputs": "Synthetic CSV; trade owner", "outputs": "Trade source specification", "subtasks": ["Confirm 14-column schema and required fields", "Agree source snapshot, correction, cancellation, and replay rules"]},
        {"id": "TMP-007", "epic": "TMP-EP04", "title": "Select and approve market-data source", "objective": "Resolve whether Yahoo Finance is acceptable for the prototype and choose a licensed production source.", "description": "Confirm price type, currency, adjustments, symbol universe, rights, correction policy, availability, and missing-price escalation; synthetic fixtures are acceptable for deterministic tests only.", "criteria": ["Provider and business owner approve data rights and use case before production.", "Price-date/price-type/currency contract and correction policy are documented.", "Daily availability and expected-session completeness thresholds have named owners."], "points": 3, "priority": "High", "status": "PLANNED", "sprint": "Sprint 2", "br": ["BR-002", "BR-005"], "fr": ["FR-002", "FR-005", "FR-007", "FR-008"], "depends": ["TMP-002"], "inputs": "Current yfinance adapter; market-data owner", "outputs": "Approved source decision and interface contract", "subtasks": ["Assess provider options, licensing, and connection method", "Approve close definition, currency, corrections, and session coverage"]},
        {"id": "TMP-008", "epic": "TMP-EP04", "title": "Approve physical data dictionary and retention model", "objective": "Review all current table grains, keys, financial types, lineage, and future retention needs.", "description": "Use the 10 physical DDL files as current-state truth; identify no enforced primary keys and no dedicated reference/reporting tables.", "criteria": ["Data dictionary lists every column and exact DDL type/nullability for all 10 tables.", "Business owner approves table grain and logical key assumptions.", "Partitioning/clustering remains absent unless measured query patterns justify it.", "Retention and Delta cleanup policy are approved before any production purge/optimization."], "points": 2, "priority": "High", "status": "PARTIALLY IMPLEMENTED", "sprint": "Sprint 2", "br": ["BR-003", "BR-004", "BR-005", "BR-007"], "fr": ["FR-003", "FR-006", "FR-007", "FR-008", "FR-009", "FR-011"], "depends": ["TMP-006", "TMP-007"], "inputs": "database/tables DDL; provider/trade contracts", "outputs": "Approved dictionary and data-retention decisions", "subtasks": ["Validate table grains, keys, constraints, and lineage", "Confirm financial types and retention/optimization choices"]},
        {"id": "TMP-009", "epic": "TMP-EP05", "title": "Maintain reproducible Databricks Asset Bundle development deployment", "objective": "Validate current development Bundle build and deploy path.", "description": "Bundle builds a wheel and deploys the six-task job/notebooks to the dev target; schedule remains paused by configuration.", "criteria": ["Bundle validation succeeds with the documented dev target and CLI profile.", "Wheel contains all six entry points and deploy uses repository source.", "Manual smoke run evidence is attached; no production readiness is inferred."], "points": 3, "priority": "High", "status": "IMPLEMENTED", "sprint": "", "br": ["BR-008"], "fr": ["FR-012"], "depends": [], "inputs": "databricks.yml; job resource; scripts/deploy.sh", "outputs": "Validated dev deployment procedure", "subtasks": ["Build wheel and validate Bundle configuration", "Deploy dev bundle and capture run evidence"]},
        {"id": "TMP-010", "epic": "TMP-EP05", "title": "Add reviewed CI checks and controlled deployment promotion", "objective": "Automate deterministic tests, Bundle validation, and reviewed artifact promotion.", "description": "Create CI using existing pytest/package/Bundle checks; add non-dev targets only after platform provisioning and identity approval.", "criteria": ["Pull request CI runs deterministic local tests and Bundle validation without credentials in logs.", "Deployment artifact/version is traceable to reviewed source revision.", "Promotion cannot unpause a schedule or deploy to an unapproved target without explicit approval."], "points": 5, "priority": "High", "status": "PLANNED", "sprint": "Sprint 3", "br": ["BR-007", "BR-008"], "fr": ["FR-012", "FR-013"], "depends": ["TMP-005", "TMP-009"], "inputs": "Approved environment/security design; current Bundle", "outputs": "CI workflow and controlled deploy procedure", "subtasks": ["Add package/test/Bundle validation pipeline", "Define artifact promotion, approvals, and rollback evidence"]},
        {"id": "TMP-011", "epic": "TMP-EP06", "title": "Ingest synthetic trade snapshot into Bronze", "objective": "Load structurally valid trade snapshots with deterministic raw lineage.", "description": "Current implementation validates the 14-field CSV and atomically overwrites Bronze after input checks.", "criteria": ["Exact schema, empty-file, and malformed-row cases are tested.", "Identical in-batch records are removed; versions of a trade remain present.", "Invalid file never overwrites existing Bronze; valid rerun yields same business rows."], "points": 5, "priority": "High", "status": "IMPLEMENTED", "sprint": "", "br": ["BR-001", "BR-003"], "fr": ["FR-001", "FR-013"], "depends": ["TMP-006"], "inputs": "CSV snapshot in UC Volume", "outputs": "cibc.bronze.trades", "subtasks": ["Validate file schema and structural integrity", "Write and audit distinct full-snapshot Bronze rows"]},
        {"id": "TMP-012", "epic": "TMP-EP06", "title": "Resolve trade snapshot disappearance and historical correction controls", "objective": "Prevent a source snapshot replacement from leaving stale or silently deleting trades.", "description": "Current Bronze retains only latest snapshot and Silver does not remove trade IDs absent from a later file. Choose event/history storage or safe snapshot reconciliation.", "criteria": ["Trade owner approves semantics for missing trade IDs, cancellation, and correction.", "A two-snapshot test proves accepted Silver matches the agreed authoritative source state.", "Backdated corrections are traceable and recompute the intended history without unrelated deletes."], "points": 5, "priority": "High", "status": "PLANNED", "sprint": "Sprint 3", "br": ["BR-003", "BR-004", "BR-005"], "fr": ["FR-004", "FR-009", "FR-013"], "depends": ["TMP-006", "TMP-011"], "inputs": "Approved trade source semantics; current Bronze/Silver behavior", "outputs": "Versioned history/reconciliation design and tested implementation", "subtasks": ["Select append-only event or snapshot reconciliation approach", "Implement and test disappearance/cancellation/correction cases"]},
        {"id": "TMP-013", "epic": "TMP-EP07", "title": "Ingest and retain daily market-price history", "objective": "Load supported daily close records with source identity and repeatable keys.", "description": "Current adapter calls Yahoo Finance, stores unadjusted closes, retries transient failures, and merges Bronze on symbol/date.", "criteria": ["Default symbols, date boundaries, provider response variants, NaN, duplicate keys, and retry errors have deterministic tests.", "Price rows carry source, currency, ingestion timestamp, and run lineage.", "Provider limitations and data rights are explicit in the source contract."], "points": 5, "priority": "High", "status": "IMPLEMENTED", "sprint": "", "br": ["BR-002"], "fr": ["FR-002", "FR-013"], "depends": ["TMP-007"], "inputs": "Daily price provider response", "outputs": "cibc.bronze.market_prices", "subtasks": ["Normalize daily provider payload and date range", "Merge price history and audit source outcomes"]},
        {"id": "TMP-014", "epic": "TMP-EP07", "title": "Harden price source decision and completeness exception process", "objective": "Make price completeness and manual resolution operationally actionable.", "description": "Use provider choice from TMP-007; define approved synthetic fixtures for test environments and operational handling for late/missing prices.", "criteria": ["Missing expected session is tied to symbol/date and owner; no stale price is used for position valuation.", "Test fixtures are deterministic and cannot be mistaken for production source data.", "Approved provider/credentials, rate limits, correction policy, and retries are configured outside source code where required."], "points": 3, "priority": "High", "status": "PLANNED", "sprint": "Sprint 2", "br": ["BR-002", "BR-005", "BR-007"], "fr": ["FR-002", "FR-005", "FR-007", "FR-013"], "depends": ["TMP-007", "TMP-013"], "inputs": "Approved provider decision; current completeness checks", "outputs": "Price-source controls and exception runbook", "subtasks": ["Define deterministic test source and provider configuration", "Document missing-price triage, correction, and retry behavior"]},
        {"id": "TMP-015", "epic": "TMP-EP08", "title": "Validate Silver trades and prices with reject history", "objective": "Publish typed records only after business validation and deterministic version selection.", "description": "Current code performs typed conversion, configured business checks, deduplication/versioning, conditional merge, and reject-history writes.", "criteria": ["Trade and price fixtures cover configured rules and reason codes.", "Silver trade business key is trade_id; price key is symbol + price_date.", "Reject writes are idempotent and preserve raw source fields/lineage; missing expected price blocks Silver update."], "points": 5, "priority": "High", "status": "IMPLEMENTED", "sprint": "", "br": ["BR-003", "BR-007"], "fr": ["FR-003", "FR-004", "FR-005", "FR-013"], "depends": ["TMP-011", "TMP-013"], "inputs": "Bronze trades and prices", "outputs": "Silver validated tables and rejects", "subtasks": ["Cast and validate typed trade/price values", "Apply dedupe/version rules and idempotent reject MERGE"]},
        {"id": "TMP-016", "epic": "TMP-EP08", "title": "Run Spark/Delta integration and end-to-end rerun validation", "objective": "Prove Delta writes, constraints, lineage, recovery, and same-input idempotency in a supported runtime.", "description": "Local Spark availability is not evidenced; docs/test_results.md records a successful dated pipeline run but its same-parameter second run was queued.", "criteria": ["Spark/Delta integration suite runs in a supported CI or Databricks test environment and results are retained.", "A second complete run with the same inputs verifies stable business keys/values and expected audit history growth.", "Failure injection confirms no unsafe publication and a documented rerun recovers the task."], "points": 2, "priority": "High", "status": "PLANNED", "sprint": "Sprint 2", "br": ["BR-003", "BR-007", "BR-008"], "fr": ["FR-003", "FR-005", "FR-009", "FR-013"], "depends": ["TMP-015"], "inputs": "Supported Spark/Delta environment; current test suite", "outputs": "Integration, idempotency, and recovery evidence", "subtasks": ["Execute Spark/Delta tests in a provisioned environment", "Run same-input job twice and compare business outputs"]},
        {"id": "TMP-017", "epic": "TMP-EP09", "title": "Build cumulative daily positions and exact-date valuation", "objective": "Publish historical positions at the agreed book/counterparty/instrument/date grain.", "description": "Current implementation aggregates signed trade quantities, carries positions across trading sessions, and joins same-day same-currency marks.", "criteria": ["Position grain and key match the approved dictionary.", "Daily buys, sells, net, closing quantity, close, and market value reconcile to independent calculations.", "Missing same-day price leaves price/value NULL and creates a break without changing quantity."], "points": 5, "priority": "High", "status": "IMPLEMENTED", "sprint": "", "br": ["BR-004", "BR-005"], "fr": ["FR-006", "FR-007", "FR-009"], "depends": ["TMP-015"], "inputs": "Silver trades and prices", "outputs": "cibc.gold.positions; position valuation breaks", "subtasks": ["Aggregate daily flows and cumulative positions", "Value at exact-date close and reconcile history"]},
        {"id": "TMP-018", "epic": "TMP-EP10", "title": "Calculate daily cash-equity mark-to-market P&L", "objective": "Separate opening position market move from trade-day economic P&L.", "description": "Current Gold formula uses prior close for opening inventory and signed trade consideration for intraday activity; cumulative proxy is not true unrealized P&L.", "criteria": ["Daily output grain is valuation_date + book_id + symbol + currency.", "Price movement and trade-day terms reconcile independently, including sells, new/flat positions, prior-close baseline, and missing prices.", "unrealized_pnl_proxy is clearly labelled as a cumulative proxy and is not presented as true unrealized P&L."], "points": 5, "priority": "High", "status": "IMPLEMENTED", "sprint": "", "br": ["BR-005"], "fr": ["FR-007", "FR-008", "FR-009"], "depends": ["TMP-017"], "inputs": "Gold positions; Silver trades/prices", "outputs": "cibc.gold.daily_pnl; P&L breaks", "subtasks": ["Calculate opening inventory price move and trade-day term", "Reconcile output and label simplified P&L limitations"]},
        {"id": "TMP-019", "epic": "TMP-EP11", "title": "Provide notebook and SQL operational reporting", "objective": "Expose current job run, audit metrics, rejections, positions, P&L, and valuation breaks.", "description": "A run-trigger notebook, read-only report notebook, and demo SQL exist. No deployed dashboard is evidenced.", "criteria": ["Run report identifies audit-derived status and clearly marks unavailable counts.", "Notebook output is read-only and totals reconcile to source tables.", "Report states that scheduled-run completeness is not evaluated and dashboard is not implemented."], "points": 3, "priority": "Medium", "status": "IMPLEMENTED", "sprint": "", "br": ["BR-006", "BR-007", "BR-008"], "fr": ["FR-010", "FR-011", "FR-012"], "depends": ["TMP-018"], "inputs": "Gold/Silver tables and Jobs API", "outputs": "Run-control/report notebooks and SQL queries", "subtasks": ["Trigger and monitor controlled Job execution", "Report audit, rejects, Gold outcomes, and unavailable metrics"]},
        {"id": "TMP-020", "epic": "TMP-EP11", "title": "Deliver user-accepted trading and P&L dashboard MVP", "objective": "Present agreed Gold reporting datasets in an approved Databricks SQL dashboard or chosen BI tool.", "description": "Dashboard technology, user roles, filters, reconciliation views, and access model remain to be confirmed; this is proposed work, not current functionality.", "criteria": ["Product owner signs off audience, filters, date/book/symbol dimensions, and exception drill-through.", "Dashboard totals reconcile to Gold daily_pnl and positions for agreed test dates.", "Dashboard access is least privilege and deployment is repeatable; no dashboard is reported as live before sign-off."], "points": 5, "priority": "Medium", "status": "PLANNED", "sprint": "Sprint 4", "br": ["BR-006"], "fr": ["FR-010"], "depends": ["TMP-002", "TMP-008", "TMP-018", "TMP-019"], "inputs": "Approved reporting requirements; Gold datasets", "outputs": "Dashboard MVP and validation evidence", "subtasks": ["Agree dashboard user journeys and query model", "Build, permission, and reconcile dashboard views"]},
        {"id": "TMP-021", "epic": "TMP-EP12", "title": "Maintain deterministic unit and transformation tests", "objective": "Protect module contracts and calculation rules with repeatable test cases.", "description": "Tests cover source parsing, transformations, DDL/query shape, packaging, notebook helpers, and financial examples; Spark-dependent cases need a compatible runtime. Three focused tests currently fail because they reference removed Silver *_constraints.sql files; current checks are consolidated into trades.sql and market_prices.sql.", "criteria": ["Business-rule tests include correction, duplicate, calendar, missing price, short position, and P&L decomposition cases.", "CI reports skips separately from passes and does not treat unavailable Spark tests as verified.", "Replace stale test references with the consolidated Silver DDL and verify its CHECK constraints.", "New failures are triaged against the baseline; no unrelated failures are silently waived."], "points": 3, "priority": "High", "status": "PARTIALLY IMPLEMENTED", "sprint": "", "br": ["BR-001", "BR-002", "BR-003", "BR-004", "BR-005", "BR-007"], "fr": ["FR-001", "FR-002", "FR-003", "FR-004", "FR-005", "FR-006", "FR-007", "FR-008"], "depends": [], "inputs": "pytest suite; supported Python/Spark runtime", "outputs": "Deterministic test evidence", "subtasks": ["Run and triage current unit/static test suite", "Update stale DDL test paths and maintain focused rule tests"]},
        {"id": "TMP-022", "epic": "TMP-EP12", "title": "Complete independent position/P&L reconciliation and UAT", "objective": "Close the gap between pipeline output and business-approved calculations.", "description": "The 2026-10-11 run has independent SQL reconciliation with zero reported mismatches; same-input rerun remained queued in the report.", "criteria": ["Trade-to-position quantities reconcile by date/book/counterparty/instrument.", "Position market value and daily P&L reconcile using approved close/baseline/trade cash-flow methodology.", "Business owner signs UAT evidence for representative long, short, flat, correction, and missing-price cases.", "A complete same-input rerun has recorded evidence; do not rely only on unit tests."], "points": 2, "priority": "High", "status": "PARTIALLY IMPLEMENTED", "sprint": "Sprint 4", "br": ["BR-003", "BR-004", "BR-005", "BR-006"], "fr": ["FR-006", "FR-007", "FR-008", "FR-009", "FR-010", "FR-013"], "depends": ["TMP-016", "TMP-018"], "inputs": "Gold outputs; independent SQL; business test cases", "outputs": "Reconciliation and UAT sign-off", "subtasks": ["Re-run independent position and P&L reconciliation", "Obtain business acceptance and capture rerun evidence"]},
        {"id": "TMP-023", "epic": "TMP-EP13", "title": "Operate the dev workflow with controlled scheduling", "objective": "Use existing Asset Bundle workflow safely and keep schedule gated on readiness.", "description": "Current Job allows one concurrent run, runs six tasks, and has a weekday 18:00 America/New_York schedule configured as PAUSED.", "criteria": ["Manual run and failure states are visible in Job UI and audit tables.", "Schedule remains paused until source owner, support contact, permissions, runbook, and alert path are approved.", "Any future unpause/change is reviewed and recorded with owner and rollback path."], "points": 3, "priority": "High", "status": "PARTIALLY IMPLEMENTED", "sprint": "", "br": ["BR-007", "BR-008"], "fr": ["FR-011", "FR-012", "FR-013"], "depends": ["TMP-009"], "inputs": "Deployed dev Job; operational readiness decisions", "outputs": "Controlled dev operating procedure", "subtasks": ["Verify run parameters, concurrency, retries, and task dependencies", "Document schedule gate and manual recovery controls"]},
        {"id": "TMP-024", "epic": "TMP-EP13", "title": "Create support runbook, alerting, and handover", "objective": "Prepare an operational handoff with actionable diagnosis and escalation.", "description": "Define monitoring on Job/audit/reject/break signals; alerting and support ownership are not implemented in the repo.", "criteria": ["Runbook describes failed task diagnosis, source repair, safe rerun, reconciliation, and escalation owner.", "Alert criteria and response targets are approved and tested; proposed five-minute detection target remains proposed until measured.", "Handover includes architecture, data contracts, access, known limitations, deployment procedure, and open risks."], "points": 3, "priority": "High", "status": "PLANNED", "sprint": "Sprint 4", "br": ["BR-007", "BR-008"], "fr": ["FR-011", "FR-012", "FR-013"], "depends": ["TMP-003", "TMP-019", "TMP-022", "TMP-023"], "inputs": "Approved NFRs; report; reconciliation; support contacts", "outputs": "Runbook, notification path, handover sign-off", "subtasks": ["Define alert rules, support rota, and escalation process", "Write recovery/runbook and conduct handover walkthrough"]},
    ],
    "sprints": [
        {"name": "Sprint 1", "weeks": "Weeks 1-2", "goal": "Lock project scope, requirement acceptance, measurable NFRs, and target access design.", "stories": ["TMP-001", "TMP-002", "TMP-003", "TMP-005"], "dependencies": "Kickoff and timely access to business, market-data, and platform decision owners.", "deliverables": "Approved scope, BR/FR baseline, NFR decision log, target security/environment design.", "demo": "Trace one trade and one price source through implemented modules and show signed acceptance criteria.", "exit": "No critical source, access, calculation, or report requirement remains ownerless."},
        {"name": "Sprint 2", "weeks": "Weeks 3-4", "goal": "Resolve market-data decisions, dictionary, price exception handling, and runtime test strategy.", "stories": ["TMP-007", "TMP-008", "TMP-014", "TMP-016"], "dependencies": "Sprint 1 approved scope; access to market-data owner; supported Databricks/Spark test environment.", "deliverables": "Source decision, DDL-backed dictionary signoff, missing-price procedure, integration/rerun evidence.", "demo": "Show a missing-session case and typed data/reject outcomes; demonstrate integration suite execution.", "exit": "Price feed is approved or explicitly remains prototype-only; unresolved runtime/test access has an owner and date."},
        {"name": "Sprint 3", "weeks": "Weeks 5-6", "goal": "Improve deployment controls and resolve trade snapshot history/correction semantics.", "stories": ["TMP-010", "TMP-012"], "dependencies": "Approved security/environment design; trade owner signs source semantics.", "deliverables": "Reviewed CI and promotion controls; tested correction/disappearance behavior.", "demo": "Show PR checks and a two-snapshot correction/cancellation case.", "exit": "Deployment path is reproducible and trade history behavior is explicit/tested."},
        {"name": "Sprint 4", "weeks": "Weeks 7-8", "goal": "Deliver reporting MVP, reconciliation/UAT evidence, and operations handover.", "stories": ["TMP-020", "TMP-022", "TMP-024"], "dependencies": "Approved reporting requirements, integration evidence, support owner, and prior sprint outputs.", "deliverables": "Dashboard MVP, signed UAT/reconciliation, runbook and alerting handover.", "demo": "Show dashboard reconciliation from source trade through daily P&L and demonstrate failure recovery.", "exit": "Product/operations sign-off; no unresolved critical risk; schedule stays paused absent production readiness approval."},
    ],
    "milestones": [
        {"id": "M-01", "name": "Scope and requirements baseline", "week": "End Week 2", "evidence": "Approved BR/FR, NFRs, RAID owners, target access design", "owner": "Product Owner / Lead Engineer", "gate": "Sources, calculation scope, roles, and acceptance evidence agreed"},
        {"id": "M-02", "name": "Source and data design sign-off", "week": "End Week 4", "evidence": "Provider decision, source contracts, dictionary, integration test result", "owner": "Data Owner / Architect", "gate": "Provider rights and supported test runtime confirmed or scope explicitly remains prototype"},
        {"id": "M-03", "name": "Engineering controls ready", "week": "End Week 6", "evidence": "CI checks, deployment procedure, trade history reconciliation tests", "owner": "Lead Engineer", "gate": "Reviewed build/deploy and correction behavior proven"},
        {"id": "M-04", "name": "Prototype acceptance and handover", "week": "End Week 8", "evidence": "Dashboard UAT, end-to-end reconciliation, runbook, open risk acceptance", "owner": "Product Owner / Operations", "gate": "Business and operations sign-off; no schedule unpause without separate production approval"},
    ],
    "risks": [
        {"id": "R-01", "type": "Risk", "description": "Yahoo Finance is not established as an approved or licensed institutional market-data source.", "probability": "High", "impact": "High", "response": "Obtain provider/data-rights decision in Sprint 2; keep production scope blocked until approved source is selected.", "owner": "Market Data Owner", "status": "Open"},
        {"id": "R-02", "type": "Risk", "description": "Full-snapshot trade Bronze can leave stale Silver trades when trade IDs disappear from later files.", "probability": "Medium", "impact": "High", "response": "Agree cancellation/deletion semantics and implement two-snapshot reconciliation test before relying on external trade feed.", "owner": "Trade Data Owner", "status": "Open"},
        {"id": "R-03", "type": "Risk", "description": "Full-history recomputation and free-form instrument/book data may not scale or satisfy control expectations.", "probability": "Medium", "impact": "High", "response": "Benchmark representative scale; establish reference ownership and incremental strategy from evidence.", "owner": "Lead Data Engineer", "status": "Open"},
        {"id": "R-04", "type": "Risk", "description": "Eight-week plan has no capacity reserve and depends on part-time approvers and test runtime access.", "probability": "Medium", "impact": "High", "response": "Reserve decision owners at Sprint start; defer dashboard polish or extend by one sprint if critical access/decisions slip.", "owner": "Project Manager", "status": "Open"},
        {"id": "A-01", "type": "Assumption", "description": "Initial scope remains USD cash equities, 10 configured instruments, and one development workspace.", "probability": "N/A", "impact": "High", "response": "Product owner confirms before Sprint 1 exit; FX, derivatives, fees, and corporate actions stay out of scope.", "owner": "Product Owner", "status": "To confirm"},
        {"id": "I-01", "type": "Issue", "description": "A documented second same-parameter Databricks run was queued and has no recorded outcome.", "probability": "N/A", "impact": "Medium", "response": "Run and compare business keys/values in a supported runtime; retain evidence.", "owner": "QA / Lead Engineer", "status": "Open"},
        {"id": "I-02", "type": "Issue", "description": "Three focused local tests fail because they read removed Silver *_constraints.sql files; the checks now reside in consolidated trades.sql and market_prices.sql.", "probability": "N/A", "impact": "Medium", "response": "Update stale test paths and assertions to the consolidated DDL, then rerun focused and full suite; application/test changes are outside this documentation task.", "owner": "Lead Engineer / QA", "status": "Open"},
        {"id": "D-01", "type": "Dependency", "description": "Production environment, service principal, data rights, source owners, and reporting audience are not evidenced.", "probability": "High", "impact": "High", "response": "Record named decision owner and due date; keep job schedule paused until readiness gate.", "owner": "Project Manager", "status": "Open"},
        {"id": "A-02", "type": "Assumption", "description": "Two-week sprint estimates use 10 story points of lead-engineer delivery capacity per sprint, with part-time BA/architect/QA support.", "probability": "N/A", "impact": "Medium", "response": "Re-estimate at planning; the eight-week baseline has no reserve and is not a committed calendar date.", "owner": "Project Manager", "status": "Proposed"},
    ],
    "decisions": [
        {"id": "DEC-01", "decision": "Retain current three-schema Medallion design (cibc.bronze/silver/gold) for prototype.", "status": "Implemented", "evidence": "database/catalog.sql and schema DDL; no separate reporting schema."},
        {"id": "DEC-02", "decision": "Use synthetic trade CSV for prototype demonstration.", "status": "Implemented", "evidence": "scripts/generate_trades.py and data/dirty_trades.csv; no upstream trading system connector."},
        {"id": "DEC-03", "decision": "Use Yahoo Finance unadjusted closes in current price adapter; production approval remains open.", "status": "Implemented / approval open", "evidence": "src/trade_pipeline/bronze/ingest_prices.py and README provider limitation."},
        {"id": "DEC-04", "decision": "Value positions at exact-date close; missing price remains NULL and creates a break.", "status": "Implemented", "evidence": "build_positions.py; docs/data_contracts.md."},
        {"id": "DEC-05", "decision": "Use simplified USD cash-equity daily P&L with opening inventory price move plus trade-day term.", "status": "Implemented / business sign-off open", "evidence": "calculate_daily_pnl.py and tests; fees, dividends, FX, corporate actions excluded."},
        {"id": "DEC-06", "decision": "Keep development schedule paused until operational readiness is explicitly approved.", "status": "Implemented configuration", "evidence": "jobs/resources/trade_pipeline.job.yml pause_status PAUSED."},
    ],
}


TESTS = [
    {"id": "TC-001", "type": "Unit / file contract", "scope": "Trade CSV exact header, empty/malformed file, raw strings, dedupe, no overwrite on invalid input", "requirements": "FR-001, FR-013", "story": "TMP-011", "expected": "Valid file writes distinct snapshot; invalid source fails before overwrite", "evidence": "tests/test_ingest_trades.py; capture supported-runtime run"},
    {"id": "TC-002", "type": "Unit / integration", "scope": "Price response normalization, retries, date window, no-price handling", "requirements": "FR-002, FR-005", "story": "TMP-013, TMP-014", "expected": "Correct close records; missing expected sessions are explicit; no invented price", "evidence": "tests/test_ingest_prices.py; test fixtures"},
    {"id": "TC-003", "type": "Data quality", "scope": "Silver cast, rules, reject reasons, duplicate/version precedence", "requirements": "FR-003, FR-004, FR-005", "story": "TMP-015", "expected": "Valid typed rows only; deterministic rejects and one selected trade version", "evidence": "tests/test_trades.py; tests/test_process_prices.py"},
    {"id": "TC-004", "type": "Calculation / reconciliation", "scope": "Daily and cumulative position, buys/sells, no-trade day, short/flat, correction", "requirements": "FR-006, FR-009", "story": "TMP-017, TMP-022", "expected": "Gold quantity matches independent trade aggregation by full grain", "evidence": "tests/test_build_positions.py; independent SQL reconciliation"},
    {"id": "TC-005", "type": "Valuation", "scope": "Exact-date price, currency, missing mark, null valuation and break", "requirements": "FR-007", "story": "TMP-017", "expected": "Value = closing quantity * exact close; missing mark does not change quantity", "evidence": "tests/test_build_positions.py; Gold break query"},
    {"id": "TC-006", "type": "P&L reconciliation", "scope": "Opening-position price move + trade-day cash-flow term; missing baseline/current close", "requirements": "FR-008", "story": "TMP-018, TMP-022", "expected": "Financial values match independent Decimal calculation; missing inputs yield NULL", "evidence": "tests/test_daily_pnl.py; documented BOOK_TECH/AAPL hand check"},
    {"id": "TC-007", "type": "Schema", "scope": "DDL column/type/nullability/key assumptions and financial precision", "requirements": "FR-003, FR-006, FR-008, FR-011", "story": "TMP-008, TMP-021", "expected": "All 10 DDL tables appear in dictionary with exact SQL definitions", "evidence": "database/tables/**/*.sql; generated Data Dictionary worksheet; update tests that still reference removed *_constraints.sql paths"},
    {"id": "TC-008", "type": "Idempotency", "scope": "Same source snapshot/date range run twice", "requirements": "FR-009, FR-013", "story": "TMP-016, TMP-022", "expected": "Business values and unique keys stable; audit history reflects both attempts", "evidence": "New supported Databricks/Delta run pair; compare business-key hashes"},
    {"id": "TC-009", "type": "Recovery", "scope": "Inject source/task failure and rerun after correction", "requirements": "FR-001, FR-011, FR-012, FR-013", "story": "TMP-016, TMP-024", "expected": "Failure is audited, downstream blocked, corrected rerun reaches consistent outputs", "evidence": "Job run URL, audit rows, recovery run and reconciliation"},
    {"id": "TC-010", "type": "Workflow integration", "scope": "Six-task DAG, parallel source branches, Bundle parameters and deployment smoke test", "requirements": "FR-012", "story": "TMP-009, TMP-010, TMP-023", "expected": "Prices/trades branches run independently, join before positions, P&L runs last", "evidence": "Bundle validate output; Job task graph and run record"},
    {"id": "TC-011", "type": "Reporting / UAT", "scope": "Run status, row counts, rejects, breaks, positions, P&L, dashboard access", "requirements": "FR-010, FR-011", "story": "TMP-019, TMP-020, TMP-022", "expected": "Report/dashboard totals reconcile to Gold; unavailable measures are clearly labeled", "evidence": "UAT scripts, screenshot/export, stakeholder sign-off"},
    {"id": "TC-012", "type": "Deployment / operations", "scope": "Environment config, least privilege, notifications, recovery, handover", "requirements": "FR-011, FR-012, FR-013", "story": "TMP-005, TMP-010, TMP-024", "expected": "Approved identity and runbook are usable; schedule remains gated until readiness approval", "evidence": "Access review, CI/deploy log, alert test, runbook walkthrough"},
]


TABLE_META = {
    "cibc.bronze.trades": ("Raw latest trade CSV snapshot", "One distinct source row per current file snapshot", "No enforced key; source identity is trade fields + version", "No physical PK/partition/clustering; small prototype", "INSERT OVERWRITE after structural validation"),
    "cibc.bronze.market_prices": ("Raw price records", "One symbol and price date", "Logical: symbol + price_date; no enforced constraint", "No physical PK/partition/clustering", "Conditional MERGE on symbol + price_date"),
    "cibc.silver.trades": ("Validated current trade versions", "One accepted row per trade_id", "trade_id", "No physical PK/partition/clustering", "Conditional MERGE; unusable present IDs removed"),
    "cibc.silver.trades_rejects": ("Trade data-quality and version rejects", "One rejected raw record/reason history row", "Logical reject key from business columns + source file + reason", "No physical PK/partition/clustering", "Insert-once MERGE reject history"),
    "cibc.silver.market_prices": ("Validated daily closing prices", "One symbol and price date", "symbol + price_date; positive close CHECK", "No physical PK/partition/clustering", "Conditional MERGE on symbol + price_date"),
    "cibc.silver.market_prices_rejects": ("Price data-quality rejects", "One rejected price record/reason history row", "Logical symbol/date/payload + reason", "No physical PK/partition/clustering", "Insert-once MERGE reject history"),
    "cibc.gold.positions": ("Daily cumulative positions and same-day valuation", "Date + book + counterparty + instrument + currency", "valuation_date + book_id + counterparty_id + instrument_id + currency", "No physical PK/partition/clustering", "Conditional scoped MERGE reconciliation"),
    "cibc.gold.daily_pnl": ("Daily book/symbol P&L and valuation inputs", "Date + book + symbol + currency", "valuation_date + book_id + symbol + currency", "No physical PK/partition/clustering", "Conditional scoped MERGE reconciliation"),
    "cibc.gold.valuation_breaks": ("Run-level missing price/baseline exceptions", "Run + instrument + date + break type, or trade-level unpriced trade", "run_id + nullable trade_id/instrument_id/date + break_type", "No physical PK/partition/clustering", "Insert-only MERGE; run history retained"),
    "cibc.gold.pipeline_run_log": ("Task-attempt audit and row/error metrics", "One Job task attempt", "run_id + task_run_id", "No physical PK/partition/clustering", "MERGE STARTED/metrics/SUCCESS or FAILED"),
}


COLUMN_META = {
    "trade_id": ("Trade business identifier", "CSV.trade_id", "Preserved as STRING; Silver key after validation", "Required; unique in Silver"),
    "counterparty_id": ("Counterparty identifier", "CSV.counterparty_id", "Preserved as STRING", "Required/nonblank for accepted trade"),
    "instrument_id": ("Configured instrument identifier", "CSV.instrument_id", "Checked against config mapping", "Required and configured; symbol must match"),
    "symbol": ("Market ticker symbol", "CSV.symbol or configured instrument mapping", "Normalized/validated against instrument map", "Required; mapped and consistent"),
    "side": ("Source trade direction", "CSV.side", "Raw value retained in Bronze/reject; used to sign quantity", "BUY or SELL"),
    "quantity": ("Trade or position quantity", "CSV.quantity or Silver trades.quantity", "DECIMAL(18,6); SELL is negative in Silver; aggregates use DECIMAL(28,6)", "Nonzero; BUY positive; SELL negative after normalization"),
    "price": ("Trade execution price", "CSV.price", "try_cast to DECIMAL(18,6)", "Required and > 0"),
    "currency": ("Currency code", "CSV.currency or price configuration", "USD-only current prototype", "Required; USD only"),
    "trade_timestamp": ("Trade event timestamp", "CSV.trade_timestamp", "try_cast to TIMESTAMP", "Required; date must match trade_date"),
    "trade_date": ("Trade business date", "CSV.trade_date", "try_cast to DATE; drives daily position activity", "Required; supported session and not future"),
    "settlement_date": ("Settlement business date", "CSV.settlement_date", "try_cast to DATE", "Required; next supported business session in current rule"),
    "book_id": ("Trading book identifier", "CSV.book_id", "Passed as opaque STRING", "Required/nonblank; book reference not persisted"),
    "version": ("Trade source version", "CSV.version", "try_cast to INT", "Positive; latest version wins"),
    "modified_timestamp": ("Trade source correction timestamp", "CSV.modified_timestamp", "try_cast to TIMESTAMP", "Required; not earlier than trade timestamp"),
    "source_file": ("Source path/file lineage", "Input Volume path", "Added at Bronze ingestion; propagated to Silver trade", "Required lineage in Bronze; nullable in Silver DDL"),
    "source_api": ("Price provider identifier", "yfinance.download", "Added by price adapter", "Provider identity recorded; approved source TBD"),
    "price_date": ("Market price effective date", "Provider date", "try_cast to DATE", "Expected supported session; not future"),
    "close": ("Unadjusted daily closing price", "Provider Close", "try_cast to DECIMAL(18,6)", "Required, finite, positive; currency USD"),
    "ingestion_timestamp": ("Ingestion event timestamp", "Pipeline runtime", "current_timestamp or provider batch timestamp", "Audit lineage"),
    "run_id": ("Pipeline run identifier", "Databricks Job run id", "Passed to task and persisted", "Required for lineage where source available"),
    "bronze_run_id": ("Bronze source run identifier", "Bronze.run_id", "Propagated selected-row lineage", "Lineage may be nullable on historical rows"),
    "bronze_ingestion_timestamp": ("Bronze ingestion timestamp", "Bronze.ingestion_timestamp", "Propagated selected-row lineage", "Lineage may be nullable on historical rows"),
    "rejection_reason": ("One or more validation/reconciliation reason codes", "Silver validation", "Pipe-separated rule names", "Required for reject record"),
    "rejected_at": ("Reject capture timestamp", "Silver processor runtime", "current_timestamp", "Required for reject record"),
    "daily_buy_quantity": ("Daily sum of positive BUY quantities", "Silver trades.quantity", "Grouped by position grain/date; DECIMAL(28,6)", "Not null; zero when no buys"),
    "daily_sell_quantity": ("Daily absolute sum of signed SELL quantities", "Silver trades.quantity", "Sum(abs(quantity)) for sells; DECIMAL(28,6)", "Not null; zero when no sells"),
    "daily_net_quantity": ("Daily signed net trade quantity", "Silver trades.quantity", "SUM(signed quantity); DECIMAL(28,6)", "Not null"),
    "closing_position_quantity": ("Cumulative closing position", "Daily net quantity", "Cumulative SUM over session timeline; DECIMAL(28,6)", "Not null; may be negative"),
    "closing_price": ("Exact-date closing mark", "Silver market_prices.close", "Join symbol/date/currency; DECIMAL(18,6)", "Nullable when no same-day close"),
    "market_value": ("Position market value", "Closing quantity * closing price", "DECIMAL(38,6)", "Nullable when close is missing"),
    "opening_quantity": ("Position at start of valuation date", "Closing quantity - daily net quantity", "DECIMAL(28,6)", "Not null"),
    "buy_quantity": ("Daily buy flow at P&L grain", "Gold positions.daily_buy_quantity", "Sum across counterparties; DECIMAL(28,6)", "Not null"),
    "sell_quantity": ("Daily sell flow at P&L grain", "Gold positions.daily_sell_quantity", "Sum across counterparties; positive absolute amount", "Not null"),
    "closing_quantity": ("Closing position at P&L grain", "Gold positions.closing_position_quantity", "Sum across counterparties", "Not null"),
    "previous_close": ("Latest available close strictly before valuation date", "Silver market_prices", "As-of prior-date join; DECIMAL(18,6)", "Nullable; required for nonzero opening position"),
    "current_close": ("Exact-date close", "Silver market_prices", "Join symbol/date/currency; DECIMAL(18,6)", "Nullable; required for daily P&L"),
    "unrealized_pnl_proxy": ("Cumulative mark-to-market less signed trade consideration proxy", "Gold valuation and trade cash flows", "DECIMAL(38,6); includes realized effects and is not true unrealized P&L", "Nullable when current valuation unavailable"),
    "daily_pnl": ("Daily simplified cash-equity P&L", "Opening position, current/prior closes, signed trade cash flow", "DECIMAL(38,6)", "Nullable when required price/baseline unavailable"),
    "calculation_status": ("Completeness status for daily P&L row", "Price/baseline availability", "COMPLETE, MISSING_CURRENT_PRICE, MISSING_BASELINE", "Required"),
    "calculated_at": ("Calculation timestamp", "Gold processor runtime", "current_timestamp", "Nullable in DDL"),
    "valuation_date": ("Date represented by position/P&L/break", "Trade/calendar/price dates", "DATE", "Required in position and P&L"),
    "break_type": ("Valuation exception category", "Gold calculation", "MISSING_MARKET_PRICE or MISSING_PNL_BASELINE", "Required"),
    "break_description": ("Human-readable exception detail", "Gold calculation", "Constructed from book/instrument/date context", "Required"),
    "detected_at": ("Break detection timestamp", "Gold processor runtime", "current_timestamp", "Required"),
    "status": ("Break or task state", "Pipeline", "OPEN for valuation breaks; STARTED/SUCCESS/FAILED for audit", "Required in business record"),
    "task_run_id": ("Databricks task attempt identifier", "Task context", "Task run plus execution count where available", "Logical audit key with run_id"),
    "task_name": ("Workflow task name", "Bundle task_key", "Recorded by audited wrapper", "Required by contract"),
    "run_date": ("UTC date task attempt started", "started_at", "Derived in audit helper", "Required by contract"),
    "started_at": ("Task attempt start timestamp", "Audit helper UTC clock", "datetime.now(timezone.utc)", "Required by contract"),
    "finished_at": ("Task attempt completion timestamp", "Audit helper UTC clock", "Set on success/failure", "Nullable while STARTED"),
    "source_table": ("Logical source table/path", "Task configuration", "Recorded by audited wrapper", "Nullable for external source"),
    "target_table": ("Logical output table", "Task configuration", "Recorded by audited wrapper", "Nullable where not applicable"),
    "requested_tickers": ("Number of requested market symbols", "Task metrics", "Count of normalized symbols", "Nullable for trade-only task"),
    "input_rows": ("Rows read/processed", "Task metrics", "Task-defined count; see audit contract", "Nullable on task start/failure"),
    "output_rows": ("Rows accepted or produced", "Task metrics", "Task-defined count; not always writes", "Nullable on task start/failure"),
    "rejected_rows": ("Rows rejected in scoped processing", "Task metrics", "Can count records with multiple reasons once", "Nullable on task start/failure"),
    "duplicate_rows": ("Duplicate/version rows", "Task metrics", "Task-specific duplicate definition", "Nullable on task start/failure"),
    "issue_counts": ("Reason-to-count map", "Task metrics", "Map of issue reason to count", "Nullable; reasons may exceed rejected row count"),
    "error_type": ("Exception class on failed attempt", "Caught exception", "type(error).__name__", "Nullable unless failed"),
    "error_message": ("Exception summary on failed attempt", "Caught exception", "str(error); sensitivity review required", "Nullable unless failed"),
    "id": ("Delta identity surrogate", "Delta table engine", "GENERATED ALWAYS AS IDENTITY", "Not a business key; omitted from MERGE source"),
    "trade_id": ("Trade identifier on valuation break", "Silver trade", "Nullable for instrument/date-level break", "Nullable"),
    "instrument_id": ("Instrument identifier on position/break", "Configured mapping or Silver trade", "STRING", "Required on accepted trade; nullable in break DDL"),
    "currency": ("Financial reporting currency", "Trade/price source", "USD-only prototype", "Required on core accepted/Gold data"),
}


def status_jira(status: str) -> str:
    return {"IMPLEMENTED": "Done", "PARTIALLY IMPLEMENTED": "In Progress", "PLANNED": "To Do", "TO BE CONFIRMED": "To Do"}[status]


def csv_text(items: list[str]) -> str:
    return "; ".join(items)


def list_subtasks() -> list[dict]:
    rows = []
    for story in PLAN["stories"]:
        for index, description in enumerate(story["subtasks"], start=1):
            rows.append({"id": f'{story["id"]}-ST{index:02}', "parent": story["id"], "description": description, "output": f'Completed evidence for {description.lower()}', "dependency": story["id"]})
    return rows


def get_table_ddl() -> list[dict]:
    records = []
    for path in sorted((ROOT / "database" / "tables").rglob("*.sql")):
        source = re.sub(r"--.*?$", "", path.read_text(encoding="utf-8"), flags=re.MULTILINE)
        match = re.search(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([\w.]+)\s*\((.*?)\)\s*USING\s+DELTA", source, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        table = match.group(1).lower()
        table_description, grain, key, strategy, write_rule = TABLE_META[table]
        body = match.group(2)
        pieces, current, depth = [], [], 0
        for character in body:
            if character in "(<":
                depth += 1
            elif character in ")>":
                depth -= 1
            if character == "," and depth == 0:
                pieces.append("".join(current).strip())
                current = []
            else:
                current.append(character)
        if current:
            pieces.append("".join(current).strip())
        for definition in pieces:
            column_match = re.match(r"([A-Za-z_][\w]*)\s+(.+)$", definition.strip(), flags=re.DOTALL)
            if not column_match:
                continue
            column, type_text = column_match.group(1), " ".join(column_match.group(2).split())
            nullable = "Generated" if "GENERATED ALWAYS AS IDENTITY" in type_text.upper() else "No" if "NOT NULL" in type_text.upper() else "Yes"
            data_type = re.sub(r"\s+NOT\s+NULL\s*$", "", type_text, flags=re.IGNORECASE)
            description, source_field, transform, quality = COLUMN_META.get(
                column,
                (column.replace("_", " ").capitalize(), "Pipeline/source record", "Preserved or derived by owning task", "See table-level validation rules"),
            )
            records.append({
                "Catalog": "cibc", "Schema": table.split(".")[1], "Table": table.split(".")[2],
                "Table purpose": table_description, "Grain": grain, "Business/unique key assumption": key,
                "Column": column, "Databricks SQL type": data_type, "Precision/scale": (re.search(r"\(([^)]*)\)", data_type).group(1) if re.search(r"\(([^)]*)\)", data_type) else "N/A"),
                "Nullable": nullable, "Default": "None declared", "Description": description,
                "Source field": source_field, "Transformation": transform, "Data quality rule": quality,
                "Storage strategy": strategy, "Write semantics": write_rule, "DDL file": path.relative_to(ROOT).as_posix(),
            })
    expected = set(TABLE_META)
    actual = {f'cibc.{item["Schema"]}.{item["Table"]}' for item in records}
    missing = expected - actual
    if missing:
        raise RuntimeError(f"Data dictionary missing DDL tables: {sorted(missing)}")
    return records


def load_color(value: str) -> PatternFill:
    return PatternFill("solid", fgColor=value)


def style_sheet(sheet, widths: dict[str, int] | None = None, freeze: str = "A2") -> None:
    sheet.freeze_panes = freeze
    sheet.sheet_view.showGridLines = False
    if sheet.max_row > 1:
        sheet.auto_filter.ref = sheet.dimensions
    for cell in sheet[1]:
        cell.fill = load_color(NAVY)
        cell.font = Font(name="Aptos", size=10, bold=True, color=WHITE)
        cell.alignment = Alignment(vertical="center", wrap_text=True)
        cell.border = Border(bottom=Side(style="medium", color=TEAL))
    sheet.row_dimensions[1].height = 34
    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            cell.font = Font(name="Aptos", size=10, color=INK)
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if cell.row % 2 == 0:
                cell.fill = load_color("F5F8FA")
    if widths:
        for column, width in widths.items():
            sheet.column_dimensions[column].width = width
    for row_number in range(2, sheet.max_row + 1):
        sheet.row_dimensions[row_number].height = 34
    sheet.sheet_properties.pageSetUpPr.fitToPage = True
    sheet.page_setup.fitToWidth = 1
    sheet.page_setup.fitToHeight = 0


def add_records(workbook: Workbook, title: str, headers: list[str], records: list[list], widths: dict[str, int] | None = None):
    sheet = workbook.create_sheet(title)
    sheet.append(headers)
    for record in records:
        sheet.append(record)
    style_sheet(sheet, widths)
    return sheet


def build_workbook(dictionary: list[dict]) -> None:
    workbook = Workbook()
    summary = workbook.active
    summary.title = "Executive Summary"
    summary.sheet_view.showGridLines = False
    summary.merge_cells("A1:F1")
    summary["A1"] = "TRADING PLATFORM | DELIVERY PLAN"
    summary["A1"].fill = load_color(NAVY)
    summary["A1"].font = Font(name="Aptos Display", size=20, bold=True, color=WHITE)
    summary["A1"].alignment = Alignment(vertical="center")
    summary.row_dimensions[1].height = 42
    summary.append(["Evidence baseline", "2026-10-14; repository + docs/test_results.md dated 2026-10-11", "", "", "", ""])
    summary.append(["Purpose", PLAN["purpose"], "", "", "", ""])
    summary.append(["Current implementation", "Six-task Databricks Workflow; 10 Delta tables in cibc.bronze/silver/gold; synthetic trades; Yahoo Finance prices; position/P&L; audit/rejects; notebooks/SQL.", "", "", "", ""])
    summary.append(["Not implemented / not evidenced", "Deployed dashboard; approved market-data contract; non-dev Bundle targets/CI; automated alerting; production identity; approved retention/RTO/RPO; successful second same-input Databricks run.", "", "", "", ""])
    summary.append(["Schedule", "Proposed 8-week / four 2-week sprints. 40 story points at assumed 10 points per sprint; no capacity reserve. Relative weeks only, not approved calendar dates.", "", "", "", ""])
    summary.append(["Prototype limitation", "USD cash equities; ten in-code instruments; approximate NYSE calendar; full-history recompute; simplified daily P&L; fees, FX, corporate actions, dividends, settlement cash, and short-sale mechanics excluded.", "", "", "", ""])
    summary.append(["Jira import", "Temporary planning IDs; map planning key to custom field, issue hierarchy and Epic Link to Jira fields, and replace planning Epic Link values with created Jira epic keys where required.", "", "", "", ""])
    summary.append(["Status legend", "IMPLEMENTED = code/config/test/run evidence; PARTIALLY IMPLEMENTED = supporting behavior with a material gap; PLANNED = not delivered; TO BE CONFIRMED = evidence/owner absent.", "", "", "", ""])
    summary.append(["Metric", "Value", "Interpretation", "", "", ""])
    metrics = [
        ("Business requirements", "=COUNTA('Business Req'!A:A)-1", "Eight business outcomes"),
        ("Functional requirements", "=COUNTA('Functional Req'!A:A)-1", "Detailed system behaviors"),
        ("Jira epics", "=COUNTA(Epics!A:A)-1", "Planning hierarchy"),
        ("Jira stories", "=COUNTA(Stories!A:A)-1", "Includes current-state and proposed work"),
        ("Planned sprint points", "=SUM(Sprints!E2:E5)", "Proposed 8-week baseline; no reserve"),
        ("Physical dictionary columns", "=COUNTA('Data Dictionary'!G:G)-1", "DDL-derived across 10 tables"),
    ]
    for metric in metrics:
        summary.append(list(metric) + ["", "", ""])
    for row_number in range(2, summary.max_row + 1):
        summary.cell(row_number, 1).font = Font(name="Aptos", size=10, bold=True, color=NAVY)
        for cell in summary[row_number]:
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if row_number % 2 == 0:
                cell.fill = load_color("F5F8FA")
    for cell in summary[9]:
        cell.fill = load_color(TEAL)
        cell.font = Font(bold=True, color=WHITE)
    for col, width in {"A": 30, "B": 78, "C": 44, "D": 15, "E": 15, "F": 15}.items():
        summary.column_dimensions[col].width = width
    summary.freeze_panes = "A2"

    business = [[r["id"], r["description"], r["why"], r["priority"], r["acceptance"], r["dependencies"], r["status"], csv_text(r["fr"])] for r in PLAN["business_requirements"]]
    add_records(workbook, "Business Req", ["BR ID", "Requirement", "Business justification", "Priority", "Acceptance criteria", "Dependencies", "Implementation status", "Functional requirements"], business, {"A": 13, "B": 48, "C": 44, "D": 14, "E": 62, "F": 34, "G": 24, "H": 30})

    functional = [[r["id"], r["br"], r["description"], r["input"], r["logic"], r["output"], r["rules"], r["errors"], r["acceptance"], r["components"], r["status"], csv_text(r["stories"])] for r in PLAN["functional_requirements"]]
    add_records(workbook, "Functional Req", ["FR ID", "Parent BR", "Functional description", "Input", "Processing logic", "Expected output", "Business rules", "Error handling", "Acceptance criteria", "Components", "Implementation status", "Jira stories"], functional, {"A": 12, "B": 14, "C": 46, "D": 36, "E": 50, "F": 38, "G": 44, "H": 42, "I": 52, "J": 36, "K": 24, "L": 28})

    nfrs = [[r["id"], r["name"], r["target"], r["status"], r["evidence"]] for r in PLAN["nfrs"]]
    add_records(workbook, "NFRs", ["NFR ID", "Quality attribute", "Acceptance target", "Status", "Evidence / gap"], nfrs, {"A": 12, "B": 24, "C": 88, "D": 24, "E": 54})

    sources = [[s[key] for key in ("id", "name", "status", "system", "format", "location", "frequency", "volume", "keys", "versioning", "quality", "ingestion", "failure", "open_items")] for s in PLAN["sources"]]
    add_records(workbook, "Data Sources", ["Source ID", "Source", "Status", "System/provider", "Format/interface", "Location", "Frequency", "Volume", "Business key", "Version/correction", "Quality rules", "Ingestion strategy", "Failure handling", "Open decisions"], sources, {"A": 14, "B": 24, "C": 42, "D": 46, "E": 40, "F": 50, "G": 38, "H": 34, "I": 40, "J": 48, "K": 58, "L": 50, "M": 48, "N": 72})

    dict_rows = [[d[key] for key in ("Catalog", "Schema", "Table", "Table purpose", "Grain", "Business/unique key assumption", "Column", "Databricks SQL type", "Precision/scale", "Nullable", "Default", "Description", "Source field", "Transformation", "Data quality rule", "Storage strategy", "Write semantics", "DDL file")] for d in dictionary]
    add_records(workbook, "Data Dictionary", ["Catalog", "Schema", "Table", "Table purpose", "Grain", "Business/unique key assumption", "Column", "Databricks SQL type", "Precision/scale", "Nullable", "Default", "Description", "Source field", "Transformation", "Data quality rule", "Storage strategy", "Write semantics", "DDL file"], dict_rows, {"A": 13, "B": 13, "C": 28, "D": 34, "E": 40, "F": 56, "G": 30, "H": 30, "I": 18, "J": 12, "K": 17, "L": 44, "M": 34, "N": 50, "O": 42, "P": 40, "Q": 42, "R": 50})

    epic_rows = [[e["id"], e["name"], e["priority"], "To Do", "Planning epic; mixed current-state evidence and target backlog. See linked story implementation status."] for e in PLAN["epics"]]
    add_records(workbook, "Epics", ["Planning ID", "Epic name", "Priority", "Jira status", "Scope note"], epic_rows, {"A": 18, "B": 55, "C": 16, "D": 18, "E": 86})

    story_rows = []
    for story in PLAN["stories"]:
        story_rows.append([story["id"], story["epic"], story["title"], story["objective"], story["description"], csv_text(story["criteria"]), story["points"], story["priority"], story["status"], story["sprint"], csv_text(story["br"]), csv_text(story["fr"]), csv_text(story["depends"]), story["inputs"], story["outputs"]])
    story_sheet = add_records(workbook, "Stories", ["Planning ID", "Epic ID", "Story title", "User story / objective", "Detailed description", "Acceptance criteria", "Story points", "Priority", "Implementation status", "Sprint", "BR IDs", "FR IDs", "Predecessor stories", "Required inputs", "Expected outputs"], story_rows, {"A": 16, "B": 16, "C": 48, "D": 54, "E": 68, "F": 84, "G": 14, "H": 15, "I": 24, "J": 16, "K": 34, "L": 42, "M": 38, "N": 42, "O": 44})

    subtasks = list_subtasks()
    add_records(workbook, "Subtasks", ["Subtask ID", "Parent story", "Description", "Expected output", "Dependency"], [[s["id"], s["parent"], s["description"], s["output"], "Parent story and its predecessors"] for s in subtasks], {"A": 22, "B": 18, "C": 70, "D": 72, "E": 42})

    criteria_rows = [[story["id"], story["title"], index, criterion] for story in PLAN["stories"] for index, criterion in enumerate(story["criteria"], start=1)]
    add_records(workbook, "Acceptance Criteria", ["Story ID", "Story title", "AC #", "Testable acceptance criterion"], criteria_rows, {"A": 16, "B": 52, "C": 10, "D": 110})

    dependency_rows = []
    for story in PLAN["stories"]:
        for predecessor in story["depends"]:
            dependency_rows.append([predecessor, story["id"], "blocks", "Story delivery dependency", story["sprint"] or "Current/complete", "Predecessor output required before dependent story acceptance"])
    workflow_edges = [("ingest_prices", "process_prices"), ("ingest_trades", "process_trades"), ("process_prices", "build_positions"), ("process_trades", "build_positions"), ("build_positions", "calculate_daily_pnl")]
    dependency_rows.extend([[a, b, "blocks", "Implemented Databricks Workflow dependency", "Current workflow", "Verified in jobs/resources/trade_pipeline.job.yml"] for a, b in workflow_edges])
    add_records(workbook, "Dependencies", ["Predecessor", "Successor", "Relationship", "Dependency type", "Sprint/status", "Rationale / expected output"], dependency_rows, {"A": 22, "B": 24, "C": 18, "D": 42, "E": 20, "F": 82})

    sprint_rows = []
    for sprint in PLAN["sprints"]:
        sprint_rows.append([sprint["name"], sprint["weeks"], sprint["goal"], csv_text(sprint["stories"]), f'=SUMIF(Stories!$J:$J,A{len(sprint_rows)+2},Stories!$G:$G)', sprint["dependencies"], sprint["deliverables"], sprint["demo"], sprint["exit"]])
    sprint_sheet = add_records(workbook, "Sprints", ["Sprint", "Timeline", "Sprint goal", "Planned stories", "Story points (formula)", "Dependencies", "Deliverables", "Demo outcome", "Exit criteria"], sprint_rows, {"A": 16, "B": 18, "C": 68, "D": 60, "E": 22, "F": 68, "G": 72, "H": 72, "I": 72})

    gantt = workbook.create_sheet("Gantt")
    gantt.append(["Planning ID", "Story", "Sprint", "Start week", "Duration (weeks)"] + [f"Week {week}" for week in range(1, 9)])
    for story in PLAN["stories"]:
        sprint_number = int(story["sprint"].split()[-1]) if story["sprint"] else 0
        if not sprint_number:
            continue
        row = gantt.max_row + 1
        gantt.append([story["id"], story["title"], story["sprint"], 2 * sprint_number - 1, 2] + [f'=--AND(COLUMN()-5>=$D{row},COLUMN()-5<$D{row}+$E{row})' for column in "FGHIJKLM"])
    for column, width in {"A": 18, "B": 58, "C": 16, "D": 14, "E": 18, **{letter: 12 for letter in "FGHIJKLM"}}.items():
        gantt.column_dimensions[column].width = width
    style_sheet(gantt, freeze="F2")
    gantt.row_dimensions[1].height = 34
    for column in "FGHIJKLM":
        gantt.conditional_formatting.add(f"{column}2:{column}{gantt.max_row}", FormulaRule(formula=[f"{column}2=1"], fill=load_color(TEAL), font=Font(color=TEAL)))
        for row_number in range(2, gantt.max_row + 1):
            gantt[f"{column}{row_number}"].number_format = ";;;"

    milestone_rows = [[m["id"], m["name"], m["week"], m["evidence"], m["owner"], m["gate"], "Proposed"] for m in PLAN["milestones"]]
    add_records(workbook, "Milestones", ["Milestone ID", "Milestone", "Target timing", "Evidence", "Accountable owner", "Exit gate", "Status"], milestone_rows, {"A": 15, "B": 42, "C": 20, "D": 68, "E": 32, "F": 78, "G": 18})

    raid_rows = [[r[k] for k in ("id", "type", "description", "probability", "impact", "response", "owner", "status")] for r in PLAN["risks"]]
    raid = add_records(workbook, "RAID", ["RAID ID", "Type", "Description", "Probability", "Impact", "Mitigation / action", "Owner", "Status"], raid_rows, {"A": 15, "B": 18, "C": 68, "D": 16, "E": 16, "F": 76, "G": 30, "H": 18})

    trace_rows = []
    for br in PLAN["business_requirements"]:
        for fr_id in br["fr"]:
            fr = next(item for item in PLAN["functional_requirements"] if item["id"] == fr_id)
            for story_id in fr["stories"]:
                story = next(item for item in PLAN["stories"] if item["id"] == story_id)
                related_tests = [test for test in TESTS if fr_id in test["requirements"] and story_id in test["story"]]
                if not related_tests:
                    related_tests = [test for test in TESTS if fr_id in test["requirements"]]
                trace_rows.append([br["id"], fr_id, story_id, csv_text([t["id"] for t in related_tests]), csv_text([t["evidence"] for t in related_tests]), story["status"]])
    add_records(workbook, "Traceability", ["Business requirement", "Functional requirement", "Jira story", "Test case(s)", "Acceptance evidence", "Implementation status"], trace_rows, {"A": 20, "B": 22, "C": 18, "D": 28, "E": 84, "F": 24})

    test_rows = [[t["id"], t["type"], t["scope"], t["requirements"], t["story"], t["expected"], t["evidence"], "Planned / existing tests to rerun"] for t in TESTS]
    add_records(workbook, "Test Plan", ["Test ID", "Test level", "Scope", "Requirement IDs", "Jira story", "Expected result", "Acceptance evidence", "Status / note"], test_rows, {"A": 14, "B": 28, "C": 76, "D": 36, "E": 30, "F": 76, "G": 84, "H": 34})

    current_rows = [
        ["Source", "Trade input", "IMPLEMENTED", "Synthetic 14-field CSV; 1,200-row sample; UC Volume input", "No real trading-system connector; full snapshot and missing-ID semantics need business decision"],
        ["Source", "Market prices", "IMPLEMENTED", "yfinance unadjusted close; ten defaults; USD", "No approved/licensed production provider evidence"],
        ["Data", "Reference data", "PARTIALLY IMPLEMENTED", "Ten instrument mappings in Python config", "No persisted/effective-dated instruments, books, or counterparties"],
        ["Pipeline", "Bronze/Silver/Gold", "IMPLEMENTED", "10 physical Delta tables; PySpark modules; validation, rejects, lineage", "No reporting schema/table or explicit table PK enforcement"],
        ["Calculations", "Positions/P&L", "IMPLEMENTED", "Position/P&L transforms and dated independent SQL reconciliation", "Simplified USD cash equities; no fees/FX/dividends/corporate actions"],
        ["Reporting", "Notebook and SQL", "IMPLEMENTED", "Run-trigger and read-only report notebooks; demo queries", "No deployed dashboard; report has known unavailable counters"],
        ["Operations", "Audit/recovery", "PARTIALLY IMPLEMENTED", "Task attempt logs; reject history; valuation breaks; safe rerun code", "No automated alerts; second identical Databricks run was queued in dated evidence"],
        ["Deployment", "Databricks Bundle", "PARTIALLY IMPLEMENTED", "Dev target, wheel build, deploy script; six-task Job", "Schedule paused; no CI/test/prod targets or production principal evidenced"],
        ["Testing", "pytest and Databricks run", "PARTIALLY IMPLEMENTED", "Dated run 2026-10-11: six tasks succeeded; positions 5,990 and P&L 835 reconciled with zero mismatches. Focused test run found 3 stale DDL-path failures.", "Tests expect removed trades_constraints.sql / market_prices_constraints.sql; constraints are in consolidated DDL. Second run unverified; Spark/Delta runtime required for full integration proof"],
    ]
    add_records(workbook, "Current State", ["Area", "Capability", "Status", "Verified evidence", "Gap / limitation"], current_rows, {"A": 18, "B": 34, "C": 24, "D": 92, "E": 96})

    # Consistent status/priority/sprint choices and visual cues across worksheets.
    status_validation = DataValidation(type="list", formula1='"IMPLEMENTED,PARTIALLY IMPLEMENTED,PLANNED,TO BE CONFIRMED"', allow_blank=True)
    priority_validation = DataValidation(type="list", formula1='"High,Medium,Low,Must,Should"', allow_blank=True)
    sprint_validation = DataValidation(type="list", formula1='"Sprint 1,Sprint 2,Sprint 3,Sprint 4"', allow_blank=True)
    story_sheet.add_data_validation(status_validation)
    status_validation.add(f"I2:I{story_sheet.max_row}")
    story_sheet.add_data_validation(priority_validation)
    priority_validation.add(f"H2:H{story_sheet.max_row}")
    story_sheet.add_data_validation(sprint_validation)
    sprint_validation.add(f"J2:J{story_sheet.max_row}")
    for column, status, fill in [("I", "IMPLEMENTED", PALE_TEAL), ("I", "PARTIALLY IMPLEMENTED", PALE_ORANGE), ("I", "PLANNED", PALE_RED), ("I", "TO BE CONFIRMED", "E9ECEF")]:
        story_sheet.conditional_formatting.add(f"{column}2:{column}{story_sheet.max_row}", FormulaRule(formula=[f'{column}2="{status}"'], fill=load_color(fill)))
    for status, fill in [("Open", PALE_RED), ("To confirm", PALE_ORANGE), ("Proposed", "E9ECEF")]:
        raid.conditional_formatting.add(f"H2:H{raid.max_row}", FormulaRule(formula=[f'H2="{status}"'], fill=load_color(fill)))
    for sheet in workbook.worksheets:
        if sheet.title != "Executive Summary":
            sheet.auto_filter.ref = sheet.dimensions
    workbook.calculation.fullCalcOnLoad = True
    workbook.calculation.forceFullCalc = True
    workbook.calculation.calcMode = "auto"
    workbook.save(OUT / "Trading_Platform_Project_Plan.xlsx")


def jira_rows() -> list[dict]:
    rows = []
    for epic in PLAN["epics"]:
        rows.append({"Issue ID": epic["id"], "Issue Type": "Epic", "Planning Key": epic["id"], "Epic Name": epic["name"], "Epic Link": "", "Parent ID": "", "Summary": epic["name"], "Description": f"Delivery epic: {epic['name']}. Includes evidenced current functionality and linked proposed completion work.", "Acceptance Criteria": "See linked stories; epic is complete when all committed child stories meet their acceptance criteria.", "Priority": epic["priority"], "Story Points": "", "Status": "To Do", "Implementation Status": "Mixed; see stories", "Dependencies": "", "Sprint": "", "Requirement ID": ""})
    for story in PLAN["stories"]:
        rows.append({"Issue ID": story["id"], "Issue Type": "Story", "Planning Key": story["id"], "Epic Name": "", "Epic Link": story["epic"], "Parent ID": "", "Summary": story["title"], "Description": f"Objective: {story['objective']}\n\nDetail: {story['description']}\n\nInputs: {story['inputs']}\n\nExpected outputs: {story['outputs']}", "Acceptance Criteria": "\n".join(f"- {criterion}" for criterion in story["criteria"]), "Priority": story["priority"], "Story Points": story["points"], "Status": status_jira(story["status"]), "Implementation Status": story["status"], "Dependencies": csv_text(story["depends"]), "Sprint": story["sprint"], "Requirement ID": csv_text(story["br"] + story["fr"])})
    for subtask in list_subtasks():
        story = next(item for item in PLAN["stories"] if item["id"] == subtask["parent"])
        rows.append({"Issue ID": subtask["id"], "Issue Type": "Sub-task", "Planning Key": subtask["id"], "Epic Name": "", "Epic Link": story["epic"], "Parent ID": subtask["parent"], "Summary": subtask["description"], "Description": f"Expected output: {subtask['output']}\nDependency: {subtask['dependency']}.", "Acceptance Criteria": subtask["output"], "Priority": story["priority"], "Story Points": "", "Status": status_jira(story["status"]), "Implementation Status": story["status"], "Dependencies": subtask["parent"], "Sprint": story["sprint"], "Requirement ID": csv_text(story["br"] + story["fr"])})
    return rows


def build_jira_csv() -> None:
    rows = jira_rows()
    columns = list(rows[0])
    with (OUT / "jira-backlog.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def ppt_color(hex_color: str) -> RGBColor:
    return RGBColor.from_string(hex_color)


def add_text(slide, x, y, w, h, text, size=16, color=INK, bold=False, font="Aptos", align=PP_ALIGN.LEFT):
    shape = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    frame = shape.text_frame
    frame.clear()
    frame.word_wrap = True
    frame.margin_left = Inches(0.05)
    frame.margin_right = Inches(0.05)
    frame.margin_top = Inches(0.03)
    frame.margin_bottom = Inches(0.02)
    paragraph = frame.paragraphs[0]
    paragraph.alignment = align
    paragraph.font.name = font
    paragraph.font.size = Pt(size)
    paragraph.font.bold = bold
    paragraph.font.color.rgb = ppt_color(color)
    paragraph.text = text
    return shape


def add_box(slide, x, y, w, h, title, body="", fill=PALE, accent=TEAL, title_size=16, body_size=11, title_color=NAVY, line=GRID):
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    shape.fill.solid()
    shape.fill.fore_color.rgb = ppt_color(fill)
    shape.line.color.rgb = ppt_color(line)
    shape.line.width = Pt(0.8)
    shape.adjustments[0] = 0.08
    stripe = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(0.07), Inches(h))
    stripe.fill.solid()
    stripe.fill.fore_color.rgb = ppt_color(accent)
    stripe.line.fill.background()
    add_text(slide, x + 0.18, y + 0.11, w - 0.30, 0.34, title, title_size, title_color, True)
    if body:
        add_text(slide, x + 0.18, y + 0.50, w - 0.31, h - 0.57, body, body_size, INK)
    return shape


def add_arrow(slide, x1, y1, x2, y2, color=TEAL, width=2):
    connector = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    connector.line.color.rgb = ppt_color(color)
    connector.line.width = Pt(width)
    connector.line.end_arrowhead = True
    return connector


def new_deck() -> Presentation:
    presentation = Presentation()
    presentation.slide_width = Inches(13.333)
    presentation.slide_height = Inches(7.5)
    presentation.core_properties.author = "Project Delivery Planning"
    presentation.core_properties.subject = "Evidence-based Databricks trading platform delivery plan"
    return presentation


def slide_base(presentation: Presentation, title: str, section: str, number: int):
    slide = presentation.slides.add_slide(presentation.slide_layouts[6])
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = ppt_color(WHITE)
    band = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, presentation.slide_width, Inches(0.12))
    band.fill.solid()
    band.fill.fore_color.rgb = ppt_color(TEAL)
    band.line.fill.background()
    add_text(slide, 0.55, 0.31, 11.8, 0.55, title, 25, NAVY, True, "Aptos Display")
    add_text(slide, 0.58, 0.88, 12.0, 0.28, section.upper(), 9, TEAL, True)
    line = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.55), Inches(7.12), Inches(12.22), Inches(0.012))
    line.fill.solid()
    line.fill.fore_color.rgb = ppt_color(GRID)
    line.line.fill.background()
    add_text(slide, 0.58, 7.17, 10.5, 0.18, f"TRADING PLATFORM  |  EVIDENCE BASELINE {AS_OF}  |  PROPOSED DELIVERY PLAN", 8, MUTED)
    add_text(slide, 12.15, 7.15, 0.55, 0.20, str(number).zfill(2), 9, MUTED, True, align=PP_ALIGN.RIGHT)
    return slide


def cover_slide(presentation: Presentation, title: str, subtitle: str, label: str):
    slide = presentation.slides.add_slide(presentation.slide_layouts[6])
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = ppt_color(NAVY)
    side = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(9.2), 0, Inches(4.2), Inches(7.5))
    side.fill.solid()
    side.fill.fore_color.rgb = ppt_color("1D4B60")
    side.line.fill.background()
    stripe = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.72), Inches(0.78), Inches(0.10), Inches(5.95))
    stripe.fill.solid()
    stripe.fill.fore_color.rgb = ppt_color(TEAL)
    stripe.line.fill.background()
    add_text(slide, 1.12, 1.02, 7.3, 0.35, label.upper(), 11, "95D8CF", True)
    add_text(slide, 1.10, 1.75, 7.8, 1.55, title, 34, WHITE, True, "Aptos Display")
    add_text(slide, 1.13, 3.52, 7.2, 1.0, subtitle, 18, "D8E4E9")
    add_text(slide, 1.13, 5.85, 7.2, 0.45, f"As of {AS_OF}  |  Current implementation and proposed next steps clearly separated", 10, "BFD0D8")
    add_box(slide, 9.66, 1.35, 2.95, 1.18, "BRONZE", "Raw trades and daily prices", "E7F0F3", TEAL, 17, 12)
    add_box(slide, 9.66, 2.92, 2.95, 1.18, "SILVER", "Typed, validated, rejected", "E7F0F3", BLUE, 17, 12)
    add_box(slide, 9.66, 4.49, 2.95, 1.18, "GOLD", "Positions, valuation, P&L", "E7F0F3", ORANGE, 17, 12)
    add_arrow(slide, 11.14, 2.55, 11.14, 2.84, "95D8CF", 2)
    add_arrow(slide, 11.14, 4.12, 11.14, 4.41, "95D8CF", 2)
    return slide


def build_architecture_deck() -> None:
    presentation = new_deck()
    cover_slide(presentation, "Trading Data &\nDaily P&L Platform", "Evidence-based current architecture and a controlled path from proof of concept to a supportable prototype.", "Technical architecture")

    slide = slide_base(presentation, "Business context and supported scope", "01 | Context", 2)
    add_box(slide, 0.65, 1.45, 3.75, 2.08, "Business outcome", "Turn synthetic trade snapshots and daily equity closes into validated trade history, daily positions, valuation, and a transparent P&L result.", "EFF4F6", TEAL, 17, 13)
    add_box(slide, 4.75, 1.45, 3.75, 2.08, "Verified prototype", "USD cash equities; 10 configured instruments; three UC schemas; six-task Databricks Job; audit, rejects, breaks, notebooks, and SQL.", "EAF3F1", BLUE, 17, 13)
    add_box(slide, 8.85, 1.45, 3.75, 2.08, "Not production-ready", "Yahoo Finance approval, real trade source, production identity/environment, monitoring alerts, retention policy, and deployed dashboard are not evidenced.", "FAF0E5", ORANGE, 17, 13)
    add_text(slide, 0.78, 4.03, 11.8, 0.55, "Initial calculation boundary", 17, NAVY, True)
    add_text(slide, 0.80, 4.58, 11.7, 1.25, "Simple cash equities in USD. No fees, settlement cash, FX, dividends, corporate actions, financing, borrow costs, or derivatives. A cumulative field called unrealized_pnl_proxy is explicitly not true unrealized P&L.", 16, INK)
    add_text(slide, 0.80, 6.25, 11.6, 0.42, "Decision gate: business and market-data owners must approve source, price definition, and calculation boundaries before production claims.", 12, RED, True)

    slide = slide_base(presentation, "Data sources and ingress contracts", "02 | Source systems", 3)
    add_box(slide, 0.65, 1.50, 3.75, 3.65, "Trades | synthetic CSV", "scripts/generate_trades.py\n14 source columns\n1,200-row checked-in sample\nLocal CSV → UC Volume\nFull snapshot; Bronze overwrite\nVersion selected in Silver\n\nReal trade-system connector: not implemented", "EFF4F6", TEAL, 17, 13)
    add_box(slide, 4.78, 1.50, 3.75, 3.65, "Prices | Yahoo Finance", "yfinance.download\nDaily unadjusted Close\n10 configured tickers; USD\nSeven-day default lookback\nRetry transient failures\nMissing expected session fails processing\n\nProvider rights/production approval: unconfirmed", "EAF3F1", BLUE, 17, 13)
    add_box(slide, 8.90, 1.50, 3.75, 3.65, "Reference | in code", "10 instrument_id → symbol mappings\nBook and counterparty are input strings\nNo persisted reference tables\nNo effective-dated mapping history\n\nOwner, cardinality, and reference validation: to be confirmed", "FAF0E5", ORANGE, 17, 13)
    add_text(slide, 0.80, 5.55, 11.7, 0.86, "Source contract gaps: production owner, delivery cutoff/completeness marker, volume/SLA, corrections/cancellations, data rights, retention, and missing-price escalation. Synthetic fixtures are for deterministic tests, not a substitute for an approved source.", 14, INK)

    slide = slide_base(presentation, "High-level solution architecture", "03 | Components", 4)
    add_box(slide, 0.55, 1.62, 2.05, 1.22, "Sources", "CSV + yfinance", "EFF4F6", TEAL, 15, 11)
    add_box(slide, 3.02, 1.35, 2.42, 1.02, "Bronze | Delta", "raw trades / raw closes", "EAF3F1", TEAL, 14, 10)
    add_box(slide, 3.02, 2.70, 2.42, 1.02, "Silver | Delta", "typed + rejects", "EAF3F1", BLUE, 14, 10)
    add_box(slide, 6.12, 1.35, 2.45, 1.02, "Gold | Delta", "positions + valuation", "FAF0E5", ORANGE, 14, 10)
    add_box(slide, 6.12, 2.70, 2.45, 1.02, "Gold | Delta", "daily P&L + breaks + audit", "FAF0E5", ORANGE, 14, 10)
    add_box(slide, 9.28, 1.62, 3.30, 1.22, "Reporting", "read-only notebook + SQL\ndashboard proposed", "EFF4F6", BLUE, 15, 11)
    add_arrow(slide, 2.62, 2.02, 2.98, 1.85)
    add_arrow(slide, 4.23, 2.40, 4.23, 2.67)
    add_arrow(slide, 5.48, 3.19, 6.08, 2.04)
    add_arrow(slide, 7.36, 2.40, 7.36, 2.67)
    add_arrow(slide, 8.62, 3.18, 9.24, 2.22)
    add_box(slide, 0.82, 4.55, 3.55, 1.20, "Compute", "PySpark package: trade_pipeline wheel; six entry points", "F3F6F7", BLUE, 14, 11)
    add_box(slide, 4.88, 4.55, 3.55, 1.20, "Governance", "Unity Catalog cibc; bronze / silver / gold; UC Volume", "F3F6F7", TEAL, 14, 11)
    add_box(slide, 8.92, 4.55, 3.55, 1.20, "Control", "Databricks Workflows; audit; rejects; run-level breaks", "F3F6F7", ORANGE, 14, 11)
    add_text(slide, 0.82, 6.10, 11.5, 0.48, "Current physical data model has 10 Delta tables. No separate reporting schema, dashboard dataset, or persisted reference schema is present.", 12, MUTED)

    slide = slide_base(presentation, "End-to-end flow and workflow dependencies", "04 | Orchestration", 5)
    nodes = [(0.55, 1.75, "ingest_prices", "Yahoo → Bronze"), (3.25, 1.75, "process_prices", "Bronze → Silver"), (0.55, 3.25, "ingest_trades", "Volume → Bronze"), (3.25, 3.25, "process_trades", "Bronze → Silver"), (6.40, 2.50, "build_positions", "Silver trades + prices → Gold positions"), (9.85, 2.50, "calculate_daily_pnl", "positions + trades + prices → Gold P&L")]
    for x, y, title, body in nodes:
        add_box(slide, x, y, 2.35 if x < 6 else 2.80, 0.92, title, body, "EFF4F6", TEAL if "prices" in title else BLUE, 13, 9)
    add_arrow(slide, 2.94, 2.20, 3.20, 2.20)
    add_arrow(slide, 2.94, 3.70, 3.20, 3.70)
    add_arrow(slide, 5.65, 2.20, 6.34, 2.75)
    add_arrow(slide, 5.65, 3.70, 6.34, 3.10)
    add_arrow(slide, 9.25, 2.96, 9.80, 2.96)
    add_text(slide, 0.75, 4.72, 11.8, 0.56, "Actual DAG: two independent source branches; build_positions joins both Silver outputs; calculate_daily_pnl follows positions.", 15, NAVY, True)
    add_text(slide, 0.75, 5.43, 11.8, 0.94, "Job controls: max_concurrent_runs=1; parameters include tickers, inclusive start/end dates, and trade_input_path. Weekday 18:00 America/New_York schedule is configured PAUSED; development Bundle mode keeps schedule paused.", 13, INK)

    slide = slide_base(presentation, "Logical model and physical grains", "05 | Data model", 6)
    add_box(slide, 0.55, 1.48, 2.48, 1.38, "Silver trades", "trade_id (one current version)\nbook / counterparty / instrument\nsigned quantity, trade price", "EAF3F1", BLUE, 15, 11)
    add_box(slide, 0.55, 3.45, 2.48, 1.38, "Silver prices", "symbol + price_date\nclose DECIMAL(18,6)\nUSD; source/run lineage", "EAF3F1", TEAL, 15, 11)
    add_box(slide, 4.05, 2.34, 2.65, 1.55, "Gold positions", "date + book + counterparty\n+ instrument + currency\ncumulative signed quantity\nexact-date mark / market value", "FAF0E5", ORANGE, 14, 11)
    add_box(slide, 7.72, 2.34, 2.48, 1.55, "Gold daily P&L", "date + book + symbol\n+ currency\nopening/closing, marks, P&L", "FAF0E5", ORANGE, 14, 11)
    add_box(slide, 10.78, 1.48, 2.00, 1.38, "Audit", "run + task attempt\nstatus / metrics / error", "EFF4F6", BLUE, 14, 10)
    add_box(slide, 10.78, 3.45, 2.00, 1.38, "Breaks", "run + instrument/date\n+ break type (or trade)", "FAF0E6", ORANGE, 14, 10)
    add_arrow(slide, 3.06, 2.18, 4.00, 2.74)
    add_arrow(slide, 3.06, 4.10, 4.00, 3.48)
    add_arrow(slide, 6.74, 3.12, 7.67, 3.12)
    add_arrow(slide, 9.14, 3.96, 10.73, 4.06)
    add_text(slide, 0.76, 5.38, 11.9, 1.05, "Physical definitions: 10 DDL tables; Bronze preserves raw strings, Silver uses typed DATE/TIMESTAMP/DECIMAL, Gold uses DECIMAL(28,6) quantities and DECIMAL(38,6) values. Business keys are documented assumptions, not declared database primary-key constraints. No partitioning/clustering is configured.", 13, INK)

    slide = slide_base(presentation, "Position and P&L calculation design", "06 | Financial logic", 7)
    add_box(slide, 0.65, 1.52, 5.75, 3.72, "Position calculation | implemented", "BUY quantity is positive; SELL quantity is negative.\n\nDaily net = SUM(signed trade quantity)\nClosing position = cumulative SUM(daily net)\nMarket value = closing position × same-day close\n\nGrain: date / book / counterparty / instrument / currency.\nNo-trade sessions carry the position. Price gaps leave value NULL and create a break.", "EAF3F1", TEAL, 17, 13)
    add_box(slide, 6.85, 1.52, 5.75, 3.72, "Daily P&L | simplified, implemented", "Opening quantity = closing quantity − daily net quantity\n\nDaily P&L = OpeningQty × (CurrentClose − PreviousClose)\n+ (DailyNetQty × CurrentClose − signed trade consideration)\n\nThe trade-day term captures execution-to-close economics without applying the full day's price move to new quantity. Missing current close, or missing prior baseline for nonzero opening inventory, yields NULL + break.", "FAF0E5", ORANGE, 16, 12)
    add_text(slide, 0.78, 5.65, 11.7, 0.90, "Example: opening 10 shares; buy 5 at 106; prior close 105; current close 108. Price move = 10×3 = 30; trade-day term = 5×108−5×106 = 10; daily P&L = 40. Cash, fees, realized/unrealized accounting, FX, corporate actions, and accounting sign-off are outside this prototype.", 12, INK)

    slide = slide_base(presentation, "Deployment architecture and operational controls", "07 | Deploy and operate", 8)
    add_box(slide, 0.55, 1.56, 2.65, 1.56, "Source repo", "Python package + notebooks\npytest suite\nBundle YAML", "EFF4F6", BLUE, 15, 11)
    add_box(slide, 3.68, 1.56, 2.65, 1.56, "Asset Bundle", "Build wheel\nvalidate / deploy\ncurrent target: dev", "EAF3F1", TEAL, 15, 11)
    add_box(slide, 6.80, 1.56, 2.65, 1.56, "Databricks Job", "Six wheel tasks\nserverless environment\nweekday schedule paused", "FAF0E5", ORANGE, 15, 11)
    add_box(slide, 9.93, 1.56, 2.80, 1.56, "Unity Catalog", "cibc schemas + Volume\nmanaged Delta tables\ngrants template", "EFF4F6", BLUE, 15, 11)
    add_arrow(slide, 3.24, 2.33, 3.63, 2.33)
    add_arrow(slide, 6.36, 2.33, 6.75, 2.33)
    add_arrow(slide, 9.48, 2.33, 9.88, 2.33)
    add_box(slide, 0.75, 4.12, 3.70, 1.52, "Current operational signals", "Task audit status/counts/errors\nSilver reject history\nRun-level valuation breaks", "EAF3F1", TEAL, 15, 11)
    add_box(slide, 4.80, 4.12, 3.70, 1.52, "Current gaps", "No automated alerting\nNo CI / non-dev target\nSchedule paused; principal placeholder", "FAF0E5", ORANGE, 15, 11)
    add_box(slide, 8.85, 4.12, 3.70, 1.52, "Readiness gates", "Provider + data rights\nLeast privilege + support owner\nRerun/UAT + retention/RTO/RPO", "EFF4F6", BLUE, 15, 11)
    add_text(slide, 0.80, 6.10, 11.7, 0.55, "Existing scripts/deploy.sh uses Databricks CLI and the Bundle. No production Bundle target or promotion pipeline is present; all future deployment architecture is proposed.", 12, MUTED)

    presentation.save(OUT / "Trading_Platform_Architecture.pptx")


def build_delivery_deck() -> None:
    presentation = new_deck()
    cover_slide(presentation, "Delivery plan &\nproject controls", "A compact, evidence-based SDLC plan for closing prototype gaps without presenting planned work as delivered.", "Executive delivery overview")

    slide = slide_base(presentation, "Project overview | evidence before roadmap", "01 | Delivery baseline", 2)
    add_box(slide, 0.65, 1.50, 3.72, 2.18, "Problem", "Reconcile synthetic equity trades and daily prices into positions, valuation, P&L, and visible operational exceptions.", "EFF4F6", TEAL, 17, 13)
    add_box(slide, 4.78, 1.50, 3.72, 2.18, "Already implemented", "Six-task Databricks Workflow; 10 Delta tables; trade/price validation; positions; simplified daily P&L; audit/rejects; notebooks/SQL.", "EAF3F1", BLUE, 17, 13)
    add_box(slide, 8.90, 1.50, 3.72, 2.18, "Next delivery", "Approved source contracts, trade snapshot semantics, CI/deploy controls, complete rerun evidence, dashboard MVP, and runbook/alerts.", "FAF0E5", ORANGE, 17, 13)
    add_text(slide, 0.82, 4.14, 11.6, 0.48, "Dated verification: 2026-10-11 run completed all six tasks; 5,990 position rows and 835 P&L rows reconciled with zero reported calculation mismatches.", 15, NAVY, True)
    add_text(slide, 0.82, 4.90, 11.7, 0.94, "Caveat: the same-parameter second run was queued and has no recorded result. The configured schedule is paused and this is a proof of concept, not a production service.", 14, RED)

    slide = slide_base(presentation, "SDLC approach | decisions flow into evidence", "02 | Requirements and traceability", 3)
    stages = [(0.62, "Discover", "Stakeholders\nSource contracts\nScope and RAID"), (3.18, "Specify", "BR / FR / NFR\nData dictionary\nCalculation policy"), (5.74, "Design", "Architecture\nDAG / table grains\nSecurity boundary"), (8.30, "Build & verify", "Stories / subtasks\nTests / reconciliation\nPR + Bundle"), (10.86, "Release", "UAT / readiness\nRunbook / monitor\nControlled schedule")]
    for x, title, body in stages:
        add_box(slide, x, 1.75, 1.85, 2.10, title, body, "EFF4F6" if x != 8.30 else "EAF3F1", TEAL if x < 5 else BLUE, 14, 11)
    for start in [2.50, 5.06, 7.62, 10.18]:
        add_arrow(slide, start, 2.80, start + 0.58, 2.80)
    add_text(slide, 0.78, 4.55, 11.8, 0.90, "Traceability is maintained from BR-001..008 → FR-001..013 → TMP story IDs → TC test cases → acceptance evidence. Decisions and gaps stay explicit in the workbook RAID and decision log.", 14, INK)
    add_text(slide, 0.78, 5.68, 11.8, 0.50, "Definition of Ready: owner, input contract, dependencies, AC, estimate, and test evidence agreed. Definition of Done: reviewed code/config, tests passed (skips disclosed), reconciled data, deploy evidence, docs, and operational owner.", 11, MUTED)

    slide = slide_base(presentation, "Jira structure | implementation status is separate", "03 | Backlog", 4)
    add_box(slide, 0.78, 1.48, 3.10, 1.08, "13 epics", "SDLC stages and engineering capabilities", "EFF4F6", TEAL, 16, 11)
    add_box(slide, 5.12, 1.48, 3.10, 1.08, "24 stories", "Requirements, design, build, test, operations", "EAF3F1", BLUE, 16, 11)
    add_box(slide, 9.46, 1.48, 3.10, 1.08, "48 subtasks", "Specific engineering outputs under stories", "FAF0E5", ORANGE, 16, 11)
    add_arrow(slide, 3.95, 2.00, 5.03, 2.00)
    add_arrow(slide, 8.29, 2.00, 9.37, 2.00)
    add_text(slide, 0.84, 3.02, 11.3, 0.40, "Planning IDs are temporary and consistent across workbook, decks, and Jira CSV.", 14, NAVY, True)
    status_cards = [("IMPLEMENTED", "Done in current repo/runtime evidence", PALE_TEAL, TEAL), ("PARTIAL", "Code exists; evidence/control gap remains", PALE_ORANGE, ORANGE), ("PLANNED", "Backlog only; not delivered", PALE_RED, RED)]
    for index, (title, body, fill, accent) in enumerate(status_cards):
        add_box(slide, 0.82 + index * 4.18, 3.68, 3.74, 1.45, title, body, fill, accent, 15, 11)
    add_text(slide, 0.84, 5.65, 11.5, 0.70, "CSV import mapping: Planning Key should map to a custom field; Issue Type, Summary, Description, Priority, Story Points, and Status to Jira fields; Epic Name/Epic Link/Parent ID to hierarchy fields. Import epics first and resolve temporary Epic Link values to generated Jira keys if required by the Jira edition.", 11, INK)

    slide = slide_base(presentation, "Dependency-driven execution sequence", "04 | Critical path", 5)
    flow = [(0.40, 1.55, "Source + scope\nTMP-001/002/006/007"), (3.02, 1.55, "Data / security\nTMP-003/005/008"), (5.65, 1.55, "Controls + history\nTMP-010/012/016"), (8.30, 1.55, "Reporting + UAT\nTMP-020/022"), (10.95, 1.55, "Handover\nTMP-024")]
    for x, y, label in flow:
        add_box(slide, x, y, 2.02, 1.02, label, "", "EFF4F6", TEAL, 12, 10)
    for x in [2.46, 5.09, 7.74, 10.39]:
        add_arrow(slide, x, 2.06, x + 0.50, 2.06)
    add_box(slide, 1.00, 3.28, 4.95, 1.32, "Parallel workstream A | trades", "Trade contract → Bronze snapshot → Silver versioning → position/P&L correction reconciliation", "EAF3F1", BLUE, 14, 11)
    add_box(slide, 7.28, 3.28, 4.95, 1.32, "Parallel workstream B | prices", "Provider decision → Bronze prices → Silver completeness → position valuation / P&L baseline", "EFF4F6", TEAL, 14, 11)
    add_text(slide, 0.95, 5.12, 11.5, 0.84, "Critical path: business/source decisions → supported integration and rerun evidence → safe deployment controls → dashboard/UAT → handover. Dashboard build can start only after reporting grain, access, and price/trade contracts are agreed.", 13, NAVY, True)

    slide = slide_base(presentation, "Proposed two-week sprint plan", "05 | Eight-week baseline", 6)
    for index, sprint in enumerate(PLAN["sprints"]):
        x = 0.66 + (index % 2) * 6.25
        y = 1.45 + (index // 2) * 2.45
        story_text = ", ".join(sprint["stories"])
        add_box(slide, x, y, 5.78, 1.78, f"{sprint['name']} | {sprint['weeks']}", f"{sprint['goal']}\n\n{story_text} | 10 SP", "EFF4F6" if index % 2 == 0 else "EAF3F1", TEAL if index % 2 == 0 else BLUE, 15, 11)
    add_text(slide, 0.82, 6.43, 11.6, 0.36, "40 SP / 8 weeks = assumed 10 SP per sprint. No reserve; relative weeks, not approved delivery dates. Use one contingency sprint if source/access decisions slip.", 11, RED, True)

    slide = slide_base(presentation, "Principal project risks and controls", "06 | RAID", 7)
    risks = [
        ("Provider approval", "High / High", "Yahoo Finance is current prototype only; data rights and SLA unconfirmed.", "Select approved source; keep production blocked until sign-off."),
        ("Snapshot semantics", "Medium / High", "Trade IDs absent from a newer file are not deleted from Silver.", "Agree cancellation/deletion rule; test two-snapshot reconciliation."),
        ("Capacity and decisions", "Medium / High", "40 SP baseline has no reserve; part-time owners and runtime access gate work.", "Book owners at kickoff; defer polish or add one sprint."),
        ("Operational readiness", "High / High", "Dev only, paused schedule, no automated alerts or production identity.", "Readiness gate; least privilege, alert test, runbook and sign-off."),
    ]
    for index, (title, rating, description, response) in enumerate(risks):
        y = 1.42 + index * 1.22
        add_box(slide, 0.68, y, 2.52, 0.91, title, rating, "FAF0E5", ORANGE, 13, 10)
        add_text(slide, 3.48, y + 0.02, 4.15, 0.84, description, 12, INK)
        add_text(slide, 7.83, y + 0.02, 4.72, 0.84, response, 12, NAVY, True)

    slide = slide_base(presentation, "Delivery gates and immediate next steps", "07 | Decisions", 8)
    add_box(slide, 0.70, 1.52, 3.74, 2.05, "1 | Confirm owners", "Trade source and correction semantics\nMarket-data rights / provider\nProduct/reporting audience\nPlatform identity and test runtime", "EFF4F6", TEAL, 16, 12)
    add_box(slide, 4.80, 1.52, 3.74, 2.05, "2 | Close evidence gaps", "Run same-input Databricks job twice\nExecute Spark/Delta integration tests\nCapture duration and failure recovery\nApprove NFR / retention / recovery targets", "EAF3F1", BLUE, 16, 12)
    add_box(slide, 8.90, 1.52, 3.74, 2.05, "3 | Gate release", "Complete dashboard UAT\nReview access and alerts\nHandover runbook and ownership\nKeep schedule paused until readiness sign-off", "FAF0E5", ORANGE, 16, 12)
    add_text(slide, 0.82, 4.30, 11.6, 0.52, "Milestones: W2 scope/requirements | W4 source/model | W6 controls/history | W8 UAT/handover.", 15, NAVY, True)
    add_text(slide, 0.82, 5.05, 11.6, 1.10, "Interview framing: a real prototype already demonstrates the Medallion flow and financial calculation checks. The plan is about controlled completion: approved sources, explicit business semantics, reproducible tests/deployments, user acceptance, and operations. Do not claim production delivery, a dashboard, or a successful rerun that is not evidenced.", 14, INK)

    presentation.save(OUT / "Trading_Platform_Delivery_Overview.pptx")


def write_readme(dictionary_count: int, jira_count: int) -> None:
    content = f'''# Trading Platform SDLC Deliverables

Generated on {AS_OF} from the repository implementation and the structured planning data in `generate_deliverables.py`.

## Files

- `Trading_Platform_Project_Plan.xlsx` — executive summary, business/functional/NFR requirements, sources, DDL-derived dictionary ({dictionary_count} physical columns across 10 tables), backlog, acceptance criteria, dependencies, sprints/Gantt, milestones, RAID, traceability, tests, and current-state evidence.
- `Trading_Platform_Architecture.pptx` — current and target architecture, sources, data flow, model, calculations, workflow, deployment, and explicit gaps.
- `Trading_Platform_Delivery_Overview.pptx` — executive delivery plan, traceability, Jira hierarchy, critical path, sprint plan, risks, current status, and next steps.
- `jira-backlog.csv` — {jira_count} Jira planning rows (epics, stories, subtasks) with acceptance criteria and dependencies.
- `Trading_Platform_Project_Deliverables.html` — a self-contained, navigable consolidation of every workbook worksheet, presentation text, Jira CSV record, and this README.
- `generate_deliverables.py` — single structured source for requirements, stories, dependencies, status, sprint plan, risks, decisions, test plan, and presentation content. The dictionary is extracted from `database/tables/**/*.sql` at generation time.

## Regenerate

From the repository root, install the two document libraries into the selected Python environment and run:

```powershell
python -m pip install openpyxl python-pptx
python docs/project-management/generate_deliverables.py
```

The generator uses the repository's physical table DDL as the data dictionary schema source. Update the `PLAN`, `TABLE_META`, or `COLUMN_META` records in the generator when requirements or enriched descriptions change; rerun it to regenerate all deliverables. No application code is changed.

## Evidence and unsupported assumptions

- Verified current implementation is distinguished from partial, planned, and unconfirmed work throughout the workbook and decks.
- The checked-in test report records a successful six-task Databricks run on 2026-10-11, including independent position/P&L reconciliation; the attempted second same-parameter run was still queued and is therefore unverified.
- A focused local test run confirmed 3 failures from stale references to removed `trades_constraints.sql` and `market_prices_constraints.sql`; the current CHECK constraints are consolidated into the Silver table DDL. The full suite summary was not captured. Spark/Delta tests require a supported runtime; do not represent skips as passes.
- Yahoo Finance is the current prototype provider, not an assumed approved banking data source. Trade data is synthetic. Production market-data rights, source contracts, volume/SLA, security identity, environments, retention, RTO/RPO, dashboard audience, and support ownership require confirmation.
- The eight-week schedule is relative and proposed, assumes 10 story points per sprint, and has no reserve. Dates are not commitments. The configured Databricks schedule remains paused until operational readiness approval.

## Jira import mapping

This is a planning CSV, not a guaranteed one-click Jira import. `Issue ID` and `Planning Key` values are temporary identifiers, not Jira-generated issue keys. Map `Planning Key` to a text custom field; map `Issue Type`, `Summary`, `Description`, `Priority`, `Status`, `Story Points`, and `Sprint` to the destination Jira fields. Map `Epic Name` and `Epic Link` to the destination edition's epic fields. Map `Parent ID` to the parent relationship field for subtasks; some Jira importers require numeric issue IDs or a separate import pass. Import epics first, capture created Jira keys, then resolve temporary Epic Link references before importing stories and subtasks. `Dependencies` contains planning IDs and should be imported as issue links only after IDs resolve. Confirm required fields, status names, story-point field, hierarchy support, and project permissions with the Jira administrator before import.

## Workbook notes

Filters, frozen headers, dropdown validations, conditional formatting, formula-driven sprint totals, and a formula/conditional-format Gantt are included. Excel recalculates formulas when opened. The Gantt uses relative weeks 1-8; no kickoff date was supplied.
'''
    (OUT / "README.md").write_text(content, encoding="utf-8")


def write_combined_html() -> None:
    workbook = load_workbook(OUT / "Trading_Platform_Project_Plan.xlsx", data_only=False)
    jira_rows_read = list(csv.DictReader((OUT / "jira-backlog.csv").open(encoding="utf-8-sig", newline="")))
    deck_files = [
        ("Architecture presentation", OUT / "Trading_Platform_Architecture.pptx"),
        ("Delivery overview presentation", OUT / "Trading_Platform_Delivery_Overview.pptx"),
    ]

    def escape(value: object) -> str:
        return html_lib.escape("" if value is None else str(value), quote=True).replace("\n", "<br>")

    def slug(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")

    parts = [
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">",
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">",
        f"<meta name=\"workbook-sheets\" content=\"{len(workbook.worksheets)}\">",
        f"<meta name=\"architecture-slides\" content=\"{len(Presentation(deck_files[0][1]).slides)}\">",
        f"<meta name=\"delivery-slides\" content=\"{len(Presentation(deck_files[1][1]).slides)}\">",
        f"<meta name=\"jira-records\" content=\"{len(jira_rows_read)}\">",
        "<title>Trading Platform | Complete Project Deliverables</title>",
        """<style>
        :root{--navy:#16324f;--teal:#007c78;--blue:#2c6e9b;--orange:#c8752d;--ink:#233746;--muted:#647786;--paper:#f3f6f7;--line:#d7e0e5;--white:#fff}
        *{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--paper);color:var(--ink);font:15px/1.55 Aptos,"Segoe UI",sans-serif}
        header{background:var(--navy);color:var(--white);padding:40px max(5vw,24px);border-bottom:8px solid var(--teal)}header h1{font-size:32px;line-height:1.15;margin:0 0 10px}header p{margin:4px 0;color:#d8e4e9;max-width:950px}
        .links{display:flex;gap:10px;flex-wrap:wrap;margin-top:20px}.links a{color:var(--navy);background:#e1f1ef;padding:8px 12px;border-radius:4px;text-decoration:none;font-weight:700}
        nav{position:sticky;top:0;background:#fff;border-bottom:1px solid var(--line);z-index:2;padding:10px max(5vw,24px);display:flex;gap:7px;flex-wrap:wrap}nav a{font-size:12px;color:var(--navy);text-decoration:none;padding:5px 8px;border:1px solid var(--line);border-radius:3px}
        main{max-width:1500px;margin:26px auto;padding:0 22px}section{background:#fff;border:1px solid var(--line);margin:0 0 22px;padding:22px 24px;box-shadow:0 2px 8px #16324f0b}h2{font-size:23px;color:var(--navy);margin:0 0 14px;border-bottom:2px solid var(--teal);padding-bottom:8px}h3{font-size:17px;color:var(--blue);margin:22px 0 8px}.note{background:#eef4f6;border-left:4px solid var(--orange);padding:12px 15px}
        .table-wrap{overflow:auto;max-height:75vh;border:1px solid var(--line)}table{border-collapse:collapse;width:max-content;min-width:100%;font-size:12px}th,td{text-align:left;vertical-align:top;border:1px solid var(--line);padding:7px 9px;max-width:580px;white-space:normal;overflow-wrap:anywhere}th{position:sticky;top:0;background:var(--navy);color:#fff;z-index:1}tbody tr:nth-child(even){background:#f5f8fa}.flow{font-family:Consolas,monospace;background:#eef4f6;padding:14px;overflow:auto;white-space:pre-wrap;border-left:4px solid var(--teal)}.slide{border:1px solid var(--line);margin:14px 0;padding:12px 16px;background:#fbfcfc}.slide h3{margin:0 0 8px}.slide p{margin:7px 0;white-space:pre-wrap}.readme{white-space:pre-wrap;overflow-wrap:anywhere;background:#f5f8fa;padding:15px;font:13px/1.5 Consolas,monospace}
        footer{padding:28px;text-align:center;color:var(--muted);font-size:12px}@media print{nav,.links{display:none}body{background:#fff}main{max-width:none;margin:0;padding:0}section{box-shadow:none;break-inside:auto}table{font-size:9px}th{position:static}.table-wrap{max-height:none;overflow:visible}}
        </style></head><body>""",
        f"<header><h1>Trading Platform Project Deliverables</h1><p>Complete textual and tabular consolidation of the project plan workbook, architecture and delivery presentations, Jira import file, and regeneration README.</p><p>Evidence baseline: {AS_OF}. Generated from the repository implementation. Implementation status is separated from proposed work.</p><div class=\"links\"><a href=\"Trading_Platform_Project_Plan.xlsx\">Excel workbook</a><a href=\"Trading_Platform_Architecture.pptx\">Architecture presentation</a><a href=\"Trading_Platform_Delivery_Overview.pptx\">Delivery overview</a><a href=\"jira-backlog.csv\">Jira CSV</a><a href=\"README.md\">Regeneration README</a></div></header>",
    ]

    toc = [("Architecture flow", "architecture-flow"), ("Workbook", "workbook-content"), ("Presentations", "presentation-content"), ("Jira import", "jira-content"), ("Regeneration README", "readme-content")]
    parts.append("<nav>" + "".join(f'<a href="#{anchor}">{escape(label)}</a>' for label, anchor in toc) + "</nav><main>")
    parts.append("<section id=\"architecture-flow\"><h2>Architecture and workflow diagrams</h2><p class=\"note\">The presentations below retain every text element. These compact flow views make the connector-based diagrams readable in a single HTML document.</p><h3>High-level solution flow</h3><div class=\"flow\">Trade CSV in Unity Catalog Volume ─┐\n                                   ├─> Bronze Delta ─> Silver Delta ─> Gold positions / valuation / P&amp;L\nYahoo Finance daily closes ────────┘                                      └─> notebook + SQL reporting\n                                                                            └─> dashboard (PLANNED)\n\nGovernance: Unity Catalog cibc | schemas bronze / silver / gold | Databricks Workflows | Python wheel</div><h3>Workflow dependencies</h3><div class=\"flow\">ingest_prices ─> process_prices ─┐\ningest_trades ─> process_trades ─┴─> build_positions ─> calculate_daily_pnl</div><h3>Deployment path</h3><div class=\"flow\">Repository + pytest ─> Databricks Asset Bundle (wheel build / validate / deploy) ─> dev Job ─> Unity Catalog Delta tables\nCurrent: dev target only; schedule PAUSED. CI promotion, production target, and production identity are PLANNED / TO BE CONFIRMED.</div></section>")

    parts.append("<section id=\"workbook-content\"><h2>Workbook content</h2><p>All cell values and formulas are reproduced below. Excel filters, dropdowns, conditional formatting, chart fills, and layout styling remain in the linked workbook.</p>")
    for sheet in workbook.worksheets:
        parts.append(f'<h3 id="sheet-{slug(sheet.title)}">{escape(sheet.title)}</h3><div class="table-wrap"><table><thead><tr>')
        headers = [cell.value for cell in sheet[1]]
        parts.extend(f"<th>{escape(header)}</th>" for header in headers)
        parts.append("</tr></thead><tbody>")
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(value is not None for value in row):
                continue
            parts.append("<tr>" + "".join(f"<td>{escape(value)}</td>" for value in row) + "</tr>")
        parts.append("</tbody></table></div>")
    parts.append("</section><section id=\"presentation-content\"><h2>Presentation content</h2><p>Slide text is preserved in slide order. Layout and connector graphics are summarized in the architecture flow above; the linked PowerPoint files retain the original visual design.</p>")
    for deck_title, deck_path in deck_files:
        deck = Presentation(deck_path)
        parts.append(f"<h3>{escape(deck_title)}</h3>")
        for slide_number, slide in enumerate(deck.slides, start=1):
            texts = [shape.text.strip() for shape in slide.shapes if shape.has_text_frame and shape.text.strip()]
            parts.append(f'<article class="slide"><h3>Slide {slide_number}</h3>')
            for text in texts:
                parts.append(f"<p>{escape(text)}</p>")
            parts.append("</article>")
    parts.append("</section><section id=\"jira-content\"><h2>Jira import content</h2><p>All logical CSV records are reproduced below, including multiline descriptions and acceptance criteria.</p><div class=\"table-wrap\"><table><thead><tr>")
    jira_headers = list(jira_rows_read[0]) if jira_rows_read else []
    parts.extend(f"<th>{escape(header)}</th>" for header in jira_headers)
    parts.append("</tr></thead><tbody>")
    for row in jira_rows_read:
        parts.append("<tr>" + "".join(f"<td>{escape(row.get(header, ''))}</td>" for header in jira_headers) + "</tr>")
    parts.append("</tbody></table></div></section>")

    readme = (OUT / "README.md").read_text(encoding="utf-8")
    parts.append(f'<section id="readme-content"><h2>Regeneration README</h2><div class="readme">{escape(readme)}</div></section>')
    parts.append(f"</main><footer>Complete project content | {AS_OF} | {len(workbook.worksheets)} workbook sheets | {sum(len(Presentation(path).slides) for _, path in deck_files)} slides | {len(jira_rows_read)} Jira rows</footer></body></html>")
    (OUT / "Trading_Platform_Project_Deliverables.html").write_text("".join(parts), encoding="utf-8")


def validate_outputs(dictionary_count: int) -> None:
    xlsx_path = OUT / "Trading_Platform_Project_Plan.xlsx"
    workbook = load_workbook(xlsx_path, data_only=False)
    expected_sheets = {"Executive Summary", "Business Req", "Functional Req", "NFRs", "Data Sources", "Data Dictionary", "Epics", "Stories", "Subtasks", "Acceptance Criteria", "Dependencies", "Sprints", "Gantt", "Milestones", "RAID", "Traceability", "Test Plan", "Current State"}
    if set(workbook.sheetnames) != expected_sheets:
        raise RuntimeError(f"Workbook worksheet mismatch: {workbook.sheetnames}")
    if workbook["Data Dictionary"].max_row - 1 != dictionary_count:
        raise RuntimeError("Data Dictionary row count does not match DDL extraction")
    if not str(workbook["Sprints"]["E2"].value).startswith("=SUMIF"):
        raise RuntimeError("Sprint point formula missing")
    for name in ["Trading_Platform_Architecture.pptx", "Trading_Platform_Delivery_Overview.pptx"]:
        deck = Presentation(OUT / name)
        if len(deck.slides) < 7:
            raise RuntimeError(f"Presentation appears incomplete: {name}")
    with (OUT / "jira-backlog.csv").open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != len(jira_rows()) or not {"Epic", "Story", "Sub-task"}.issubset({row["Issue Type"] for row in rows}):
        raise RuntimeError("Jira CSV row types/count failed validation")
    html_document = (OUT / "Trading_Platform_Project_Deliverables.html").read_text(encoding="utf-8")
    if f'name="workbook-sheets" content="{len(expected_sheets)}"' not in html_document:
        raise RuntimeError("Combined HTML workbook sheet count failed validation")
    if f'name="jira-records" content="{len(rows)}"' not in html_document:
        raise RuntimeError("Combined HTML Jira row count failed validation")
    if any(f'id="sheet-{re.sub(r"[^a-z0-9]+", "-", sheet.lower()).strip("-")}"' not in html_document for sheet in expected_sheets):
        raise RuntimeError("Combined HTML is missing a workbook worksheet")

    def html_value(value: object) -> str:
        return html_lib.escape("" if value is None else str(value), quote=True).replace("\n", "<br>")

    for sheet in workbook.worksheets:
        for row in sheet.iter_rows(values_only=True):
            for value in row:
                if value is not None and html_value(value) not in html_document:
                    raise RuntimeError(f"Combined HTML is missing workbook content from {sheet.title}")
    for deck_name in ["Trading_Platform_Architecture.pptx", "Trading_Platform_Delivery_Overview.pptx"]:
        for slide in Presentation(OUT / deck_name).slides:
            for shape in slide.shapes:
                if shape.has_text_frame and shape.text.strip() and html_value(shape.text.strip()) not in html_document:
                    raise RuntimeError(f"Combined HTML is missing presentation text from {deck_name}")
    for row in rows:
        for value in row.values():
            if value and html_value(value) not in html_document:
                raise RuntimeError("Combined HTML is missing Jira CSV content")
    readme_content = (OUT / "README.md").read_text(encoding="utf-8")
    if html_value(readme_content) not in html_document:
        raise RuntimeError("Combined HTML is missing regeneration README content")
    story_ids = {story["id"] for story in PLAN["stories"]}
    epic_ids = {epic["id"] for epic in PLAN["epics"]}
    if len(story_ids) != len(PLAN["stories"]) or len(epic_ids) != len(PLAN["epics"]):
        raise RuntimeError("Planning IDs must be unique")
    graph = {story["id"]: story["depends"] for story in PLAN["stories"]}
    visited, active = set(), set()

    def visit(issue_id: str) -> None:
        if issue_id in active:
            raise RuntimeError(f"Dependency cycle detected at {issue_id}")
        if issue_id in visited:
            return
        active.add(issue_id)
        for predecessor in graph[issue_id]:
            if predecessor not in graph:
                raise RuntimeError(f"Unknown dependency in {issue_id}: {predecessor}")
            visit(predecessor)
        active.remove(issue_id)
        visited.add(issue_id)

    for story in PLAN["stories"]:
        if story["epic"] not in epic_ids:
            raise RuntimeError(f"Unknown epic on {story['id']}")
        if not story["criteria"] or not story["points"]:
            raise RuntimeError(f"Story lacks acceptance criteria or estimate: {story['id']}")
        visit(story["id"])
    for functional in PLAN["functional_requirements"]:
        if not functional["stories"]:
            raise RuntimeError(f"Functional requirement lacks story mapping: {functional['id']}")
        if any(story_id not in story_ids for story_id in functional["stories"]):
            raise RuntimeError(f"Functional requirement has an unknown story: {functional['id']}")
        parent = next((requirement for requirement in PLAN["business_requirements"] if requirement["id"] == functional["br"]), None)
        if parent is None or functional["id"] not in parent["fr"]:
            raise RuntimeError(f"Broken BR-to-FR traceability: {functional['id']}")
    for requirement in PLAN["business_requirements"]:
        if any(fr_id not in {fr["id"] for fr in PLAN["functional_requirements"]} for fr_id in requirement["fr"]):
            raise RuntimeError(f"Business requirement has an unknown functional requirement: {requirement['id']}")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    dictionary = get_table_ddl()
    build_workbook(dictionary)
    build_architecture_deck()
    build_delivery_deck()
    build_jira_csv()
    write_readme(len(dictionary), len(jira_rows()))
    write_combined_html()
    validate_outputs(len(dictionary))
    print(f"Generated and validated deliverables in {OUT}")
    print(f"DDL columns: {len(dictionary)} across {len(TABLE_META)} tables")
    print(f"Jira planning rows: {len(jira_rows())} ({len(PLAN['epics'])} epics, {len(PLAN['stories'])} stories, {len(list_subtasks())} subtasks)")


if __name__ == "__main__":
    main()