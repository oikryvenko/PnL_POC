# Trading Platform SDLC Deliverables

Generated on 2026-10-14 from the repository implementation and the structured planning data in `generate_deliverables.py`.

## Files

- `Trading_Platform_Project_Plan.xlsx` — executive summary, business/functional/NFR requirements, sources, DDL-derived dictionary (138 physical columns across 10 tables), backlog, acceptance criteria, dependencies, sprints/Gantt, milestones, RAID, traceability, tests, and current-state evidence.
- `Trading_Platform_Architecture.pptx` — current and target architecture, sources, data flow, model, calculations, workflow, deployment, and explicit gaps.
- `Trading_Platform_Delivery_Overview.pptx` — executive delivery plan, traceability, Jira hierarchy, critical path, sprint plan, risks, current status, and next steps.
- `jira-backlog.csv` — 85 Jira planning rows (epics, stories, subtasks) with acceptance criteria and dependencies.
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
