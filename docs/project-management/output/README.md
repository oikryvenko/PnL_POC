# Trading Platform SDLC Deliverables

Generated on 2026-10-14 from the repository implementation and `Trading_Project_Master.xlsx`.

## Files

- `Trading_Platform_Project_Plan.xlsx` — requirements, source specifications, master-sourced data dictionary (138 physical columns), backlog, acceptance criteria, dependencies, sprint capacity, Gantt, milestones, RAID, traceability, tests, and current-state evidence.
- `Trading_Platform_Architecture.pptx` — current and target architecture, sources, data flow, model, calculations, workflow, deployment, and explicit gaps.
- `Trading_Platform_Delivery_Overview.pptx` — executive delivery plan plus native-shape eight-week Gantt and dependency detail. CPM dates/float are explicitly not calculated because no activity-level durations/calendar are evidenced.
- `jira-backlog.csv` — 79 Jira planning rows (epics, stories, subtasks) with acceptance criteria and dependencies.
- `Trading_Platform_Project_Deliverables.html` — a self-contained, navigable consolidation of every workbook worksheet, presentation text, Jira CSV record, and this README.
- `Trading_Project_Master.xlsx` — sole editable project-management data source. The generator reads it and never overwrites it.
- `generate_deliverables.py` — validates the master, stages reports in a temporary sibling directory, and publishes to `output/` only after validation succeeds.

## Regenerate

From the repository root, install the two document libraries into the selected Python environment and run:

```powershell
python -m pip install openpyxl python-pptx
python docs/project-management/generate_deliverables.py
```

Edit the master workbook, save it, then run the command from the repository root. Optional `--master-workbook` and `--output-dir` arguments support test copies. Reports publish as one validated set in `docs/project-management/output/`. No application code is changed.

## Data Status

Story implementation statuses from the master: BLOCKED: 1; IMPLEMENTED: 8; NOT VERIFIED: 1; PARTIALLY IMPLEMENTED: 5; PLANNED: 9.

Source decisions are maintained in `Data_Sources`:
- Trade transactions [IMPLEMENTED (synthetic prototype source)]: Real source owner, legal entity/system, delivery format contract, completeness watermark, file naming, correction/deletion semantics, volume/SLA, retention and access classification.
- Daily market prices [IMPLEMENTED (prototype provider; not approved for banking production)]: Market-data owner/provider approval and license, adjustment/close definition, correction policy, availability SLA, symbol mapping, market calendar and missing-data escalation.
- Minimal reference data [PARTIALLY IMPLEMENTED (in-code mappings only)]: Reference-data ownership, effective dating, books/counterparties validation, and need for persisted mappings must be confirmed.

Sprint capacity and optional follow-on scope are maintained in `Sprints`:
Sprint 1: 9/10 SP; Sprint 2: 9/10 SP; Sprint 3: 10/10 SP; Sprint 4: 10/10 SP; Sprint 5 (Optional): 10/10 SP. Timelines are relative; CPM dates and float are not calculated from sprint windows or story points. The development schedule remains paused unless a separate readiness decision is recorded.

## Jira import mapping

This is a planning CSV, not a guaranteed one-click Jira import. `Issue ID` and `Planning Key` values are temporary identifiers, not Jira-generated issue keys. Map `Planning Key` to a text custom field; map `Issue Type`, `Summary`, `Description`, `Priority`, `Status`, `Story Points`, and `Sprint` to the destination Jira fields. Map `Epic Name` and `Epic Link` to the destination edition's epic fields. Map `Parent ID` to the parent relationship field for subtasks; some Jira importers require numeric issue IDs or a separate import pass. Import epics first, capture created Jira keys, then resolve temporary Epic Link references before importing stories and subtasks. `Dependencies` contains planning IDs and should be imported as issue links only after IDs resolve. Confirm required fields, status names, story-point field, hierarchy support, and project permissions with the Jira administrator before import.

## Workbook / Report Notes

The report workbook retains filters, frozen headers, dropdowns, conditional formatting, formulas, and a master-sourced eight-week workstream Gantt. Gantt bars show sprint allocation windows, not task-level durations. The editable master has structured tables and validation. Excel recalculates formulas when opened.
