# Project Management Report Generator

## Source Workbook

Edit [Trading_Project_Master.xlsx](Trading_Project_Master.xlsx). It is the single editable source for project requirements, data sources and dictionary, epics/stories/subtasks, dependencies, sprints, architecture, tests, RAID, milestones, stakeholders/RACI, governance, and decisions.

Normal report generation never writes to the master workbook. Keep a version-controlled backup before changing project data. Do not edit generated files under `output/`; regenerate them from the master instead.

## Editable Worksheets

- `Project_Overview`: title, purpose, evidence baseline, status dashboard, and decisions required.
- `Requirements`: BR, FR, and NFR rows. IDs must be unique; each BR's related IDs must resolve to FR IDs; each FR has a valid parent BR and story references.
- `Data_Sources` and `Data_Dictionary`: source contracts and physical/logical column definitions. The current dictionary has 138 physical columns across 10 tables; update it in the master when definitions change.
- `Jira_Backlog`: epics, stories, and subtasks. Issue IDs are stable planning IDs. Stories require positive story points, acceptance criteria, a valid epic, and valid BR/FR/dependency references. Subtasks require a valid story parent and matching epic.
- `Delivery_Plan`: `WORKSTREAM` rows drive the Gantt; `STORY` rows retain story-level schedule inputs. Activity IDs must be unique and each baseline story must map once. Sprint windows are planning allocations, not activity durations.
- `Milestones`, `RAID`, `Stakeholders_RACI`, `Test_Plan`, `Architecture`, `NFRs`, `Sprints`, `Dependencies`, `Decisions`, `Governance`, `Change_Control`, `TPM_Scenarios`, and `Resource_Capacity` maintain supporting delivery information.

## Relationships and Validation

Separate multiple IDs in a cell with semicolons. `Jira_Backlog.Dependencies` contains predecessor story IDs. `Requirements.Related IDs` links BRs to FRs and FRs to stories. `Delivery_Plan.Story IDs` maps workstreams to stories. Milestone dependencies use story IDs. Architecture task predecessors use `Component ID` values.

The generator checks required sheets and columns, unique requirement/Jira/milestone/architecture IDs, parent and epic relationships, requirement references, dependency references and cycles, acceptance criteria, positive story points, sprint allocations, capacity limits, and workstream mappings. Any critical validation error stops report publication.

## Generate and Validate

Run from the repository root:

```powershell
python docs/project-management/generate_deliverables.py
```

Optional arguments support a test copy of the workbook and a separate output location:

```powershell
python docs/project-management/generate_deliverables.py --master-workbook path/to/test-copy.xlsx --output-dir path/to/test-output
```

The generator reads the workbook with `openpyxl`, builds reports in a temporary directory, validates them, then publishes the output directory only if generation succeeds. If validation fails, the previously published output directory is preserved. The output directory must not contain the master workbook.

## Generated Files

Reports are published to `docs/project-management/output/`:

- `Trading_Platform_Project_Plan.xlsx`
- `Trading_Platform_Architecture.pptx`
- `Trading_Platform_Delivery_Overview.pptx`
- `jira-backlog.csv`
- `Trading_Platform_Project_Deliverables.html`
- Generated report `README.md`

The editable master remains at `docs/project-management/Trading_Project_Master.xlsx` and is never part of the generated output directory.

## Jira Cloud Import

The CSV uses internal planning IDs; these are not Jira issue keys. Map `Issue Type`, `Summary`, `Description`, `Acceptance Criteria`, `Priority`, `Story Points`, `Status`, and `Sprint` to the target project fields. Map `Epic Name`, `Epic Link`, `Parent ID`, and `Dependencies` to the Jira edition's hierarchy and issue-link fields. Some Jira Cloud imports require a separate pass: import epics, capture Jira keys, then resolve epic/parent/link references. Configure the ID matching field and required fields with the Jira administrator. CSV import does not automatically update existing Jira issues unless matching keys and update behavior are explicitly configured.

## Evidence Boundaries

The project is a technical proof of concept. The current trade input is synthetic and the prototype market-data provider is Yahoo Finance; production rights/contracts are not confirmed. The checked-in evidence documents one successful six-task run on 2026-10-11; its same-input follow-up was queued and has no result recorded. A focused test run identified three stale references to removed Silver constraint scripts. The schedule is relative, proposed, and not a commitment; its Gantt shows sprint windows and does not calculate CPM dates or float.
