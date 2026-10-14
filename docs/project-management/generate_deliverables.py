"""Generate interview-ready SDLC deliverables from one structured planning source.

Run from the repository root with:
    python docs/project-management/generate_deliverables.py

The DDL-backed data dictionary is read from database/tables at generation time.
"""

from __future__ import annotations

import argparse
import csv
import html as html_lib
import logging
import os
import re
import shutil
import tempfile
import uuid
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
PROJECT_DIR = Path(__file__).resolve().parent
MASTER_PATH = PROJECT_DIR / "Trading_Project_Master.xlsx"
OUTPUT_DIR = PROJECT_DIR / "output"
OUT = PROJECT_DIR
AS_OF = "2026-10-14"
logger = logging.getLogger("project_management_generator")

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


# Runtime project data is loaded from Trading_Project_Master.xlsx before generation.
PLAN: dict = {}
TESTS: list[dict] = []
GANTT_ACTIVITY_DEFS: list[dict] = []

def csv_text(items: list[str]) -> str:
    return "; ".join(items)


def get_gantt_activities() -> list[dict]:
    story_by_id = {story["id"]: story for story in PLAN["stories"]}
    activity_ids = {definition["id"] for definition in GANTT_ACTIVITY_DEFS}
    story_to_activity = {
        story_id: definition["id"]
        for definition in GANTT_ACTIVITY_DEFS
        for story_id in definition["stories"]
    }
    activities = []
    for definition in GANTT_ACTIVITY_DEFS:
        stories = [story_by_id[story_id] for story_id in definition["stories"]]
        sprints = {story["sprint"] for story in stories}
        if len(sprints) != 1 or "" in sprints:
            raise RuntimeError(f"Gantt workstream must map to exactly one scheduled sprint: {definition['id']}")
        sprint_name = next(iter(sprints))
        sprint = next(item for item in PLAN["sprints"] if item["name"].split(" (")[0] == sprint_name.split(" (")[0])
        start_week = definition["start_week"]
        finish_week = definition["finish_week"]
        duration_weeks = definition["duration_weeks"]
        if duration_weeks <= 0 or finish_week - start_week + 1 != duration_weeks:
            raise RuntimeError(f"Gantt duration does not match start/finish weeks for {definition['id']}")
        sprint_weeks = re.search(r"(\d+)\s*[-–]\s*(\d+)", sprint["weeks"])
        if sprint_weeks:
            sprint_start, sprint_finish = map(int, sprint_weeks.groups())
            if start_week < sprint_start or finish_week > sprint_finish:
                raise RuntimeError(f"Gantt activity {definition['id']} lies outside {sprint_name} window {sprint['weeks']}")
        predecessor_stories = sorted({predecessor for story in stories for predecessor in story["depends"]})
        expected_predecessors = {
            story_to_activity[predecessor]
            for predecessor in predecessor_stories
            if predecessor in story_to_activity and story_to_activity[predecessor] != definition["id"]
        } | {predecessor for predecessor in predecessor_stories if predecessor not in story_to_activity}
        supplied_predecessors = set(definition.get("predecessor_ids", []))
        if supplied_predecessors != expected_predecessors:
            raise RuntimeError(
                f"Delivery_Plan {definition['id']}: predecessor IDs do not match Jira story dependencies; "
                f"expected={sorted(expected_predecessors)}, supplied={sorted(supplied_predecessors)}"
            )
        predecessor_activities = sorted(predecessor for predecessor in supplied_predecessors if predecessor in activity_ids)
        external_predecessors = sorted(supplied_predecessors - activity_ids)
        implementation_states = {story["status"] for story in stories}
        if "BLOCKED" in implementation_states:
            evidence_status = "BLOCKED"
        elif "NOT VERIFIED" in implementation_states:
            evidence_status = "NOT VERIFIED"
        elif implementation_states == {"IMPLEMENTED"}:
            evidence_status = "IMPLEMENTED"
        elif implementation_states == {"PLANNED"}:
            evidence_status = "PLANNED"
        else:
            evidence_status = "PARTIALLY IMPLEMENTED"
        activities.append({
            **definition,
            "sprint": sprint_name,
            "baseline": sprint["baseline"],
            "start_week": start_week,
            "finish_week": finish_week,
            "duration_weeks": duration_weeks,
            "duration_basis": definition["duration_basis"],
            "story_points": sum(story["points"] for story in stories),
            "predecessor_stories": predecessor_stories,
            "predecessor_activities": predecessor_activities,
            "external_predecessors": external_predecessors,
            "evidence_status": evidence_status,
            "schedule_status": "PLANNED",
        })
    return activities


def gantt_dependency_edges() -> list[dict]:
    activities = get_gantt_activities()
    activity_by_id = {activity["id"]: activity for activity in activities}
    story_to_activity = {
        story_id: activity["id"]
        for activity in activities
        for story_id in activity["stories"]
    }
    edges = {}
    for activity in activities:
        for predecessor_ref in activity["predecessor_activities"]:
            predecessor = activity_by_id[predecessor_ref]
            key = (predecessor_ref, activity["id"])
            edges[key] = {
                "predecessor": predecessor_ref,
                "successor": activity["id"],
                "predecessor_story": "",
                "successor_story": csv_text(activity["stories"]),
                "type": "Finish-to-start",
                "forward_in_sprint_windows": predecessor["finish_week"] < activity["start_week"],
                "milestone": activity["milestone"],
            }
    return list(edges.values())


def get_gantt_dependency_register() -> list[dict]:
    stories = {story["id"]: story for story in PLAN["stories"]}
    milestone_by_id = {milestone["id"]: milestone for milestone in PLAN["milestones"]}
    rows = []
    for activity in get_gantt_activities():
        if not activity["predecessor_stories"]:
            continue
        predecessors = []
        for predecessor_id in activity["predecessor_stories"]:
            predecessor = stories.get(predecessor_id)
            if predecessor is not None and predecessor["status"] == "IMPLEMENTED":
                predecessors.append(f"current {predecessor_id}")
            else:
                predecessors.append(predecessor_id)
        milestone = milestone_by_id[activity["milestone"]]
        rows.append({
            "successor": activity["id"],
            "successor_story_ids": csv_text(activity["stories"]),
            "predecessors": csv_text(predecessors),
            "relationship": "Finish-to-start (planning dependency)",
            "milestone_impact": f"{milestone['id']} {milestone['week']} — {milestone['name']}",
            "baseline": True,
        })

    optional_milestone = milestone_by_id["M-05"]
    for story in PLAN["stories"]:
        if story["sprint"].split(" (")[0] != "Sprint 5" or not story["depends"]:
            continue
        predecessor_labels = [
            f"current {predecessor}" if stories.get(predecessor, {}).get("status") == "IMPLEMENTED" else predecessor
            for predecessor in sorted(story["depends"])
        ]
        rows.append({
            "successor": story["id"],
            "successor_story_ids": story["id"],
            "predecessors": csv_text(predecessor_labels),
            "relationship": "Finish-to-start (optional follow-on)",
            "milestone_impact": f"{optional_milestone['id']} {optional_milestone['week']} — optional dashboard/data sign-off",
            "baseline": False,
        })
    return rows


def list_subtasks() -> list[dict]:
    if "subtasks" in PLAN:
        return PLAN["subtasks"]
    rows = []
    for story in PLAN["stories"]:
        for index, description in enumerate(story["subtasks"], start=1):
            rows.append({"id": f'{story["id"]}-ST{index:02}', "parent": story["id"], "description": description, "output": f'Completed evidence for {description.lower()}', "dependency": story["id"]})
    return rows


def _read_master_sheet(workbook, sheet_name: str, required_columns: set[str]) -> list[dict]:
    if sheet_name not in workbook.sheetnames:
        raise ValueError(f"Master workbook is missing worksheet '{sheet_name}'")
    sheet = workbook[sheet_name]
    header_values = [cell.value for cell in sheet[1]]
    if not header_values or any(not isinstance(value, str) or not value.strip() for value in header_values):
        raise ValueError(f"{sheet_name}: row 1 must contain non-empty column names")
    headers = [value.strip() for value in header_values]
    duplicates = sorted({name for name in headers if headers.count(name) > 1})
    if duplicates:
        raise ValueError(f"{sheet_name}: duplicate column names: {duplicates}")
    missing = sorted(required_columns - set(headers))
    if missing:
        raise ValueError(f"{sheet_name}: missing required columns: {missing}")
    rows = []
    for row_number, values in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
        if not any(value is not None and str(value).strip() for value in values):
            continue
        record = dict(zip(headers, values))
        record["_row_number"] = row_number
        rows.append(record)
    return rows


def _master_text(value) -> str:
    return "" if value is None else str(value).strip()


def _master_refs(value) -> list[str]:
    if value is None:
        return []
    return [part.strip() for part in re.split(r"[;\n,]+", str(value)) if part.strip()]


def _master_acceptance(value, sheet_name: str, row_number: int) -> list[str]:
    text = _master_text(value)
    criteria = [re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", line).strip() for line in text.splitlines()]
    criteria = [criterion for criterion in criteria if criterion]
    if not criteria and text:
        criteria = [text]
    if not criteria:
        raise ValueError(f"{sheet_name} row {row_number}: acceptance criteria are required")
    return criteria


def load_master_model(master_path: Path = MASTER_PATH) -> tuple[dict, list[dict], list[dict]]:
    """Read the project model from the user-maintained master workbook."""
    master_path = Path(master_path)
    if not master_path.is_file():
        raise FileNotFoundError(f"Master workbook not found: {master_path}")
    try:
        workbook = load_workbook(master_path, read_only=True, data_only=True)
    except Exception as exc:
        raise ValueError(f"Could not open master workbook '{master_path}': {exc}") from exc

    required_sheets = {
        "Project_Overview", "Requirements", "Data_Dictionary", "Jira_Backlog", "Delivery_Plan",
        "Milestones", "RAID", "Stakeholders_RACI", "Test_Plan", "Architecture", "Data_Sources",
        "NFRs", "Sprints", "Dependencies", "Decisions", "Governance", "Change_Control",
        "TPM_Scenarios", "Resource_Capacity",
    }
    missing_sheets = sorted(required_sheets - set(workbook.sheetnames))
    if missing_sheets:
        raise ValueError(f"Master workbook is missing required worksheets: {missing_sheets}")

    project_rows = _read_master_sheet(workbook, "Project_Overview", {"Record Type", "Record ID", "Name", "Status", "Evidence"})
    requirement_rows = _read_master_sheet(workbook, "Requirements", {"Requirement ID", "Type", "Parent ID", "Description", "Acceptance Criteria", "Implementation Status", "Related IDs"})
    dictionary_rows = _read_master_sheet(workbook, "Data_Dictionary", {"Catalog", "Schema", "Table", "Column", "Databricks SQL type", "Nullable"})
    backlog_rows = _read_master_sheet(workbook, "Jira_Backlog", {"Issue ID", "Issue Type", "Epic ID", "Epic Name", "Parent ID", "Summary", "Acceptance Criteria", "Priority", "Jira Status", "Implementation Status", "Dependencies", "Sprint"})
    delivery_rows = _read_master_sheet(workbook, "Delivery_Plan", {"Record Type", "Activity ID", "Activity / Story", "Story IDs", "Sprint", "Start Week", "Finish Week", "Duration Weeks", "Milestone ID"})
    milestone_rows = _read_master_sheet(workbook, "Milestones", {"Milestone ID", "Milestone", "Planned date/week", "Exit evidence", "Accountable owner", "Status"})
    raid_rows = _read_master_sheet(workbook, "RAID", {"ID", "Type", "Description", "Probability", "Impact", "Owner", "Mitigation", "Due date/week", "Trigger", "Escalation path", "Status"})
    stakeholder_rows = _read_master_sheet(workbook, "Stakeholders_RACI", {"Record Type", "Stakeholder Role", "Responsibility", "Activity"})
    test_rows = _read_master_sheet(workbook, "Test_Plan", {"Test ID", "Test level", "Scope", "Requirement IDs", "Jira story", "Expected result", "Acceptance evidence"})
    architecture_rows = _read_master_sheet(workbook, "Architecture", {"Component ID", "Section", "Component", "Description", "Implementation Status", "Evidence / Source", "Predecessor Component IDs"})
    source_rows = _read_master_sheet(workbook, "Data_Sources", {"Source ID", "Source", "Status", "System/provider", "Format/interface", "Open decisions"})
    nfr_rows = _read_master_sheet(workbook, "NFRs", {"NFR ID", "Quality attribute", "Acceptance target", "Status", "Evidence / gap"})
    sprint_rows = _read_master_sheet(workbook, "Sprints", {"Sprint", "Timeline", "Plan classification", "Capacity (SP)", "Goal", "Planned stories", "Dependencies", "Deliverables", "Demonstration", "Exit criteria"})
    dependency_rows = _read_master_sheet(workbook, "Dependencies", {"Predecessor ID", "Successor ID", "Relationship", "Dependency Type"})
    decision_rows = _read_master_sheet(workbook, "Decisions", {"Decision ID", "Decision", "Status", "Evidence / Rationale"})
    governance_rows = _read_master_sheet(workbook, "Governance", {"Activity", "Purpose", "Participants", "Frequency", "Outputs", "Escalation Route"})
    change_rows = _read_master_sheet(workbook, "Change_Control", {"Change ID", "Request", "Justification", "Scope Impact", "Schedule Impact", "Resource Impact", "Risk Impact", "Decision", "Approver", "Status"})
    scenario_rows = _read_master_sheet(workbook, "TPM_Scenarios", {"Scenario ID", "Scenario", "Trigger", "Impact", "Dependencies", "Mitigation", "Escalation", "Contingency", "Decision Required", "Status"})
    resource_rows = _read_master_sheet(workbook, "Resource_Capacity", {"Delivery role", "Capacity assumption", "Workstream allocation", "Estimated effort (person-days)", "Capacity constraints / status"})

    project_meta = {row["Record ID"]: row for row in project_rows if _master_text(row["Record Type"]).upper() == "META"}
    if not _master_text(project_meta.get("PROJECT_TITLE", {}).get("Name")) or not _master_text(project_meta.get("PURPOSE", {}).get("Name")):
        raise ValueError("Project_Overview requires PROJECT_TITLE and PURPOSE metadata rows")
    requirements_by_type: dict[str, list[dict]] = {"BR": [], "FR": []}
    requirement_ids: set[str] = set()
    for row in requirement_rows:
        requirement_id = _master_text(row["Requirement ID"])
        kind = _master_text(row["Type"]).upper()
        if not requirement_id:
            raise ValueError(f"Requirements row {row['_row_number']}: Requirement ID is required")
        if requirement_id in requirement_ids:
            raise ValueError(f"Requirements row {row['_row_number']}: duplicate ID '{requirement_id}'")
        requirement_ids.add(requirement_id)
        if kind == "BR":
            requirements_by_type["BR"].append({
                "id": requirement_id, "description": _master_text(row["Description"]),
                "why": _master_text(row.get("Business Justification")), "priority": _master_text(row.get("Priority")),
                "acceptance": "; ".join(_master_acceptance(row["Acceptance Criteria"], "Requirements", row["_row_number"])),
                "dependencies": _master_text(row.get("Dependencies")), "status": _master_text(row["Implementation Status"]),
                "fr": _master_refs(row["Related IDs"]),
            })
        elif kind == "FR":
            requirements_by_type["FR"].append({
                "id": requirement_id, "br": _master_text(row["Parent ID"]), "description": _master_text(row["Description"]),
                "input": _master_text(row.get("Input")), "logic": _master_text(row.get("Processing Logic")),
                "output": _master_text(row.get("Expected Output")), "rules": _master_text(row.get("Business Rules")),
                "errors": _master_text(row.get("Error Handling")),
                "acceptance": "; ".join(_master_acceptance(row["Acceptance Criteria"], "Requirements", row["_row_number"])),
                "components": _master_text(row.get("Components")), "status": _master_text(row["Implementation Status"]),
                "stories": _master_refs(row["Related IDs"]),
            })
        elif kind != "NFR":
            raise ValueError(f"Requirements row {row['_row_number']}: Type must be BR, FR, or NFR")

    nfrs = [{
        "id": _master_text(row["NFR ID"]), "name": _master_text(row["Quality attribute"]),
        "target": _master_text(row["Acceptance target"]), "status": _master_text(row["Status"]),
        "evidence": _master_text(row["Evidence / gap"]),
    } for row in nfr_rows]
    nfr_ids = [item["id"] for item in nfrs]
    if not nfr_ids or len(nfr_ids) != len(set(nfr_ids)) or any(not item for item in nfr_ids):
        raise ValueError("NFRs: IDs must be present and unique")

    issue_by_id: dict[str, dict] = {}
    allowed_issue_types = {"Epic", "Story", "Sub-task"}
    for row in backlog_rows:
        issue_id = _master_text(row["Issue ID"])
        issue_type = _master_text(row["Issue Type"])
        if not issue_id or issue_id in issue_by_id:
            raise ValueError(f"Jira_Backlog row {row['_row_number']}: Issue ID must be present and unique")
        if issue_type not in allowed_issue_types:
            raise ValueError(f"Jira_Backlog row {row['_row_number']}: unsupported Issue Type '{issue_type}'")
        issue_by_id[issue_id] = row
    epics = [{"id": _master_text(row["Issue ID"]), "name": _master_text(row["Epic Name"]) or _master_text(row["Summary"]), "priority": _master_text(row["Priority"]), "jira_status": _master_text(row["Jira Status"])} for row in backlog_rows if _master_text(row["Issue Type"]) == "Epic"]
    epic_ids = {item["id"] for item in epics}
    stories = []
    subtask_rows = [row for row in backlog_rows if _master_text(row["Issue Type"]) == "Sub-task"]
    subtasks_by_parent: dict[str, list[str]] = defaultdict(list)
    subtask_records = []
    for row in subtask_rows:
        parent_id = _master_text(row["Parent ID"])
        description = _master_text(row.get("Subtask Description")) or _master_text(row["Summary"])
        subtasks_by_parent[parent_id].append(description)
        subtask_records.append({
            "id": _master_text(row["Issue ID"]), "parent": parent_id, "description": description,
            "output": _master_text(row.get("Acceptance Criteria")) or _master_text(row.get("Description")),
            "dependency": _master_text(row["Dependencies"]), "epic": _master_text(row["Epic ID"]),
            "status": _master_text(row["Implementation Status"]), "jira_status": _master_text(row["Jira Status"]),
        })
    for row in backlog_rows:
        if _master_text(row["Issue Type"]) != "Story":
            continue
        story_id = _master_text(row["Issue ID"])
        try:
            points = int(row["Story Points"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Jira_Backlog row {row['_row_number']}: Story Points must be a positive integer") from exc
        if points <= 0:
            raise ValueError(f"Jira_Backlog row {row['_row_number']}: Story Points must be positive")
        stories.append({
            "id": story_id, "epic": _master_text(row["Epic ID"]), "title": _master_text(row["Summary"]),
            "objective": _master_text(row.get("Objective")), "description": _master_text(row.get("Description")),
            "criteria": _master_acceptance(row["Acceptance Criteria"], "Jira_Backlog", row["_row_number"]),
            "points": points, "priority": _master_text(row["Priority"]),
            "jira_status": _master_text(row["Jira Status"]), "status": _master_text(row["Implementation Status"]),
            "sprint": _master_text(row["Sprint"]), "br": _master_refs(row.get("BR IDs")),
            "fr": _master_refs(row.get("FR IDs")), "depends": _master_refs(row["Dependencies"]),
            "inputs": _master_text(row.get("Inputs")), "outputs": _master_text(row.get("Expected Outputs")),
            "subtasks": subtasks_by_parent.get(story_id, []),
        })
    story_ids = {item["id"] for item in stories}
    if not epics or not stories:
        raise ValueError("Jira_Backlog must contain at least one Epic and Story")
    for row in backlog_rows:
        issue_type = _master_text(row["Issue Type"])
        if issue_type == "Story" and _master_text(row["Epic ID"]) not in epic_ids:
            raise ValueError(f"Jira_Backlog story {row['Issue ID']}: invalid Epic ID '{row['Epic ID']}'")
        if issue_type == "Sub-task":
            parent_id = _master_text(row["Parent ID"])
            parent = next((story for story in stories if story["id"] == parent_id), None)
            if parent is None:
                raise ValueError(f"Jira_Backlog subtask {row['Issue ID']}: invalid Parent ID '{parent_id}'")
            if _master_text(row["Epic ID"]) != parent["epic"]:
                raise ValueError(f"Jira_Backlog subtask {row['Issue ID']}: Epic ID does not match its parent")
    br_ids = {item["id"] for item in requirements_by_type["BR"]}
    fr_ids = {item["id"] for item in requirements_by_type["FR"]}
    for story in stories:
        if not story["title"] or not story["jira_status"] or not story["status"]:
            raise ValueError(f"Jira_Backlog story {story['id']}: Summary and both statuses are required")
        unknown = {
            "BR": sorted(set(story["br"]) - br_ids),
            "FR": sorted(set(story["fr"]) - fr_ids),
            "dependencies": sorted(set(story["depends"]) - story_ids),
        }
        if any(unknown.values()):
            raise ValueError(f"Jira_Backlog story {story['id']}: invalid references {unknown}")
    for requirement in requirements_by_type["BR"]:
        unknown = sorted(set(requirement["fr"]) - fr_ids)
        if unknown:
            raise ValueError(f"Business requirement {requirement['id']}: unknown FR references {unknown}")
    for requirement in requirements_by_type["FR"]:
        if requirement["br"] not in br_ids:
            raise ValueError(f"Functional requirement {requirement['id']}: invalid parent BR '{requirement['br']}'")
        unknown = sorted(set(requirement["stories"]) - story_ids)
        if unknown:
            raise ValueError(f"Functional requirement {requirement['id']}: unknown Jira story references {unknown}")

    dependency_graph = {story["id"]: story["depends"] for story in stories}
    visited: set[str] = set()
    active: set[str] = set()

    def visit(issue_id: str) -> None:
        if issue_id in active:
            raise ValueError(f"Jira_Backlog: circular dependency detected at '{issue_id}'")
        if issue_id in visited:
            return
        active.add(issue_id)
        for predecessor in dependency_graph[issue_id]:
            visit(predecessor)
        active.remove(issue_id)
        visited.add(issue_id)

    for issue_id in dependency_graph:
        visit(issue_id)

    sprints = []
    sprint_by_key = {}
    for row in sprint_rows:
        name = _master_text(row["Sprint"])
        try:
            capacity = int(row["Capacity (SP)"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Sprints row {row['_row_number']}: Capacity (SP) must be a positive integer") from exc
        if not name or name in sprint_by_key or capacity <= 0:
            raise ValueError(f"Sprints row {row['_row_number']}: Sprint name and positive capacity must be unique")
        sprint = {
            "name": name, "weeks": _master_text(row["Timeline"]),
            "baseline": _master_text(row["Plan classification"]) == "8-week baseline", "capacity": capacity,
            "goal": _master_text(row["Goal"]), "stories": _master_refs(row["Planned stories"]),
            "dependencies": _master_text(row["Dependencies"]), "deliverables": _master_text(row["Deliverables"]),
            "demo": _master_text(row["Demonstration"]), "exit": _master_text(row["Exit criteria"]),
        }
        sprint_by_key[name.split(" (")[0]] = sprint
        sprints.append(sprint)
    for sprint_key, sprint in sprint_by_key.items():
        assigned = {story["id"] for story in stories if story["sprint"].split(" (")[0] == sprint_key}
        listed = set(sprint["stories"])
        if assigned != listed:
            raise ValueError(f"Sprints '{sprint['name']}': Planned stories mismatch; missing={sorted(assigned-listed)}, extra={sorted(listed-assigned)}")
        points = sum(story["points"] for story in stories if story["sprint"].split(" (")[0] == sprint_key)
        if points > sprint["capacity"]:
            raise ValueError(f"Sprints '{sprint['name']}': capacity exceeded ({points}/{sprint['capacity']} SP)")
    for story in stories:
        if story["sprint"] and story["sprint"].split(" (")[0] not in sprint_by_key:
            raise ValueError(f"Jira_Backlog story {story['id']}: unknown Sprint '{story['sprint']}'")

    milestone_records = []
    milestone_ids = set()
    for row in milestone_rows:
        milestone_id = _master_text(row["Milestone ID"])
        if not milestone_id or milestone_id in milestone_ids:
            raise ValueError(f"Milestones row {row['_row_number']}: Milestone ID must be present and unique")
        milestone_ids.add(milestone_id)
        milestone_records.append({
            "id": milestone_id, "name": _master_text(row["Milestone"]), "week": _master_text(row["Planned date/week"]),
            "evidence": _master_text(row["Exit evidence"]), "owner": _master_text(row["Accountable owner"]),
            "dependencies": _master_refs(row.get("Dependencies")), "approvals": _master_text(row.get("Approval requirements")),
            "gate": _master_text(row.get("Exit criteria")), "status": _master_text(row["Status"]),
        })
    gantt_defs = []
    gantt_ids = set()
    for row in delivery_rows:
        if _master_text(row["Record Type"]).upper() != "WORKSTREAM":
            continue
        activity_id = _master_text(row["Activity ID"])
        mapped_stories = _master_refs(row["Story IDs"])
        if not activity_id or activity_id in gantt_ids or not mapped_stories or set(mapped_stories) - story_ids:
            raise ValueError(f"Delivery_Plan row {row['_row_number']}: Activity ID must be unique and Story IDs valid")
        gantt_ids.add(activity_id)
        activity_sprints = {story["sprint"].split(" (")[0] for story in stories if story["id"] in mapped_stories}
        if len(activity_sprints) != 1 or next(iter(activity_sprints)) != _master_text(row["Sprint"]).split(" (")[0]:
            raise ValueError(f"Delivery_Plan {activity_id}: mapped stories must share the activity Sprint")
        try:
            start_week = int(row["Start Week"])
            finish_week = int(row["Finish Week"])
            duration_weeks = int(row["Duration Weeks"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Delivery_Plan {activity_id}: Start Week, Finish Week, and Duration Weeks must be integers") from exc
        if start_week <= 0 or finish_week < start_week or duration_weeks <= 0 or finish_week - start_week + 1 != duration_weeks:
            raise ValueError(f"Delivery_Plan {activity_id}: inconsistent start, finish, and duration weeks")
        milestone_id = _master_text(row["Milestone ID"])
        if milestone_id and milestone_id not in milestone_ids:
            raise ValueError(f"Delivery_Plan {activity_id}: unknown milestone '{milestone_id}'")
        gantt_defs.append({
            "id": activity_id, "name": _master_text(row["Activity / Story"]), "stories": mapped_stories,
            "milestone": milestone_id, "start_week": start_week, "finish_week": finish_week,
            "duration_weeks": duration_weeks, "duration_basis": _master_text(row.get("Duration Basis")),
            "owner": _master_text(row.get("Owner Role")), "predecessor_ids": _master_refs(row.get("Predecessor IDs")),
            "schedule_status": _master_text(row.get("Schedule Status")),
        })
    baseline_story_ids = {story["id"] for story in stories if story["sprint"] and sprint_by_key[story["sprint"].split(" (")[0]]["baseline"]}
    gantt_story_ids = [story_id for definition in gantt_defs for story_id in definition["stories"] if story_id in baseline_story_ids]
    if len(gantt_story_ids) != len(set(gantt_story_ids)) or set(gantt_story_ids) != baseline_story_ids:
        raise ValueError(f"Delivery_Plan workstreams must map each baseline story exactly once; missing={sorted(baseline_story_ids-set(gantt_story_ids))}, extra={sorted(set(gantt_story_ids)-baseline_story_ids)}")

    architecture_ids = [_master_text(row["Component ID"]) for row in architecture_rows]
    if any(not value for value in architecture_ids) or len(architecture_ids) != len(set(architecture_ids)):
        raise ValueError("Architecture component IDs must be present and unique")
    architecture_id_set = set(architecture_ids)
    for row in architecture_rows:
        for predecessor in _master_refs(row.get("Predecessor Component IDs")):
            if predecessor not in architecture_id_set:
                raise ValueError(f"Architecture {row['Component ID']}: unknown predecessor component '{predecessor}'")
    workflow_components = {_master_text(row["Component"]) for row in architecture_rows if _master_text(row["Section"]) == "Workflow Task"}
    known_dependencies = story_ids | epic_ids | architecture_id_set | workflow_components
    for row in dependency_rows:
        predecessor, successor = _master_text(row["Predecessor ID"]), _master_text(row["Successor ID"])
        if predecessor not in known_dependencies or successor not in known_dependencies:
            raise ValueError(f"Dependencies row {row['_row_number']}: unknown reference '{predecessor}' -> '{successor}'")
    for milestone in milestone_records:
        unknown = sorted(set(milestone["dependencies"]) - story_ids)
        if unknown:
            raise ValueError(f"Milestone {milestone['id']}: unknown story dependencies {unknown}")

    model = {
        "title": _master_text(project_meta["PROJECT_TITLE"]["Name"]),
        "purpose": _master_text(project_meta["PURPOSE"]["Name"]),
        "metadata": {key: _master_text(row.get("Name")) for key, row in project_meta.items()},
        "status_legend": ["IMPLEMENTED", "PARTIALLY IMPLEMENTED", "PLANNED", "BLOCKED", "NOT VERIFIED"],
        "business_requirements": requirements_by_type["BR"], "functional_requirements": requirements_by_type["FR"],
        "nfrs": [{"id": _master_text(r["NFR ID"]), "name": _master_text(r["Quality attribute"]), "target": _master_text(r["Acceptance target"]), "status": _master_text(r["Status"]), "evidence": _master_text(r["Evidence / gap"])} for r in nfr_rows],
        "sources": [{"id": _master_text(r["Source ID"]), "name": _master_text(r["Source"]), "status": _master_text(r["Status"]), "system": _master_text(r["System/provider"]), "format": _master_text(r["Format/interface"]), "location": _master_text(r.get("Location")), "frequency": _master_text(r.get("Frequency")), "volume": _master_text(r.get("Volume")), "keys": _master_text(r.get("Business key")), "versioning": _master_text(r.get("Version/correction")), "quality": _master_text(r.get("Quality rules")), "ingestion": _master_text(r.get("Ingestion strategy")), "failure": _master_text(r.get("Failure handling")), "open_items": _master_text(r.get("Open decisions"))} for r in source_rows],
        "data_dictionary": [{key: value for key, value in row.items() if not key.startswith("_")} for row in dictionary_rows],
        "epics": [{"id": _master_text(r["Issue ID"]), "name": _master_text(r["Epic Name"]) or _master_text(r["Summary"]), "priority": _master_text(r["Priority"]), "jira_status": _master_text(r["Jira Status"])} for r in backlog_rows if _master_text(r["Issue Type"]) == "Epic"],
        "stories": stories, "sprints": sprints, "milestones": milestone_records,
        "risks": [{"id": _master_text(r["ID"]), "type": _master_text(r["Type"]), "description": _master_text(r["Description"]), "probability": _master_text(r["Probability"]), "impact": _master_text(r["Impact"]), "owner": _master_text(r["Owner"]), "response": _master_text(r["Mitigation"]), "status": _master_text(r["Status"])} for r in raid_rows],
        "raid_details": {_master_text(r["ID"]): {"due": _master_text(r["Due date/week"]), "trigger": _master_text(r["Trigger"]), "escalation": _master_text(r["Escalation path"])} for r in raid_rows},
        "decisions": [{"id": _master_text(r["Decision ID"]), "decision": _master_text(r["Decision"]), "status": _master_text(r["Status"]), "evidence": _master_text(r["Evidence / Rationale"])} for r in decision_rows],
        "stakeholders": [{"role": _master_text(r["Stakeholder Role"]), "responsibility": _master_text(r["Responsibility"]), "engagement": _master_text(r["Engagement Approach"]), "authority": _master_text(r["Decision Authority"]), "frequency": _master_text(r["Communication Frequency"]), "assignment": _master_text(r["Assignment / Evidence Status"])} for r in stakeholder_rows if _master_text(r["Record Type"]).upper() == "STAKEHOLDER"],
        "raci_roles": [],
        "raci": [],
        "subtasks": subtask_records,
        "resource_plan": [{"role": _master_text(r["Delivery role"]), "capacity": _master_text(r["Capacity assumption"]), "allocation": _master_text(r["Workstream allocation"]), "effort": r["Estimated effort (person-days)"], "constraints": _master_text(r["Capacity constraints / status"])} for r in resource_rows],
        "governance": [{"activity": _master_text(r["Activity"]), "purpose": _master_text(r["Purpose"]), "participants": _master_text(r["Participants"]), "frequency": _master_text(r["Frequency"]), "outputs": _master_text(r["Outputs"]), "escalation": _master_text(r["Escalation Route"])} for r in governance_rows],
        "scenarios": [{"id": _master_text(r["Scenario ID"]), "title": _master_text(r["Scenario"]), "trigger": _master_text(r["Trigger"]), "impact": _master_text(r["Impact"]), "dependencies": _master_text(r["Dependencies"]), "mitigation": _master_text(r["Mitigation"]), "escalation": _master_text(r["Escalation"]), "contingency": _master_text(r["Contingency"]), "decision": _master_text(r["Decision Required"]), "status": _master_text(r["Status"])} for r in scenario_rows],
        "change_register": [{"id": _master_text(r["Change ID"]), "request": _master_text(r["Request"]), "justification": _master_text(r["Justification"]), "scope": _master_text(r["Scope Impact"]), "schedule": _master_text(r["Schedule Impact"]), "resource": _master_text(r["Resource Impact"]), "risk": _master_text(r["Risk Impact"]), "decision": _master_text(r["Decision"]), "approver": _master_text(r["Approver"]), "status": _master_text(r["Status"])} for r in change_rows],
        "status_dashboard": [{"area": _master_text(r["Record ID"]), "rag": _master_text(r["Status"]), "detail": _master_text(r["Name"]), "evidence": _master_text(r["Evidence"])} for r in project_rows if _master_text(r["Record Type"]).upper() == "STATUS"],
        "decision_required": [_master_text(r["Name"]) for r in project_rows if _master_text(r["Record Type"]).upper() == "DECISION_REQUIRED"],
        "critical_path_note": _master_text(project_meta.get("CRITICAL_PATH_METHOD", {}).get("Name")) or "Key delivery dependencies; CPM is not calculated.",
        "architecture": [{key: value for key, value in row.items() if not key.startswith("_")} for row in architecture_rows],
        "dependencies": [{key: value for key, value in row.items() if not key.startswith("_")} for row in dependency_rows],
        "delivery_plan": [{key: value for key, value in row.items() if not key.startswith("_")} for row in delivery_rows],
    }
    header_values = [cell.value for cell in workbook["Stakeholders_RACI"][1]]
    model["raci_roles"] = [header for header in header_values if header not in {"Record Type", "Stakeholder Role", "Responsibility", "Engagement Approach", "Decision Authority", "Communication Frequency", "Assignment / Evidence Status", "Activity"}]
    model["raci"] = [{"activity": _master_text(r["Activity"]), **{role: _master_text(r[role]) for role in model["raci_roles"]}} for r in stakeholder_rows if _master_text(r["Record Type"]).upper() == "RACI"]
    tests = [{"id": _master_text(r["Test ID"]), "type": _master_text(r["Test level"]), "scope": _master_text(r["Scope"]), "requirements": _master_text(r["Requirement IDs"]), "story": _master_text(r["Jira story"]), "expected": _master_text(r["Expected result"]), "evidence": _master_text(r["Acceptance evidence"])} for r in test_rows]
    test_ids = [test["id"] for test in tests]
    if any(not test_id for test_id in test_ids) or len(test_ids) != len(set(test_ids)):
        raise ValueError("Test_Plan: Test IDs must be present and unique")
    for test in tests:
        unknown_requirements = sorted(set(_master_refs(test["requirements"])) - requirement_ids)
        unknown_stories = sorted(set(_master_refs(test["story"])) - story_ids)
        if unknown_requirements or unknown_stories:
            raise ValueError(f"Test_Plan {test['id']}: invalid references requirements={unknown_requirements}, stories={unknown_stories}")
    workbook.close()
    return model, tests, gantt_defs


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
    summary.title = "Executive Dashboard"
    summary.sheet_view.showGridLines = False
    summary.merge_cells("A1:F1")
    summary["A1"] = "TRADING PLATFORM | EXECUTIVE PROJECT DASHBOARD"
    summary["A1"].fill = load_color(NAVY)
    summary["A1"].font = Font(name="Aptos Display", size=20, bold=True, color=WHITE)
    summary["A1"].alignment = Alignment(vertical="center")
    summary.row_dimensions[1].height = 42
    summary.append(["Evidence baseline", AS_OF + "; repository plus dated run evidence 2026-10-11", "", "", "", ""])
    summary.append(["Purpose", PLAN["purpose"], "", "", "", ""])
    summary.append(["Area", "RAG", "Current position", "Evidence / caveat", "Decision owner", "Next action"])
    for item in PLAN["status_dashboard"]:
        summary.append([item["area"], item["rag"], item["detail"], item["evidence"], "Role TBD", "See decisions and RAID"])
    summary.append(["Planning metric", "Value", "Meaning", "", "", ""])
    metrics = [
        ("Business requirements", "=COUNTA('Business Req'!A:A)-1", "Eight business outcomes"),
        ("Functional requirements", "=COUNTA('Functional Req'!A:A)-1", "Thirteen functional behaviors"),
        ("Jira epics / stories / subtasks", "=COUNTA(Epics!A:A)-1&\" / \"&(COUNTA(Stories!A:A)-1)&\" / \"&(COUNTA(Subtasks!A:A)-1)", "Temporary planning IDs"),
        ("Total estimated backlog points", "=SUM(Stories!G2:G100)", "Includes already implemented/reference scope; not remaining effort"),
        ("8-week baseline points / capacity", "=SUM(Sprints!G2:G5)&\" / \"&SUM(Sprints!F2:F5)", "38 SP planned / 40 assumed; 2 SP headroom"),
        ("Optional follow-on points", "=Sprints!G6", "Sprint 5 Weeks 9-10; not in baseline"),
        ("DDL dictionary columns", "=COUNTA('Data Dictionary'!G:G)-1", "All 10 physical tables"),
    ]
    for metric in metrics:
        summary.append(list(metric) + ["", "", ""])
    summary.append(["Completed deliverables (verified prototype)", "Six-task dev workflow; 10 Delta tables; synthetic 14-column trade CSV; Yahoo Finance adapter; Silver validation/rejects; positions and simplified daily P&L; audit/breaks; two notebooks and SQL.", "", "", "", ""])
    summary.append(["Upcoming milestone", "Target", "Exit evidence", "Accountable role", "Dependencies", "Approval gate"])
    for milestone in PLAN["milestones"]:
        summary.append([milestone["name"], milestone["week"], milestone["evidence"], milestone["owner"], csv_text(milestone["dependencies"]), milestone["approvals"]])
    summary.append(["Top risks", "Owner", "Trigger", "Mitigation", "Escalation", "Status"])
    for risk in PLAN["risks"][:4]:
        detail = PLAN["raid_details"][risk["id"]]
        summary.append([risk["description"], risk["owner"], detail["trigger"], risk["response"], detail["escalation"], risk["status"]])
    summary.append(["Key dependencies", "Source owners, market-data rights, supported Spark/Delta test runtime, access/security reviewer, Product Control acceptance, support owner.", "", "", "", ""])
    summary.append(["Decisions required", "", "", "", "", ""])
    for decision in PLAN["decision_required"]:
        summary.append([decision, "", "", "", "", ""])
    for row_number in range(2, summary.max_row + 1):
        summary.cell(row_number, 1).font = Font(name="Aptos", size=10, bold=True, color=NAVY)
        for cell in summary[row_number]:
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if row_number % 2 == 0:
                cell.fill = load_color("F5F8FA")
    for row_number in [4, 10, 19, 24, 29, 34]:
        if row_number <= summary.max_row:
            for cell in summary[row_number]:
                cell.fill = load_color(TEAL)
                cell.font = Font(bold=True, color=WHITE)
    for col, width in {"A": 42, "B": 42, "C": 68, "D": 75, "E": 38, "F": 66}.items():
        summary.column_dimensions[col].width = width
    summary.freeze_panes = "A2"
    for row_number in range(2, summary.max_row + 1):
        summary.row_dimensions[row_number].height = 36
    for rag, color in [("GREEN", PALE_TEAL), ("AMBER", PALE_ORANGE), ("RED", PALE_RED), ("NOT VERIFIED", "E9ECEF")]:
        summary.conditional_formatting.add(f"B5:B{summary.max_row}", FormulaRule(formula=[f'B5="{rag}"'], fill=load_color(color)))

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
    for index, sprint in enumerate(PLAN["sprints"], start=2):
        sprint_rows.append([sprint["name"], sprint["weeks"], "8-week baseline" if sprint["baseline"] else "Optional follow-on", sprint["goal"], csv_text(sprint["stories"]), sprint["capacity"], f'=SUMIF(Stories!$J:$J,A{index},Stories!$G:$G)', f"=F{index}-G{index}", sprint["dependencies"], sprint["deliverables"], sprint["demo"], sprint["exit"]])
    sprint_sheet = add_records(workbook, "Sprints", ["Sprint", "Timeline", "Plan classification", "Sprint goal", "Planned stories", "Capacity (SP)", "Planned SP (formula)", "Headroom / (over) (formula)", "Dependencies", "Deliverables", "Demonstration", "Exit criteria"], sprint_rows, {"A": 20, "B": 35, "C": 24, "D": 68, "E": 60, "F": 18, "G": 24, "H": 28, "I": 68, "J": 72, "K": 72, "L": 72})
    for row_number in range(2, sprint_sheet.max_row + 1):
        sprint_sheet.conditional_formatting.add(f"H{row_number}", FormulaRule(formula=[f"H{row_number}<0"], fill=load_color(PALE_RED)))
        sprint_sheet.conditional_formatting.add(f"H{row_number}", FormulaRule(formula=[f"H{row_number}>=0"], fill=load_color(PALE_TEAL)))

    gantt = workbook.create_sheet("Gantt")
    gantt_headers = ["Activity ID", "Workstream / task", "Jira story IDs", "Duration basis", "Start week", "Finish week", "Predecessor activity / story IDs", "Milestone", "Implementation evidence status", "Schedule status"] + [f"Week {week}" for week in range(1, 9)]
    gantt.append(gantt_headers)
    gantt_activities = get_gantt_activities()
    for activity in gantt_activities:
        row_number = gantt.max_row + 1
        predecessor_ids = activity["predecessor_activities"] + activity["external_predecessors"]
        gantt.append([
            activity["id"], activity["name"], csv_text(activity["stories"]),
            activity["duration_basis"], activity["start_week"], activity["finish_week"],
            csv_text(predecessor_ids) or "None", activity["milestone"],
            activity["evidence_status"], activity["schedule_status"],
            *[f'=--AND(COLUMN()-10>=$E{row_number},COLUMN()-10<=$F{row_number})' for _ in range(8)],
        ])
    marker_row_number = gantt.max_row + 1
    milestone_cells = ["" for _ in range(8)]
    for milestone in PLAN["milestones"]:
        week_match = re.search(r"Week\s+(\d+)", milestone["week"])
        if week_match and 1 <= int(week_match.group(1)) <= 8:
            week_index = int(week_match.group(1)) - 1
            milestone_cells[week_index] = milestone["id"]
    gantt.append(["", "Milestone markers", "", "Week-end gates", "", "", "", "", "", "", *milestone_cells])
    for column, width in {"A": 14, "B": 36, "C": 32, "D": 48, "E": 12, "F": 12, "G": 38, "H": 16, "I": 30, "J": 20, **{letter: 12 for letter in "KLMNOPQR"}}.items():
        gantt.column_dimensions[column].width = width
    style_sheet(gantt, freeze="K2")
    gantt.auto_filter.ref = f"A1:R{marker_row_number - 1}"
    gantt.row_dimensions[1].height = 42
    for row_number in range(2, marker_row_number):
        gantt.row_dimensions[row_number].height = 34
        gantt.conditional_formatting.add(f"K{row_number}:R{row_number}", FormulaRule(formula=[f"K{row_number}=1"], fill=load_color("D8DEE3")))
        evidence_fill = {
            "IMPLEMENTED": PALE_TEAL,
            "PARTIALLY IMPLEMENTED": PALE_ORANGE,
            "PLANNED": "E9ECEF",
            "BLOCKED": PALE_RED,
            "NOT VERIFIED": "E9ECEF",
        }.get(gantt.cell(row_number, 9).value, "E9ECEF")
        gantt.cell(row_number, 9).fill = load_color(evidence_fill)
        for column in "KLMNOPQR":
            gantt[f"{column}{row_number}"].number_format = ";;;"
    for cell in gantt[marker_row_number]:
        cell.fill = load_color(PALE_ORANGE)
        cell.font = Font(name="Aptos", size=9, bold=True, color=NAVY)
        cell.alignment = Alignment(vertical="center", horizontal="center", wrap_text=True)
    gantt.row_dimensions[marker_row_number].height = 28
    note_row = marker_row_number + 2
    gantt.cell(note_row, 1, "Scheduling note")
    gantt.cell(note_row, 2, "Bars show assigned two-week sprint windows only; they are not task-level durations. No kickoff date, activity duration, resource-levelled calendar, CPM dates, or float is evidenced. Sprint 5 (Weeks 9-10) is optional follow-on and is not plotted.")
    gantt.merge_cells(start_row=note_row, start_column=2, end_row=note_row, end_column=18)
    gantt.cell(note_row, 1).font = Font(bold=True, color=NAVY)
    gantt.cell(note_row, 2).alignment = Alignment(wrap_text=True, vertical="top")
    gantt.row_dimensions[note_row].height = 45

    milestone_rows = [[m["id"], m["name"], m["week"], m["evidence"], m["owner"], csv_text(m["dependencies"]), m["approvals"], m["gate"], "PLANNED"] for m in PLAN["milestones"]]
    milestone_sheet = add_records(workbook, "Milestone Tracker", ["Milestone ID", "Milestone", "Planned date/week", "Exit evidence", "Accountable owner", "Dependencies", "Approval requirements", "Exit criteria", "Status"], milestone_rows, {"A": 15, "B": 46, "C": 32, "D": 74, "E": 42, "F": 42, "G": 70, "H": 78, "I": 20})

    raid_rows = []
    for risk in PLAN["risks"]:
        detail = PLAN["raid_details"][risk["id"]]
        raid_rows.append([risk["id"], risk["type"], risk["description"], risk["probability"], risk["impact"], risk["owner"], risk["response"], detail["due"], detail["trigger"], detail["escalation"], risk["status"]])
    raid = add_records(workbook, "RAID", ["ID", "Type", "Description", "Probability", "Impact", "Owner", "Mitigation / action", "Due date/week", "Trigger", "Escalation path", "Status"], raid_rows, {"A": 15, "B": 18, "C": 68, "D": 16, "E": 16, "F": 32, "G": 76, "H": 30, "I": 72, "J": 72, "K": 20})

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

    current_rows = [[
        _master_text(item["Section"]), _master_text(item["Component"]), _master_text(item["Implementation Status"]),
        _master_text(item["Description"]), _master_text(item["Evidence / Source"]),
    ] for item in PLAN["architecture"]]
    add_records(workbook, "Current State", ["Area", "Capability", "Status", "Verified evidence", "Gap / limitation"], current_rows, {"A": 18, "B": 34, "C": 24, "D": 92, "E": 96})

    stakeholder_rows = [[item["role"], item["responsibility"], item["engagement"], item["authority"], item["frequency"], item["assignment"]] for item in PLAN["stakeholders"]]
    add_records(workbook, "Stakeholder Register", ["Stakeholder role", "Responsibility", "Engagement approach", "Decision authority", "Communication frequency", "Assignment / evidence status"], stakeholder_rows, {"A": 34, "B": 70, "C": 62, "D": 70, "E": 52, "F": 58})

    raci_rows = [[activity["activity"]] + [activity[role] for role in PLAN["raci_roles"]] for activity in PLAN["raci"]]
    raci_sheet = add_records(workbook, "RACI Matrix", ["Activity"] + PLAN["raci_roles"], raci_rows, {"A": 52, **{chr(66 + index): 28 for index in range(len(PLAN["raci_roles"]))}})
    raci_validation = DataValidation(type="list", formula1='"A,R,C,I,A/R"', allow_blank=False)
    raci_sheet.add_data_validation(raci_validation)
    raci_validation.add(f"B2:K{raci_sheet.max_row}")

    delivery_rows = []
    story_by_id = {story["id"]: story for story in PLAN["stories"]}
    for activity in PLAN["delivery_plan"]:
        if _master_text(activity["Record Type"]).upper() != "WORKSTREAM":
            continue
        story_ids = _master_refs(activity["Story IDs"])
        statuses = {story_by_id[story_id]["status"] for story_id in story_ids}
        implementation_status = next(iter(statuses)) if len(statuses) == 1 else "PARTIALLY IMPLEMENTED"
        delivery_rows.append([
            _master_text(activity["Activity ID"]), _master_text(activity["Activity / Story"]),
            _master_text(activity.get("Owner Role")), activity["Duration Weeks"], activity["Start Week"],
            activity["Finish Week"], _master_text(activity.get("Predecessor IDs")) or "None",
            _master_text(activity["Milestone ID"]) or "None", implementation_status, csv_text(story_ids),
        ])
    add_records(workbook, "Integrated Delivery Plan", ["Workstream ID", "Activity", "Proposed owner role", "Duration (weeks)", "Start week", "End week", "Predecessors", "Milestone", "Implementation status", "Story IDs"], delivery_rows, {"A": 18, "B": 64, "C": 48, "D": 22, "E": 20, "F": 20, "G": 42, "H": 22, "I": 26, "J": 44})

    resource_rows = [[item["role"], item["capacity"], item["allocation"], item["effort"], "Baseline effort = capacity per sprint × 4 sprints", item["constraints"]] for item in PLAN["resource_plan"]]
    add_records(workbook, "Resource Capacity", ["Delivery role", "Capacity assumption", "Workstream allocation", "Estimated effort (person-days)", "Estimate basis", "Capacity constraints / status"], resource_rows, {"A": 36, "B": 44, "C": 72, "D": 32, "E": 48, "F": 76})

    governance_rows = [[item["activity"], item["purpose"], item["participants"], item["frequency"], item["outputs"], item["escalation"]] for item in PLAN["governance"]]
    add_records(workbook, "Governance", ["Activity", "Purpose", "Participants", "Frequency", "Outputs", "Escalation route"], governance_rows, {"A": 38, "B": 72, "C": 66, "D": 48, "E": 64, "F": 78})

    change_rows = [[item["id"], item["request"], item["justification"], item["scope"], item["schedule"], item["resource"], item["risk"], item["decision"], item["approver"], item["status"]] for item in PLAN["change_register"]]
    add_records(workbook, "Change Control", ["Change request", "Request / status", "Business justification", "Scope impact", "Schedule impact", "Resource impact", "Risk impact", "Decision", "Approver", "Status"], change_rows, {"A": 20, "B": 58, "C": 64, "D": 64, "E": 46, "F": 44, "G": 66, "H": 64, "I": 58, "J": 22})

    scenario_rows = [[item[key] for key in ("id", "title", "trigger", "impact", "dependencies", "mitigation", "escalation", "contingency", "decision", "status")] for item in PLAN["scenarios"]]
    add_records(workbook, "TPM Scenarios", ["Scenario ID", "Label", "Trigger", "Delivery impact", "Critical dependencies", "Mitigation", "Escalation", "Contingency", "Decision required", "Status"], scenario_rows, {"A": 14, "B": 58, "C": 70, "D": 78, "E": 72, "F": 82, "G": 72, "H": 76, "I": 82, "J": 42})

    # Consistent status/priority/sprint choices and visual cues across worksheets.
    status_validation = DataValidation(type="list", formula1='"IMPLEMENTED,PARTIALLY IMPLEMENTED,PLANNED,BLOCKED,NOT VERIFIED"', allow_blank=True)
    priority_validation = DataValidation(type="list", formula1='"High,Medium,Low,Must,Should"', allow_blank=True)
    sprint_validation = DataValidation(type="list", formula1='"Sprint 1,Sprint 2,Sprint 3,Sprint 4"', allow_blank=True)
    story_sheet.add_data_validation(status_validation)
    status_validation.add(f"I2:I{story_sheet.max_row}")
    story_sheet.add_data_validation(priority_validation)
    priority_validation.add(f"H2:H{story_sheet.max_row}")
    story_sheet.add_data_validation(sprint_validation)
    sprint_validation.add(f"J2:J{story_sheet.max_row}")
    for column, status, fill in [("I", "IMPLEMENTED", PALE_TEAL), ("I", "PARTIALLY IMPLEMENTED", PALE_ORANGE), ("I", "PLANNED", PALE_RED), ("I", "BLOCKED", PALE_RED), ("I", "NOT VERIFIED", "E9ECEF")]:
        story_sheet.conditional_formatting.add(f"{column}2:{column}{story_sheet.max_row}", FormulaRule(formula=[f'{column}2="{status}"'], fill=load_color(fill)))
    for status, fill in [("Open", PALE_RED), ("To confirm", PALE_ORANGE), ("Proposed", "E9ECEF")]:
        raid.conditional_formatting.add(f"H2:H{raid.max_row}", FormulaRule(formula=[f'H2="{status}"'], fill=load_color(fill)))
    for sheet in workbook.worksheets:
        if sheet.title != "Executive Dashboard":
            sheet.auto_filter.ref = sheet.dimensions
    workbook.calculation.fullCalcOnLoad = True
    workbook.calculation.forceFullCalc = True
    workbook.calculation.calcMode = "auto"
    workbook.save(OUT / "Trading_Platform_Project_Plan.xlsx")


def jira_rows() -> list[dict]:
    rows = []
    for epic in PLAN["epics"]:
        rows.append({"Issue ID": epic["id"], "Issue Type": "Epic", "Planning Key": epic["id"], "Epic Name": epic["name"], "Epic Link": "", "Parent ID": "", "Summary": epic["name"], "Description": f"Delivery epic: {epic['name']}. Includes evidenced current functionality and linked proposed completion work.", "Acceptance Criteria": "See linked stories; epic is complete when all committed child stories meet their acceptance criteria.", "Priority": epic["priority"], "Story Points": "", "Status": epic["jira_status"], "Implementation Status": "Mixed; see stories", "Dependencies": "", "Sprint": "", "Requirement ID": ""})
    for story in PLAN["stories"]:
        rows.append({"Issue ID": story["id"], "Issue Type": "Story", "Planning Key": story["id"], "Epic Name": "", "Epic Link": story["epic"], "Parent ID": "", "Summary": story["title"], "Description": f"Objective: {story['objective']}\n\nDetail: {story['description']}\n\nInputs: {story['inputs']}\n\nExpected outputs: {story['outputs']}", "Acceptance Criteria": "\n".join(f"- {criterion}" for criterion in story["criteria"]), "Priority": story["priority"], "Story Points": story["points"], "Status": story["jira_status"], "Implementation Status": story["status"], "Dependencies": csv_text(story["depends"]), "Sprint": story["sprint"], "Requirement ID": csv_text(story["br"] + story["fr"])})
    for subtask in list_subtasks():
        story = next(item for item in PLAN["stories"] if item["id"] == subtask["parent"])
        rows.append({"Issue ID": subtask["id"], "Issue Type": "Sub-task", "Planning Key": subtask["id"], "Epic Name": "", "Epic Link": story["epic"], "Parent ID": subtask["parent"], "Summary": subtask["description"], "Description": f"Expected output: {subtask['output']}\nDependency: {subtask['dependency']}.", "Acceptance Criteria": subtask["output"], "Priority": story["priority"], "Story Points": "", "Status": subtask["status"], "Implementation Status": subtask["status"], "Dependencies": subtask["dependency"], "Sprint": story["sprint"], "Requirement ID": csv_text(story["br"] + story["fr"])})
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


SPEAKER_NOTES = {
    "cover": "TPM perspective: frame this as an evidence-led proof of concept and proposed enterprise delivery plan. Avoid implying Morgan Stanley sponsorship, production use, approvals, or outcomes not evidenced.",
    "01 | Context": "TPM perspective: lead with the business outcome and boundaries. Separate a demonstrated calculation from institutional data rights, control sign-off, and production readiness.",
    "02 | Source systems": "TPM perspective: identify owners and service contracts as dependencies. Explain that provider selection is a decision gate, not a technical assumption the team can make alone.",
    "03 | Components": "TPM perspective: explain the architecture in terms of ownership, contracts, control points, and the smallest viable target state. Keep proposed controls visibly distinct.",
    "04 | Orchestration": "TPM perspective: show which workstreams can proceed in parallel and where the data dependencies join. Do not call this a calculated critical path.",
    "05 | Data model": "TPM perspective: emphasize grain, financial precision, lineage, and business-key assumptions. Ask control owners to approve definitions before scaling.",
    "06 | Financial logic": "TPM perspective: state assumptions before results. Use reconciliation and Product Control acceptance as gates; do not generalize the prototype formula to all instruments.",
    "07 | Deploy and operate": "TPM perspective: treat identity, promotion, alerting, support, rerun evidence, and schedule activation as explicit readiness decisions.",
    "08 | Current and target": "TPM perspective: preserve the current architecture unless an approved requirement demands change. Sequence target controls behind source, security, and support decisions.",
    "09 | Quality and reconciliation": "TPM perspective: define evidence and defect thresholds before the test cycle; a failed financial reconciliation is a release blocker until triaged and accepted.",
    "10 | Security and operations": "TPM perspective: assign named control owners before deployment. A grants template or a successful dev run is not evidence of production approval.",
    "01 | Delivery baseline": "TPM perspective: establish the actual baseline first, then state the proposed work. Quote dated run evidence and its limitations accurately.",
    "02 | Business value": "TPM perspective: connect measurable outcomes to acceptance evidence. Targets marked proposed remain discussion points until owners approve them.",
    "03 | Stakeholders": "TPM perspective: clarify decision rights and escalation routes. Roles shown are proposed stakeholder functions, not named participants or completed interviews.",
    "04 | Requirements": "TPM perspective: show the control chain from business outcome to acceptance evidence. Traceability makes scope changes assessable rather than informal.",
    "05 | Delivery strategy": "TPM perspective: plan around capability outcomes and dependencies. Current implementation items are evidence, not new delivery points.",
    "06 | Roadmap": "TPM perspective: use relative weeks and capacity assumptions only. Reforecast when source, access, or stakeholder decisions move; optional work is not hidden in the baseline.",
    "07 | Dependency drivers": "TPM perspective: distinguish a dependency-led sequence from a CPM critical path. We lack activity durations, resource leveling, and float to calculate the latter.",
    "08 | Risk and governance": "TPM perspective: review triggers and owners weekly; escalate before the milestone is missed. Governance is deliberately proportionate to a small team.",
    "09 | Test and release": "TPM perspective: require reproducible evidence, business reconciliation, operational acceptance, and an explicit no-go path. Skips are not passes.",
    "10 | Status": "TPM perspective: report variance and uncertainty without spin. State known failures, unverified results, and the decision or owner needed to recover.",
    "11 | Next steps": "TPM perspective: close the highest-risk decisions first, preserve the paused schedule, and make any added scope pass change control.",
    "12 | Integrated Gantt": "TPM perspective: explain the bars as proposed sprint allocation windows, not task durations. The schedule uses relative weeks only; status colors distinguish evidence from planned schedule. Point out that no kickoff date or activity-level duration was supplied.",
    "13 | Dependency detail": "TPM perspective: walk the exact story-level finish-to-start relationships, including current but undated prerequisites. Explicitly state that CPM dates and float are not calculated because points and sprint windows do not provide activity durations or resource leveling.",
}


def add_speaker_notes(slide, text: str) -> None:
    slide.notes_slide.notes_text_frame.text = text


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
    add_speaker_notes(slide, SPEAKER_NOTES.get(section, "TPM perspective: connect the technical evidence to an accountable decision, dependency, acceptance gate, or risk. Label assumptions and unverified results explicitly."))
    return slide


def cover_slide(presentation: Presentation, title: str, subtitle: str, label: str, notes: str | None = None):
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
    add_speaker_notes(slide, notes or SPEAKER_NOTES["cover"])
    return slide


def _architecture_by_id() -> dict[str, dict]:
    return {_master_text(row.get("Component ID")): row for row in PLAN.get("architecture", [])}


def _architecture_description(component_id: str, fallback: str = "") -> str:
    row = _architecture_by_id().get(component_id)
    return _master_text(row.get("Description")) if row else fallback


def add_architecture_register_slides(presentation: Presentation) -> None:
    architecture = PLAN.get("architecture", [])
    for page_index, start in enumerate(range(0, len(architecture), 9), start=1):
        subset = architecture[start:start + 9]
        slide = slide_base(
            presentation,
            f"Architecture Register | Components {start + 1}-{start + len(subset)}",
            f"11 | Master workbook architecture register ({page_index})",
            11 + page_index,
        )
        columns = [
            ("Component ID", 0.88), ("Section", 1.12), ("Component", 1.55),
            ("Description", 4.15), ("Implementation Status", 1.30),
            ("Evidence / Source", 1.72), ("Requirement / Story IDs", 1.25),
            ("Predecessor Component IDs", 1.20),
        ]
        table_left, table_top, table_width, table_height = 0.55, 1.45, 12.22, 5.38
        table = slide.shapes.add_table(len(subset) + 1, len(columns), Inches(table_left), Inches(table_top), Inches(table_width), Inches(table_height)).table
        for column_index, (header, width) in enumerate(columns):
            table.columns[column_index].width = Inches(width)
            cell = table.cell(0, column_index)
            cell.text = header
            cell.fill.solid()
            cell.fill.fore_color.rgb = ppt_color(NAVY)
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            for paragraph in cell.text_frame.paragraphs:
                paragraph.font.name = "Aptos"
                paragraph.font.size = Pt(7.3)
                paragraph.font.bold = True
                paragraph.font.color.rgb = ppt_color(WHITE)
        for row_index, record in enumerate(subset, start=1):
            values = [
                record.get("Component ID", ""), record.get("Section", ""), record.get("Component", ""),
                record.get("Description", ""), record.get("Implementation Status", ""),
                record.get("Evidence / Source", ""), record.get("Requirement / Story IDs", ""),
                record.get("Predecessor Component IDs", ""),
            ]
            for column_index, value in enumerate(values):
                cell = table.cell(row_index, column_index)
                cell.text = _master_text(value)
                cell.fill.solid()
                cell.fill.fore_color.rgb = ppt_color(WHITE if row_index % 2 else "F3F6F7")
                cell.vertical_anchor = MSO_ANCHOR.TOP
                for paragraph in cell.text_frame.paragraphs:
                    paragraph.font.name = "Aptos"
                    paragraph.font.size = Pt(6.8)
                    paragraph.font.color.rgb = ppt_color(INK)
        add_speaker_notes(slide, "TPM perspective: use this register to distinguish the existing component boundary from proposed controls. Component statuses and descriptions are read from the master Architecture worksheet; do not treat PLANNED or BLOCKED rows as delivered.")


def build_architecture_deck() -> None:
    presentation = new_deck()
    architecture = _architecture_by_id()
    cover_slide(presentation, "Trading Data &\nDaily P&L Platform", "Evidence-based current architecture and a controlled path from proof of concept to a supportable prototype.", "Technical architecture")

    slide = slide_base(presentation, "Business context and supported scope", "01 | Context", 2)
    add_box(slide, 0.65, 1.45, 3.75, 2.08, "Business outcome", _architecture_description("ARCH-01"), "EFF4F6", TEAL, 17, 13)
    add_box(slide, 4.75, 1.45, 3.75, 2.08, "Verified prototype", _architecture_description("ARCH-02"), "EAF3F1", BLUE, 17, 13)
    gaps = [row for row in PLAN.get("architecture", []) if "PLANNED" in _master_text(row.get("Implementation Status")) or "BLOCKED" in _master_text(row.get("Implementation Status"))]
    gap_summary = "\n".join(f"{row['Component']}: {row['Implementation Status']}" for row in gaps[:4])
    add_box(slide, 8.85, 1.45, 3.75, 2.08, "Proposed / blocked", gap_summary, "FAF0E5", ORANGE, 17, 11)
    add_text(slide, 0.78, 4.03, 11.8, 0.55, "Initial calculation boundary", 17, NAVY, True)
    add_text(slide, 0.80, 4.58, 11.7, 1.25, "Simple cash equities in USD. No fees, settlement cash, FX, dividends, corporate actions, financing, borrow costs, or derivatives. A cumulative field called unrealized_pnl_proxy is explicitly not true unrealized P&L.", 16, INK)
    add_text(slide, 0.80, 6.25, 11.6, 0.42, "Decision gate: business and market-data owners must approve source, price definition, and calculation boundaries before production claims.", 12, RED, True)

    slide = slide_base(presentation, "Data sources and ingress contracts", "02 | Source systems", 3)
    add_box(slide, 0.65, 1.50, 3.75, 3.65, f"{architecture['ARCH-03']['Component']} | {architecture['ARCH-03']['Implementation Status']}", architecture["ARCH-03"]["Description"], "EFF4F6", TEAL, 14, 11)
    add_box(slide, 4.78, 1.50, 3.75, 3.65, f"{architecture['ARCH-04']['Component']} | {architecture['ARCH-04']['Implementation Status']}", architecture["ARCH-04"]["Description"], "EAF3F1", BLUE, 14, 11)
    add_box(slide, 8.90, 1.50, 3.75, 3.65, f"{architecture['ARCH-05']['Component']} | {architecture['ARCH-05']['Implementation Status']}", architecture["ARCH-05"]["Description"], "FAF0E5", ORANGE, 14, 11)
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
    add_box(slide, 0.82, 4.55, 3.55, 1.20, architecture["ARCH-11"]["Component"], architecture["ARCH-11"]["Description"], "F3F6F7", BLUE, 14, 10)
    add_box(slide, 4.88, 4.55, 3.55, 1.20, architecture["ARCH-06"]["Component"], architecture["ARCH-06"]["Description"], "F3F6F7", TEAL, 14, 10)
    add_box(slide, 8.92, 4.55, 3.55, 1.20, architecture["ARCH-12"]["Component"], architecture["ARCH-12"]["Description"], "F3F6F7", ORANGE, 14, 10)
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

    slide = slide_base(presentation, "Current prototype and proposed target state", "08 | Current and target", 9)
    current_components = [row for row in PLAN["architecture"] if "IMPLEMENTED" in _master_text(row.get("Implementation Status")) or "PARTIALLY" in _master_text(row.get("Implementation Status"))]
    target_components = [row for row in PLAN["architecture"] if "PLANNED" in _master_text(row.get("Implementation Status")) or "BLOCKED" in _master_text(row.get("Implementation Status"))]
    current_text = "\n\n".join(f"{row['Component']}: {row['Description']} [{row['Implementation Status']}]" for row in current_components[:6])
    target_text = "\n\n".join(f"{row['Component']}: {row['Description']} [{row['Implementation Status']}]" for row in target_components)
    add_box(slide, 0.65, 1.47, 5.82, 3.95, "Current | master Architecture worksheet", current_text, "EAF3F1", TEAL, 15, 9)
    add_box(slide, 6.86, 1.47, 5.82, 3.95, "Target | master Architecture worksheet", target_text, "FAF0E5", ORANGE, 15, 9)
    add_text(slide, 0.82, 5.85, 11.8, 0.60, "Target-state components are backlog proposals, not provisioned controls. No new schema, data provider, BI platform, or CI service is assumed by this design.", 13, RED, True)

    slide = slide_base(presentation, "Data quality and reconciliation controls", "09 | Quality and reconciliation", 10)
    add_box(slide, 0.65, 1.50, 3.72, 3.58, "Ingestion / Silver", "Exact CSV header and structural checks\nTyped conversion; business rules\nVersion and duplicate selection\nReject history with reason codes\nExpected price-session completeness gate", "EFF4F6", TEAL, 16, 12)
    add_box(slide, 4.78, 1.50, 3.72, 3.58, "Gold controls", "Position key: date/book/counterparty/instrument/currency\n\nP&L key: date/book/symbol/currency\n\nNULL for missing required marks/baselines; create valuation breaks", "EAF3F1", BLUE, 16, 12)
    add_box(slide, 8.90, 1.50, 3.72, 3.58, "Evidence and gaps", "Unit and fixture tests in repository\n2026-10-11 run: 5,990 positions and 835 P&L rows; zero reported independent SQL mismatches\n\nSame-input second run: NOT VERIFIED\n3 focused local tests fail on stale DDL paths", "FAF0E5", ORANGE, 15, 11)
    add_text(slide, 0.82, 5.50, 11.6, 0.78, "Release gate: rerun in a supported Spark/Delta environment, reconcile by business grain, close or disposition test failures, obtain Product Control acceptance, then record evidence.", 13, NAVY, True)

    slide = slide_base(presentation, "Security, deployment and operations readiness", "10 | Security and operations", 11)
    add_box(slide, 0.65, 1.47, 3.72, 3.80, "Verified controls", "Unity Catalog catalog/schemas and Volume\n\nGrant template with principal placeholder\n\nCLI authentication kept outside source\n\nTask audit, error details, rejects, breaks\n\nAsset Bundle builds/deploys dev wheel", "EAF3F1", TEAL, 16, 12)
    add_box(slide, 4.78, 1.47, 3.72, 3.80, "Not verified / missing", "No production service principal or non-dev target evidenced\n\nNo CI workflow in repository\n\nNo automated failure notification\n\nRetention, RTO/RPO, support SLA not approved\n\nSchedule PAUSED; no go-live approval", "FAF0E5", ORANGE, 16, 12)
    add_box(slide, 8.90, 1.47, 3.72, 3.80, "Readiness decision", "Confirm data classification and access\n\nReview least privilege and secrets\n\nTest alerts, rerun, recovery and rollback\n\nApprove owner, retention and support process\n\nExplicit no-go if any critical control is open", "EFF4F6", BLUE, 16, 12)
    add_text(slide, 0.82, 5.68, 11.6, 0.58, "GitHub/CI is a proposed delivery control in this plan; the checked-in repository currently shows the Databricks Asset Bundle and deploy script, not a CI workflow.", 12, MUTED)

    add_architecture_register_slides(presentation)
    presentation.save(OUT / "Trading_Platform_Architecture.pptx")


def build_delivery_deck() -> None:
    presentation = new_deck()
    slide = slide_base(presentation, "Project overview | problem, objectives and scope", "01 | Delivery baseline", 1)
    add_box(slide, 0.65, 1.46, 3.72, 2.26, "Business problem", "Trade snapshots and daily equity prices need controlled validation, position aggregation, valuation, P&L and exception visibility.", "EFF4F6", TEAL, 16, 12)
    add_box(slide, 4.80, 1.46, 3.72, 2.26, "Objective", PLAN["purpose"], "EAF3F1", BLUE, 16, 12)
    add_box(slide, 8.95, 1.46, 3.72, 2.26, "Technology and scope", "Databricks; PySpark / SQL; Delta Lake; Unity Catalog; Workflows; Asset Bundle; synthetic trades; prototype prices.", "FAF0E5", ORANGE, 16, 12)
    add_text(slide, 0.82, 4.15, 11.7, 0.80, "Current implementation is a technical proof of concept. The brief says Azure Databricks, while workspace evidence indicates AWS; target cloud needs confirmation. No production banking deployment or approval is claimed.", 14, RED, True)
    add_text(slide, 0.82, 5.25, 11.6, 0.65, "Boundaries: USD equities, ten configured symbols, simplified daily P&L; FX, derivatives, fees, dividends and corporate actions are excluded.", 13, MUTED)

    slide = slide_base(presentation, "Business value and measurable success criteria", "02 | Business value", 3)
    criteria = [
        ("Trade completeness", "Exact 14-column input contract; invalid file cannot replace the prior Bronze snapshot."),
        ("Position accuracy", "Daily signed quantities reconcile by date, book, counterparty and instrument."),
        ("P&L integrity", "Daily P&L reconciles using approved inputs; missing required marks remain NULL with breaks."),
        ("Operational control", "Every task attempt is auditable; identical-input rerun preserves business outputs (proposed acceptance; rerun not verified)."),
    ]
    for index, (title, body) in enumerate(criteria):
        x = 0.68 + (index % 2) * 6.25
        y = 1.48 + (index // 2) * 1.83
        add_box(slide, x, y, 5.75, 1.42, title, body, "EFF4F6" if index % 2 == 0 else "EAF3F1", TEAL if index % 2 == 0 else BLUE, 16, 12)
    add_text(slide, 0.82, 5.55, 11.5, 0.70, "Verified evidence: 2026-10-11 Databricks run had six successful tasks; 5,990 positions and 835 P&L rows had zero reported independent SQL mismatches. This is one run, not a service-level commitment.", 12, INK)

    slide = slide_base(presentation, "Stakeholder landscape and decision ownership", "03 | Stakeholders", 4)
    add_box(slide, 0.65, 1.48, 3.72, 3.88, "Business and control", "Sponsor: scope/capacity/go-no-go\n\nBA / Product Owner: requirements and acceptance\n\nProduct Control: calculation and reconciliation approval\n\nTrading Operations: trade source and operating semantics", "EFF4F6", TEAL, 16, 12)
    add_box(slide, 4.80, 1.48, 3.72, 3.88, "Technology delivery", "TPM: integrated plan, RAID, dependencies, governance\n\nSolution Architect: architecture and decisions\n\nData Engineering: implementation and technical evidence\n\nQA: test evidence and defect visibility", "EAF3F1", BLUE, 16, 12)
    add_box(slide, 8.95, 1.48, 3.72, 3.88, "Control / operations", "Information Security: access and policy review\n\nProduction Support: monitoring, recovery and handover\n\nMarket Data Owner: provider rights and service contract\n\nNamed participants and approvals: NOT VERIFIED", "FAF0E5", ORANGE, 16, 12)
    add_text(slide, 0.82, 5.72, 11.7, 0.50, "RACI is proposed for planning; role ownership must be confirmed at mobilisation.", 13, MUTED)

    slide = slide_base(presentation, "Solution architecture | preserve the working core", "04 | Architecture", 5)
    add_box(slide, 0.55, 1.58, 2.18, 1.26, "Inputs", "Synthetic trade CSV\nYahoo Finance prototype", "EFF4F6", TEAL, 15, 11)
    add_box(slide, 3.18, 1.36, 2.34, 1.03, "Bronze", "Raw Delta + lineage", "EAF3F1", TEAL, 15, 11)
    add_box(slide, 3.18, 2.78, 2.34, 1.03, "Silver", "Typed data + rejects", "EAF3F1", BLUE, 15, 11)
    add_box(slide, 6.06, 1.36, 2.70, 1.03, "Gold positions", "Daily inventory + value", "FAF0E5", ORANGE, 15, 11)
    add_box(slide, 6.06, 2.78, 2.70, 1.03, "Gold P&L", "Daily result + breaks + audit", "FAF0E5", ORANGE, 15, 11)
    add_box(slide, 9.34, 1.58, 3.18, 1.26, "Consumers", "Report notebook + SQL\nDashboard: PLANNED", "EFF4F6", BLUE, 15, 11)
    add_arrow(slide, 2.77, 2.20, 3.13, 1.90)
    add_arrow(slide, 4.36, 2.42, 4.36, 2.73)
    add_arrow(slide, 5.59, 3.30, 6.00, 1.88)
    add_arrow(slide, 7.40, 2.42, 7.40, 2.73)
    add_arrow(slide, 8.83, 3.29, 9.29, 2.18)
    add_text(slide, 0.78, 4.62, 11.8, 1.0, "Orchestration: two independent source branches → Silver join at build_positions → calculate_daily_pnl. Databricks Workflow, Unity Catalog cibc and Asset Bundle dev target are implemented; production targets and security controls remain gated.", 14, INK)

    slide = slide_base(presentation, "Requirements traceability | from outcome to evidence", "04 | Requirements", 6)
    business_req = next(item for item in PLAN["business_requirements"] if item["id"] == "BR-005")
    functional_req = next(item for item in PLAN["functional_requirements"] if item["id"] == "FR-008")
    example_story_id = functional_req["stories"][0]
    example_story = next(item for item in PLAN["stories"] if item["id"] == example_story_id)
    example_test = next((item for item in TESTS if example_story_id in _master_refs(item["story"])), None)
    test_id = example_test["id"] if example_test else "Test not mapped"
    chain = [(0.58, business_req["id"], business_req["description"]), (3.15, functional_req["id"], functional_req["description"]), (6.08, example_story_id, example_story["title"]), (9.02, test_id, example_test["scope"] if example_test else "No test mapped")]
    for x, title, body in chain:
        add_box(slide, x, 1.65, 2.38, 1.45, title, body, "EFF4F6", TEAL, 15, 11)
    for x in [2.98, 5.91, 8.84]:
        add_arrow(slide, x, 2.37, x + 0.15, 2.37)
    add_box(slide, 1.12, 3.82, 5.00, 1.30, "Acceptance", functional_req["acceptance"], "EAF3F1", BLUE, 15, 10)
    evidence_text = example_test["evidence"] if example_test else "No linked test evidence is recorded in the master workbook."
    add_box(slide, 7.17, 3.82, 5.00, 1.30, "Evidence", evidence_text, "FAF0E5", ORANGE, 15, 10)
    add_text(slide, 0.85, 5.65, 11.4, 0.54, "The workbook traceability matrix covers all BR → FR → story → test/evidence mappings; IDs are shared with the CSV.", 13, NAVY, True)

    slide = slide_base(presentation, "Delivery strategy | capability-based backlog", "05 | Delivery strategy", 7)
    total_points = sum(story["points"] for story in PLAN["stories"])
    add_box(slide, 0.66, 1.48, 3.70, 1.25, f"{len(PLAN['epics'])} epics", "Outcome-oriented capabilities", "EFF4F6", TEAL, 16, 11)
    add_box(slide, 4.80, 1.48, 3.70, 1.25, f"{len(PLAN['stories'])} stories", f"{total_points} estimated points across current + proposed work", "EAF3F1", BLUE, 16, 11)
    add_box(slide, 8.93, 1.48, 3.70, 1.25, f"{len(list_subtasks())} subtasks", "Specific evidence under each story", "FAF0E5", ORANGE, 16, 11)
    add_text(slide, 0.82, 3.12, 11.6, 0.62, "Delivery approach: clarify scope and acceptance → design/data contracts → build in parallel workstreams → verify with tests and reconciliation → release only after readiness gate.", 15, NAVY, True)
    add_text(slide, 0.82, 4.05, 11.5, 0.80, "8-week baseline: 38 SP planned against 40 assumed capacity (10 SP/sprint); 2 SP aggregate headroom, not a guaranteed reserve. Optional Sprint 5: 10 SP for data approval, price exception operations and dashboard.", 14, INK)
    add_text(slide, 0.82, 5.15, 11.5, 0.78, "Current implementation stories retain evidence status. Their point values are reference estimates, not remaining-work effort or actual velocity.", 12, MUTED)

    slide = slide_base(presentation, "Integrated delivery roadmap | relative weeks", "06 | Roadmap", 8)
    story_by_id = {story["id"]: story for story in PLAN["stories"]}
    milestone_by_sprint = {"Sprint 1": "M-01", "Sprint 2": "M-02", "Sprint 3": "M-03", "Sprint 4": "M-04", "Sprint 5": "M-05"}
    for index, sprint in enumerate(PLAN["sprints"]):
        y = 1.34 + index * 1.02
        points = sum(story_by_id[story_id]["points"] for story_id in sprint["stories"])
        title = f"{sprint['weeks']} | {points} SP"
        if not sprint["baseline"]:
            title += " | optional"
        add_box(slide, 0.68, y, 2.65, 0.78, title, milestone_by_sprint[sprint["name"].split(" ")[0] + " " + sprint["name"].split(" ")[1]] if "(" in sprint["name"] else milestone_by_sprint[sprint["name"]], "EFF4F6" if sprint["baseline"] else "FAF0E5", TEAL if sprint["baseline"] else ORANGE, 12, 9)
        add_text(slide, 3.62, y + 0.05, 3.30, 0.67, csv_text(sprint["stories"]), 10, NAVY, True)
        add_text(slide, 7.05, y + 0.05, 5.05, 0.67, sprint["goal"], 11, INK)
    add_text(slide, 0.82, 6.55, 11.6, 0.30, "Baseline: 38/40 SP; 2 SP headroom. Optional Sprint 5 is follow-on, not contingency capacity committed to this baseline. Relative weeks only.", 10, MUTED)

    slide = slide_base(presentation, "Dependency drivers and delivery constraints", "07 | Dependency drivers", 9)
    add_box(slide, 0.70, 1.50, 5.70, 1.30, "Trade workstream", "Source contract → Bronze snapshot → Silver version/correction rules → positions → P&L reconciliation", "EAF3F1", BLUE, 15, 11)
    add_box(slide, 6.92, 1.50, 5.70, 1.30, "Price workstream", "Provider decision → Bronze prices → Silver completeness → position valuation / P&L baseline", "EFF4F6", TEAL, 15, 11)
    add_arrow(slide, 6.44, 2.15, 6.84, 2.15)
    add_box(slide, 2.30, 3.38, 8.55, 1.22, "Join and release dependencies", "Both Silver outputs → positions → P&L → supported-runtime tests / reconciliation → UAT → handover", "FAF0E5", ORANGE, 15, 11)
    add_text(slide, 0.86, 5.12, 11.4, 1.10, PLAN["critical_path_note"] + "\n\nExternal constraints: market-data rights, source-owner decisions, Spark/Delta test access, Security review, Product Control sign-off, and support availability.", 12, RED, True)

    slide = slide_base(presentation, "Risk and governance | proportionate control", "08 | Risk and governance", 10)
    risk_items = PLAN["risks"][:4]
    for index, risk in enumerate(risk_items):
        y = 1.34 + index * 0.82
        add_text(slide, 0.72, y, 3.10, 0.28, f"{risk['id']} | {risk['probability']} / {risk['impact']}", 11, ORANGE, True)
        add_text(slide, 3.85, y, 8.70, 0.42, risk["description"], 11, INK)
    add_box(slide, 0.75, 4.80, 5.75, 1.18, "Cadence", "Daily engineering stand-up; weekly workstream + RAID; design/security reviews at gates; biweekly steering; milestone and release-readiness approvals.", "EFF4F6", TEAL, 14, 10)
    add_box(slide, 6.78, 4.80, 5.75, 1.18, "Escalation", "Trigger/owner/date in RAID; unresolved delivery blocker → TPM; business/scope trade-off → Sponsor; policy/security/data rights → accountable control owner.", "EAF3F1", BLUE, 14, 10)

    slide = slide_base(presentation, "Testing and release readiness", "09 | Test and release", 11)
    add_box(slide, 0.65, 1.46, 3.72, 3.78, "Test evidence", "Unit and schema tests\n\nData-quality/reject tests\n\nPosition and P&L reconciliation\n\nSame-input idempotency + failure recovery\n\nUAT and deployment smoke test", "EFF4F6", TEAL, 16, 12)
    add_box(slide, 4.80, 1.46, 3.72, 3.78, "Known status", "2026-10-11: six tasks succeeded; 5,990 positions and 835 P&L rows had zero reported mismatches\n\n3 focused tests fail on removed DDL file references\n\nSame-input second Job run: NOT VERIFIED", "FAF0E5", ORANGE, 15, 11)
    add_box(slide, 8.95, 1.46, 3.72, 3.78, "No-go until", "Supported Spark/Delta tests pass or disposition is approved\n\nProduct Control accepts results\n\nLeast privilege, alerts, recovery and support proven\n\nSource rights/retention/release owner approved", "EAF3F1", BLUE, 15, 11)
    add_text(slide, 0.82, 5.67, 11.7, 0.52, "No CI workflow, production Bundle target, automated alerting, or schedule activation is evidenced in this repository.", 12, RED, True)

    slide = slide_base(presentation, "Project status | what is and is not demonstrated", "10 | Status", 12)
    implemented_stories = [story for story in PLAN["stories"] if story["status"] == "IMPLEMENTED"]
    partial_stories = [story for story in PLAN["stories"] if story["status"] == "PARTIALLY IMPLEMENTED"]
    planned_stories = [story for story in PLAN["stories"] if story["status"] in {"PLANNED", "BLOCKED", "NOT VERIFIED"}]
    implemented_summary = "; ".join(f"{story['id']} {story['title']}" for story in implemented_stories[:4])
    partial_summary = "; ".join(f"{story['id']} {story['status']}" for story in partial_stories[:5])
    planned_summary = "; ".join(f"{story['id']} {story['status']}" for story in planned_stories[:5])
    add_box(slide, 0.65, 1.48, 3.72, 2.00, "IMPLEMENTED STORIES", implemented_summary or "No stories marked implemented in the master.", "EAF3F1", TEAL, 14, 10)
    add_box(slide, 4.80, 1.48, 3.72, 2.00, "PARTIALLY IMPLEMENTED", partial_summary or "No stories marked partial in the master.", "FAF0E5", ORANGE, 14, 10)
    add_box(slide, 8.95, 1.48, 3.72, 2.00, "PLANNED / BLOCKED / NOT VERIFIED", planned_summary or "No open stories in the master.", "F8E8E6", RED, 13, 9)
    add_text(slide, 0.82, 4.16, 11.6, 0.84, "Overall status: AMBER for a demonstrated proof of concept; NOT VERIFIED for the proposed schedule, named resources, and enterprise readiness. Quality has known focused test failures.", 14, NAVY, True)
    add_text(slide, 0.82, 5.26, 11.6, 0.62, "Evidence baseline: repository and docs/test_results.md dated 2026-10-11; this plan is dated 2026-10-14.", 12, MUTED)
    status_examples = "; ".join(f"{story['id']}: {story['status']}" for story in PLAN["stories"] if story["id"] in {"TMP-018", "TMP-016", "TMP-022"})
    add_text(slide, 0.82, 6.02, 11.6, 0.42, f"Master backlog status example: {status_examples}", 10, MUTED, True)

    slide = slide_base(presentation, "Outcomes and next steps", "11 | Next steps", 13)
    add_box(slide, 0.65, 1.48, 3.72, 2.34, "Demonstrated", "End-to-end Medallion path, auditable tasks, reject handling, position/P&L calculations, and independent SQL reconciliation for one documented run.", "EAF3F1", TEAL, 16, 12)
    add_box(slide, 4.80, 1.48, 3.72, 2.34, "Leadership lessons", "Make source ownership explicit; protect financial acceptance; plan dependencies before sprint commitments; surface test gaps early; separate prototype evidence from production claims.", "EFF4F6", BLUE, 16, 12)
    add_box(slide, 8.95, 1.48, 3.72, 2.34, "Immediate decisions", "Name decision owners; approve trade/price contracts; provision test runtime; close stale tests; confirm methodology; retain optional scope until capacity and audience are approved.", "FAF0E5", ORANGE, 16, 12)
    add_text(slide, 0.82, 4.42, 11.6, 0.84, "Production-readiness roadmap: source and rights → access/security → CI and repeatable deployment → test/reconciliation and UAT → monitoring/recovery and support → explicit release approval.", 14, NAVY, True)
    add_text(slide, 0.82, 5.55, 11.6, 0.66, "Keep the scheduled Job paused unless a separate readiness review approves activation.", 13, RED, True)

    add_integrated_gantt_slides(presentation)

    presentation.save(OUT / "Trading_Platform_Delivery_Overview.pptx")


def add_integrated_gantt_slides(presentation: Presentation) -> None:
    activities = get_gantt_activities()
    activity_by_id = {activity["id"]: activity for activity in activities}
    dependency_edges = gantt_dependency_edges()

    slide = slide_base(
        presentation,
        "Integrated Project Delivery Plan — Timeline & Dependencies",
        "12 | Integrated Gantt",
        14,
    )
    add_text(slide, 0.58, 1.20, 7.3, 0.24, "KEY DELIVERY DEPENDENCIES — ILLUSTRATIVE BASELINE", 9.5, ORANGE, True)
    add_text(slide, 8.00, 1.20, 4.75, 0.24, "Relative weeks; no kickoff date supplied", 8.5, MUTED, False, align=PP_ALIGN.RIGHT)

    id_x, task_x, duration_x, predecessor_x, points_x, status_x = 0.43, 0.96, 2.46, 2.94, 4.06, 4.48
    timeline_x, timeline_width = 5.62, 7.08
    week_width = timeline_width / 8
    header_y, rows_y, row_height = 1.48, 1.82, 0.365
    headings = [
        (id_x, 0.48, "ID"), (task_x, 1.43, "WORKSTREAM / TASK"),
        (duration_x, 0.43, "WINDOW"), (predecessor_x, 1.08, "PREDECESSORS"),
        (points_x, 0.34, "SP"), (status_x, 1.02, "EVIDENCE"),
    ]
    for x, width, label in headings:
        add_text(slide, x, header_y, width, 0.24, label, 7.5, MUTED, True)
    for week in range(1, 9):
        x = timeline_x + (week - 1) * week_width
        add_text(slide, x, header_y, week_width, 0.24, f"WEEK {week}", 8, NAVY, True, align=PP_ALIGN.CENTER)
        grid = slide.shapes.add_connector(
            MSO_CONNECTOR.STRAIGHT,
            Inches(x), Inches(rows_y - 0.04), Inches(x), Inches(rows_y + row_height * len(activities)),
        )
        grid.line.color.rgb = ppt_color(GRID)
        grid.line.width = Pt(0.55)
    right_grid = slide.shapes.add_connector(
        MSO_CONNECTOR.STRAIGHT,
        Inches(timeline_x + timeline_width), Inches(rows_y - 0.04),
        Inches(timeline_x + timeline_width), Inches(rows_y + row_height * len(activities)),
    )
    right_grid.line.color.rgb = ppt_color(GRID)
    right_grid.line.width = Pt(0.55)

    row_centers: dict[str, float] = {}
    for row_index, activity in enumerate(activities):
        y = rows_y + row_index * row_height
        center_y = y + row_height / 2
        row_centers[activity["id"]] = center_y
        if row_index % 2 == 1:
            band = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.40), Inches(y), Inches(12.34), Inches(row_height))
            band.fill.solid()
            band.fill.fore_color.rgb = ppt_color("F7F9FA")
            band.line.fill.background()
            band.z_order = 1
        add_text(slide, id_x, y + 0.04, 0.48, 0.23, activity["id"], 7.5, NAVY, True)
        add_text(slide, task_x, y + 0.025, 1.43, 0.29, activity["name"], 7.6, INK, True)
        add_text(slide, duration_x, y + 0.04, 0.43, 0.23, "2w*", 7.5, MUTED, False, align=PP_ALIGN.CENTER)
        predecessor_labels = activity["predecessor_activities"] + activity["external_predecessors"]
        predecessor_text = "/".join(item.replace("WS-", "W") for item in predecessor_labels) if predecessor_labels else "-"
        add_text(slide, predecessor_x, y + 0.04, 1.08, 0.23, predecessor_text, 7.1, MUTED, False)
        add_text(slide, points_x, y + 0.04, 0.34, 0.23, str(activity["story_points"]), 7.5, NAVY, True, align=PP_ALIGN.CENTER)
        short_status = {
            "IMPLEMENTED": "DONE",
            "PARTIALLY IMPLEMENTED": "PARTIAL",
            "PLANNED": "PLANNED",
            "BLOCKED": "BLOCKED",
            "NOT VERIFIED": "NOT VERIFIED",
        }.get(activity["evidence_status"], "MIXED")
        status_color = {
            "IMPLEMENTED": NAVY,
            "PARTIALLY IMPLEMENTED": BLUE,
            "PLANNED": MUTED,
            "BLOCKED": RED,
            "NOT VERIFIED": ORANGE,
        }.get(activity["evidence_status"], ORANGE)
        add_text(slide, status_x, y + 0.04, 1.02, 0.23, short_status, 7.3, status_color, True)

        bar_x = timeline_x + (activity["start_week"] - 1) * week_width + 0.025
        bar_width = activity["duration_weeks"] * week_width - 0.05
        bar = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(bar_x), Inches(y + 0.09), Inches(bar_width), Inches(0.19))
        bar.fill.solid()
        bar.fill.fore_color.rgb = ppt_color("D8DEE3")
        bar.line.color.rgb = ppt_color("AAB6BE")
        bar.line.width = Pt(0.45)
        bar.adjustments[0] = 0.08
        add_text(slide, bar_x, y + 0.075, bar_width, 0.20, f"{activity['story_points']} SP", 7.5, NAVY, True, align=PP_ALIGN.CENTER)

        separator = slide.shapes.add_connector(
            MSO_CONNECTOR.STRAIGHT,
            Inches(0.40), Inches(y + row_height), Inches(12.74), Inches(y + row_height),
        )
        separator.line.color.rgb = ppt_color("E6EBEE")
        separator.line.width = Pt(0.4)

    # Only draw time-scaled FS connectors when one sprint window finishes before
    # the successor window begins. Same-sprint ordering is documented on slide 15.
    for edge in dependency_edges:
        predecessor = activity_by_id[edge["predecessor"]]
        successor = activity_by_id[edge["successor"]]
        if not edge["forward_in_sprint_windows"] or successor["start_week"] != predecessor["finish_week"] + 1:
            continue
        x = timeline_x + predecessor["finish_week"] * week_width
        connector = slide.shapes.add_connector(
            MSO_CONNECTOR.STRAIGHT,
            Inches(x), Inches(row_centers[predecessor["id"]]),
            Inches(x), Inches(row_centers[successor["id"]]),
        )
        connector.line.color.rgb = ppt_color(BLUE)
        connector.line.width = Pt(1.1)
        connector.line.end_arrowhead = True

    milestone_y = rows_y + row_height * len(activities) + 0.12
    for milestone in PLAN["milestones"]:
        week_match = re.search(r"Week\s+(\d+)", milestone["week"])
        if not week_match or "optional" in milestone["week"].lower():
            continue
        week = int(week_match.group(1))
        if week > 8:
            continue
        x = timeline_x + week * week_width
        diamond = slide.shapes.add_shape(MSO_SHAPE.DIAMOND, Inches(x - 0.075), Inches(milestone_y), Inches(0.15), Inches(0.15))
        diamond.fill.solid()
        diamond.fill.fore_color.rgb = ppt_color(ORANGE)
        diamond.line.color.rgb = ppt_color(ORANGE)
        add_text(slide, x - 0.28, milestone_y + 0.14, 0.56, 0.18, milestone["id"], 7, NAVY, True, align=PP_ALIGN.CENTER)

    legend_y = 6.38
    legend_items = [
        (0.52, NAVY, "Implemented evidence (current, undated)", 2.10),
        (2.90, BLUE, "Partially implemented evidence", 2.10),
        (5.30, "D8DEE3", "Planned sprint window", 1.95),
    ]
    for x, color, label, label_width in legend_items:
        swatch = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(legend_y + 0.02), Inches(0.16), Inches(0.13))
        swatch.fill.solid()
        swatch.fill.fore_color.rgb = ppt_color(color)
        swatch.line.color.rgb = ppt_color(color)
        add_text(slide, x + 0.22, legend_y, label_width, 0.22, label, 7.3, INK)
    diamond_legend = slide.shapes.add_shape(MSO_SHAPE.DIAMOND, Inches(7.55), Inches(legend_y + 0.01), Inches(0.15), Inches(0.15))
    diamond_legend.fill.solid()
    diamond_legend.fill.fore_color.rgb = ppt_color(ORANGE)
    diamond_legend.line.color.rgb = ppt_color(ORANGE)
    add_text(slide, 7.77, legend_y, 1.0, 0.22, "Milestone", 7.3, INK)
    critical_legend = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(8.93), Inches(legend_y + 0.01), Inches(0.30), Inches(0.15))
    critical_legend.fill.solid()
    critical_legend.fill.fore_color.rgb = ppt_color(WHITE)
    critical_legend.line.color.rgb = ppt_color(RED)
    critical_legend.line.width = Pt(1.1)
    add_text(slide, 9.33, legend_y - 0.01, 3.35, 0.24, "Zero-float CPM critical path: none calculated", 7.1, RED, True)
    add_text(slide, 0.52, 6.67, 12.10, 0.26, "* Bars show two-week sprint allocation windows, not task durations. M-05 and the blocked dashboard are optional Week 9-10 follow-on; not on this 8-week axis.", 8, MUTED)

    detail = slide_base(presentation, "Detailed Dependencies & Critical-Path Readiness", "13 | Dependency detail", 15)
    add_text(detail, 0.58, 1.20, 7.5, 0.24, "KEY DELIVERY DEPENDENCIES — ILLUSTRATIVE BASELINE", 9.2, ORANGE, True)
    add_text(detail, 8.30, 1.20, 4.40, 0.24, "CPM dates and float are not calculated", 8.5, RED, True, align=PP_ALIGN.RIGHT)

    def dependency_node(x: float, y: float, title: str, body: str, current: bool = False) -> None:
        fill = "EAF3F1" if current else "EFF4F6"
        accent = NAVY if current else TEAL
        add_box(detail, x, y, 1.56, 0.73, title, body, fill, accent, 9.2, 7.2)

    node_x = [0.55, 2.47, 4.39, 6.31]
    activity_by_id = {activity["id"]: activity for activity in activities}
    def node_title(activity_id: str) -> str:
        activity = activity_by_id[activity_id]
        return f"{activity_id} {activity['name']}"

    def node_stories(activity_id: str) -> str:
        return csv_text(activity_by_id[activity_id]["stories"])

    dependency_node(node_x[0], 1.60, node_title("WS-01"), node_stories("WS-01"))
    dependency_node(node_x[1], 1.60, node_title("WS-03"), node_stories("WS-03"))
    dependency_node(node_x[2], 1.60, node_title("WS-06"), node_stories("WS-06"))
    for left_index in (0, 1):
        add_arrow(detail, node_x[left_index] + 1.58, 1.97, node_x[left_index + 1] - 0.05, 1.97, BLUE, 1.2)

    dependency_node(node_x[0], 2.55, node_title("WS-02"), node_stories("WS-02"))
    dependency_node(node_x[1], 2.55, node_title("WS-07"), node_stories("WS-07"))
    add_arrow(detail, node_x[0] + 1.58, 2.92, node_x[1] - 0.05, 2.92, BLUE, 1.2)

    dependency_node(node_x[0], 3.50, "Current Silver", "TMP-015", current=True)
    dependency_node(node_x[1], 3.50, node_title("WS-08"), node_stories("WS-08"))
    dependency_node(node_x[2], 3.50, node_title("WS-10"), node_stories("WS-10"))
    dependency_node(node_x[3], 3.50, node_title("WS-11"), node_stories("WS-11"))
    for left_index in (0, 1, 2):
        add_arrow(detail, node_x[left_index] + 1.58, 3.87, node_x[left_index + 1] - 0.05, 3.87, BLUE, 1.2)

    add_box(
        detail, 8.22, 1.54, 4.48, 2.12, "CPM status: NOT CALCULATED",
        "Earliest start / finish: NOT AVAILABLE\nLatest start / finish: NOT AVAILABLE\nTotal float: NOT AVAILABLE\n\nThe repository has story points and sprint windows, not activity-level elapsed durations, a kickoff calendar, resource leveling, or approved lag/constraint data. No activity is outlined as critical.",
        "FAF0E5", ORANGE, 14, 9.5,
    )
    add_box(
        detail, 8.22, 3.82, 4.48, 1.12, "Milestone impact",
        "M-01 W2: requirements / trade contract\nM-02 W4: source / access decisions\nM-03 W6: controls / trade history\nM-04 W8: test, reconciliation and handover",
        "EFF4F6", BLUE, 12, 8.7,
    )

    add_text(detail, 0.58, 4.58, 7.45, 0.22, "Detailed predecessor register (FS = finish-to-start; current prerequisites are undated)", 8.6, NAVY, True)
    register = [row for row in get_gantt_dependency_register() if row["baseline"]]
    table_x = [0.60, 3.75, 5.70]
    table_w = [3.05, 1.88, 2.15]
    headers = ["PREDECESSOR STORY IDs", "SUCCESSOR", "MILESTONE IMPACT"]
    for x, width, label in zip(table_x, table_w, headers):
        add_text(detail, x, 4.82, width, 0.19, label, 7.2, MUTED, True)
    for row_index, dependency in enumerate(register):
        pred, succ, impact = dependency["predecessors"], dependency["successor_story_ids"], dependency["milestone_impact"]
        if dependency["successor"] == "TMP-020":
            succ += " (BLOCKED)"
        y = 5.04 + row_index * 0.18
        if row_index % 2 == 1:
            band = detail.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.56), Inches(y - 0.015), Inches(7.42), Inches(0.18))
            band.fill.solid()
            band.fill.fore_color.rgb = ppt_color("F5F8FA")
            band.line.fill.background()
            band.z_order = 1
        add_text(detail, table_x[0], y, table_w[0], 0.16, pred, 6.8, INK)
        add_text(detail, table_x[1], y, table_w[1], 0.16, succ, 6.8, INK, True)
        add_text(detail, table_x[2], y, table_w[2], 0.16, impact, 6.8, INK)
    add_text(detail, 0.60, 6.72, 12.0, 0.20, "CPM inputs required: task durations, calendar start, complete lag-aware dependencies, and resource constraints. Optional Sprint 5 predecessors remain in the workbook/Jira register.", 7.6, RED, True)


def write_readme(dictionary_count: int, jira_count: int) -> None:
    implementation_counts = defaultdict(int)
    for story in PLAN["stories"]:
        implementation_counts[story["status"]] += 1
    sprint_summary = "; ".join(
        f"{sprint['name']}: {sum(story['points'] for story in PLAN['stories'] if story['sprint'].split(' (')[0] == sprint['name'].split(' (')[0])}/{sprint['capacity']} SP"
        for sprint in PLAN["sprints"]
    )
    source_notes = "\n".join(f"- {source['name']} [{source['status']}]: {source['open_items']}" for source in PLAN["sources"])
    status_summary = "; ".join(f"{status}: {count}" for status, count in sorted(implementation_counts.items()))
    content = f'''# Trading Platform SDLC Deliverables

Generated on {AS_OF} from the repository implementation and `Trading_Project_Master.xlsx`.

## Files

- `Trading_Platform_Project_Plan.xlsx` — requirements, source specifications, master-sourced data dictionary ({dictionary_count} physical columns), backlog, acceptance criteria, dependencies, sprint capacity, Gantt, milestones, RAID, traceability, tests, and current-state evidence.
- `Trading_Platform_Architecture.pptx` — current and target architecture, sources, data flow, model, calculations, workflow, deployment, and explicit gaps.
- `Trading_Platform_Delivery_Overview.pptx` — executive delivery plan plus native-shape eight-week Gantt and dependency detail. CPM dates/float are explicitly not calculated because no activity-level durations/calendar are evidenced.
- `jira-backlog.csv` — {jira_count} Jira planning rows (epics, stories, subtasks) with acceptance criteria and dependencies.
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

Story implementation statuses from the master: {status_summary}.

Source decisions are maintained in `Data_Sources`:
{source_notes}

Sprint capacity and optional follow-on scope are maintained in `Sprints`:
{sprint_summary}. Timelines are relative; CPM dates and float are not calculated from sprint windows or story points. The development schedule remains paused unless a separate readiness decision is recorded.

## Jira import mapping

This is a planning CSV, not a guaranteed one-click Jira import. `Issue ID` and `Planning Key` values are temporary identifiers, not Jira-generated issue keys. Map `Planning Key` to a text custom field; map `Issue Type`, `Summary`, `Description`, `Priority`, `Status`, `Story Points`, and `Sprint` to the destination Jira fields. Map `Epic Name` and `Epic Link` to the destination edition's epic fields. Map `Parent ID` to the parent relationship field for subtasks; some Jira importers require numeric issue IDs or a separate import pass. Import epics first, capture created Jira keys, then resolve temporary Epic Link references before importing stories and subtasks. `Dependencies` contains planning IDs and should be imported as issue links only after IDs resolve. Confirm required fields, status names, story-point field, hierarchy support, and project permissions with the Jira administrator before import.

## Workbook / Report Notes

The report workbook retains filters, frozen headers, dropdowns, conditional formatting, formulas, and a master-sourced eight-week workstream Gantt. Gantt bars show sprint allocation windows, not task-level durations. The editable master has structured tables and validation. Excel recalculates formulas when opened.
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
            notes = slide.notes_slide.notes_text_frame.text.strip()
            if notes:
                parts.append(f'<p class="note"><strong>Speaker notes:</strong> {escape(notes)}</p>')
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
    expected_sheets = {"Executive Dashboard", "Business Req", "Functional Req", "NFRs", "Data Sources", "Data Dictionary", "Epics", "Stories", "Subtasks", "Acceptance Criteria", "Dependencies", "Sprints", "Gantt", "Milestone Tracker", "RAID", "Traceability", "Test Plan", "Current State", "Stakeholder Register", "RACI Matrix", "Integrated Delivery Plan", "Resource Capacity", "Governance", "Change Control", "TPM Scenarios"}
    if set(workbook.sheetnames) != expected_sheets:
        raise RuntimeError(f"Workbook worksheet mismatch: {workbook.sheetnames}")
    if workbook["Data Dictionary"].max_row - 1 != dictionary_count:
        raise RuntimeError("Data Dictionary row count does not match DDL extraction")
    if not str(workbook["Sprints"]["G2"].value).startswith("=SUMIF"):
        raise RuntimeError("Sprint point formula missing")
    decks = {}
    for name in ["Trading_Platform_Architecture.pptx", "Trading_Platform_Delivery_Overview.pptx"]:
        deck = Presentation(OUT / name)
        decks[name] = deck
        if len(deck.slides) < 7:
            raise RuntimeError(f"Presentation appears incomplete: {name}")
    delivery_deck = decks["Trading_Platform_Delivery_Overview.pptx"]
    gantt_slide = delivery_deck.slides[-2]
    detail_slide = delivery_deck.slides[-1]
    gantt_text = "\n".join(shape.text for shape in gantt_slide.shapes if shape.has_text_frame)
    detail_text = "\n".join(shape.text for shape in detail_slide.shapes if shape.has_text_frame)
    if "Integrated Project Delivery Plan — Timeline & Dependencies" not in gantt_text:
        raise RuntimeError("Executive Gantt slide title is missing")
    if "KEY DELIVERY DEPENDENCIES — ILLUSTRATIVE BASELINE" not in gantt_text or "KEY DELIVERY DEPENDENCIES — ILLUSTRATIVE BASELINE" not in detail_text:
        raise RuntimeError("Illustrative-baseline disclaimer missing from Gantt slides")
    if "CPM status: NOT CALCULATED" not in detail_text or "Total float: NOT AVAILABLE" not in detail_text:
        raise RuntimeError("CPM limitations are not explicit on dependency slide")
    for milestone_id in ("M-01", "M-02", "M-03", "M-04"):
        if milestone_id not in gantt_text or milestone_id not in detail_text:
            raise RuntimeError(f"Gantt milestone is missing: {milestone_id}")
    diamond_count = sum(
        1 for shape in gantt_slide.shapes
        if shape.shape_type == 1 and shape.auto_shape_type == MSO_SHAPE.DIAMOND
    )
    if diamond_count < 5:
        raise RuntimeError("Gantt must contain four milestone diamonds and one legend diamond")

    # Validate the new slides' printable bounds and prevent overlapping text boxes.
    for slide in (gantt_slide, detail_slide):
        text_shapes = [shape for shape in slide.shapes if shape.has_text_frame and shape.text.strip()]
        for shape in slide.shapes:
            if shape.left < 0 or shape.top < 0 or shape.left + shape.width > delivery_deck.slide_width or shape.top + shape.height > delivery_deck.slide_height:
                raise RuntimeError("Gantt slide contains a shape outside the 16:9 canvas")
        for index, first in enumerate(text_shapes):
            for second in text_shapes[index + 1:]:
                overlap_x = min(first.left + first.width, second.left + second.width) - max(first.left, second.left)
                overlap_y = min(first.top + first.height, second.top + second.height) - max(first.top, second.top)
                if overlap_x > 0.02 * 914400 and overlap_y > 0.02 * 914400:
                    raise RuntimeError(f"Overlapping Gantt slide text: {first.text!r} / {second.text!r}")

    activities = get_gantt_activities()
    baseline_story_ids = {
        story_id for sprint in PLAN["sprints"] if sprint["baseline"] for story_id in sprint["stories"]
    }
    gantt_story_ids = [story_id for activity in activities if activity["baseline"] for story_id in activity["stories"]]
    if len(gantt_story_ids) != len(set(gantt_story_ids)) or set(gantt_story_ids) != baseline_story_ids:
        raise RuntimeError("Integrated Gantt activities must cover each baseline story exactly once")
    for activity in activities:
        if activity["finish_week"] - activity["start_week"] + 1 != activity["duration_weeks"]:
            raise RuntimeError(f"Gantt schedule is inconsistent for {activity['id']}")
    for sprint in PLAN["sprints"]:
        if sprint["baseline"]:
            points = sum(next(story["points"] for story in PLAN["stories"] if story["id"] == story_id) for story_id in sprint["stories"])
            if points > sprint["capacity"]:
                raise RuntimeError(f"Sprint capacity exceeded: {sprint['name']} ({points}/{sprint['capacity']} SP)")
    gantt_sheet = workbook["Gantt"]
    if not str(gantt_sheet["K2"].value).startswith("=--AND") or gantt_sheet.max_row < len(activities) + 3:
        raise RuntimeError("Excel Gantt is missing the shared workstream schedule")
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
        for slide in decks[deck_name].slides:
            for shape in slide.shapes:
                if shape.has_text_frame and shape.text.strip() and html_value(shape.text.strip()) not in html_document:
                    raise RuntimeError(f"Combined HTML is missing presentation text from {deck_name}")
            note_text = slide.notes_slide.notes_text_frame.text.strip()
            if note_text and html_value(note_text) not in html_document:
                raise RuntimeError(f"Combined HTML is missing speaker notes from {deck_name}")
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


def generate_reports(master_path: Path = MASTER_PATH, output_dir: Path = OUTPUT_DIR) -> Path:
    """Build reports from the master workbook, validating fully before publication."""
    global OUT, PLAN, TESTS, GANTT_ACTIVITY_DEFS

    master_path = Path(master_path).resolve()
    output_dir = Path(output_dir).resolve()
    if output_dir == master_path or master_path.is_relative_to(output_dir):
        raise ValueError("Output directory must not be the master workbook or contain it")

    model, tests, gantt_definitions = load_master_model(master_path)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage_dir = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.stage-", dir=output_dir.parent))
    backup_dir = output_dir.with_name(f".{output_dir.name}.backup-{uuid.uuid4().hex}")
    original_state = (OUT, PLAN, TESTS, GANTT_ACTIVITY_DEFS)
    try:
        OUT = stage_dir
        PLAN = model
        TESTS = tests
        GANTT_ACTIVITY_DEFS = gantt_definitions
        dictionary = model["data_dictionary"]
        build_workbook(dictionary)
        build_architecture_deck()
        build_delivery_deck()
        build_jira_csv()
        write_readme(len(dictionary), len(jira_rows()))
        write_combined_html()
        validate_outputs(len(dictionary))

        had_previous_output = output_dir.exists()
        if had_previous_output:
            os.replace(output_dir, backup_dir)
        try:
            os.replace(stage_dir, output_dir)
        except Exception:
            if had_previous_output and backup_dir.exists():
                os.replace(backup_dir, output_dir)
            raise
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        logger.info("Generated and validated reports from %s into %s", master_path, output_dir)
        return output_dir
    except Exception:
        logger.exception("Report generation failed; previously published output was preserved")
        raise
    finally:
        OUT, PLAN, TESTS, GANTT_ACTIVITY_DEFS = original_state
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
        if backup_dir.exists() and output_dir.exists():
            shutil.rmtree(backup_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate project reports from the master Excel workbook")
    parser.add_argument("--master-workbook", type=Path, default=MASTER_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    output_path = generate_reports(args.master_workbook, args.output_dir)
    print(f"Generated and validated deliverables in {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())