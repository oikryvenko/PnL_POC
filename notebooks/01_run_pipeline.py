# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Run the trade pipeline
# MAGIC
# MAGIC Submits the **existing** `trade_pipeline` Databricks Job (six tasks:
# MAGIC `ingest_prices -> process_prices`, `ingest_trades -> process_trades`, `build_positions -> calculate_daily_pnl`).
# MAGIC
# MAGIC * Nothing is submitted unless **confirm_run = YES**. The confirmation is one-shot: it resets to `NO` after each submission attempt.
# MAGIC * Blank dates / tickers use the Job defaults (default tickers, last 7 days up to today).
# MAGIC * `tickers` and the dates drive market-price ingestion; trades are always reloaded from the full CSV in the Volume.
# MAGIC * Submission is not success: the status cell polls the run until it reaches `SUCCESS` or `FAILED`.

# COMMAND ----------

dbutils.widgets.text("start_date", "", "1. Start date (YYYY-MM-DD, blank = default)")
dbutils.widgets.text("end_date", "", "2. End date (YYYY-MM-DD, blank = today)")
dbutils.widgets.text("tickers", "", "3. Tickers (comma-separated, blank = default)")
dbutils.widgets.text("job_name", "trade_pipeline", "4. Job name")
dbutils.widgets.text("job_id", "", "5. Job ID override (optional)")
dbutils.widgets.dropdown("wait_for_completion", "YES", ["YES", "NO"], "6. Wait for completion")
dbutils.widgets.dropdown("confirm_run", "NO", ["NO", "YES"], "7. Confirm: submit the Job")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers (pure Python, unit-tested in `tests/test_notebooks.py`)

# COMMAND ----------

import re
from datetime import date

# Parameter names defined in jobs/resources/trade_pipeline.job.yml.
JOB_PARAMETER_NAMES = ("tickers", "start_date", "end_date")
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
SUBMITTED_STATES = {"PENDING", "QUEUED", "BLOCKED", "WAITING_FOR_RETRY", "WAITING"}
RUNNING_STATES = {"RUNNING", "TERMINATING"}


def parse_optional_date(value, field_name):
    """Return an ISO date string, or "" when blank (the Job then applies its default)."""
    value = (value or "").strip()
    if not value:
        return ""
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        raise ValueError(f"{field_name} must be YYYY-MM-DD, got {value!r}") from None


def normalize_ticker_list(value):
    """Uppercase, validate and de-duplicate a comma-separated ticker list; "" means Job default."""
    tickers = []
    for item in (value or "").split(","):
        ticker = item.strip().upper()
        if not ticker:
            continue
        if not TICKER_PATTERN.match(ticker):
            raise ValueError(f"Invalid ticker {item.strip()!r}")
        if ticker not in tickers:
            tickers.append(ticker)
    return ",".join(tickers)


def build_job_parameters(start_date, end_date, tickers, today=None):
    """Validate widget values and return the Job parameters to submit."""
    start = parse_optional_date(start_date, "start_date")
    end = parse_optional_date(end_date, "end_date")
    effective_end = end or (today or date.today()).isoformat()
    if start and start > effective_end:
        raise ValueError(f"start_date {start} must not be later than end_date {effective_end}")
    return {"tickers": normalize_ticker_list(tickers), "start_date": start, "end_date": end}


def classify_run_state(life_cycle_state, result_state):
    """Map Jobs API states to SUBMITTED / RUNNING / SUCCESS / FAILED."""
    life_cycle_state = (life_cycle_state or "").upper()
    result_state = (result_state or "").upper()
    if result_state == "SUCCESS":
        return "SUCCESS"
    if result_state:
        return "FAILED"  # FAILED, TIMEDOUT, CANCELED, SUCCESS_WITH_FAILURES, UPSTREAM_FAILED, ...
    if life_cycle_state in RUNNING_STATES:
        return "RUNNING"
    if life_cycle_state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
        return "FAILED"
    return "SUBMITTED"  # no state yet, or PENDING / QUEUED / BLOCKED


def select_job_ids(jobs, job_name):
    """Pick Job IDs by exact name, else by bundle dev-mode name '[dev <user>] <job_name>'."""
    jobs = list(jobs)
    exact = [job_id for job_id, name in jobs if name == job_name]
    if exact:
        return exact
    return [job_id for job_id, name in jobs if name and name.startswith("[") and name.endswith(f"] {job_name}")]


def enum_value(value):
    """SDK enums -> plain strings."""
    return getattr(value, "value", value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validate inputs and resolve the Job

# COMMAND ----------

import html
import time
import uuid

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

# Read every widget once; confirm_run may be removed further down.
job_parameters = build_job_parameters(
    dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date"), dbutils.widgets.get("tickers")
)
job_name = dbutils.widgets.get("job_name").strip()
job_id_override = dbutils.widgets.get("job_id").strip()
wait_for_completion = dbutils.widgets.get("wait_for_completion") == "YES"
confirmed = dbutils.widgets.get("confirm_run") == "YES"

try:
    w = WorkspaceClient()  # notebook-native authentication
    current_user = w.current_user.me().user_name
except Exception as exc:
    raise RuntimeError(f"Databricks authentication failed: {exc}") from exc


def resolve_job_id(client, name, override):
    if override:
        if not override.isdigit():
            raise ValueError(f"job_id must be numeric, got {override!r}")
        return int(override)
    candidates = select_job_ids(
        ((job.job_id, job.settings.name if job.settings else None) for job in client.jobs.list()), name
    )
    if len(candidates) != 1:
        raise RuntimeError(
            f"Expected exactly one Job named {name!r} (or '[dev <user>] {name}'), found {candidates}. "
            "Set the job_id widget explicitly."
        )
    return candidates[0]


try:
    job_id = resolve_job_id(w, job_name, job_id_override)
    job = w.jobs.get(job_id)  # also confirms the Job exists and is visible to this user
except DatabricksError as exc:
    raise RuntimeError(f"Could not resolve the Job (API error {exc.error_code}): {exc}") from exc

schedule = job.settings.schedule
print(f"User:        {current_user}")
print(f"Job:         {job.settings.name} (job_id={job_id})")
print(f"Schedule:    {schedule.quartz_cron_expression} {schedule.timezone_id} [{enum_value(schedule.pause_status)}]" if schedule else "Schedule:    none")
print(f"Parameters:  {job_parameters}  (blank = Job default)")
print(f"Confirmed:   {confirmed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Submit (only with confirm_run = YES)

# COMMAND ----------

run_id = None
try:
    # Re-read here so re-running only this cell after a submission (widget removed) cannot resubmit.
    confirmed = dbutils.widgets.get("confirm_run") == "YES"
except Exception:
    confirmed = False

if not confirmed:
    print("NOT SUBMITTED - set 'confirm_run' to YES and run this notebook again to submit the Job.")
else:
    # One-shot confirmation: reset to NO (on next widget creation) before submitting, so a re-run cannot resubmit.
    dbutils.widgets.remove("confirm_run")
    try:
        active_run = next(iter(w.jobs.list_runs(job_id=job_id, active_only=True)), None)
        if active_run is not None:
            run_id = active_run.run_id
            print(f"NOT SUBMITTED - run {run_id} is already active; monitoring that run instead.")
        else:
            # The idempotency token stops SDK/network retries of this request from creating a second run.
            submission = w.jobs.run_now(
                job_id=job_id, job_parameters=job_parameters, idempotency_token=str(uuid.uuid4())
            )
            run_id = submission.response.run_id
            print(f"SUBMITTED - run_id={run_id}. Submission does not mean success; see the status below.")
    except DatabricksError as exc:
        raise RuntimeError(f"Job submission failed (API error {exc.error_code}): {exc}") from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execution status
# MAGIC `SUBMITTED` (queued / pending) -> `RUNNING` -> `SUCCESS` or `FAILED`.

# COMMAND ----------

POLL_SECONDS = 30
MAX_WAIT_SECONDS = 60 * 60


def run_status(run):
    state = run.state
    life_cycle = enum_value(state.life_cycle_state) if state else None
    result = enum_value(state.result_state) if state else None
    return classify_run_state(life_cycle, result), life_cycle, result, (state.state_message if state else "")


if run_id is None:
    print("No run to monitor.")
else:
    try:
        run = w.jobs.get_run(run_id)
        displayHTML(f'<a href="{html.escape(run.run_page_url or "")}" target="_blank">Open Job run {run_id}</a>')
        status, life_cycle, result, message = run_status(run)
        print(f"{time.strftime('%H:%M:%S')}  {status}  (life_cycle={life_cycle}, result={result}) {message or ''}")
        waited = 0
        while wait_for_completion and status in {"SUBMITTED", "RUNNING"} and waited < MAX_WAIT_SECONDS:
            time.sleep(POLL_SECONDS)
            waited += POLL_SECONDS
            run = w.jobs.get_run(run_id)
            new_status, life_cycle, result, message = run_status(run)
            if new_status != status:
                print(f"{time.strftime('%H:%M:%S')}  {new_status}  (life_cycle={life_cycle}, result={result}) {message or ''}")
            status = new_status
    except DatabricksError as exc:
        raise RuntimeError(f"Could not read run {run_id} (API error {exc.error_code}): {exc}") from exc

    print(f"\nFINAL STATUS FOR THIS CHECK: {status}")
    if status in {"SUBMITTED", "RUNNING"}:
        print("Run is still in progress - re-run this cell (or open the link) to check again.")

    task_rows = [
        (task.task_key, *run_status(task)[:3], str(task.run_id))
        for task in (run.tasks or [])
    ]
    if task_rows:
        display(
            spark.createDataFrame(
                task_rows, "task_key string, status string, life_cycle_state string, result_state string, task_run_id string"
            ).orderBy("task_key")
        )
