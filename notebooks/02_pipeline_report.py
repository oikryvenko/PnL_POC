# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Pipeline report
# MAGIC
# MAGIC Read-only report over the existing Unity Catalog tables of the `trade_pipeline` Job.
# MAGIC
# MAGIC | Widget | Blank means |
# MAGIC |---|---|
# MAGIC | `run_id` | latest Job run whose six tasks all succeeded in `cibc.gold.pipeline_run_log` |
# MAGIC | `processing_date` | latest `valuation_date` in `cibc.gold.daily_pnl` |
# MAGIC | `job_id` | resolve the Job by name (`trade_pipeline` or bundle dev name `[dev <user>] trade_pipeline`) |
# MAGIC | `schedule_active_since` | missing scheduled runs are **not** evaluated (pause history is not exposed by the Jobs API) |
# MAGIC
# MAGIC **How to read the numbers**
# MAGIC * The audit log has one row per *task attempt* (`run_id` = Databricks Job run id). Runs are counted by distinct `run_id`, never by audit rows.
# MAGIC * Run metrics come from the audit log: `input_rows` = rows **processed**, `output_rows` = rows **accepted/produced**, `rejected_rows`, `duplicate_rows`.
# MAGIC   Inserted / updated / deleted counts are **not stored** and are shown as `NOT AVAILABLE`.
# MAGIC * Gold positions / P&L sections show the **current** table content for the processing date. Gold rows keep the `run_id` of the run that last *changed* them, so they cannot be filtered by run.
# MAGIC * Reject tables keep history: a reject is recorded once (first time seen) and its `run_id` is the **Bronze lineage run id**.

# COMMAND ----------

dbutils.widgets.text("run_id", "", "1. Run ID (blank = latest successful)")
dbutils.widgets.text("processing_date", "", "2. Processing date (blank = latest)")
dbutils.widgets.text("job_name", "trade_pipeline", "3. Job name")
dbutils.widgets.text("job_id", "", "4. Job ID override (optional)")
dbutils.widgets.text("monitor_days", "30", "5. Monitoring window (days)")
dbutils.widgets.text("schedule_active_since", "", "6. Schedule unpaused since (YYYY-MM-DD)")

# COMMAND ----------

# Pure-Python helpers (no Spark / dbutils; unit-tested in tests/test_notebooks.py).
from datetime import date, timedelta

PIPELINE_TASKS = (
    "ingest_prices", "process_prices", "ingest_trades", "process_trades", "build_positions", "calculate_daily_pnl",
)
NOT_AVAILABLE = "NOT AVAILABLE"


def classify_run_state(life_cycle_state, result_state):
    """Map Jobs API states to SUBMITTED / RUNNING / SUCCESS / FAILED."""
    life_cycle_state = (life_cycle_state or "").upper()
    result_state = (result_state or "").upper()
    if result_state == "SUCCESS":
        return "SUCCESS"
    if result_state:
        return "FAILED"
    if life_cycle_state in {"RUNNING", "TERMINATING"}:
        return "RUNNING"
    if life_cycle_state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
        return "FAILED"
    return "SUBMITTED"


def derive_audit_status(task_statuses):
    """Run status from the latest audit attempt of each task ({task_name: status})."""
    statuses = [task_statuses.get(task) for task in PIPELINE_TASKS]
    if "FAILED" in statuses:
        return "FAILED"
    if all(status == "SUCCESS" for status in statuses):
        return "SUCCESS"
    if "STARTED" in statuses:
        return "RUNNING"
    return "INCOMPLETE"  # e.g. tasks not started because an upstream task never logged


def audit_metric(tasks, task_name, field):
    """Value of one audit metric, or NOT AVAILABLE when the task did not finish successfully.

    Failed/started attempts store 0 for unknown metrics, so 0 there is not a real count.
    """
    row = tasks.get(task_name)
    if row is None:
        return f"{NOT_AVAILABLE} (task not logged)"
    if row.get("status") != "SUCCESS":
        return f"{NOT_AVAILABLE} (task {row.get('status')})"
    value = row.get(field)
    return NOT_AVAILABLE if value is None else str(value)


def classify_trigger(trigger):
    """Jobs API trigger type -> SCHEDULED / MANUAL / OTHER:<type>."""
    trigger = (trigger or "").upper()
    if trigger == "PERIODIC":
        return "SCHEDULED"
    if trigger == "ONE_TIME":
        return "MANUAL"  # 'Run now' in the UI or the run-now API (e.g. notebook 01)
    return f"OTHER:{trigger}" if trigger else NOT_AVAILABLE


def expected_schedule_dates(first_day, last_day, weekdays=(0, 1, 2, 3, 4)):
    """Dates on which the cron '0 0 18 ? * MON-FRI' fires (NYSE holidays are NOT skipped by cron)."""
    days = []
    current = first_day
    while current <= last_day:
        if current.weekday() in weekdays:
            days.append(current)
        current += timedelta(days=1)
    return days


def select_job_ids(jobs, job_name):
    """Pick Job IDs by exact name, else by bundle dev-mode name '[dev <user>] <job_name>'."""
    jobs = list(jobs)
    exact = [job_id for job_id, name in jobs if name == job_name]
    if exact:
        return exact
    return [job_id for job_id, name in jobs if name and name.startswith("[") and name.endswith(f"] {job_name}")]


def enum_value(value):
    return getattr(value, "value", value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: resolve run, processing date and Job metadata

# COMMAND ----------

import html
from datetime import datetime, timezone

from pyspark.sql import functions as F

# Business dates and displayed timestamps use the Job's schedule timezone.
spark.conf.set("spark.sql.session.timeZone", "America/New_York")


def show(df, empty_message):
    """display() a DataFrame, or print a clear message when it is empty."""
    if df.isEmpty():
        print(empty_message)
    else:
        display(df)


# Latest attempt per (run, task): repaired tasks have several attempts under the same run_id.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW latest_task_attempts AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY run_id, task_name ORDER BY started_at DESC) AS attempt_rank,
               MIN(started_at) OVER (PARTITION BY run_id) AS run_started_at
        FROM cibc.gold.pipeline_run_log
    ) WHERE attempt_rank = 1
""")

# One row per Job run (not per task) with an audit-derived status.
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW audit_runs AS
    SELECT run_id,
           MIN(run_started_at) AS started_at,
           MAX(finished_at) AS finished_at,
           COUNT(*) AS tasks_logged,
           COUNT_IF(status = 'SUCCESS') AS tasks_succeeded,
           COUNT_IF(status = 'FAILED') AS tasks_failed,
           COUNT_IF(status = 'STARTED') AS tasks_started,
           CASE WHEN COUNT_IF(status = 'FAILED') > 0 THEN 'FAILED'
                WHEN COUNT_IF(status = 'SUCCESS' AND task_name IN {PIPELINE_TASKS}) = {len(PIPELINE_TASKS)} THEN 'SUCCESS'
                WHEN COUNT_IF(status = 'STARTED') > 0 THEN 'RUNNING'
                ELSE 'INCOMPLETE' END AS audit_status
    FROM latest_task_attempts
    GROUP BY run_id
""")

run_id = dbutils.widgets.get("run_id").strip()
if not run_id:
    latest = spark.sql("""
        SELECT run_id FROM audit_runs
        ORDER BY CASE WHEN audit_status = 'SUCCESS' THEN 0 ELSE 1 END, started_at DESC LIMIT 1
    """).first()
    run_id = latest["run_id"] if latest else None

processing_date_text = dbutils.widgets.get("processing_date").strip()
if processing_date_text:
    processing_date = date.fromisoformat(processing_date_text)
else:
    processing_date = spark.sql("SELECT MAX(valuation_date) AS d FROM cibc.gold.daily_pnl").first()["d"]

monitor_days = int(dbutils.widgets.get("monitor_days") or 30)
active_since_text = dbutils.widgets.get("schedule_active_since").strip()
schedule_active_since = date.fromisoformat(active_since_text) if active_since_text else None

# Jobs API metadata (trigger types, schedule, live status). The report still works without it.
w = job = job_id = None
api_error = None
try:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    override = dbutils.widgets.get("job_id").strip()
    if override:
        job_id = int(override)
    else:
        candidates = select_job_ids(
            ((j.job_id, j.settings.name if j.settings else None) for j in w.jobs.list()),
            dbutils.widgets.get("job_name").strip(),
        )
        if len(candidates) != 1:
            raise RuntimeError(f"Expected one matching Job, found {candidates}; set the job_id widget")
        job_id = candidates[0]
    job = w.jobs.get(job_id)
except Exception as exc:  # auth, permission or API errors: degrade to audit-only reporting
    api_error = f"{type(exc).__name__}: {exc}"

schedule = job.settings.schedule if job else None
print(f"Run ID:          {run_id or NOT_AVAILABLE + ' (no audit rows)'}")
print(f"Processing date: {processing_date or NOT_AVAILABLE + ' (daily_pnl is empty)'}")
print(f"Job:             {f'{job.settings.name} (job_id={job_id})' if job else NOT_AVAILABLE + f' - {api_error}'}")
if schedule:
    print(f"Schedule:        {schedule.quartz_cron_expression} {schedule.timezone_id} [{enum_value(schedule.pause_status)}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Run summary

# COMMAND ----------

task_rows = (
    spark.sql("SELECT * FROM latest_task_attempts WHERE run_id = :run_id", args={"run_id": run_id})
    .drop("issue_counts")
    .collect()  # at most six rows
    if run_id else []
)
tasks = {row["task_name"]: row.asDict() for row in task_rows}

job_status, trigger_type, run_page_url = NOT_AVAILABLE, NOT_AVAILABLE, None
if w and run_id and run_id.isdigit():
    try:
        run = w.jobs.get_run(int(run_id))
        job_status = classify_run_state(enum_value(run.state.life_cycle_state), enum_value(run.state.result_state))
        trigger_type = classify_trigger(enum_value(run.trigger))
        run_page_url = run.run_page_url
    except Exception as exc:
        job_status = f"{NOT_AVAILABLE} ({type(exc).__name__})"


def count_where(table, column):
    """Rows of `table` currently stamped with the selected run id in `column`."""
    if not run_id:
        return NOT_AVAILABLE
    return str(spark.sql(f"SELECT COUNT(*) AS n FROM {table} WHERE {column} = :run_id", args={"run_id": run_id}).first()["n"])


started = min((r["started_at"] for r in task_rows), default=None)
finished = max((r["finished_at"] for r in task_rows if r["finished_at"]), default=None)
audit_status = derive_audit_status({name: row["status"] for name, row in tasks.items()}) if tasks else NOT_AVAILABLE

# (metric, measure, value, source)
summary = [
    ("Job execution status", "STATUS", job_status, "Jobs API"),
    ("Pipeline status (from task audit rows)", "STATUS", audit_status, "pipeline_run_log"),
    ("Trigger type", "STATUS", trigger_type, "Jobs API"),
    ("Tasks logged / succeeded", "STATUS", f"{len(tasks)} / {sum(r['status'] == 'SUCCESS' for r in tasks.values())} of 6", "pipeline_run_log"),
    ("Started at (America/New_York)", "TIMESTAMP", str(started) if started else NOT_AVAILABLE, "pipeline_run_log"),
    ("Finished at (America/New_York)", "TIMESTAMP", str(finished) if finished and audit_status in ("SUCCESS", "FAILED") else NOT_AVAILABLE, "pipeline_run_log"),
    ("Bronze trades read from CSV", "PROCESSED", audit_metric(tasks, "ingest_trades", "input_rows"), "ingest_trades.input_rows"),
    ("Bronze trades newly inserted", "INSERTED", count_where("cibc.bronze.trades", "run_id"), "bronze.trades.run_id (insert-only)"),
    ("Bronze trade duplicates (in CSV or already ingested)", "DUPLICATE", audit_metric(tasks, "ingest_trades", "duplicate_rows"), "ingest_trades.duplicate_rows"),
    ("Bronze prices downloaded and merged", "PROCESSED", audit_metric(tasks, "ingest_prices", "output_rows"), "ingest_prices.output_rows"),
    ("Bronze prices inserted or updated", "INSERTED+UPDATED (still stamped)", count_where("cibc.bronze.market_prices", "run_id"), "bronze.market_prices.run_id"),
    ("Bronze prices inserted / updated separately", "INSERTED / UPDATED", NOT_AVAILABLE, "not stored in audit"),
    ("Bronze trades examined by Silver", "PROCESSED", audit_metric(tasks, "process_trades", "input_rows"), "process_trades.input_rows"),
    ("Silver trades accepted", "ACCEPTED", audit_metric(tasks, "process_trades", "output_rows"), "process_trades.output_rows"),
    ("Silver trades inserted or updated", "INSERTED+UPDATED (still stamped)", count_where("cibc.silver.trades", "bronze_run_id"), "silver.trades.bronze_run_id"),
    ("Silver trades inserted / updated / deleted", "INSERTED / UPDATED / DELETED", NOT_AVAILABLE, "not stored in audit"),
    ("Rejected trades", "REJECTED", audit_metric(tasks, "process_trades", "rejected_rows"), "process_trades.rejected_rows"),
    ("Duplicate trades", "DUPLICATE (subset of rejected)", audit_metric(tasks, "process_trades", "duplicate_rows"), "process_trades.duplicate_rows"),
    ("New trade reject-history rows", "INSERTED", count_where("cibc.silver.trades_rejects", "run_id"), "silver.trades_rejects.run_id"),
    ("Bronze prices in Silver scope", "PROCESSED", audit_metric(tasks, "process_prices", "input_rows"), "process_prices.input_rows"),
    ("Silver prices accepted", "ACCEPTED", audit_metric(tasks, "process_prices", "output_rows"), "process_prices.output_rows"),
    ("Silver prices inserted / updated", "INSERTED / UPDATED", NOT_AVAILABLE, "not stored in audit"),
    ("Rejected prices", "REJECTED", audit_metric(tasks, "process_prices", "rejected_rows"), "process_prices.rejected_rows"),
    ("Duplicate prices", "DUPLICATE (subset of rejected)", audit_metric(tasks, "process_prices", "duplicate_rows"), "process_prices.duplicate_rows"),
    ("New price reject-history rows", "INSERTED", NOT_AVAILABLE, "rejects_written not stored; reject run_id is Bronze lineage"),
    ("Gold positions calculated (full history)", "PROCESSED", audit_metric(tasks, "build_positions", "output_rows"), "build_positions.output_rows"),
    ("Gold positions inserted / updated / deleted", "INSERTED / UPDATED / DELETED", NOT_AVAILABLE, "not stored in audit"),
    ("Gold P&L rows calculated (full history)", "PROCESSED", audit_metric(tasks, "calculate_daily_pnl", "output_rows"), "calculate_daily_pnl.output_rows"),
    ("Gold P&L inserted / updated / deleted", "INSERTED / UPDATED / DELETED", NOT_AVAILABLE, "not stored in audit"),
    ("Valuation breaks recorded for this run", "INSERTED (run history)", count_where("cibc.gold.valuation_breaks", "run_id"), "valuation_breaks.run_id"),
]
if run_page_url:
    displayHTML(f'<a href="{html.escape(run_page_url)}" target="_blank">Open Job run {html.escape(run_id)}</a>')
display(spark.createDataFrame(summary, "metric string, measure string, value string, source string"))

if task_rows:
    display(
        spark.sql(
            """SELECT task_name, status, started_at, finished_at, input_rows, output_rows, rejected_rows,
                      duplicate_rows, error_type, error_message
               FROM latest_task_attempts WHERE run_id = :run_id ORDER BY started_at""",
            args={"run_id": run_id},
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current table totals (all runs, not just the selected run)

# COMMAND ----------

display(spark.sql("""
    SELECT 'bronze.trades' AS table_name, COUNT(*) AS current_rows FROM cibc.bronze.trades
    UNION ALL SELECT 'bronze.market_prices', COUNT(*) FROM cibc.bronze.market_prices
    UNION ALL SELECT 'silver.trades', COUNT(*) FROM cibc.silver.trades
    UNION ALL SELECT 'silver.trades_rejects (history)', COUNT(*) FROM cibc.silver.trades_rejects
    UNION ALL SELECT 'silver.market_prices', COUNT(*) FROM cibc.silver.market_prices
    UNION ALL SELECT 'silver.market_prices_rejects (history)', COUNT(*) FROM cibc.silver.market_prices_rejects
    UNION ALL SELECT 'gold.positions', COUNT(*) FROM cibc.gold.positions
    UNION ALL SELECT 'gold.daily_pnl', COUNT(*) FROM cibc.gold.daily_pnl
    UNION ALL SELECT 'gold.valuation_breaks (run history)', COUNT(*) FROM cibc.gold.valuation_breaks
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Rejections
# MAGIC Per-run counts come from the audit `issue_counts`. A row failing several rules is counted under each reason,
# MAGIC so reason counts can exceed `rejected_rows`.

# COMMAND ----------

show(
    spark.sql(
        """SELECT task_name, reason, reason_count
           FROM latest_task_attempts LATERAL VIEW explode(issue_counts) r AS reason, reason_count
           WHERE run_id = :run_id AND task_name IN ('process_trades', 'process_prices')
           ORDER BY task_name, reason_count DESC""",
        args={"run_id": run_id},
    ) if run_id else spark.range(0),
    "No per-run rejection reasons recorded for the selected run.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rejected trades (reject history; `first_recorded_in_selected_run` uses the Bronze lineage run id)

# COMMAND ----------

show(
    spark.sql(
        """SELECT reason, COUNT(*) AS reject_history_rows, COUNT_IF(run_id = :run_id) AS first_recorded_in_selected_run
           FROM cibc.silver.trades_rejects LATERAL VIEW explode(split(rejection_reason, '[|]')) r AS reason
           GROUP BY reason ORDER BY reject_history_rows DESC""",
        args={"run_id": run_id or ""},
    ),
    "No rejected trades recorded.",
)
show(
    spark.sql(
        """SELECT trade_id, symbol, instrument_id, version, side, quantity, price, trade_date, rejection_reason,
                  source_file, run_id AS bronze_run_id, ingestion_timestamp AS bronze_ingestion_timestamp,
                  rejected_at, run_id = :run_id AS first_recorded_in_selected_run
           FROM cibc.silver.trades_rejects
           ORDER BY rejected_at DESC, trade_id, version""",
        args={"run_id": run_id or ""},
    ),
    "No rejected trades recorded.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rejected market prices (reject history; `run_id` is the Bronze row's run, not necessarily the run that rejected it)

# COMMAND ----------

show(
    spark.sql(
        """SELECT reason, COUNT(*) AS reject_history_rows
           FROM cibc.silver.market_prices_rejects LATERAL VIEW explode(split(rejection_reason, '[|]')) r AS reason
           GROUP BY reason ORDER BY reject_history_rows DESC"""
    ),
    "No rejected market prices recorded.",
)
show(
    spark.sql(
        """SELECT symbol, price_date, close, currency, rejection_reason, source_api,
                  run_id AS bronze_run_id, ingestion_timestamp AS bronze_ingestion_timestamp, rejected_at
           FROM cibc.silver.market_prices_rejects
           ORDER BY rejected_at DESC, symbol, price_date"""
    ),
    "No rejected market prices recorded.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Positions on the processing date
# MAGIC `gold.positions` is per counterparty and has no symbol: counterparty rows are summed per book/instrument,
# MAGIC and the symbol comes from a one-row-per-instrument lookup so the join cannot multiply rows.
# MAGIC Market value stays NULL if any contributing row has no price.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMP VIEW book_positions AS
    WITH instrument_symbols AS (
        SELECT instrument_id, MIN(symbol) AS symbol FROM cibc.silver.trades GROUP BY instrument_id
    )
    SELECT p.valuation_date, p.book_id, p.instrument_id, s.symbol, p.currency,
           SUM(p.closing_position_quantity) AS closing_quantity,
           MAX(p.closing_price) AS closing_price,
           CASE WHEN COUNT(p.market_value) = COUNT(*) THEN SUM(p.market_value) END AS market_value,
           COUNT(*) AS counterparty_rows
    FROM cibc.gold.positions p
    LEFT JOIN instrument_symbols s ON p.instrument_id = s.instrument_id
    WHERE p.valuation_date = :processing_date
    GROUP BY p.valuation_date, p.book_id, p.instrument_id, s.symbol, p.currency
""", args={"processing_date": processing_date})

show(spark.sql("SELECT * FROM book_positions ORDER BY book_id, symbol"), f"No positions for {processing_date}.")
show(
    spark.sql("""
        SELECT valuation_date, book_id, currency,
               SUM(closing_quantity) AS closing_quantity_all_instruments,
               CASE WHEN COUNT(market_value) = COUNT(*) THEN SUM(market_value) END AS market_value,
               COUNT(*) - COUNT(market_value) AS instruments_missing_price
        FROM book_positions GROUP BY valuation_date, book_id, currency ORDER BY book_id
    """),
    f"No positions for {processing_date}.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Daily P&L on the processing date
# MAGIC `total_daily_pnl` is NULL when any row is missing valuation data; `daily_pnl_complete_rows_only` sums the rest.

# COMMAND ----------

pnl = spark.table("cibc.gold.daily_pnl").filter(F.col("valuation_date") == F.lit(processing_date))


def pnl_totals(*group_columns):
    return (
        pnl.groupBy(*group_columns)
        .agg(
            F.when(F.count("daily_pnl") == F.count(F.lit(1)), F.sum("daily_pnl")).alias("total_daily_pnl"),
            F.sum("daily_pnl").alias("daily_pnl_complete_rows_only"),
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("calculation_status") != "COMPLETE", 1).otherwise(0)).alias("incomplete_rows"),
        )
        .orderBy(*group_columns)
    )


empty_pnl = f"No daily P&L rows for {processing_date}."
show(pnl_totals("valuation_date"), empty_pnl)
show(pnl_totals("valuation_date", "book_id"), empty_pnl)
show(pnl_totals("valuation_date", "symbol"), empty_pnl)
show(
    pnl.select(
        "valuation_date", "book_id", "symbol", "currency", "opening_quantity", "buy_quantity", "sell_quantity",
        "closing_quantity", "previous_close", "current_close", "market_value", "daily_pnl", "calculation_status",
    ).orderBy("book_id", "symbol"),
    empty_pnl,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Valuation breaks (selected run)
# MAGIC Breaks are run-level history: each run keeps its own rows.

# COMMAND ----------

breaks = spark.table("cibc.gold.valuation_breaks").filter(F.col("run_id") == F.lit(run_id or ""))
show(
    breaks.groupBy("break_type", "status").agg(F.count(F.lit(1)).alias("breaks")).orderBy("break_type"),
    "No valuation breaks for the selected run.",
)
show(
    breaks.filter(F.col("valuation_date") == F.lit(processing_date))
    .select(
        "valuation_date",
        F.coalesce("instrument_id", F.lit("-")).alias("instrument_id"),
        F.coalesce("trade_id", F.lit("-")).alias("trade_id"),
        "break_type",
        "break_description",
        "status",
        "detected_at",
    )
    .orderBy("instrument_id", "trade_id", "break_type"),
    f"No valuation breaks on {processing_date} for the selected run.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Daily execution monitoring
# MAGIC One row per **Job run** (Jobs API, plus audit-only runs). Trigger type comes from the Jobs API:
# MAGIC `PERIODIC` = scheduled, `ONE_TIME` = manual. A manual run never counts as a scheduled execution.
# MAGIC Missing scheduled runs are evaluated only when the schedule is UNPAUSED and `schedule_active_since` is set.

# COMMAND ----------

window_start = datetime.now(timezone.utc) - timedelta(days=monitor_days)

api_runs = []
if w and job_id:
    try:
        for r in w.jobs.list_runs(job_id=job_id, start_time_from=int(window_start.timestamp() * 1000)):
            api_runs.append((
                str(r.run_id),
                classify_trigger(enum_value(r.trigger)),
                classify_run_state(enum_value(r.state.life_cycle_state), enum_value(r.state.result_state)) if r.state else NOT_AVAILABLE,
                datetime.fromtimestamp(r.start_time / 1000, tz=timezone.utc) if r.start_time else None,
            ))
    except Exception as exc:
        api_error = f"{type(exc).__name__}: {exc}"
        api_runs = []
if api_error:
    print(f"Jobs API unavailable - trigger types and API-only runs are NOT AVAILABLE: {api_error}")

spark.createDataFrame(
    api_runs, "run_id string, trigger_type string, job_status string, start_time timestamp"
).createOrReplaceTempView("api_runs")

executions = spark.sql("""
    SELECT COALESCE(a.run_id, l.run_id) AS run_id,
           TO_DATE(COALESCE(a.start_time, l.started_at)) AS business_date,
           COALESCE(a.job_status, l.audit_status) AS status,
           COALESCE(a.trigger_type, 'NOT AVAILABLE') AS trigger_type,
           l.audit_status, l.tasks_logged
    FROM api_runs a FULL OUTER JOIN audit_runs l ON a.run_id = l.run_id
    WHERE COALESCE(a.start_time, l.started_at) >= :window_start
""", args={"window_start": window_start})
executions.createOrReplaceTempView("executions")

show(spark.sql("SELECT * FROM executions ORDER BY business_date DESC, run_id DESC"), "No executions in the window.")

# COMMAND ----------

# Expected scheduled dates: only when the schedule is live and its start date is known.
schedule_live = bool(schedule) and enum_value(schedule.pause_status) == "UNPAUSED"
now_ny = spark.sql("SELECT current_timestamp() AS t").first()["t"]  # session timezone = America/New_York
last_due = now_ny.date() if now_ny.hour >= 19 else now_ny.date() - timedelta(days=1)
evaluate_missing = schedule_live and schedule_active_since is not None and bool(api_runs)
expected = (
    expected_schedule_dates(max(schedule_active_since, window_start.date()), last_due) if evaluate_missing else []
)
if not evaluate_missing:
    reason = (
        "schedule is PAUSED or not configured" if not schedule_live
        else "schedule_active_since widget is blank" if schedule_active_since is None
        else "no Jobs API run metadata"
    )
    print(f"Missing scheduled executions: {NOT_AVAILABLE} ({reason}).")

spark.createDataFrame([(d,) for d in expected], "business_date date").createOrReplaceTempView("expected_schedule")

daily = spark.sql(f"""
    WITH per_day AS (
        SELECT business_date,
               COUNT(DISTINCT run_id) AS executions,
               COUNT(DISTINCT CASE WHEN status = 'SUCCESS' THEN run_id END) AS successful,
               COUNT(DISTINCT CASE WHEN status = 'FAILED' THEN run_id END) AS failed,
               COUNT(DISTINCT CASE WHEN status NOT IN ('SUCCESS', 'FAILED') THEN run_id END) AS in_progress_or_incomplete,
               COUNT(DISTINCT CASE WHEN trigger_type = 'SCHEDULED' THEN run_id END) AS scheduled_runs,
               COUNT(DISTINCT CASE WHEN trigger_type = 'SCHEDULED' AND status = 'SUCCESS' THEN run_id END) AS scheduled_successful,
               COUNT(DISTINCT CASE WHEN trigger_type = 'MANUAL' THEN run_id END) AS manual_runs,
               COUNT(DISTINCT CASE WHEN trigger_type NOT IN ('SCHEDULED', 'MANUAL') THEN run_id END) AS other_or_unknown_trigger
        FROM executions GROUP BY business_date
    )
    SELECT COALESCE(d.business_date, e.business_date) AS business_date,
           COALESCE(d.executions, 0) AS executions,
           COALESCE(d.successful, 0) AS successful,
           COALESCE(d.failed, 0) AS failed,
           COALESCE(d.in_progress_or_incomplete, 0) AS in_progress_or_incomplete,
           COALESCE(d.executions, 0) > 1 AS multiple_executions,
           COALESCE(d.scheduled_runs, 0) AS scheduled_runs,
           COALESCE(d.scheduled_successful, 0) AS scheduled_successful,
           COALESCE(d.manual_runs, 0) AS manual_runs,
           COALESCE(d.other_or_unknown_trigger, 0) AS other_or_unknown_trigger,
           CASE WHEN NOT {str(evaluate_missing).upper()} THEN 'NOT AVAILABLE'
                WHEN e.business_date IS NULL THEN 'NOT SCHEDULED'
                WHEN COALESCE(d.scheduled_runs, 0) = 0 THEN 'MISSING'
                WHEN COALESCE(d.scheduled_successful, 0) = 0 THEN 'SCHEDULED RUN FAILED'
                ELSE 'OK' END AS scheduled_execution_check
    FROM per_day d FULL OUTER JOIN expected_schedule e ON d.business_date = e.business_date
    ORDER BY business_date DESC
""")
show(daily, "No executions or expected scheduled dates in the window.")
