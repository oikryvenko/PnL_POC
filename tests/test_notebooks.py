"""Syntax checks and unit tests for the pure-Python helper cells of the Databricks notebooks."""

from __future__ import annotations

import ast
from datetime import date
from pathlib import Path

import pytest

NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"
RUN_NOTEBOOK = NOTEBOOKS / "01_run_pipeline.py"
REPORT_NOTEBOOK = NOTEBOOKS / "02_pipeline_report.py"


def _helper_namespace(path: Path) -> dict:
    """Execute only the cell that defines the helpers (it uses no Spark/dbutils)."""
    cells = path.read_text(encoding="utf-8").split("# COMMAND ----------")
    helper_cell = next(cell for cell in cells if "def classify_run_state" in cell)
    namespace: dict = {}
    exec(compile(helper_cell, str(path), "exec"), namespace)
    return namespace


@pytest.fixture(scope="module")
def run_nb():
    return _helper_namespace(RUN_NOTEBOOK)


@pytest.fixture(scope="module")
def report_nb():
    return _helper_namespace(REPORT_NOTEBOOK)


@pytest.mark.parametrize("path", [RUN_NOTEBOOK, REPORT_NOTEBOOK])
def test_notebook_is_valid_databricks_source(path):
    source = path.read_text(encoding="utf-8")
    assert source.startswith("# Databricks notebook source")
    ast.parse(source)


def test_job_parameter_names_match_job_yaml(run_nb):
    job_yaml = (Path(__file__).resolve().parents[1] / "jobs/resources/trade_pipeline.job.yml").read_text()
    for name in run_nb["JOB_PARAMETER_NAMES"]:
        assert f"- name: {name}" in job_yaml
    params = run_nb["build_job_parameters"]("2026-09-01", "2026-09-30", "aapl, msft,AAPL")
    assert params == {"tickers": "AAPL,MSFT", "start_date": "2026-09-01", "end_date": "2026-09-30"}
    assert set(params) == set(run_nb["JOB_PARAMETER_NAMES"])


def test_blank_inputs_use_job_defaults(run_nb):
    assert run_nb["build_job_parameters"]("", " ", "") == {"tickers": "", "start_date": "", "end_date": ""}


@pytest.mark.parametrize(
    "start, end, tickers",
    [
        ("2026-13-01", "", ""),
        ("2026-09-30", "2026-09-01", ""),
        ("", "", "AAPL;DROP TABLE"),
    ],
)
def test_invalid_inputs_are_rejected(run_nb, start, end, tickers):
    with pytest.raises(ValueError):
        run_nb["build_job_parameters"](start, end, tickers)


def test_start_after_default_end_is_rejected(run_nb):
    with pytest.raises(ValueError):
        run_nb["build_job_parameters"]("2026-10-12", "", "", today=date(2026, 10, 11))


@pytest.mark.parametrize("nb_name", ["run_nb", "report_nb"])
@pytest.mark.parametrize(
    "life_cycle, result, expected",
    [
        (None, None, "SUBMITTED"),
        ("PENDING", None, "SUBMITTED"),
        ("QUEUED", None, "SUBMITTED"),
        ("RUNNING", None, "RUNNING"),
        ("TERMINATING", None, "RUNNING"),
        ("TERMINATED", "SUCCESS", "SUCCESS"),
        ("TERMINATED", "FAILED", "FAILED"),
        ("TERMINATED", "CANCELED", "FAILED"),
        ("TERMINATED", "SUCCESS_WITH_FAILURES", "FAILED"),
        ("INTERNAL_ERROR", None, "FAILED"),
    ],
)
def test_classify_run_state(request, nb_name, life_cycle, result, expected):
    assert request.getfixturevalue(nb_name)["classify_run_state"](life_cycle, result) == expected


@pytest.mark.parametrize("nb_name", ["run_nb", "report_nb"])
def test_select_job_ids_prefers_exact_then_dev_name(request, nb_name):
    select = request.getfixturevalue(nb_name)["select_job_ids"]
    jobs = [(1, "[dev someone] trade_pipeline"), (2, "trade_pipeline_old"), (3, None)]
    assert select(jobs, "trade_pipeline") == [1]
    assert select([*jobs, (4, "trade_pipeline")], "trade_pipeline") == [4]
    assert select(jobs, "other") == []


def test_derive_audit_status(report_nb):
    derive = report_nb["derive_audit_status"]
    tasks = report_nb["PIPELINE_TASKS"]
    assert derive({task: "SUCCESS" for task in tasks}) == "SUCCESS"
    assert derive({**{task: "SUCCESS" for task in tasks}, "build_positions": "FAILED"}) == "FAILED"
    assert derive({"ingest_prices": "STARTED"}) == "RUNNING"
    assert derive({"ingest_prices": "SUCCESS"}) == "INCOMPLETE"


def test_audit_metric_never_invents_values(report_nb):
    metric = report_nb["audit_metric"]
    tasks = {
        "process_trades": {"status": "SUCCESS", "output_rows": 1150, "rejected_rows": 0},
        "process_prices": {"status": "FAILED", "output_rows": 0},
    }
    assert metric(tasks, "process_trades", "output_rows") == "1150"
    assert metric(tasks, "process_trades", "rejected_rows") == "0"
    assert metric(tasks, "process_prices", "output_rows").startswith("NOT AVAILABLE")
    assert metric(tasks, "build_positions", "output_rows").startswith("NOT AVAILABLE")


def test_classify_trigger(report_nb):
    classify = report_nb["classify_trigger"]
    assert classify("PERIODIC") == "SCHEDULED"
    assert classify("ONE_TIME") == "MANUAL"
    assert classify("RETRY") == "OTHER:RETRY"
    assert classify(None) == "NOT AVAILABLE"


def test_expected_schedule_dates_are_weekdays(report_nb):
    days = report_nb["expected_schedule_dates"](date(2026, 10, 9), date(2026, 10, 13))
    assert days == [date(2026, 10, 9), date(2026, 10, 12), date(2026, 10, 13)]
