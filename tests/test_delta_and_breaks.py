from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from trade_pipeline.common.delta_stats import read_delete_count, read_merge_counts
from trade_pipeline.gold import break_report

ROOT = Path(__file__).parents[1]


def test_read_merge_counts_reads_databricks_result_columns() -> None:
    row = {"num_affected_rows": 4, "num_updated_rows": 1, "num_deleted_rows": 0, "num_inserted_rows": 3}
    assert read_merge_counts(row) == {"inserted_rows": 3, "updated_rows": 1, "deleted_rows": 0}


def test_read_merge_counts_fails_clearly_on_unexpected_columns() -> None:
    with pytest.raises(ValueError, match="Unexpected MERGE result columns"):
        read_merge_counts({"num_affected_rows": 4})
    with pytest.raises(ValueError, match="Unexpected MERGE result columns"):
        read_merge_counts(None)  # an empty result must not become zeros


def test_read_delete_count() -> None:
    assert read_delete_count({"num_affected_rows": 2}) == 2
    with pytest.raises(ValueError, match="Unexpected DELETE result columns"):
        read_delete_count({"rows": 2})


def test_break_writer_only_inserts_so_reruns_of_a_run_add_nothing() -> None:
    spark = MagicMock()
    fake = {name: MagicMock() for name in ("pyspark", "pyspark.sql", "pyspark.sql.types", "pyspark.sql.functions")}
    with patch.dict(sys.modules, fake):
        break_report.write_valuation_breaks(spark, "run-1", MagicMock())
    sql = spark.sql.call_args.args[0]
    assert "MERGE INTO cibc.gold.valuation_breaks" in sql
    assert "ON target.run_id = source.run_id" in sql
    for key in ("trade_id", "instrument_id", "valuation_date"):
        assert f"target.{key} <=> source.{key}" in sql  # NULL-safe, so trade-less breaks still match
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "UPDATE" not in sql and "DELETE" not in sql  # earlier runs' breaks are kept as history
    assert "(id" not in sql and " id," not in sql


@pytest.mark.parametrize("name", ["trades_constraints.sql", "market_prices_constraints.sql"])
def test_constraint_scripts_can_be_run_twice(name) -> None:
    sql = (ROOT / "database" / "tables" / "silver" / name).read_text(encoding="utf-8")
    adds = sql.count("ADD CONSTRAINT")
    assert adds > 0 and sql.count("DROP CONSTRAINT IF EXISTS") == adds
