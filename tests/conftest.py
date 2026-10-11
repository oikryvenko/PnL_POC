from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def stats_spark():
    """Mock SparkSession whose MERGE/DELETE statements return Delta-style result rows."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = {
        "num_inserted_rows": 2,
        "num_updated_rows": 1,
        "num_deleted_rows": 0,
        "num_affected_rows": 3,
        "first_date": date(2026, 9, 1),
        "instruments": ["INS001", "INS002"],
        "symbols": ["AAPL", "TSLA"],
    }
    return spark


def statements(spark) -> list[str]:
    return [call.args[0] for call in spark.sql.call_args_list]