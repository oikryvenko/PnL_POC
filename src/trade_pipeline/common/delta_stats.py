"""Row counts reported by Delta MERGE and DELETE statements."""

from __future__ import annotations

MERGE_FIELDS = ("num_inserted_rows", "num_updated_rows", "num_deleted_rows")


def _fields(result_row, expected: tuple[str, ...], statement: str) -> dict:
    """Return the result row as a dict, failing clearly if the runtime returns other columns."""
    fields = result_row.asDict() if hasattr(result_row, "asDict") else dict(result_row or {})
    missing = [name for name in expected if name not in fields]
    if missing:
        raise ValueError(f"Unexpected {statement} result columns {sorted(fields)}; missing {missing}")
    return fields


def read_merge_counts(result_row) -> dict[str, int]:
    """Turn the one-row result of spark.sql("MERGE ...") into counts (unchanged rows are not touched)."""
    fields = _fields(result_row, MERGE_FIELDS, "MERGE")
    return {
        "inserted_rows": int(fields["num_inserted_rows"]),
        "updated_rows": int(fields["num_updated_rows"]),
        "deleted_rows": int(fields["num_deleted_rows"]),
    }


def read_delete_count(result_row) -> int:
    """Turn the one-row result of spark.sql("DELETE ...") into the number of deleted rows."""
    return int(_fields(result_row, ("num_affected_rows",), "DELETE")["num_affected_rows"])