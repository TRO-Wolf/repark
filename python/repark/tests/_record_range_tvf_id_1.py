"""Record the RANGE-TVF-ID-1 Spark oracle — the range() table-function cells.

Runs every ``range(...)`` SQL cell plus the three ``spark.range`` DataFrame-door
calls against live PySpark (ambient interpreter, ``local[2]``, UTC) and writes
the schema string, per-field nullability, and rows — or the exception class and
first error line — into ``range_tvf_id_1/range_tvf_id_1_spark_oracle.json``
beside the pins. Re-running it re-derives the cells from live Spark; ``--check``
compares against the committed fixture and exits non-zero on drift.

Run with a PySpark 4.1.2 interpreter::

    SPARK_LOCAL_IP=127.0.0.1 python python/repark/tests/_record_range_tvf_id_1.py

Not collected by pytest.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
_FIXTURE = _HERE / "range_tvf_id_1" / "range_tvf_id_1_spark_oracle.json"

_SQL_CELLS: dict[str, str] = {
    "range_n": "SELECT * FROM range(3)",
    "range_a_b": "SELECT * FROM range(1, 4)",
    "range_a_b_step": "SELECT * FROM range(0, 10, 3)",
    "range_a_b_step_parts": "SELECT * FROM range(0, 10, 3, 2)",
    "range_negative_step": "SELECT * FROM range(5, 0, -2)",
    "range_empty": "SELECT * FROM range(0)",
    "select_id": "SELECT id FROM range(10)",
    "select_value": "SELECT value FROM range(3)",
    "alias_col": "SELECT r.id FROM range(3) AS r",
    "alias_rename": "SELECT x FROM range(3) AS r(x)",
    "sum_id": "SELECT sum(id) FROM range(10)",
    "range_zero_step": "SELECT * FROM range(0, 10, 0)",
    "range_string_arg": "SELECT * FROM range('3')",
}

_DATAFRAME_CELLS: dict[str, tuple[int, ...]] = {
    "spark_range_n": (3,),
    "spark_range_a_b": (1, 4),
    "spark_range_a_b_step": (0, 10, 3),
}


def spark_session() -> Any:
    """A local Spark session with the UI off and the session zone on UTC."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-range-tvf-id-1-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def record_sql_cell(session: Any, cell_id: str, query: str) -> dict[str, Any]:
    """Run one SQL cell and capture its schema string, nullability, and rows."""
    try:
        frame = session.sql(query)
        return {
            "sql": query,
            "schema": frame.schema.simpleString(),
            "nullable": [field.nullable for field in frame.schema.fields],
            "rows": [list(row) for row in frame.collect()],
        }
    except Exception as error:
        return {
            "sql": query,
            "error_class": type(error).__name__,
            "error": str(error).split("\n")[0][:300],
        }


def record_dataframe_cell(session: Any, cell_id: str, args: tuple[int, ...]) -> dict[str, Any]:
    """Run one spark.range call and capture its schema string and rows."""
    frame = session.range(*args)
    return {
        "call": f"spark.range{args}",
        "schema": frame.schema.simpleString(),
        "nullable": [field.nullable for field in frame.schema.fields],
        "rows": [list(row) for row in frame.collect()],
    }


def record_all(session: Any) -> dict[str, Any]:
    """Derive every oracle cell from live Spark in fixture order."""
    from pyspark import __version__ as pyspark_version

    cells: dict[str, Any] = {}
    for cell_id, query in _SQL_CELLS.items():
        cells[cell_id] = record_sql_cell(session, cell_id, query)
    for cell_id, args in _DATAFRAME_CELLS.items():
        cells[cell_id] = record_dataframe_cell(session, cell_id, args)
    return {"spark_version": pyspark_version, "cells": cells}


def main(argv: list[str]) -> int:
    """Record the fixture, or check live Spark against it under --check."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parsed = parser.parse_args(argv)
    session = spark_session()
    try:
        derived = record_all(session)
    finally:
        session.stop()
    if not parsed.check:
        _FIXTURE.write_text(json.dumps(derived, indent=1) + "\n", encoding="utf-8")
        return 0
    committed = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    if committed != derived:
        print("RANGE-TVF-ID-1 oracle drift: live Spark differs from the fixture", file=sys.stderr)
        for cell_id in derived["cells"]:
            if committed.get("cells", {}).get(cell_id) != derived["cells"][cell_id]:
                print(f"drifted cell: {cell_id}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
