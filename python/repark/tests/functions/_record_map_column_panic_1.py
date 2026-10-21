"""Re-derive the MAP-COLUMN-PANIC-1 Spark oracle from live PySpark 4.1.2.

Each cell is one ``SELECT`` over the three-row temp view ``map_column_panic_1_src``
(``FROM range(3)``, so the planner cannot fold a column into a literal) in a ``local[1]``
session at zone ``UTC`` with ANSI on, Spark's default. A cell records the collected rows
(maps as key-sorted pairs, structs as ordered field pairs, timestamps as ISO text),
the result schema (``simpleString`` and the top-level nullable flags), or the error
condition, SQLSTATE and message.

Run with a PySpark 4.1.2 interpreter::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 /tmp/sparkenv/bin/python \\
        python/repark/tests/functions/_record_map_column_panic_1.py

``--check`` compares a fresh recording against the committed fixture and exits non-zero on
drift. Not collected by pytest.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
FIXTURE = _HERE / "map_column_panic_1_spark_oracle.json"
VIEW_NAME = "map_column_panic_1_src"
VIEW_SQL = (
    f"CREATE OR REPLACE TEMP VIEW {VIEW_NAME} AS SELECT CAST(id AS INT) AS id, "
    "CAST(id AS BIGINT) AS big, concat('k', id) AS k, "
    "timestamp_micros(1767323045123456 + id * 86400000000) AS ts, "
    "CAST(NULL AS STRING) AS nk, CAST(NULL AS INT) AS nv, "
    "CASE WHEN id = 1 THEN CAST(NULL AS INT) ELSE CAST(id AS INT) END AS mid FROM range(3)"
)
ORDER = f"FROM {VIEW_NAME} ORDER BY id"
CELLS: tuple[tuple[str, str], ...] = (
    ("column_value", f"SELECT id, map('a', id) AS m {ORDER}"),
    (
        "column_value_one_row",
        f"SELECT id, map('a', id) AS m FROM {VIEW_NAME} WHERE id = 1",
    ),
    ("function_value", f"SELECT id, map('a', year(ts)) AS m {ORDER}"),
    ("cast_value", f"SELECT id, map('a', CAST(id AS BIGINT)) AS m {ORDER}"),
    ("ntz_cast_value", f"SELECT id, map('a', CAST(ts AS TIMESTAMP_NTZ)) AS m {ORDER}"),
    ("timestamp_value", f"SELECT id, map('a', ts) AS m {ORDER}"),
    ("struct_value", f"SELECT id, map('a', named_struct('x', id, 'y', k)) AS m {ORDER}"),
    ("array_value", f"SELECT id, map('a', array(id, id + 1)) AS m {ORDER}"),
    ("map_value", f"SELECT id, map('a', map('b', id)) AS m {ORDER}"),
    ("two_pairs", f"SELECT id, map('a', id, 'b', id + 1) AS m {ORDER}"),
    ("column_key", f"SELECT id, map(k, id) AS m {ORDER}"),
    ("int_key", f"SELECT id, map(id, k) AS m {ORDER}"),
    ("widen_bigint", f"SELECT id, map('a', id, 'b', big) AS m {ORDER}"),
    ("widen_decimal", f"SELECT id, map('a', id, 'b', 2.5) AS m {ORDER}"),
    ("null_value", f"SELECT id, map('a', nv) AS m {ORDER}"),
    ("partial_null_value", f"SELECT id, map('a', mid, 'b', id) AS m {ORDER}"),
    ("lookup", f"SELECT id, map('a', id)['a'] AS v {ORDER}"),
    ("map_keys", f"SELECT id, map_keys(map(k, id, 'b', id + 1)) AS v {ORDER}"),
    ("map_values", f"SELECT id, map_values(map('a', id, 'b', id + 1)) AS v {ORDER}"),
    ("size", f"SELECT id, size(map('a', id, 'b', id + 1)) AS v {ORDER}"),
    (
        "predicate",
        f"SELECT id FROM {VIEW_NAME} WHERE map('a', id)['a'] = 1",
    ),
    ("literal_control", f"SELECT id, map('a', 1, 'b', 2) AS m {ORDER}"),
    ("empty_control", f"SELECT id, map() AS m {ORDER}"),
    (
        "ordering_refusal",
        f"SELECT id FROM {VIEW_NAME} WHERE map('a', id) = map('b', id)",
    ),
    ("create_map_unresolved", f"SELECT id, create_map('a', id) AS m {ORDER}"),
    ("duplicate_key", f"SELECT id, map('a', id, 'a', id + 1) AS m {ORDER}"),
    ("duplicate_literal", "SELECT map('a', 1, 'a', 2) AS m"),
    ("null_key_column", f"SELECT id, map(nk, id) AS m {ORDER}"),
    ("null_key_cast", f"SELECT id, map(CAST(NULL AS STRING), id) AS m {ORDER}"),
    ("null_key_literal", "SELECT map(NULL, 1) AS m"),
    ("odd_arity_three", f"SELECT id, map('a', id, 'b') AS m {ORDER}"),
    ("odd_arity_one", f"SELECT id, map('a') AS m {ORDER}"),
    ("odd_arity_literal", "SELECT map('a', 1, 'b') AS m"),
)


def plain(value: Any) -> Any:
    """Turn a collected value into JSON-safe data.

    Args:
        value: A value from a collected row.

    Returns:
        ``{"map": [[key, value], ...]}`` for a map, its pairs sorted by key because PySpark
        builds a Python dict from a Java hash map and so loses Spark's entry order;
        ``{"struct": [[name, value], ...]}`` for a row; a list for an array; ISO text for a
        timestamp or date; text for a decimal; the value itself otherwise.
    """
    if isinstance(value, dict):
        pairs = [[plain(key), plain(item)] for key, item in value.items()]
        return {"map": sorted(pairs, key=lambda pair: repr(pair[0]))}
    if hasattr(value, "asDict") and hasattr(value, "__fields__"):
        return {"struct": [[name, plain(value[name])] for name in value.__fields__]}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def error_cell(error: Exception) -> dict[str, Any]:
    """Capture a Spark failure as its condition, SQLSTATE and message.

    Args:
        error: The raised PySpark exception.

    Returns:
        The error cell.
    """
    condition = getattr(error, "getCondition", lambda: None)()
    sqlstate = getattr(error, "getSqlState", lambda: None)()
    text = str(error).split("\nJVM stacktrace")[0].strip()
    return {
        "error": condition,
        "sqlstate": sqlstate,
        "class": type(error).__name__,
        "message": text,
    }


def run_cell(session: Any, statement: str) -> dict[str, Any]:
    """Run one statement and capture its rows and schema, or its error.

    Args:
        session: A live PySpark session.
        statement: The ``SELECT`` to run.

    Returns:
        The cell body.
    """
    try:
        frame = session.sql(statement)
        schema = frame.schema
        rows = frame.collect()
    except Exception as error:
        return error_cell(error)
    return {
        "type": schema.simpleString(),
        "nullable": [field.nullable for field in schema.fields],
        "rows": [[plain(item) for item in row] for row in rows],
    }


def record() -> dict[str, Any]:
    """Run every cell on live Spark and return the fixture body.

    Returns:
        The fixture: provenance fields and the ordered cells.
    """
    import pyspark
    from pyspark.sql import SparkSession

    os.environ["TZ"] = "UTC"
    time.tzset()
    session = (
        SparkSession.builder.master("local[1]")
        .appName("map-column-panic-1-record")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.sql(VIEW_SQL)
    cells = [
        {"id": name, "sql": statement, **run_cell(session, statement)} for name, statement in CELLS
    ]
    session.stop()
    return {
        "spark": pyspark.__version__,
        "zone": "UTC",
        "ansi": "true",
        "view": VIEW_SQL,
        "cells": cells,
    }


def main(argv: list[str] | None = None) -> int:
    """Write the fixture, or compare a fresh recording with it under ``--check``.

    Args:
        argv: The command-line arguments; ``None`` reads ``sys.argv``.

    Returns:
        The process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args(argv)
    fresh = record()
    text = json.dumps(fresh, indent=1, sort_keys=True, ensure_ascii=False) + "\n"
    if arguments.check:
        return 0 if FIXTURE.read_text(encoding="utf-8") == text else 1
    FIXTURE.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
