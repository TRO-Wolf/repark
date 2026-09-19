"""Re-derive the CAST-MAP-SPELL-1 Spark oracle from live PySpark 4.1.2.

Runs every ``CAST(… AS MAP<…>)`` SQL cell plus the three DataFrame-door ``.cast``
calls against live PySpark (ambient interpreter, ``local[2]``, UTC) and writes the
schema string, per-field nullability, and rows - or the exception class and first
error line - into ``cast_map_spell_1/cast_map_spell_1_spark_oracle.json`` beside the
pins. Re-running it re-derives the cell set from live Spark; ``--check`` compares
against the committed fixture and exits non-zero on drift.

Run with a PySpark 4.1.2 interpreter::

    SPARK_LOCAL_IP=127.0.0.1 python python/repark/tests/_record_cast_map_spell_1.py

Not collected by pytest.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
_FIXTURE = _HERE / "cast_map_spell_1" / "cast_map_spell_1_spark_oracle.json"

_SQL_CELLS: dict[str, str] = {
    "null_map_str_int": "SELECT CAST(NULL AS MAP<STRING, INT>) AS m",
    "null_map_str_list_int": "SELECT CAST(NULL AS MAP<STRING, ARRAY<INT>>) AS m",
    "empty_map_str_int": "SELECT CAST(map() AS MAP<STRING, INT>) AS m",
    "widen_value": "SELECT CAST(map('a', 1) AS MAP<STRING, BIGINT>) AS m",
    "value_to_string": "SELECT CAST(map('a', 1) AS MAP<STRING, STRING>) AS m",
    "key_int_to_string": "SELECT CAST(map(1, 'x') AS MAP<STRING, STRING>) AS m",
    "nested_map_in_map": (
        "SELECT CAST(map('a', map('b', 1)) AS MAP<STRING, MAP<STRING, BIGINT>>) AS m"
    ),
    "array_of_map": "SELECT CAST(array(map('a', 1)) AS ARRAY<MAP<STRING, BIGINT>>) AS a",
    "struct_with_map": (
        "SELECT CAST(named_struct('m', map('a', 1)) AS STRUCT<m: MAP<STRING, BIGINT>>) AS s"
    ),
    "map_list_value_widen": (
        "SELECT CAST(map('k', array(1, 2)) AS MAP<STRING, ARRAY<BIGINT>>) AS m"
    ),
    "map_colon_struct_spelling": (
        "SELECT CAST(map('a', named_struct('x', 1)) AS MAP<STRING, STRUCT<x: BIGINT>>) AS m"
    ),
    "lowercase_spelling": "SELECT CAST(map('a', 1) AS map<string,bigint>) AS m",
    "invalid_value_cast_ansi": "SELECT CAST(map('a', 'x') AS MAP<STRING, INT>) AS m",
    "int_to_map_refuses": "SELECT CAST(1 AS MAP<STRING, INT>) AS m",
    "try_cast_invalid": "SELECT try_cast(map('a', 'x') AS MAP<STRING, INT>) AS m",
    "null_key_map_cast": ("SELECT CAST(map('a', CAST(NULL AS INT)) AS MAP<STRING, BIGINT>) AS m"),
    "union_with_typed_null": (
        "SELECT map('k1', array(1, 2)) AS m UNION ALL SELECT CAST(NULL AS MAP<STRING, ARRAY<INT>>)"
    ),
}

_LEGACY_SQL = "SELECT CAST(map('a', 'x') AS MAP<STRING, INT>) AS m"

_DATAFRAME_CELLS: tuple[str, ...] = (
    "df_cast_maptype",
    "df_cast_ddl_string",
    "df_cast_nested",
)


def _jsonable(value: Any) -> Any:
    """Return ``value`` with PySpark rows rendered as plain JSON values."""
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "asDict"):
        return _jsonable(value.asDict(recursive=True))
    return value


def _record_sql_cell(session: Any, key: str, sql: str) -> dict[str, Any]:
    """Run one SQL cell on live Spark and return its recorded oracle shape."""
    try:
        frame = session.sql(sql)
        rows = [[_jsonable(value) for value in row] for row in frame.collect()]
        return {
            "sql": sql,
            "schema": frame.schema.simpleString(),
            "nullable": [field.nullable for field in frame.schema.fields],
            "rows": rows,
        }
    except Exception as error:
        return {
            "sql": sql,
            "error_class": type(error).__name__,
            "error": str(error).splitlines()[0][:300],
        }


def _record_dataframe_cells(session: Any) -> dict[str, dict[str, Any]]:
    """Run the three DataFrame-door ``.cast`` cells on live Spark."""
    from pyspark.sql import functions as spark_functions
    from pyspark.sql import types as spark_types

    base = session.sql("SELECT map('a', 1) AS m")
    nested_base = session.sql("SELECT map('k', array(1)) AS m")
    calls: dict[str, Any] = {
        "df_cast_maptype": lambda: base.select(
            spark_functions.col("m")
            .cast(spark_types.MapType(spark_types.StringType(), spark_types.LongType()))
            .alias("m")
        ),
        "df_cast_ddl_string": lambda: base.select(
            spark_functions.col("m").cast("map<string,bigint>").alias("m")
        ),
        "df_cast_nested": lambda: nested_base.select(
            spark_functions.col("m").cast("map<string,array<bigint>>").alias("m")
        ),
    }
    cells: dict[str, dict[str, Any]] = {}
    for key, call in calls.items():
        try:
            frame = call()
            rows = [[row[0]] for row in frame.collect()]
            cells[key] = {
                "call": key,
                "schema": frame.schema.simpleString(),
                "nullable": [field.nullable for field in frame.schema.fields],
                "rows": [[_jsonable(value) for value in row] for row in rows],
            }
        except Exception as error:
            cells[key] = {
                "call": key,
                "error_class": type(error).__name__,
                "error": str(error).splitlines()[0][:300],
            }
    return cells


def record_oracle() -> dict[str, Any]:
    """Derive every oracle cell from a fresh live Spark 4.1.2 session."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        cells: dict[str, dict[str, Any]] = {}
        for key, sql in _SQL_CELLS.items():
            cells[key] = _record_sql_cell(session, key, sql)
            print(key, json.dumps(cells[key], default=str)[:230], flush=True)
        cells.update(_record_dataframe_cells(session))
        for key in _DATAFRAME_CELLS:
            print(key, json.dumps(cells[key], default=str)[:230], flush=True)
        session.conf.set("spark.sql.ansi.enabled", "false")
        try:
            frame = session.sql(_LEGACY_SQL)
            rows = [[_jsonable(value) for value in row] for row in frame.collect()]
            cells["invalid_value_cast_legacy"] = {
                "sql": _LEGACY_SQL,
                "ansi": False,
                "schema": frame.schema.simpleString(),
                "rows": rows,
            }
        except Exception as error:
            cells["invalid_value_cast_legacy"] = {
                "sql": _LEGACY_SQL,
                "ansi": False,
                "error_class": type(error).__name__,
                "error": str(error).splitlines()[0][:300],
            }
        print("invalid_value_cast_legacy", flush=True)
        return {"spark_version": session.version, "ansi_default": True, "cells": cells}
    finally:
        session.stop()


def check_oracle() -> int:
    """Compare a fresh live derivation against the committed fixture."""
    oracle = record_oracle()
    expected = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    if oracle["cells"] == expected["cells"]:
        print("cast_map_spell_1 oracle matches the committed fixture")
        return 0
    for key in sorted(set(oracle["cells"]) | set(expected["cells"])):
        if oracle["cells"].get(key) != expected["cells"].get(key):
            print(f"drift in cell {key}:")
            print(f"  recorded:  {json.dumps(expected['cells'].get(key), default=str)}")
            print(f"  live now:  {json.dumps(oracle['cells'].get(key), default=str)}")
    return 1


def main(argv: list[str] | None = None) -> int:
    """Record the fixture, or check it against live Spark under ``--check``."""
    parser = argparse.ArgumentParser(description="Re-derive the cast-map oracle.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Compare live Spark against the committed fixture instead of writing it.",
    )
    args = parser.parse_args(argv)
    if args.check:
        return check_oracle()
    _FIXTURE.write_text(json.dumps(record_oracle(), indent=1, default=str) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
