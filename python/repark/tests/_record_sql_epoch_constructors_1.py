"""Re-derive the SQL-EPOCH-CONSTRUCTORS-1 Spark oracle from live PySpark 4.1.2.

Every input in ``INPUTS`` runs through ``timestamp_seconds``, ``timestamp_millis``
and ``timestamp_micros`` on the SQL door and the DataFrame door, under session
zones ``UTC`` and ``America/New_York`` with ``spark.sql.ansi.enabled`` false and
true. Each cell records ``unix_micros`` of the answer, or the error condition,
SQLSTATE and first message line, plus the result type from ``df.schema``. Only
``unix_micros`` crosses into Python, so no PySpark ``datetime`` conversion can
fail on a year outside Python's range. ``PROBES`` records whether the
``to_timestamp_*`` spellings resolve on Spark's SQL door.

Run with a PySpark 4.1.2 interpreter::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 /tmp/sparkenv/bin/python \\
        python/repark/tests/_record_sql_epoch_constructors_1.py

``--check`` compares a fresh recording against the committed fixture and exits
non-zero on drift. Not collected by pytest.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
FIXTURE = _HERE / "sql_epoch_constructors_1_spark_oracle.json"
ZONES: tuple[str, ...] = ("UTC", "America/New_York")
ANSI_MODES: tuple[str, ...] = ("false", "true")
FUNCTIONS: tuple[str, ...] = ("timestamp_seconds", "timestamp_millis", "timestamp_micros")
INPUTS: tuple[tuple[str, str], ...] = (
    ("int-zero", "0"),
    ("int-one", "1"),
    ("int-neg-one", "-1"),
    ("bigint-one", "CAST(1 AS BIGINT)"),
    ("bigint-neg-one", "CAST(-1 AS BIGINT)"),
    ("smallint-one", "CAST(1 AS SMALLINT)"),
    ("tinyint-one", "CAST(1 AS TINYINT)"),
    ("sec-2100", "4102444800"),
    ("sec-1900", "-2208988800"),
    ("sec-9999", "253402300799"),
    ("sec-0001", "-62135596800"),
    ("millis-2100", "4102444800000"),
    ("millis-1900", "-2208988800000"),
    ("micros-2100", "4102444800000000"),
    ("micros-1900", "-2208988800000000"),
    ("sec-bound-fit", "9223372036854"),
    ("sec-bound-over", "9223372036855"),
    ("sec-bound-neg-fit", "-9223372036854"),
    ("sec-bound-neg-over", "-9223372036855"),
    ("millis-bound-fit", "9223372036854775"),
    ("millis-bound-over", "9223372036854776"),
    ("bigint-max", "9223372036854775807"),
    ("bigint-min", "-9223372036854775808"),
    ("double-frac", "CAST(1.5 AS DOUBLE)"),
    ("double-neg-frac", "CAST(-1.5 AS DOUBLE)"),
    ("double-trunc", "CAST(1.9 AS DOUBLE)"),
    ("double-neg-trunc", "CAST(-1.9 AS DOUBLE)"),
    ("double-big", "CAST(1E15 AS DOUBLE)"),
    ("double-huge", "CAST(1E300 AS DOUBLE)"),
    ("dec-frac", "CAST(1.5 AS DECIMAL(10, 2))"),
    ("dec-neg-frac", "CAST(-1.5 AS DECIMAL(10, 2))"),
    ("dec-int", "CAST(1500000000 AS DECIMAL(12, 0))"),
    ("dec-big", "CAST(123456789012345678901234567890.5 AS DECIMAL(38, 1))"),
    ("dec-submicro", "CAST(0.0000005 AS DECIMAL(10, 7))"),
    ("dec-micro", "CAST(0.000001 AS DECIMAL(10, 6))"),
    ("dec-neg-submicro", "CAST(-0.0000005 AS DECIMAL(10, 7))"),
    ("dec-huge-int", "CAST(99999999999999999999999999999 AS DECIMAL(38, 0))"),
    ("double-submicro-pos", "CAST(0.0000001 AS DOUBLE)"),
    ("double-submicro-neg", "CAST(-0.0000001 AS DOUBLE)"),
    ("double-halfmicro", "CAST(0.0000005 AS DOUBLE)"),
    ("double-near-micro", "CAST(0.0000009 AS DOUBLE)"),
    ("float-frac", "CAST(1.5 AS FLOAT)"),
    ("double-neg-huge", "CAST(-1E300 AS DOUBLE)"),
    ("double-nan", "CAST('NaN' AS DOUBLE)"),
    ("double-inf", "CAST('Infinity' AS DOUBLE)"),
    ("bool-true", "TRUE"),
    ("ts-input", "TIMESTAMP '2020-01-01 00:00:00'"),
    ("str-one", "'1'"),
    ("str-ts", "'1970-01-01 00:00:01'"),
    ("str-bad", "'abc'"),
    ("str-empty", "''"),
    ("null-bigint", "CAST(NULL AS BIGINT)"),
    ("null-bare", "NULL"),
    ("null-string", "CAST(NULL AS STRING)"),
    ("dec-sec-max-exact", "CAST(9223372036854.775807 AS DECIMAL(19,6))"),
    ("dec-sec-max-plus-half", "CAST(9223372036854.7758075 AS DECIMAL(20,7))"),
    ("dec-sec-max-plus-1exact", "CAST(9223372036854.775808 AS DECIMAL(19,6))"),
    ("dec-sec-max-plus-1half", "CAST(9223372036854.7758085 AS DECIMAL(20,7))"),
    ("dec-sec-max-minus-half", "CAST(9223372036854.7758065 AS DECIMAL(20,7))"),
    ("dec-sec-max-minus-1exact", "CAST(9223372036854.775806 AS DECIMAL(19,6))"),
    ("dec-sec-min-exact", "CAST(-9223372036854.775808 AS DECIMAL(19,6))"),
    ("dec-sec-min-minus-half", "CAST(-9223372036854.7758085 AS DECIMAL(20,7))"),
    ("dec-sec-min-minus-1exact", "CAST(-9223372036854.775809 AS DECIMAL(19,6))"),
    ("dec-sec-min-minus-1half", "CAST(-9223372036854.7758095 AS DECIMAL(20,7))"),
    ("dec-sec-min-plus-half", "CAST(-9223372036854.7758075 AS DECIMAL(20,7))"),
    ("dec-sec-min-plus-1exact", "CAST(-9223372036854.775807 AS DECIMAL(19,6))"),
    ("dec-sec-19nine-half", "CAST(9999999999999.9999995 AS DECIMAL(20,7))"),
    ("dec-sec-19nine-exact", "CAST(9999999999999.999999 AS DECIMAL(19,6))"),
    ("dec-sec-19nine-9frac", "CAST(9999999999999.9999999 AS DECIMAL(20,7))"),
    ("dec-sec-20digit-half", "CAST(10000000000000.0000005 AS DECIMAL(21,7))"),
    ("dec-sec-20digit-exact", "CAST(10000000000000.000000 AS DECIMAL(20,6))"),
    ("dec-sec-neg19nine-half", "CAST(-9999999999999.9999995 AS DECIMAL(20,7))"),
    ("dec-sec-neg19nine-exact", "CAST(-9999999999999.999999 AS DECIMAL(19,6))"),
    ("dec-sec-neg20digit-half", "CAST(-10000000000000.0000005 AS DECIMAL(21,7))"),
    ("dec-sec-neg20digit-exact", "CAST(-10000000000000.000000 AS DECIMAL(20,6))"),
    ("dec-sec-max-trailing-zero", "CAST(9223372036854.7758070 AS DECIMAL(20,7))"),
)
PROBE_FUNCTIONS: tuple[str, ...] = (
    "to_timestamp_seconds",
    "to_timestamp_millis",
    "to_timestamp_micros",
)


def error_cell(error: Exception) -> dict[str, Any]:
    """Capture a Spark failure as its condition, SQLSTATE and first line.

    Args:
        error: The raised PySpark exception.

    Returns:
        The error cell.
    """
    condition = getattr(error, "getCondition", lambda: None)()
    sqlstate = getattr(error, "getSqlState", lambda: None)()
    return {"error": condition, "sqlstate": sqlstate, "message": str(error).split("\n")[0].strip()}


def run_sql_value(session: Any, function: str, expression: str) -> dict[str, Any]:
    """Run one SQL-door call and capture its micros or its error.

    Args:
        session: A live PySpark session already set to the cell zone and ANSI mode.
        function: The epoch constructor under test.
        expression: The SQL argument expression.

    Returns:
        ``{"us": int | None}`` on success, else the error cell.
    """
    try:
        row = session.sql(f"SELECT unix_micros({function}({expression})) AS us").collect()[0]
    except Exception as error:
        return error_cell(error)
    return {"us": row["us"]}


def run_sql_type(session: Any, function: str, expression: str) -> dict[str, Any]:
    """Read one SQL-door call's result type without collecting rows.

    Args:
        session: A live PySpark session already set to the cell zone and ANSI mode.
        function: The epoch constructor under test.
        expression: The SQL argument expression.

    Returns:
        ``{"type": str}`` on success, else the error cell.
    """
    try:
        schema = session.sql(f"SELECT {function}({expression}) AS v").schema
    except Exception as error:
        return error_cell(error)
    return {"type": schema["v"].dataType.simpleString()}


def run_df_value(session: Any, functions: Any, function: str, expression: str) -> dict[str, Any]:
    """Run one DataFrame-door call and capture its micros or its error.

    Args:
        session: A live PySpark session already set to the cell zone and ANSI mode.
        functions: The ``pyspark.sql.functions`` module.
        function: The epoch constructor under test.
        expression: The SQL argument expression, selected as column ``v`` first.

    Returns:
        ``{"us": int | None}`` on success, else the error cell.
    """
    try:
        call = getattr(functions, function)
        frame = session.sql(f"SELECT {expression} AS v").select(
            functions.unix_micros(call("v")).alias("us")
        )
        row = frame.collect()[0]
    except Exception as error:
        return error_cell(error)
    return {"us": row["us"]}


def run_df_type(session: Any, functions: Any, function: str, expression: str) -> dict[str, Any]:
    """Read one DataFrame-door call's result type without collecting rows.

    Args:
        session: A live PySpark session already set to the cell zone and ANSI mode.
        functions: The ``pyspark.sql.functions`` module.
        function: The epoch constructor under test.
        expression: The SQL argument expression, selected as column ``v`` first.

    Returns:
        ``{"type": str}`` on success, else the error cell.
    """
    try:
        call = getattr(functions, function)
        schema = session.sql(f"SELECT {expression} AS v").select(call("v").alias("v")).schema
    except Exception as error:
        return error_cell(error)
    return {"type": schema["v"].dataType.simpleString()}


def record_cell(
    session: Any, functions: Any, zone: str, ansi: str, label: str, expression: str
) -> dict[str, Any]:
    """Record every door's answer for one input across the three functions.

    Args:
        session: A live PySpark session already set to ``zone`` and ``ansi``.
        functions: The ``pyspark.sql.functions`` module.
        zone: The session zone.
        ansi: ``"true"`` or ``"false"``.
        label: The input label.
        expression: The SQL argument expression.

    Returns:
        One fixture cell.
    """
    cell: dict[str, Any] = {"zone": zone, "ansi": ansi, "input": label, "expr": expression}
    for function in FUNCTIONS:
        cell[function] = {
            "sql": run_sql_value(session, function, expression),
            "sql_type": run_sql_type(session, function, expression),
            "df": run_df_value(session, functions, function, expression),
            "df_type": run_df_type(session, functions, function, expression),
        }
    return cell


def record_probes(session: Any, zone: str, ansi: str) -> list[dict[str, Any]]:
    """Record whether the ``to_timestamp_*`` spellings resolve on Spark's SQL door.

    Args:
        session: A live PySpark session already set to ``zone`` and ``ansi``.
        zone: The session zone.
        ansi: ``"true"`` or ``"false"``.

    Returns:
        One probe row per spelling.
    """
    probes: list[dict[str, Any]] = []
    for function in PROBE_FUNCTIONS:
        probes.append(
            {
                "zone": zone,
                "ansi": ansi,
                "function": function,
                "sql": run_sql_value(session, function, "1"),
                "sql_type": run_sql_type(session, function, "1"),
            }
        )
    return probes


def record(session: Any) -> dict[str, Any]:
    """Record every cell against a live PySpark session.

    Args:
        session: A live PySpark 4.1.2 session.

    Returns:
        The fixture document.
    """
    import pyspark.sql.functions as functions

    cells: list[dict[str, Any]] = []
    probes: list[dict[str, Any]] = []
    for zone in ZONES:
        for ansi in ANSI_MODES:
            session.conf.set("spark.sql.session.timeZone", zone)
            session.conf.set("spark.sql.ansi.enabled", ansi)
            cells.extend(
                record_cell(session, functions, zone, ansi, label, expression)
                for label, expression in INPUTS
            )
            probes.extend(record_probes(session, zone, ansi))
    return {
        "banner": session.version,
        "zone_banner": {zone: zone for zone in ZONES},
        "recorder": "python/repark/tests/_record_sql_epoch_constructors_1.py",
        "cells": cells,
        "probes": probes,
    }


def comparable(document: dict[str, Any]) -> dict[str, Any]:
    """Drop the banner so two recordings compare on value.

    Args:
        document: A fixture document.

    Returns:
        The cells and probes without recording metadata.
    """
    return {"cells": document["cells"], "probes": document["probes"]}


def main() -> int:
    """Record the oracle, or compare a fresh recording with the committed fixture.

    Returns:
        The process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--out", type=Path, default=FIXTURE)
    arguments = parser.parse_args()
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    document = record(session)
    if arguments.check:
        committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
        if comparable(committed) != comparable(document):
            print("sql-epoch-constructors-1 oracle drift", file=sys.stderr)
            return 1
        print(f"sql-epoch-constructors-1 oracle matches ({len(document['cells'])} cells)")
        return 0
    arguments.out.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {len(document['cells'])} cells to {arguments.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
