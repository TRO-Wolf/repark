"""Record Spark 4.1.2's lattice for the ICE-TSNS-COERCION-1 temporal set.

The source columns stand where RePark's nanosecond columns stand: ``src_ntz`` for
``timestamp_ns`` and ``src_ltz`` for ``timestamptz_ns``. Both ANSI modes and the
five session zones are recorded. The warehouse directory comes from ``--warehouse``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

ZONES = (
    "UTC",
    "America/New_York",
    "Asia/Kolkata",
    "Australia/Lord_Howe",
    "-03:30",
)
FORMS = ("coalesce", "array", "case", "if", "nvl", "ifnull", "union")
SIBLINGS = (
    "untyped_null",
    "typed_null_ts",
    "typed_null_ntz",
    "us_column_ts",
    "us_column_ntz",
    "us_literal_ts",
    "us_literal_ntz",
    "date_literal",
    "literal_9999",
    "literal_0001",
    "column_far",
)
SOURCES = ("ns", "tzns")


def sibling_sql(sibling: str) -> str:
    """Return the Spark SQL of one sibling."""
    return {
        "untyped_null": "NULL",
        "typed_null_ts": "CAST(NULL AS TIMESTAMP)",
        "typed_null_ntz": "CAST(NULL AS TIMESTAMP_NTZ)",
        "us_column_ts": "sib_ts",
        "us_column_ntz": "sib_ntz",
        "us_literal_ts": "TIMESTAMP '2026-01-02 03:04:05.123456'",
        "us_literal_ntz": "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'",
        "date_literal": "DATE '2026-01-02'",
        "literal_9999": "TIMESTAMP '9999-12-31 00:00:00'",
        "literal_0001": "TIMESTAMP '0001-01-01 00:00:00'",
        "column_far": "far",
    }[sibling]


def statement_sql(form: str, source: str, sibling: str) -> str:
    """Return the Spark statement for one card spelling."""
    left = "src_ntz" if source == "ns" else "src_ltz"
    right = sibling_sql(sibling)
    if form == "union":
        return f"SELECT id, {left} AS v FROM src UNION ALL SELECT id, {right} AS v FROM src"
    if form == "coalesce":
        expr = f"coalesce({left}, {right})"
    elif form == "nvl":
        expr = f"nvl({left}, {right})"
    elif form == "ifnull":
        expr = f"ifnull({left}, {right})"
    elif form == "array":
        expr = f"array({left}, {right})[0]"
    elif form == "case":
        expr = f"CASE WHEN id > 0 THEN {left} ELSE {right} END"
    elif form == "if":
        expr = f"if(id > 0, {left}, {right})"
    else:
        raise KeyError(form)
    return f"SELECT id, {expr} AS v FROM src"


def one_cell(spark: SparkSession, statement: str) -> dict[str, Any]:
    """Return the type, text and epoch micros of ``statement``."""
    try:
        frame = spark.sql(statement)
        type_name = frame.schema["v"].dataType.simpleString()
    except Exception as error:
        return {"error": str(error).splitlines()[0][:240]}
    try:
        taken = frame.selectExpr(
            "id", "CAST(v AS STRING) AS text", "unix_micros(v) AS micros"
        ).collect()
        rows = [
            {
                "id": row["id"],
                "text": None if row["text"] is None else str(row["text"]),
                "micros": row["micros"],
            }
            for row in taken
        ]
    except Exception:
        taken = frame.selectExpr("id", "CAST(v AS STRING) AS text").collect()
        rows = [
            {"id": row["id"], "text": None if row["text"] is None else str(row["text"])}
            for row in taken
        ]
    return {"type": type_name, "rows": rows}


def open_spark(zone: str, warehouse: str) -> SparkSession:
    """Open a local Spark session in ``zone``."""
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.warehouse.dir", warehouse)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW src AS SELECT 1 AS id, "
        "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456' AS src_ntz, "
        "TIMESTAMP '2026-01-02 03:04:05.123456' AS src_ltz, "
        "TIMESTAMP '2026-01-02 03:04:05.123456' AS sib_ts, "
        "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456' AS sib_ntz, "
        "TIMESTAMP '9999-12-31 00:00:00' AS far "
        "UNION ALL SELECT 2, "
        "TIMESTAMP_NTZ '1969-12-31 23:59:59.999999', "
        "TIMESTAMP '1969-12-31 23:59:59.999999', "
        "TIMESTAMP '1969-12-31 23:59:59.999999', "
        "TIMESTAMP_NTZ '1969-12-31 23:59:59.999999', "
        "TIMESTAMP '9999-12-31 00:00:00'"
    )
    return spark


def main() -> None:
    """Write the lattice JSON."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--zones", default="")
    args = parser.parse_args()
    zones = tuple(item for item in args.zones.split(",") if item) or ZONES
    cells: dict[str, Any] = {}
    version = ""
    for zone in zones:
        spark = open_spark(zone, str(args.warehouse))
        version = version or spark.version
        for ansi in ("true", "false"):
            spark.conf.set("spark.sql.ansi.enabled", ansi)
            for source in SOURCES:
                for form in FORMS:
                    for sibling in SIBLINGS:
                        key = f"{zone}|{ansi}|{source}|{form}|{sibling}"
                        cells[key] = one_cell(spark, statement_sql(form, source, sibling))
        spark.stop()
    payload = {"spark": version, "zones": list(zones), "cells": cells}
    args.output.write_text(json.dumps(payload, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
