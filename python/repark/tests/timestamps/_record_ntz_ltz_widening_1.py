from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

ORACLE_PATH: Path = Path(__file__).with_name("ntz_ltz_widening_1_spark_oracle.json")
NY, KOLKATA, UTC = "America/New_York", "Asia/Kolkata", "UTC"
NTZ, LTZ = "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"
GAP, FOLD, PLAIN = "2024-03-10 02:30:00", "2024-11-03 01:30:00", "2024-06-01 09:00:00"
INSTANT = "to_timestamp_ltz('2024-01-01 02:00:00')"


def _ntz(wall: str) -> str:
    return f"TIMESTAMP_NTZ '{wall}'"


def _cell(
    kind: str, zone: str, session_type: str, site: str, a: str, b: str = ""
) -> dict[str, Any]:
    return {
        "kind": kind,
        "zone": zone,
        "timestamp_type": session_type,
        "site": site,
        "a": a,
        "b": b,
    }


CELLS: dict[str, dict[str, Any]] = {
    "eq_gap_ny_ntz": _cell(
        "PROTECTED", NY, NTZ, "expr", f"{_ntz(GAP)} = to_timestamp_ltz('2024-03-10 03:30:00')"
    ),
    "case_plain_kolkata_ntz": _cell(
        "PROTECTED", KOLKATA, NTZ, "expr", f"CASE WHEN 1 = 1 THEN {_ntz(PLAIN)} ELSE {INSTANT} END"
    ),
    "date_trunc_hour_gap_ny_ltz": _cell(
        "TARGET", NY, LTZ, "expr", f"date_trunc('HOUR', {_ntz(GAP)})"
    ),
    "date_trunc_day_plain_kolkata_ntz": _cell(
        "TARGET", KOLKATA, NTZ, "expr", f"date_trunc('DAY', {_ntz(PLAIN)})"
    ),
    "date_trunc_hour_plain_utc_ltz": _cell(
        "TARGET", UTC, LTZ, "expr", f"date_trunc('HOUR', {_ntz(PLAIN)})"
    ),
    "sql_union_ntz_only_ny_ntz": _cell("PROTECTED", NY, NTZ, "sql_union", _ntz(GAP), _ntz(PLAIN)),
    "sql_union_ltz_only_ny_ntz": _cell(
        "PROTECTED", NY, NTZ, "sql_union", INSTANT, "to_timestamp_ltz('2024-03-10 03:30:00')"
    ),
    "df_union_ntz_only_kolkata_ltz": _cell(
        "PROTECTED", KOLKATA, LTZ, "df_union", _ntz(GAP), _ntz(PLAIN)
    ),
    "sql_union_gap_ny_ltz": _cell("PROTECTED", NY, LTZ, "sql_union", _ntz(GAP), INSTANT),
    "coalesce_ntz_only_ny_ntz": _cell(
        "PROTECTED", NY, NTZ, "expr", f"coalesce({_ntz(GAP)}, {_ntz(PLAIN)})"
    ),
    "date_trunc_hour_ltz_gap_ny_ntz": _cell(
        "PROTECTED", NY, NTZ, "expr", "date_trunc('HOUR', to_timestamp_ltz('2024-03-10 03:30:00'))"
    ),
    "date_trunc_day_date_ny_ltz": _cell(
        "PROTECTED", NY, LTZ, "expr", "date_trunc('DAY', DATE '2024-03-10')"
    ),
}


def frame(session: Any, functions: Any, cell: dict[str, Any]) -> Any:
    site, a, b = cell["site"], cell["a"], cell["b"]
    if site == "sql_union":
        return session.sql(f"SELECT 1 AS id, {a} AS v UNION ALL SELECT 2 AS id, {b} AS v")
    if site == "df_union":
        first = session.range(1).select(functions.lit(1).alias("id"), functions.expr(a).alias("v"))
        second = session.range(1).select(functions.lit(2).alias("id"), functions.expr(b).alias("v"))
        return first.union(second)
    return session.range(1).select(functions.lit(1).alias("id"), functions.expr(a).alias("v"))


def measure(session: Any, functions: Any, cell: dict[str, Any]) -> list[Any]:
    df = frame(session, functions, cell)
    field = df.schema["v"]
    kind = field.dataType.simpleString()
    micros = "CAST(unix_micros(v) AS STRING)" if kind == "timestamp" else "CAST(NULL AS STRING)"
    rows = df.selectExpr("id", "CAST(v AS STRING) AS s", f"{micros} AS u").orderBy("id").collect()
    return [kind, bool(field.nullable), [[row[0], row[1], row[2]] for row in rows]]


def main() -> int:
    from pyspark.sql import SparkSession, functions

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    cells: dict[str, Any] = {}
    for name, cell in CELLS.items():
        spark.conf.set("spark.sql.session.timeZone", cell["zone"])
        spark.conf.set("spark.sql.timestampType", cell["timestamp_type"])
        spark.conf.set("spark.sql.ansi.enabled", "false")
        cells[name] = {**cell, "spark": measure(spark, functions, cell)}
    recorded = {"spark_version": spark.version, "cells": cells}
    spark.stop()
    if "--rewrite" in sys.argv:
        ORACLE_PATH.write_text(
            json.dumps(recorded, indent=1, sort_keys=True) + "\n", encoding="utf-8"
        )
        return 0
    stored = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
    drift = [
        name for name in cells if stored["cells"].get(name, {}).get("spark") != cells[name]["spark"]
    ]
    print(
        json.dumps(
            {"drift": drift, "cells": len(cells), "java_home": bool(os.environ.get("JAVA_HOME"))}
        )
    )
    return 1 if drift else 0


if __name__ == "__main__":
    raise SystemExit(main())
