"""Record the fold-1 oracle: the twelve shapes plus struct probes on main.

Run with the BASELINE module (``/tmp/xcoerce0/.venv/bin/python``): every
shape over sources ``ns`` and ``tzns``, zones UTC and America/New_York, ANSI
on and off, on the SQL door and the four DataFrame shapes the verifier ran.
Writes ``ice_tsns_coercion_1_slice1_fold1.json`` mapping each cell to its
columns (names, Arrow types, nullability, exact values) or its error.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _ice_tsns_coercion_1_doors as doors
import pyarrow as pa
import pyarrow.compute as pc
from _ice_tsns_coercion_1_spellings import SRC

from repark import functions

TESTS = Path(__file__).resolve().parent
OUT = TESTS / "ice_tsns_coercion_1_slice1_fold1.json"
ZONES = ("UTC", "America/New_York")
ANSIS = ("true", "false")
SOURCES = ("ns", "tzns")


def _select(projection: str, tail: str = "ORDER BY id") -> str:
    """Return one SELECT template over the shared source table."""
    return f"SELECT {projection} FROM {SRC} {tail}"


_WIN = "WINDOW w AS (ORDER BY id) ORDER BY id"
_UNION_INT = "UNION ALL SELECT struct({c}, 5) FROM " + SRC
_UNION_STR = "UNION ALL SELECT struct({c}, 'x') FROM " + SRC
_STRUCT_CASE = "id, CASE WHEN id > 1 THEN struct({c}, NULL) ELSE struct({c}, 5) END AS v"
SQL_SHAPES: tuple[tuple[str, str], ...] = (
    ("convert_timezone", _select("id, convert_timezone(NULL, {c}) AS v")),
    ("from_utc_is_null", _select("id, from_utc_timestamp({c}, NULL) IS NULL AS v")),
    ("from_utc_where", _select("id", "WHERE from_utc_timestamp({c}, NULL) IS NULL ORDER BY id")),
    ("to_utc_is_null", _select("id, to_utc_timestamp({c}, NULL) IS NULL AS v")),
    ("string_agg", _select("string_agg({c}, NULL) AS v", "")),
    ("lag", _select("id, lag({c}, NULL) OVER w AS v", _WIN)),
    ("lag_default", _select("id, lag({c}, NULL, NULL) OVER w AS v", _WIN)),
    ("lead", _select("id, lead({c}, NULL) OVER w AS v", _WIN)),
    ("nth_value", _select("id, nth_value({c}, NULL) OVER w AS v", _WIN)),
    ("union_int", _select("struct({c}, NULL) AS v", _UNION_INT)),
    ("union_str", _select("struct({c}, NULL) AS v", _UNION_STR)),
    ("struct_case", _select(_STRUCT_CASE)),
    ("struct", _select("id, struct({c}, NULL) AS v")),
    ("struct_typeof", _select("id, typeof(struct({c}, NULL)) AS v")),
    ("struct_json", _select("id, to_json(struct({c}, NULL)) AS v")),
)
DF_SHAPES = ("from_utc_is_null", "convert_timezone", "lag", "string_agg")
UNORDERED = ("union_int", "union_str")


def _encode(array: pa.Array) -> list[Any]:
    """Return ``array`` as JSON-safe values with exact timestamp ticks."""
    if pa.types.is_timestamp(array.type):
        return pc.cast(array, pa.int64()).to_pylist()
    if pa.types.is_struct(array.type):
        out: list[Any] = []
        for index in range(len(array)):
            if array.is_null()[index].as_py():
                out.append(None)
                continue
            row = {}
            for number in range(array.type.num_fields):
                field = array.type.field(number)
                row[field.name] = _encode(array.field(number)[index : index + 1])[0]
            out.append(row)
        return out
    return array.to_pylist()


def _capture(frame: Any) -> dict[str, Any]:
    """Return the exact columns of one answered query."""
    table = frame.toArrow()
    columns = []
    for number in range(table.num_columns):
        field = table.schema.field(number)
        columns.append(
            {
                "name": field.name,
                "type": str(table.schema.types[number]),
                "nullable": field.nullable,
                "values": _encode(table.column(number).combine_chunks()),
            }
        )
    return {"columns": columns}


def _measure(spark: Any, door: str, source: str, shape: str, template: str) -> dict[str, Any]:
    """Run one cell and return its outcome or its error."""
    try:
        if door == "sql":
            return _capture(spark.sql(template.format(c=source)))
        frame = spark.table(SRC)
        if shape == "from_utc_is_null":
            moment = functions.from_utc_timestamp(functions.col(source), functions.lit(None))
            return _capture(frame.select("id", moment.isNull().alias("v")).orderBy("id"))
        if shape == "convert_timezone":
            return _capture(
                frame.select(
                    "id", functions.expr(f"convert_timezone(NULL, {source})").alias("v")
                ).orderBy("id")
            )
        if shape == "lag":
            return _capture(
                frame.selectExpr("id", f"lag({source}, NULL) OVER (ORDER BY id) AS v").orderBy("id")
            )
        return _capture(frame.agg(functions.expr(f"string_agg({source}, NULL)").alias("v")))
    except Exception as error:
        return {"error": doors.error_text(error)}


def main() -> None:
    """Record every fold-1 oracle cell."""
    outcomes: dict[str, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory() as scratch:
        for zone_index, zone in enumerate(ZONES):
            for ansi in ANSIS:
                warehouse = Path(scratch) / f"fold1-{zone_index}-{ansi}"
                warehouse.mkdir()
                spark = doors.open_session(zone, warehouse)
                spark.conf.set("spark.sql.ansi.enabled", ansi)
                try:
                    for source in SOURCES:
                        for shape, template in SQL_SHAPES:
                            cell = f"{zone}|{ansi}|sql|{source}|{shape}"
                            outcomes[cell] = _measure(spark, "sql", source, shape, template)
                        for shape in DF_SHAPES:
                            cell = f"{zone}|{ansi}|df|{source}|{shape}"
                            outcomes[cell] = _measure(spark, "df", source, shape, "")
                finally:
                    spark.stop()
    OUT.write_text(json.dumps(outcomes, indent=1, sort_keys=True) + "\n")
    errors = sum(1 for outcome in outcomes.values() if "error" in outcome)
    print(f"cells={len(outcomes)} errors={errors}")


if __name__ == "__main__":
    main()
