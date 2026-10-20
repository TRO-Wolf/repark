"""The ICE-TSNS-TYPE-PLANNER-1 rule as a harness: every expectation is derived here.

Three clauses: a NULL beside a nanosecond value is nanoseconds; a written
``TIMESTAMP`` cast over a nanosecond value floors to microseconds; mixed units
widen to the finest. A cell states one statement with markers and the harness
writes its twin: ``{N}`` becomes a NULL typed as the site, ``<<x>>`` becomes
``x`` cast to the site type. A cell is green when the statement and its twin
give one outcome.

pins: ice-tsns-type-planner-1/C-006
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import pyarrow as pa

from repark import Window, functions

SRC = "ice.ns.src"
NS_NAMES = {"ns": "timestamp_ns", "tzns": "timestamptz_ns", "tzns_utc": "timestamptz_ns"}
UNIT_ORDER = ("s", "ms", "us", "ns")
ANSWERED: dict[str, bool] = {}
ZONE = ["UTC"]
_WIDEN = re.compile(r"<<(.+?)>>")
_NOISE = re.compile(
    r"__repark_[a-z_]+_[0-9a-f]{16,}"
    r"|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"|ice\.ns\.t\d+|__repark_tt_\d+"
    r"|0x[0-9a-f]+"
)


def finest(units: list[str]) -> str:
    """Return the finest of ``units``, the unit a mixed position takes."""
    return max(units, key=UNIT_ORDER.index)


def site_type(column: str, zoned: bool | None = None) -> str:
    """Return the nanosecond SQL type name of a site over ``column``."""
    if zoned is None:
        return NS_NAMES[column]
    return "timestamptz_ns" if zoned else "timestamp_ns"


def typed(expr: str, type_name: str) -> str:
    """Return ``expr`` cast to ``type_name``."""
    return f"CAST({expr} AS {type_name})"


def statement_of(text: str, column: str) -> str:
    """Return the statement: markers removed, the NULL left untyped."""
    plain = _WIDEN.sub(lambda match: match.group(1), text)
    return (
        plain.replace("{N}", "NULL")
        .replace("{FN}", "F.lit(None)")
        .replace("{c}", column)
        .replace("{S}", SRC)
        .replace("{n}", NS_NAMES[column])
    )


def twin_of(text: str, column: str, zoned: bool | None = None) -> str:
    """Return the twin: every marked operand cast to the site type."""
    name = site_type(column, zoned)
    widened = _WIDEN.sub(lambda match: typed(match.group(1), name), text)
    return (
        widened.replace("{N}", typed("NULL", name))
        .replace("{FN}", f"F.expr('{typed('NULL', name)}')")
        .replace("{c}", column)
        .replace("{S}", SRC)
        .replace("{n}", NS_NAMES[column])
    )


def has_markers(text: str) -> bool:
    """Return whether ``text`` holds a marker the twin rewrites."""
    return "{N}" in text or "{FN}" in text or bool(_WIDEN.search(text))


def ticks_of(column: pa.ChunkedArray) -> list[Any]:
    """Return timestamp ticks as integers, through list layers; other values as text."""
    kind = column.type
    if pa.types.is_timestamp(kind):
        return column.cast(pa.int64()).to_pylist()
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        out: list[Any] = []
        for chunk in column.chunks:
            for index in range(len(chunk)):
                item = chunk[index]
                if not item.is_valid:
                    out.append(None)
                else:
                    out.append(ticks_of(pa.chunked_array([item.values])))
        return out
    if pa.types.is_struct(kind):
        fields = [
            ticks_of(pa.chunked_array([chunk.field(index) for chunk in column.chunks]))
            for index in range(kind.num_fields)
        ]
        return [list(row) for row in zip(*fields, strict=True)]
    try:
        return [repr(item) for item in column.to_pylist()]
    except (OverflowError, ValueError, pa.ArrowInvalid):
        return [str(chunk) for chunk in column.chunks]


def capture(build: Callable[[], Any]) -> dict[str, Any]:
    """Return the Arrow types and ticks of one frame, or its error class."""
    try:
        table = build().toArrow()
    except Exception as error:
        lines = [line for line in str(error).splitlines() if line.strip()]
        return {
            "error_type": type(error).__name__,
            "error": _NOISE.sub("<X>", lines[-1] if lines else "")[:300],
        }
    return {
        "types": [str(kind) for kind in table.schema.types],
        "rows": [ticks_of(table.column(index)) for index in range(table.num_columns)],
    }


def capture_native(build: Callable[[], Any]) -> dict[str, Any]:
    """Return the Arrow types and ticks of one native-door frame, or its error class."""
    try:
        table = build().to_arrow()
    except Exception as error:
        lines = [line for line in str(error).splitlines() if line.strip()]
        return {
            "error_type": type(error).__name__,
            "error": _NOISE.sub("<X>", lines[-1] if lines else "")[:300],
        }
    return {
        "types": [str(kind) for kind in table.schema.types],
        "rows": [ticks_of(table.column(index)) for index in range(table.num_columns)],
    }


def category(outcome: dict[str, Any], ordered: bool = True) -> Any:
    """Return what two outcomes must share: the error class, or types and values."""
    if outcome.get("skip"):
        return ("skip",)
    if "error_type" in outcome and "stored" not in outcome:
        return ("error", outcome["error_type"])
    if "error" in outcome:
        return ("error", outcome.get("error_type"), repr(outcome.get("stored")))
    if "stored" in outcome:
        return ("stored", outcome.get("parquet_type", ""), repr(outcome["stored"]))
    rows = outcome["rows"]
    if not ordered:
        rows = [sorted(column, key=repr) for column in rows]
    return ("rows", tuple(outcome["types"]), repr(rows))


def answered(outcome: dict[str, Any]) -> bool:
    """Return whether the statement answered or stored without an error."""
    return "error_type" not in outcome and "error" not in outcome and not outcome.get("skip")


def note(cell: str, outcome: dict[str, Any]) -> None:
    """Record whether the statement of ``cell`` answered on this module."""
    ANSWERED[cell] = answered(outcome)


def differ(left: dict[str, Any], right: dict[str, Any], ordered: bool = True) -> str:
    """Return an empty string when the two outcomes share a category, else both."""
    if category(left, ordered) == category(right, ordered):
        return ""
    return f"{_brief(left)} != {_brief(right)}"


def _brief(outcome: dict[str, Any]) -> str:
    return repr(outcome)[:260]


def run_text(spark: Any, text: str) -> Callable[[], Any]:
    """Return a thunk running one SQL statement."""
    return lambda: spark.sql(text)


def run_code(spark: Any, code: str) -> Callable[[], Any]:
    """Return a thunk evaluating one DataFrame expression over the source table."""
    scope = {"spark": spark, "F": functions, "pa": pa, "W": Window, "T": spark.table(SRC)}
    return lambda: eval(code, scope)


def open_session(zone: str, warehouse: Path, ansi: str = "") -> Any:
    """Open the shared v3 session in ``zone`` with the microsecond side table."""
    spark = doors.open_session(zone, warehouse)
    if ansi:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
    spark.sql(
        "CREATE TABLE ice.ns.us (id INT, ts TIMESTAMP, ntz TIMESTAMP_NTZ, s STRING, d DATE, "
        "n BIGINT) USING iceberg TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(
        "INSERT INTO ice.ns.us SELECT 1, TIMESTAMP '2026-01-02 03:04:05.123456', "
        "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456', 'a', DATE '2026-01-02', 10"
    ).toArrow()
    spark.sql("INSERT INTO ice.ns.us SELECT 2, NULL, NULL, NULL, NULL, NULL").toArrow()
    return spark


def store(spark: Any, warehouse: Path, target: str, select: str) -> dict[str, Any]:
    """Run ``INSERT INTO t (id, v, k) <select>`` into a fresh ``target`` table."""
    table = doors.fresh_table()
    result: dict[str, Any] = {}
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, v {target}, k INT, c {target}) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3')"
        )
        try:
            spark.sql(select.replace("{T}", table)).toArrow()
        except Exception as error:
            result["error"] = doors.error_text(error)
            result["error_type"] = type(error).__name__
        pairs, parquet_type = doors.stored(warehouse, table)
        result["stored"] = pairs
        if parquet_type:
            result["parquet_type"] = parquet_type
    except Exception as error:
        result["error"] = doors.error_text(error)
        result["error_type"] = type(error).__name__
        result["stored"] = []
    finally:
        doors.cleanup(spark, warehouse, table)
    return result


@dataclass
class Group:
    """Cells that share one session: a name, a zone, and the runner of its cells."""

    name: str
    family: str
    zone: str
    run: Callable[[Path], dict[str, str]]
    ansi: str = ""
    cells: int = 0
    notes: dict[str, str] = field(default_factory=dict)


def reds(results: dict[str, str]) -> dict[str, str]:
    """Return the red cells of one group run."""
    return {cell: reason for cell, reason in results.items() if reason}
