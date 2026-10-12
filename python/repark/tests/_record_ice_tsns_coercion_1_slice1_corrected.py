"""Record corrected-twin oracle values for the 220 Q-E cells on the baseline module.

Run with the BASELINE module (``/tmp/xcoerce0/.venv/bin/python``): for each
Q-E cell, (1) run the twin spelling with every untyped NULL wrapped in the
kernel — the form's own NULL through the twin role plus the door's own NULL
branch; (2) where that errors for a reason unrelated to the NULL, store the
same door's plain source column instead. Writes
``ice_tsns_coercion_1_slice1_corrected_twins.json`` mapping each cell to its
outcome plus which oracle it used.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _ice_tsns_coercion_1_doors as doors
from _ice_tsns_coercion_1_doors import SRC
from _ice_tsns_coercion_1_spellings import ZONES, kernel_name

from repark import functions

TESTS = Path(__file__).resolve().parent
OUT = TESTS / "ice_tsns_coercion_1_slice1_corrected_twins.json"
QE_DOORS = ("union_null_insert", "df_union_null_append", "df_functions_append")
QE_FORMS = ("coalesce", "array", "case", "if", "union")
QE_SOURCES = ("ns", "tzns_utc")
QE_TARGETS = ("ts_ns", "tz_ns")


def _kernel(source: str) -> str:
    """Return the kernel call wrapping one NULL for ``source``."""
    return f"{kernel_name(source != 'ns')}(NULL)"


def _run_corrected(cell: doors.Cell, door: str, kernel: str) -> None:
    """Run the corrected-twin write for one union door."""
    if door == "union_null_insert":
        cell.sql(
            f"INSERT INTO {cell.table} (id, v, k) {cell.select()} WHERE id > 1 "
            f"UNION ALL SELECT 1, {kernel}, 0"
        )
    else:
        absent = cell.spark.sql(f"SELECT 9 AS id, {kernel} AS v, 0 AS k")
        cell.spark.sql(cell.select()).unionByName(absent).writeTo(cell.table).append()


def _run_plain(cell: doors.Cell) -> None:
    """Store the plain source column through the functions-API frame."""
    value = functions.col(cell.source)
    cell.spark.table(SRC).select(
        functions.col("id"),
        value.alias("v"),
        functions.lit(0).alias("k"),
    ).writeTo(cell.table).append()


def _run_functions_corrected(cell: doors.Cell, kernel: str) -> None:
    """Run the corrected-twin write for the functions-API door."""
    value = functions.element_at(
        functions.array(functions.col(cell.source), functions.expr(kernel)), 1
    )
    cell.spark.table(SRC).select(
        functions.col("id"),
        value.alias("v"),
        functions.lit(0).alias("k"),
    ).writeTo(cell.table).append()


def _finish(spark: Any, warehouse: Path, table: str, result: dict[str, Any]) -> dict[str, Any]:
    """Read back one attempt's outcome and clean up its table."""
    try:
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


def _corrected_outcome(
    spark: Any,
    warehouse: Path,
    target: str,
    source: str,
    form: str,
    door: str,
) -> dict[str, Any]:
    """Run one Q-E cell's oracle and return the outcome plus its kind."""
    kernel = _kernel(source)
    table = doors.fresh_table()
    result: dict[str, Any] = {}
    try:
        doors.create_target(spark, table, target, source, door)
        if door == "df_functions_append":
            twin_cell = doors.Cell(spark, table, source, form, "untyped_null", "twin")
            try:
                _run_functions_corrected(twin_cell, kernel)
            except Exception:
                doors.cleanup(spark, warehouse, table)
                table = doors.fresh_table()
                doors.create_target(spark, table, target, source, door)
                plain = doors.Cell(spark, table, source, form, "untyped_null", "plain")
                _run_plain(plain)
                result["oracle"] = 2
            else:
                result["oracle"] = 1
        else:
            twin_cell = doors.Cell(spark, table, source, form, "untyped_null", "twin")
            _run_corrected(twin_cell, door, kernel)
            result["oracle"] = 1
    except Exception as error:
        result["error"] = doors.error_text(error)
        result["error_type"] = type(error).__name__
        return _finish(spark, warehouse, table, result)
    return _finish(spark, warehouse, table, result)


def main() -> None:
    """Record every Q-E oracle cell."""
    import tempfile

    outcomes: dict[str, dict[str, Any]] = {}
    kinds: dict[int, int] = {1: 0, 2: 0}
    with tempfile.TemporaryDirectory() as scratch:
        for index, zone in enumerate(ZONES):
            warehouse = Path(scratch) / f"qe-{index}"
            warehouse.mkdir()
            spark = doors.open_session(zone, warehouse)
            try:
                for door in QE_DOORS:
                    forms = ("array",) if door == "df_functions_append" else QE_FORMS
                    for target in QE_TARGETS:
                        for source in QE_SOURCES:
                            for form in forms:
                                cell = f"{zone}|{target}|{source}|{form}|{door}"
                                outcome = _corrected_outcome(
                                    spark, warehouse, target, source, form, door
                                )
                                outcomes[cell] = outcome
                                if "error" not in outcome and outcome.get("oracle") in kinds:
                                    kinds[outcome["oracle"]] += 1
            finally:
                spark.stop()
    OUT.write_text(json.dumps(outcomes, indent=1, sort_keys=True) + "\n")
    print(f"cells={len(outcomes)} oracle1={kinds[1]} oracle2={kinds[2]}")


if __name__ == "__main__":
    main()
