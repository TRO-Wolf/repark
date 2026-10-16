"""The parking repros of R-017's seven verifier rounds as named cells.

Each cell is one statement with rule markers. ``store`` cells put the value in a
nanosecond column of the source's kind; ``update`` cells seed the table first;
``read`` cells compare the Arrow answer. A cell with a ``plain`` text must also
equal that text: the source rows are not NULL, so the site is the column.

pins: ice-tsns-type-planner-1/C-006
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import _ice_tsns_type_planner_1_rule as rule

ZONES = ("UTC", "America/New_York")
COLUMNS = ("ns", "tzns")
INSERT = "INSERT INTO {T} (id, v, k) SELECT id, %s, 0 FROM {S}"
_CELLS: dict[str, Any] = json.loads(
    (Path(__file__).resolve().parent / "ice_tsns_type_planner_1_repros.json").read_text(
        encoding="utf-8"
    )
)

STORE: list[list[Any]] = _CELLS["store"]

STATEMENTS: list[list[Any]] = _CELLS["statements"]

UPDATES: list[list[Any]] = _CELLS["updates"]

READS: list[list[Any]] = _CELLS["reads"]
ANSWER_ONLY = frozenset(_CELLS["answer_only"])


def _target(column: str) -> str:
    return rule.NS_NAMES[column]


def _seeded(spark: Any, scratch: Path, column: str, statement: str) -> dict[str, Any]:
    table = doors.fresh_table()
    result: dict[str, Any] = {}
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, v {_target(column)}, k INT, c {_target(column)}) "
            "USING iceberg TBLPROPERTIES ('format-version' = '3')"
        )
        spark.sql(
            f"INSERT INTO {table} (id, k, c) SELECT id, 0, {column} FROM {rule.SRC}"
        ).toArrow()
        try:
            spark.sql(statement.replace("{T}", table)).toArrow()
        except Exception as error:
            result["error"] = doors.error_text(error)
            result["error_type"] = type(error).__name__
        pairs, parquet_type = doors.stored(scratch, table)
        result["stored"] = pairs
        if parquet_type:
            result["parquet_type"] = parquet_type
    finally:
        doors.cleanup(spark, scratch, table)
    return result


def _judge(
    cell: str,
    measure: Callable[[str], dict[str, Any]],
    text: str,
    column: str,
    zoned: bool,
    plain: str,
    ordered: bool = True,
) -> str:
    statement = measure(rule.statement_of(text, column))
    rule.note(cell, statement)
    reason = ""
    if rule.has_markers(text):
        twin = measure(rule.twin_of(text, column, zoned or column != "ns"))
        reason = rule.differ(statement, twin, ordered)
    if not reason and plain:
        reason = rule.differ(statement, measure(rule.statement_of(plain, column)), ordered)
    return reason


def _answers(cell: str, measure: Callable[[str], dict[str, Any]], text: str, column: str) -> str:
    outcome = measure(rule.statement_of(text, column))
    rule.note(cell, outcome)
    return "" if rule.answered(outcome) else repr(outcome)[:260]


def _run(zone: str) -> Callable[[Path], dict[str, str]]:
    def run(scratch: Path) -> dict[str, str]:
        spark = rule.open_session(zone, scratch)
        out: dict[str, str] = {}
        try:
            for column in COLUMNS:
                target = _target(column)

                def stored(statement: str, target: str = target) -> dict[str, Any]:
                    return rule.store(spark, scratch, target, statement)

                def updated(statement: str, column: str = column) -> dict[str, Any]:
                    return _seeded(spark, scratch, column, statement)

                def read(statement: str) -> dict[str, Any]:
                    return rule.capture(rule.run_text(spark, statement))

                for name, text, zoned, plain in STORE:
                    cell = f"repro|{name}|{column}|{zone}"
                    out[cell] = _judge(
                        cell, stored, INSERT % text, column, zoned, plain and INSERT % plain
                    )
                for name, text, zoned, plain in STATEMENTS:
                    cell = f"repro|{name}|{column}|{zone}"
                    out[cell] = _judge(cell, stored, text, column, zoned, plain)
                for name, text, zoned, plain in UPDATES:
                    cell = f"repro|{name}|{column}|{zone}"
                    out[cell] = _judge(cell, updated, text, column, zoned, plain)
                for name, text, zoned, plain in READS:
                    cell = f"repro|{name}|{column}|{zone}"
                    if name in ANSWER_ONLY:
                        out[cell] = _answers(cell, read, text, column)
                    else:
                        out[cell] = _judge(cell, read, text, column, zoned, plain, ordered=False)
        finally:
            spark.stop()
        return out

    return run


def repro_groups() -> list[rule.Group]:
    """Return the named parking repros, one group per zone."""
    count = (len(STORE) + len(STATEMENTS) + len(UPDATES) + len(READS)) * len(COLUMNS)
    return [
        rule.Group(name=f"repro|{zone}", family="repro", zone=zone, run=_run(zone), cells=count)
        for zone in ZONES
    ]
