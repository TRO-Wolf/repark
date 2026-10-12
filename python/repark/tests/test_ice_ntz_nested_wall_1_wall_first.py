"""Mixed-struct writes store Spark's rows with either operand first.

pins: ice-ntz-nested-wall-1/C-003
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from ice_ntz_nested_wall_1_helpers import open_session
from test_ice_ntz_nested_wall_1 import _explain_text, _mask_plan, error_line

RECORDED = json.loads(Path(__file__).with_name("ice_ntz_nested_wall_1_wall_first.json").read_text())
LOCALIZE = "__repark_localize_nested_ntz__"


def read_rows(spark: Any, table: str, leaf: str = "st") -> list[list[Any]]:
    """Return ``[id, leaf microseconds, n]`` rows sorted by id."""
    arrow = spark.sql(f"SELECT id, {leaf}.v AS v, {leaf}.n AS n FROM {table}").toArrow()
    ticks = arrow.column("v").cast(pa.int64(), safe=False).to_pylist()
    rows = [
        list(row)
        for row in zip(
            arrow.column("id").to_pylist(), ticks, arrow.column("n").to_pylist(), strict=True
        )
    ]
    rows.sort(key=lambda row: row[0])
    return rows


@pytest.mark.parametrize("zone", sorted(RECORDED["zones"]))
def test_either_operand_order_stores_sparks_rows(tmp_path: Path, zone: str) -> None:
    """Each shape stores Spark's recorded rows, wall operand first or instant operand first."""
    spark = open_session(zone, tmp_path)
    try:
        for statement in RECORDED["seed"]:
            spark.sql(statement)
        differing = []
        for cell in RECORDED["zones"][zone]:
            for statement in cell["pre"]:
                spark.sql(statement)
            spark.sql(cell["sql"])
            got = read_rows(spark, cell["table"])
            if got != cell["rows"]:
                differing.append((cell["id"], got, cell["rows"]))
        assert differing == []
    finally:
        spark.stop()


def run_step(spark: Any, zone: str, step: list[str]) -> str | None:
    """Run one recorded step and return the EXPLAIN text it asks for."""
    kind = step[0]
    if kind == "sql":
        frame = spark.sql(step[1])
        if step[1].startswith("EXPLAIN"):
            frame.toArrow()
        return None
    if kind == "view":
        spark.sql(step[1]).createOrReplaceTempView(step[2])
        return None
    if kind == "zone":
        spark.conf.set("spark.sql.session.timeZone", step[1] or zone)
        return None
    return _explain_text(spark, step[1])


def test_named_cells_equal_main(tmp_path: Path) -> None:
    """The named cells keep main's wall rows, main's error and main's EXPLAIN text.

    A row a cell lists in ``moved`` holds an instant and stores Spark's value.
    """
    main = RECORDED["main"]
    zone = main["zone"]
    spark = open_session(zone, tmp_path)
    try:
        for statement in RECORDED["seed"]:
            spark.sql(statement)
        for cell in main["named"]:
            shown = None
            raised = None
            try:
                for step in cell["steps"]:
                    shown = run_step(spark, zone, step) or shown
            except Exception as error:
                raised = [type(error).__name__, error_line(error)[:300]]
                spark.conf.set("spark.sql.session.timeZone", zone)
            assert raised == cell.get("error"), cell["id"]
            moved = cell.get("moved", [])
            expected = [row for row in cell["rows"] if row[0] not in moved]
            expected += [row for row in cell.get("spark_rows", []) if row[0] in moved]
            expected.sort(key=lambda row: row[0])
            assert read_rows(spark, cell["table"], cell["leaf"]) == expected, cell["id"]
            if "explain" in cell:
                assert shown is not None, cell["id"]
                assert LOCALIZE not in shown, cell["id"]
                assert _mask_plan(shown) == _mask_plan(cell["explain"]), cell["id"]
    finally:
        spark.stop()
