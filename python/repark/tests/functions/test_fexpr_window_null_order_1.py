"""FEXPR-WINDOW-NULL-ORDER-1: an `F.expr` fragment's `ORDER BY` places NULLs as Spark does.

Spark puts NULLs first for ``ASC`` and last for ``DESC`` unless the text says ``NULLS FIRST`` or
``NULLS LAST``. The cells and their answers come from the unit's step 0 against PySpark 4.1.2
(``fexpr_window_null_order_1_spark_oracle.json``); the native-door cells hold main's own answer,
because that door keeps its own dialect.
"""

from __future__ import annotations

import json
import struct
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession, Window
from repark.spark import functions

_ORACLE_PATH = Path(__file__).with_name("fexpr_window_null_order_1_spark_oracle.json")
_ROWS = (
    "(1, 10, 'b', CAST(1.5 AS DOUBLE), 1), "
    "(2, CAST(NULL AS INT), 'e', CAST(2.5 AS DOUBLE), 1), "
    "(3, 30, CAST(NULL AS STRING), CAST(3.5 AS DOUBLE), 2), "
    "(4, 20, 'a', CAST(NULL AS DOUBLE), 2), "
    "(5, 40, 'd', CAST(0.5 AS DOUBLE), 1)"
)
_COLUMNS = "t(id, ki, ks, kd, g)"
_NATIVE_SOURCE = f"WITH w AS (SELECT * FROM (VALUES {_ROWS}) AS {_COLUMNS}) "


def _cells() -> list[dict[str, Any]]:
    """Return every recorded cell."""
    return json.loads(_ORACLE_PATH.read_text(encoding="utf-8"))["cells"]


def _plain(value: Any) -> Any:
    """Encode one answer value the way the step-0 probe recorded it."""
    if isinstance(value, float):
        if value != value or value == 0.0:
            return "f64:" + struct.pack(">d", value).hex()
        return value
    if isinstance(value, Decimal):
        return "dec:" + str(value)
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame as plain lists."""
    return [[_plain(cell) for cell in row] for row in frame.collect()]


def _frame(spark: ReparkSession) -> Any:
    """Register the step-0 rows as the temp view ``w`` and return it."""
    spark.sql(f"SELECT * FROM VALUES {_ROWS} AS {_COLUMNS}").createOrReplaceTempView("w")
    return spark.table("w")


def _door_actions(spark: ReparkSession) -> dict[str, Callable[[dict[str, Any]], list[list[Any]]]]:
    """Map each step-0 door to the call that answers one cell on it."""
    w = _frame(spark)
    return {
        "fexpr": lambda cell: _fragment(w, cell),
        "fexpr_withcolumn": lambda cell: _rows(
            w.withColumn("r", functions.expr(cell["text"])).select("id", "r").orderBy("id")
        ),
        "selectexpr": lambda cell: _select_expr(w, cell),
        "sparksql": lambda cell: _spark_sql(spark, cell),
        "native": lambda cell: _native(cell),
        "orderby_fexpr": lambda cell: _rows(w.orderBy(functions.expr(cell["text"])).select("id")),
        "sort_string": lambda cell: _rows(w.sort(functions.expr(cell["text"])).select("id")),
        "orderby_fexpr_method": lambda cell: _rows(
            w.orderBy(_sort_key(functions.expr, cell)).select("id")
        ),
        "sort_col": lambda cell: _rows(w.sort(_sort_key(functions.col, cell)).select("id")),
        "typed_window": lambda cell: _rows(
            w.select(
                "id",
                functions.row_number()
                .over(Window.orderBy(_sort_key(functions.col, cell)))
                .alias("r"),
            ).orderBy("id")
        ),
        "fexpr_agg": lambda cell: _rows(w.agg(functions.expr(cell["text"]).alias("r"))),
        "fexpr_groupby": lambda cell: _rows(
            w.groupBy("g").agg(functions.expr(cell["text"]).alias("r")).orderBy("g")
        ),
    }


def _sort_key(build: Callable[[str], Any], cell: dict[str, Any]) -> Any:
    """Build the typed sort key a ``T|typed|<key>|<method>`` cell names."""
    return getattr(build(cell["cell"].split("|")[2]), cell["text"])()


def _fragment(w: Any, cell: dict[str, Any]) -> list[list[Any]]:
    """Answer an ``F.expr`` cell: a window column, the dedup filter, or a scalar."""
    family = cell["cell"].split("|")[0]
    expression = functions.expr(cell["text"])
    if family == "D":
        return sorted(_rows(w.withColumn("r", expression).filter("r = 1").select("id")))
    if family == "X":
        return _rows(w.limit(1).select(expression))
    return _rows(w.select("id", expression.alias("r")).orderBy("id"))


def _select_expr(w: Any, cell: dict[str, Any]) -> list[list[Any]]:
    """Answer a ``selectExpr`` cell."""
    if cell["cell"].startswith("A|"):
        return _rows(w.selectExpr(f"{cell['text']} AS r"))
    return _rows(w.selectExpr("id", f"{cell['text']} AS r").orderBy("id"))


def _spark_sql(spark: ReparkSession, cell: dict[str, Any]) -> list[list[Any]]:
    """Answer a ``spark.sql`` cell."""
    if cell["cell"].startswith("A|"):
        return _rows(spark.sql(f"SELECT g, {cell['text']} AS r FROM w GROUP BY g ORDER BY g"))
    if cell["cell"].startswith("X|"):
        return _rows(spark.sql(f"SELECT {cell['text']}"))
    return _rows(spark.sql(f"SELECT id, {cell['text']} AS r FROM w ORDER BY id"))


def _native(cell: dict[str, Any]) -> list[list[Any]]:
    """Answer a native ``repark.sql()`` cell."""
    if cell["cell"].startswith("A|"):
        query = f"SELECT g, {cell['text']} AS r FROM w GROUP BY g ORDER BY g"
    else:
        query = f"SELECT id, {cell['text']} AS r FROM w ORDER BY id"
    return _rows(repark.sql(_NATIVE_SOURCE + query))


_GROUPS = sorted({(cell["group"], cell["door"]) for cell in _cells()})


@pytest.mark.parametrize(
    ("group", "door"), _GROUPS, ids=[f"{group}-{door}" for group, door in _GROUPS]
)
def test_every_recorded_cell_answers_as_recorded(
    spark: ReparkSession, group: str, door: str
) -> None:
    """Each cell of the group answers its recorded rows: Spark's, or main's on the native door."""
    action = _door_actions(spark)[door]
    cells = [cell for cell in _cells() if cell["group"] == group and cell["door"] == door]
    assert cells
    wrong = []
    for cell in cells:
        answer = action(cell)
        if answer != cell["expected"]:
            wrong.append((cell["cell"], cell["text"], answer, cell["expected"]))
    assert wrong == [], f"{len(wrong)} of {len(cells)} cells differ; first: {wrong[:3]}"


def test_the_fixed_groups_cover_every_wrong_step_zero_cell() -> None:
    """The fixed groups hold the 358 cells step 0 measured wrong on main, on both fragment doors."""
    fixed = [cell for cell in _cells() if cell["group"] == "fixed"]
    assert len(fixed) == 358
    assert {cell["door"] for cell in fixed} == {"fexpr", "fexpr_withcolumn"}
    assert {cell["cell"].split("|")[0] for cell in fixed} == {"W", "D", "X"}
