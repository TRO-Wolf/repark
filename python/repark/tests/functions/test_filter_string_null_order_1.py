"""FILTER-STRING-NULL-ORDER-1: a string ``filter`` ``ORDER BY`` places NULLs as Spark does.

Spark puts NULLs first for ``ASC`` and last for ``DESC`` unless the text says ``NULLS FIRST`` or
``NULLS LAST``. The cells and their answers come from the unit's step 0 against PySpark 4.1.2
(``filter_string_null_order_1_spark_oracle.json``). The native-door cells hold the base build's
own answer, because that door keeps its own dialect.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession, Window
from repark.spark import functions

_ORACLE_PATH = Path(__file__).with_name("filter_string_null_order_1_spark_oracle.json")
_ROWS = (
    "(1, 10, 'b', CAST(1.5 AS DOUBLE)), "
    "(2, CAST(NULL AS INT), 'e', CAST(2.5 AS DOUBLE)), "
    "(3, 30, CAST(NULL AS STRING), CAST(3.5 AS DOUBLE)), "
    "(4, 20, 'a', CAST(NULL AS DOUBLE)), "
    "(5, 40, 'd', CAST(0.5 AS DOUBLE))"
)
_COLUMNS = "t(id, ki, ks, kd)"
_SOURCE = f"SELECT * FROM VALUES {_ROWS} AS {_COLUMNS}"
_NATIVE_SOURCE = f"WITH w AS (SELECT * FROM (VALUES {_ROWS}) AS {_COLUMNS}) "
_PY_ROWS = [
    (1, 10, "b", 1.5),
    (2, None, "e", 2.5),
    (3, 30, None, 3.5),
    (4, 20, "a", None),
    (5, 40, "d", 0.5),
]
_SCHEMA = "id INT, ki INT, ks STRING, kd DOUBLE"


def _cells() -> list[dict[str, Any]]:
    """Return every recorded cell."""
    return json.loads(_ORACLE_PATH.read_text(encoding="utf-8"))["cells"]


def _plain(value: Any) -> Any:
    """Encode one answer value the way the step-0 probe recorded it."""
    if isinstance(value, float) and value != value:
        return "NaN"
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame as plain lists."""
    return [[_plain(cell) for cell in row] for row in frame.collect()]


def _ids(frame: Any) -> list[list[Any]]:
    """Collect a frame's sorted ``id`` column."""
    return _rows(frame.select("id").orderBy("id"))


def _case_sensitive(spark: ReparkSession, predicate: str) -> list[list[Any]]:
    """Answer one predicate on a frame built while ``spark.sql.caseSensitive`` is true."""
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        return _ids(spark.table("w").filter(predicate))
    finally:
        spark.conf.set("spark.sql.caseSensitive", "false")


def _predicate_doors(spark: ReparkSession) -> dict[str, Callable[[str], list[list[Any]]]]:
    """Map each predicate door of step 0 to the call that answers one predicate."""
    w = spark.table("w")
    from_sql = spark.sql(_SOURCE)
    created = spark.createDataFrame(_PY_ROWS, _SCHEMA)
    derived = w.withColumn("z", functions.lit(1)).select("id", "ki", "ks", "kd", "z")
    native = repark.sql(f"{_NATIVE_SOURCE}SELECT * FROM w")
    return {
        "filter_str_table": lambda p: _ids(w.filter(p)),
        "where_str_table": lambda p: _ids(w.where(p)),
        "filter_str_sql_frame": lambda p: _ids(from_sql.filter(p)),
        "filter_str_created": lambda p: _ids(created.filter(p)),
        "filter_str_derived": lambda p: _ids(derived.filter(p)),
        "filter_str_case_sensitive": lambda p: _case_sensitive(spark, p),
        "filter_fexpr": lambda p: _ids(w.filter(functions.expr(p))),
        "sparksql_where": lambda p: _rows(spark.sql(f"SELECT id FROM w WHERE {p} ORDER BY id")),
        "selectexpr": lambda p: _rows(w.selectExpr("id", f"({p}) AS r").orderBy("id")),
        "native_filter_str": lambda p: _ids(native.filter(p)),
        "native_sql_where": lambda p: _rows(
            repark.sql(f"{_NATIVE_SOURCE}SELECT id FROM w WHERE {p} ORDER BY id")
        ),
    }


def _join_doors(spark: ReparkSession) -> dict[str, Callable[[str], list[list[Any]]]]:
    """Map each joined-frame door to the call that filters on ``<left key> = <scalar subquery>``."""
    w = spark.table("w")
    right = spark.sql(f"SELECT * FROM VALUES {_ROWS} AS r(rid, rki, rks, rkd)")
    joined = w.join(right, w["id"] == right["rid"])

    def column_form(text: str) -> list[list[Any]]:
        key, subquery = text.split(" = ", 1)
        return _ids(joined.filter(w[key] == functions.expr(subquery)))

    return {
        "join_filter_column": column_form,
        "join_filter_str": lambda text: _ids(joined.filter(text)),
    }


def _control_doors(spark: ReparkSession) -> dict[str, Callable[[], list[list[Any]]]]:
    """Map each control cell to its call: the sorts, the typed Window and each door's window."""
    w = spark.table("w")
    col = functions.col
    native = _NATIVE_SOURCE
    return {
        "sort_int_desc": lambda: _rows(w.orderBy(col("ki").desc()).select("id")),
        "sort_int_asc": lambda: _rows(w.sort("ki").select("id")),
        "sort_string_desc": lambda: _rows(w.orderBy(functions.desc("ks")).select("id")),
        "sort_double_asc": lambda: _rows(w.orderBy(col("kd").asc()).select("id")),
        "typed_window_double_desc": lambda: _rows(
            w.select(
                "id", functions.row_number().over(Window.orderBy(col("kd").desc())).alias("r")
            ).orderBy("id")
        ),
        "typed_window_int_asc": lambda: _rows(
            w.select("id", functions.row_number().over(Window.orderBy("ki")).alias("r")).orderBy(
                "id"
            )
        ),
        "sparksql_window_int": lambda: _rows(
            spark.sql("SELECT id, row_number() OVER (ORDER BY ki) AS r FROM w ORDER BY id")
        ),
        "sparksql_order_by_string_desc": lambda: _rows(
            spark.sql("SELECT id FROM w ORDER BY ks DESC")
        ),
        "fexpr_window_string_desc": lambda: _rows(
            w.select(
                "id", functions.expr("row_number() OVER (ORDER BY ks DESC)").alias("r")
            ).orderBy("id")
        ),
        "selectexpr_window_double": lambda: _rows(
            w.selectExpr("id", "row_number() OVER (ORDER BY kd) AS r").orderBy("id")
        ),
        "native_window_int": lambda: _rows(
            repark.sql(
                f"{native}SELECT id, row_number() OVER (ORDER BY ki) AS r FROM w ORDER BY id"
            )
        ),
        "native_order_by_int": lambda: _rows(repark.sql(f"{native}SELECT id FROM w ORDER BY ki")),
    }


def _answer(spark: ReparkSession, cell: dict[str, Any]) -> list[list[Any]]:
    """Answer one recorded cell on its door."""
    if cell["kind"] == "control":
        return _control_doors(spark)[cell["cell"]]()
    if cell["kind"] == "join":
        return _join_doors(spark)[cell["door"]](cell["text"])
    return _predicate_doors(spark)[cell["door"]](cell["text"])


_GROUPS = sorted({(cell["group"], cell["kind"], cell["door"]) for cell in _cells()})


@pytest.mark.parametrize(
    ("group", "kind", "door"),
    _GROUPS,
    ids=[f"{group}-{kind}-{door}" for group, kind, door in _GROUPS],
)
def test_every_recorded_cell_answers_as_recorded(
    spark: ReparkSession, group: str, kind: str, door: str
) -> None:
    """Each cell of the group answers its recorded rows: Spark's, or the base's on native."""
    spark.sql(_SOURCE).createOrReplaceTempView("w")
    cells = [
        cell
        for cell in _cells()
        if (cell["group"], cell["kind"], cell["door"]) == (group, kind, door)
    ]
    assert cells
    wrong = []
    for cell in cells:
        answer = _answer(spark, cell)
        if answer != cell["expected"]:
            wrong.append((cell["cell"], answer, cell["expected"]))
    assert wrong == [], f"{len(wrong)} of {len(cells)} cells differ; first: {wrong[:3]}"


def test_the_fixed_group_covers_every_wrong_step_zero_cell() -> None:
    """The fixed group holds the 126 cells step 0 measured wrong on the base, on eight doors."""
    fixed = [cell for cell in _cells() if cell["group"] == "fixed"]
    assert len(fixed) == 126
    assert {cell["door"] for cell in fixed} == {
        "filter_str_table",
        "where_str_table",
        "filter_str_sql_frame",
        "filter_str_created",
        "filter_str_derived",
        "filter_str_case_sensitive",
        "join_filter_column",
        "join_filter_str",
    }
    assert {cell["source"] for cell in _cells() if cell["group"] == "native"} == {"repark-base"}
