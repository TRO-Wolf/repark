"""LRS-1 — the facade refuses higher-order columns where the engine would leak an internal error.

Every shape here **works in Spark** (measured against a live PySpark 4.1.2 — design §7), so each
refusal says so and names the workaround. What is pinned is that the refusal fires, that it is
catchable and explanatory, that the named workaround actually works, and that ordinary columns are
not caught by it.

Ledger: ``task/lrs-1-higher-order-refusals-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import UnsupportedOperationException
from repark.spark import Window
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs1-refusals").getOrCreate()


def _frame():
    return _session().createDataFrame([([1, 2, 3], 1), ([9], 2)], "a array<int>, k int")


def _hof():
    return F.exists("a", lambda x: x > 2)


# ---- nesting, both argument positions --------------------------------------------------------


def test_a_higher_order_call_in_a_value_argument_is_refused() -> None:
    """Refused before the engine leaks the internal-error class the guard abolishes:
    ``AnalysisException: unresolved LambdaVariable x_0``.
    """
    with pytest.raises(UnsupportedOperationException, match="value argument"):
        _frame().select(F.exists(F.array(F.exists("a", lambda y: y > 4)), lambda x: x)).toArrow()


def test_a_higher_order_call_in_a_lambda_body_is_still_refused() -> None:
    """Both argument positions are one guard; a change that fixes one must not drop the other."""
    with pytest.raises(UnsupportedOperationException, match="lambda"):
        _frame().select(F.exists("a", lambda x: F.exists("a", lambda y: y > 4))).toArrow()


# ---- the SQL-text and window paths ------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "build"),
    [
        ("orderBy", lambda column: Window.orderBy(column)),
        ("partitionBy", lambda column: Window.partitionBy(column)),
    ],
)
def test_a_higher_order_window_key_is_refused(label: str, build) -> None:
    """Both window positions refused; ungated they fail internally (a ``SanityCheckPlan`` dump
    for ``orderBy``, a "likely caused by a bug" internal error for ``partitionBy``).
    """
    with pytest.raises(UnsupportedOperationException, match="higher-order function column"):
        _frame().select(F.count("k").over(build(_hof()))).toArrow()


@pytest.mark.parametrize("operation", ["cube", "rollup"])
def test_a_higher_order_grouping_set_key_is_refused(operation: str) -> None:
    """These lower the column to SQL TEXT the facade's dialect cannot read back, surfacing a raw
    ``ParserError`` quoting generated SQL. Refused before the text is built.
    """
    with pytest.raises(UnsupportedOperationException, match="higher-order function column"):
        getattr(_frame(), operation)(_hof()).count().toArrow()


def test_the_workaround_the_message_names_actually_works() -> None:
    """A refusal that names a workaround is only honest if the workaround works. Project first,
    then group — and the answer matches Spark's (True/2, NULL/2 for this frame).
    """
    got = _frame().select(_hof().alias("e")).cube("e").count().toArrow().to_pylist()
    assert sorted((row["e"] is None, row["count"]) for row in got) == [(False, 2), (True, 2)]


# ---- the refusals must not be wider than the defect --------------------------------------------


def test_ordinary_columns_are_not_caught_by_any_of_these_refusals() -> None:
    """The guard asks whether the expression carries a higher-order function; anything else must
    pass untouched — an over-firing refusal is a worse regression than the internal error.
    """
    frame = _frame()
    assert frame.cube("k").count().toArrow().num_rows > 0
    assert frame.rollup("k").count().toArrow().num_rows > 0
    windowed = frame.select(F.count("k").over(Window.partitionBy("k").orderBy("k")).alias("c"))
    assert windowed.toArrow().num_rows == 2


def test_a_higher_order_column_still_works_everywhere_it_worked_before() -> None:
    """Untouched paths keep working: select, alias, groupBy, and a lambda body that captures an
    outer column.
    """
    frame = _frame()
    assert frame.select(_hof().alias("e")).toArrow().column("e").to_pylist() == [True, True]
    assert frame.groupBy(_hof()).count().toArrow().num_rows == 1
    captured = _session().sql("SELECT array(1, 2, 3) AS a, 2 AS threshold")
    got = captured.select(F.exists("a", lambda x: x > F.col("threshold")).alias("e"))
    assert got.toArrow().column("e").to_pylist() == [True]
