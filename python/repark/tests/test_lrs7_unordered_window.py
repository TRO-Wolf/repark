"""LRS-7 — a window with no ORDER BY frames the whole partition, as Spark documents.

``count(v).over(Window.partitionBy("k"))`` is ordinary PySpark and failed here with
``Internal error: ORDER BY column cannot be empty. This issue was likely caused by a bug in
DataFusion's code``. Spark documents two defaults — ordered windows frame ``RANGE … CURRENT ROW``,
unordered ones frame ``ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`` — and DataFusion
supplies only the first.

Every expected value below is Spark's own answer for the same frame, taken from a live PySpark
4.1.2 (design §7), not read back from repark.

Ledger: ``task/lrs-7-unordered-window-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import AnalysisException
from repark.spark import Window
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs7-unordered-window").getOrCreate()


def _frame():
    return _session().createDataFrame([(1, 10), (1, 20), (2, 30)], "k int, v int")


def _over(column, window):
    return _frame().select(column.over(window).alias("c")).toArrow().column("c").to_pylist()


@pytest.mark.parametrize(
    ("label", "column", "expected"),
    [
        ("count", F.count("v"), [2, 2, 1]),
        ("sum", F.sum("v"), [30, 30, 30]),
        ("max", F.max("v"), [20, 20, 30]),
        ("first", F.first("v"), [10, 10, 30]),
        ("last", F.last("v"), [20, 20, 30]),
    ],
)
def test_an_aggregate_over_an_unordered_window_frames_the_whole_partition(
    label: str, column, expected: list[int]
) -> None:
    """Each expected list is Spark's answer for the same query."""
    assert _over(column, Window.partitionBy("k")) == expected


def test_an_unordered_window_with_no_partition_is_the_whole_frame() -> None:
    """``Window.partitionBy()`` — no keys, no ordering. Spark gives the global total per row."""
    assert _over(F.sum("v"), Window.partitionBy()) == [60, 60, 60]


@pytest.mark.parametrize(
    "label",
    ["row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "lag", "lead", "nth_value"],
)
def test_a_function_that_needs_an_ordering_is_refused_on_an_unordered_window(label: str) -> None:
    """Spark refuses all of these, and so must repark — supplying a default frame to them would
    have answered where Spark raises.

    The split is read off the function's KIND, not a name list: Spark's ordering-requiring set is
    exactly the window UDFs, while ``first`` / ``last``, which Spark allows, arrive as aggregates.
    A name list would drift the first time a function is added.
    """
    columns = {
        "row_number": F.row_number(),
        "rank": F.rank(),
        "dense_rank": F.dense_rank(),
        "percent_rank": F.percent_rank(),
        "cume_dist": F.cume_dist(),
        "lag": F.lag("v"),
        "lead": F.lead("v"),
        "nth_value": F.nth_value("v", 1),
    }
    with pytest.raises(AnalysisException, match="requires window to be ordered"):
        _over(columns[label], Window.partitionBy("k"))


@pytest.mark.parametrize(
    ("label", "window", "expected"),
    [
        ("partitionBy + orderBy", Window.partitionBy("k").orderBy("v"), [1, 2, 1]),
        ("orderBy only", Window.orderBy("v"), [1, 2, 3]),
        (
            "explicit frame",
            Window.partitionBy("k").rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            ),
            [2, 2, 1],
        ),
    ],
)
def test_windows_that_already_worked_are_untouched(label: str, window, expected: list[int]) -> None:
    """The default only applies when there is no ordering AND no explicit frame. An ordered window
    keeps DataFusion's ``RANGE … CURRENT ROW`` default, which is already Spark's.
    """
    assert _over(F.count("v"), window) == expected


def test_the_default_frame_does_not_disturb_the_signed_count_cast() -> None:
    """``approx_count_distinct`` carries a CAST that ``over`` peels and re-applies; it must still
    come back signed when the frame is the one this unit supplies.
    """
    got = (
        _frame()
        .select(F.approx_count_distinct("v").over(Window.partitionBy("k")).alias("c"))
        .toArrow()
    )
    assert str(got.schema.field("c").type) == "int64"
    assert got.column("c").to_pylist() == [2, 2, 1]
