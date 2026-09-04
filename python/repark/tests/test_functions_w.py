"""FN-W — window functions (value + Arrow type).

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow
path (``to_arrow()``): value AND type. ``lag`` / ``lead`` / ``nth_value`` keep
the source column type (no IntegerType cast). ``percent_rank`` / ``cume_dist``
are Float64. ``ignoreNulls`` is an honest cut (not a parameter).

NULL-source ``lag``/``lead`` is a SEMANTIC-HAZARD pin: the neighboring row's
NULL is returned, not skipped.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession, Window
from repark.errors import IllegalArgumentException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.window import WindowSpec


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-w").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _ordered_window() -> WindowSpec:
    return Window.partitionBy("g").orderBy("id")


# lag / lead


def test_lag_default_offset_first_row_is_null(spark: ReparkSession) -> None:
    assert callable(F.lag)
    frame = spark.createDataFrame(
        [("A", 1, 10), ("A", 2, 20), ("A", 3, 30), ("B", 1, 100)],
        ["g", "id", "v"],
    )
    table = _table(
        frame.select("g", "id", "v", F.lag("v").over(_ordered_window()).alias("lag_v")).orderBy(
            "g", "id"
        )
    )
    assert table.column("lag_v").to_pylist() == [None, 10, 20, None]
    assert table.schema.field("lag_v").type == table.schema.field("v").type
    assert not pa.types.is_int32(table.schema.field("lag_v").type) or pa.types.is_int32(
        table.schema.field("v").type
    )


def test_lead_default_offset_last_row_is_null(spark: ReparkSession) -> None:
    assert callable(F.lead)
    frame = spark.createDataFrame(
        [("A", 1, 10), ("A", 2, 20), ("A", 3, 30), ("B", 1, 100)],
        ["g", "id", "v"],
    )
    table = _table(
        frame.select("g", "id", "v", F.lead("v").over(_ordered_window()).alias("lead_v")).orderBy(
            "g", "id"
        )
    )
    assert table.column("lead_v").to_pylist() == [20, 30, None, None]
    assert table.schema.field("lead_v").type == table.schema.field("v").type


def test_lag_and_lead_explicit_default(spark: ReparkSession) -> None:
    frame = spark.createDataFrame(
        [("A", 1, 10), ("A", 2, 20), ("A", 3, 30)],
        ["g", "id", "v"],
    )
    window = _ordered_window()
    table = _table(
        frame.select(
            "id",
            F.lag("v", 1, 0).over(window).alias("lag_v"),
            F.lead("v", 1, 0).over(window).alias("lead_v"),
        ).orderBy("id")
    )
    assert table.column("lag_v").to_pylist() == [0, 10, 20]
    assert table.column("lead_v").to_pylist() == [20, 30, 0]
    assert table.schema.field("lag_v").type == table.schema.field("lead_v").type


def test_lag_lead_null_source_row_is_returned(spark: ReparkSession) -> None:
    """SEMANTIC-HAZARD: a NULL source row is lagged/led as NULL, not skipped."""
    frame = spark.createDataFrame(
        [("A", 1, 10), ("A", 2, None), ("A", 3, 30)],
        ["g", "id", "v"],
    )
    window = _ordered_window()
    table = _table(
        frame.select(
            "id",
            F.lag("v").over(window).alias("lag_v"),
            F.lead("v").over(window).alias("lead_v"),
        ).orderBy("id")
    )
    assert table.column("lag_v").to_pylist() == [None, 10, None]
    assert table.column("lead_v").to_pylist() == [None, 30, None]


def test_lag_preserves_string_input_type(spark: ReparkSession) -> None:
    frame = spark.createDataFrame(
        [("A", 1, "x"), ("A", 2, "y"), ("A", 3, "z")],
        ["g", "id", "s"],
    )
    table = _table(
        frame.select("id", "s", F.lag("s").over(_ordered_window()).alias("lag_s")).orderBy("id")
    )
    assert table.column("lag_s").to_pylist() == [None, "x", "y"]
    assert table.schema.field("lag_s").type == table.schema.field("s").type
    assert pa.types.is_string(table.schema.field("lag_s").type) or pa.types.is_large_string(
        table.schema.field("lag_s").type
    )


# nth_value


def test_nth_value_is_one_based(spark: ReparkSession) -> None:
    assert callable(F.nth_value)
    frame = spark.createDataFrame(
        [("A", 1, 10), ("A", 2, 20), ("A", 3, 30)],
        ["g", "id", "v"],
    )
    window = _ordered_window()
    table = _table(
        frame.select(
            "id",
            "v",
            F.nth_value("v", 1).over(window).alias("n1"),
            F.nth_value("v", 2).over(window).alias("n2"),
            F.nth_value("v", 3).over(window).alias("n3"),
        ).orderBy("id")
    )
    assert table.column("n1").to_pylist() == [10, 10, 10]
    assert table.column("n2").to_pylist() == [None, 20, 20]
    assert table.column("n3").to_pylist() == [None, None, 30]
    assert table.schema.field("n1").type == table.schema.field("v").type


def test_nth_value_rejects_non_positive_offset() -> None:
    with pytest.raises(IllegalArgumentException, match="positive integer"):
        F.nth_value("v", 0)


# percent_rank / cume_dist


def test_percent_rank_is_float64(spark: ReparkSession) -> None:
    assert callable(F.percent_rank)
    frame = spark.createDataFrame(
        [("A", 1), ("A", 2), ("A", 3)],
        ["g", "id"],
    )
    table = _table(
        frame.select("id", F.percent_rank().over(_ordered_window()).alias("pr")).orderBy("id")
    )
    values = [float(value) for value in table.column("pr").to_pylist()]
    assert values == pytest.approx([0.0, 0.5, 1.0])
    assert pa.types.is_floating(table.schema.field("pr").type)
    assert pa.types.is_float64(table.schema.field("pr").type)


def test_cume_dist_is_float64(spark: ReparkSession) -> None:
    assert callable(F.cume_dist)
    frame = spark.createDataFrame(
        [("A", 1), ("A", 2), ("A", 3)],
        ["g", "id"],
    )
    table = _table(
        frame.select("id", F.cume_dist().over(_ordered_window()).alias("cd")).orderBy("id")
    )
    values = [float(value) for value in table.column("cd").to_pylist()]
    assert values == pytest.approx([1.0 / 3.0, 2.0 / 3.0, 1.0])
    assert pa.types.is_floating(table.schema.field("cd").type)
    assert pa.types.is_float64(table.schema.field("cd").type)


# Honest cuts


def test_ignore_nulls_is_not_a_parameter() -> None:
    with pytest.raises(TypeError):
        F.lag("v", ignoreNulls=True)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        F.lead("v", ignoreNulls=True)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        F.nth_value("v", 1, ignoreNulls=True)  # type: ignore[call-arg]


def test_last_ignorenulls_window_skips_trailing_null(spark: ReparkSession) -> None:
    """FN-LAST-1: last(ignorenulls) window. pins: fn-fix-1-registry-rows/C-003"""
    frame = spark.createDataFrame(
        [("a", 1), ("a", 2), ("a", 3), ("a", None), ("b", 4), ("b", 6)],
        ["k", "v"],
    )
    window = (
        Window.partitionBy("k")
        .orderBy(F.col("v").asc_nulls_last())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    table = _table(
        frame.select(
            "k",
            F.first("v").over(window).alias("first_w"),
            F.last("v").over(window).alias("last_w"),
            F.last("v", True).over(window).alias("last_ign_w"),
        ).distinct()
    )
    answers = sorted(table.to_pylist(), key=lambda row: row["k"])
    assert [(row["k"], row["first_w"], row["last_w"]) for row in answers] == [
        ("a", 1, None),
        ("b", 4, 6),
    ]
    assert [row["last_ign_w"] for row in answers] == [3, 6]
