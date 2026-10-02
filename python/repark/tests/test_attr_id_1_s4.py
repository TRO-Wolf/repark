from __future__ import annotations

from typing import Any

import pytest

from repark import ReparkSession, _native
from repark.errors import AnalysisException
from repark.spark.column import Column
from repark.spark.dataframe import DataFrame


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-s4").getOrCreate()


@pytest.fixture(params=[False, True], ids=["insensitive", "sensitive"])
def ruled_spark(spark: ReparkSession, request: Any) -> ReparkSession:
    spark.conf.set("spark.sql.caseSensitive", "true" if request.param else "false")
    return spark


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 2), (2, 1), (3, 3)], ["x", "y"])


def test_s4_origin_slots_deleted() -> None:
    """S4 deletes the origin-encoding slots; ids and qualifiers carry identity."""
    assert "_origin_plan_id" not in Column.__slots__
    assert "_origin_field" not in Column.__slots__
    assert "_origin_map" not in DataFrame.__slots__
    assert "_origin_not_emitted" not in DataFrame.__slots__
    assert "_attr_id" in Column.__slots__
    assert "_qualifiers" in Column.__slots__
    assert "_unemitted_attr_ids" in DataFrame.__slots__


def test_s4_join_token_has_no_qcol(ruled_spark: ReparkSession) -> None:
    """Join tokens carry attribute ids, never the deleted QCOL shape."""
    frame = _frame(ruled_spark)
    token = frame.alias("l").x.join_sql_part()
    assert token.startswith("__REPARK_ATTR_")
    assert "__REPARK_QCOL_" not in token


def test_s4_reversed_qualified_join_sides_exact(ruled_spark: ReparkSession) -> None:
    """A condition listing the right parent first still sides each token exactly."""
    frame = _frame(ruled_spark)
    other = frame.alias("r")
    joined = frame.join(other, other.x == frame.x)
    assert sorted(tuple(row) for row in joined.collect()) == [
        (1, 2, 1, 2),
        (2, 1, 2, 1),
        (3, 3, 3, 3),
    ]


def test_s4_mixed_aliased_unaliased_join(ruled_spark: ReparkSession) -> None:
    """One aliased side plus one bare side resolve to the diagonal."""
    frame = _frame(ruled_spark)
    aliased = frame.alias("l")
    assert sorted(tuple(row) for row in frame.join(aliased, frame.x == aliased.x).collect()) == [
        (1, 2, 1, 2),
        (2, 1, 2, 1),
        (3, 3, 3, 3),
    ]
    assert sorted(tuple(row) for row in aliased.join(frame, aliased.x == frame.x).collect()) == [
        (1, 2, 1, 2),
        (2, 1, 2, 1),
        (3, 3, 3, 3),
    ]


def test_s4_filter_lineage_simple_join(ruled_spark: ReparkSession) -> None:
    """Lineage-sharing equi-join keeps the Spark diagonal."""
    frame = _frame(ruled_spark)
    child = frame.filter(frame.y > 1)
    joined = frame.join(child, frame.x == child.x)
    assert sorted(tuple(row) for row in joined.collect()) == [(1, 2, 1, 2), (3, 3, 3, 3)]


def test_s4_filter_lineage_compound_join_refuses(ruled_spark: ReparkSession) -> None:
    """Lineage-sharing compound arms refuse; Spark reports ambiguity too."""
    frame = _frame(ruled_spark)
    child = frame.filter(frame.y > 1)
    with pytest.raises(AnalysisException, match=r"multi-token comparison arms"):
        _ = frame.join(child, (frame.x + 1) == (frame.x + child.x)).count()


def test_s4_mixed_compound_arms_divergence(ruled_spark: ReparkSession) -> None:
    """DIVERGENCE: mixed compound arms run where Spark reports ambiguity.

    ``(l.x + r.y) == (l.y + r.x)`` resolves each token to its named side and keeps
    the base diagonal; Spark raises ``ambiguous`` on the shared lineage.
    """
    frame = _frame(ruled_spark)
    left = frame.alias("l")
    right = frame.alias("r")
    joined = left.join(right, (left.x + right.y) == (left.y + right.x))
    assert sorted(tuple(row) for row in joined.collect()) == [
        (1, 2, 1, 2),
        (2, 1, 2, 1),
        (3, 3, 3, 3),
    ]


def test_s4_select_engine_names_shrunk(ruled_spark: ReparkSession) -> None:
    """Duplicate select outputs take positional ``__repark_sel_{n}`` engine names."""
    frame = _frame(ruled_spark)
    selected = frame.select(frame.x, frame.x.alias("x"))
    assert selected._engine_names == ["__repark_sel_0", "__repark_sel_1"]
    assert list(_native.logical_column_names(selected._plan())) == [
        "__repark_sel_0",
        "__repark_sel_1",
    ]
