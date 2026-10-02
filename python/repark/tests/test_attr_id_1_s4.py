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


def test_s4_third_frame_unknown_id_raises_engine_error(ruled_spark: ReparkSession) -> None:
    """A token whose id sits on neither join side is never sided; the engine refuses."""
    left = ruled_spark.createDataFrame([(1,)], ["k"])
    right = ruled_spark.createDataFrame([(1,)], ["k"])
    third = ruled_spark.createDataFrame([(1,)], ["k"])
    with pytest.raises(AnalysisException, match=r"UNRESOLVED_COLUMN"):
        _ = left.join(right, third.k == 1).count()


def test_s4_select_engine_names_shrunk(ruled_spark: ReparkSession) -> None:
    """Duplicate select outputs take positional ``__repark_sel_{n}`` engine names."""
    frame = _frame(ruled_spark)
    selected = frame.select(frame.x, frame.x.alias("x"))
    assert selected._engine_names == ["__repark_sel_0", "__repark_sel_1"]
    assert list(_native.logical_column_names(selected._plan())) == [
        "__repark_sel_0",
        "__repark_sel_1",
    ]


def _dup_join(spark: ReparkSession) -> Any:
    """Join with duplicate ``b`` and asymmetric sides (left fills 0/2, right 0/20)."""
    left_base = spark.createDataFrame([(1, 2), (3, None)], ["a", "b"])
    right_base = spark.createDataFrame([(1, 20), (3, None)], ["a", "b"])
    left = left_base.select(left_base.a.alias("aa"), left_base.b)
    return left, right_base, left.join(right_base, left.aa == right_base.a)


def test_s4_fillna_dup_name_binds_parent_source(ruled_spark: ReparkSession) -> None:
    """A parent ref through ``fillna`` binds its own side's filled output."""
    left, _right, joined = _dup_join(ruled_spark)
    filled = joined.fillna(0)
    assert filled.columns == ["aa", "b", "a", "b"]
    assert sorted(row[0] for row in filled.select(left["b"]).collect()) == [0, 2]


def test_s4_replace_dup_name_binds_parent_source(ruled_spark: ReparkSession) -> None:
    """A parent ref through ``replace`` binds its own side's replaced output."""
    left, _right, joined = _dup_join(ruled_spark)
    replaced = joined.replace(2, 200)
    assert replaced.columns == ["aa", "b", "a", "b"]
    assert {row[0] for row in replaced.select(left["b"]).collect()} == {200, None}


def test_s4_eq_null_safe_shared_lineage_sides(ruled_spark: ReparkSession) -> None:
    """``IS NOT DISTINCT FROM`` separates comparison arms like ``=`` in a self-join."""
    frame = ruled_spark.createDataFrame([(1, 2), (3, None)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b.eqNullSafe(frame.b))
    assert joined.columns == ["aa", "b", "a", "b"]
    assert sorted(tuple(row) for row in joined.collect()) == [
        (1, 2, 1, 2),
        (3, None, 3, None),
    ]


def test_s4_expression_output_parent_ref_uses_engine_name(ruled_spark: ReparkSession) -> None:
    """A parent ref past an arithmetic output still resolves by engine name."""
    frame = ruled_spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    projected = frame.select((frame.b + 1).alias("b"), frame.a)
    assert [row[0] for row in projected.select(frame.b).orderBy(frame.a).collect()] == [3, 5]


def test_s4_expression_output_parent_ref_stays_unbound_on_dup_names(
    ruled_spark: ReparkSession,
) -> None:
    """A parent ref past an arithmetic output on duplicate names raises, never binds."""
    left, right, joined = _dup_join(ruled_spark)
    projected = joined.select((left["b"] + 1).alias("b"), joined["aa"], joined["a"], right["b"])
    assert projected.columns == ["b", "aa", "a", "b"]
    with pytest.raises(AnalysisException):
        projected.select(left["b"]).collect()


def test_s4_same_frame_twin_getitem_stays_written_ref(spark: ReparkSession) -> None:
    """A getitem column whose SQL already spells its engine is left for the engine (insensitive)."""
    frame = spark.createDataFrame([(1, 2)], ["id", "ID"])
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        frame.select(frame["id"]).collect()


def test_s4_unheld_sort_marker_funnels_to_oldest(spark: ReparkSession) -> None:
    """A marked parent ref no output holds sorts by the oldest project hit (insensitive)."""
    frame = spark.createDataFrame([(1, 30), (2, None), (3, 10), (4, 20)], ["id", "v"])
    twins = frame.select((frame.v + 1).alias("V"), (frame.v * -1).alias("v"), frame.id)
    assert [tuple(row) for row in twins.orderBy(frame.v.desc()).collect()] == [
        (31, -30, 1),
        (21, -20, 4),
        (11, -10, 3),
        (None, None, 2),
    ]
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        twins.orderBy(frame.v).collect()


def test_s4_pass_through_child_twin_parent_ref_binds(spark: ReparkSession) -> None:
    """A twin parent ref binds by position on any pass-through child frame (insensitive)."""
    frame = spark.createDataFrame([(1, 2)], ["id", "ID"])
    assert frame.filter("1 > 0").select(frame["id"]).collect()[0][0] == 1
    assert frame.alias("t").select(frame["id"]).collect()[0][0] == 1
    assert frame.select("*").select(frame["id"]).collect()[0][0] == 1


def test_s4_join_side_parent_ref_binds_by_position(spark: ReparkSession) -> None:
    """A join-side parent ref binds its own side's output (insensitive)."""
    left = spark.createDataFrame([(1, 10)], ["id", "v"]).alias("l")
    right = spark.createDataFrame([(1, "x")], ["ID", "name"]).alias("r")
    joined = left.join(right, left["id"] == right["ID"], "inner")
    assert joined.select(left.id).collect() == [(1,)]
    assert joined.select(left["id"]).collect() == [(1,)]


def test_s4_select_output_parent_ref_binds_held_source(spark: ReparkSession) -> None:
    """A parent ref past a duplicate select output binds the held source (insensitive)."""
    frame = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    projected = frame.select(frame.v, frame.v.alias("V"))
    assert [row[0] for row in projected.select(frame.v).collect()] == [10, 20]


def test_s4_renamed_output_parent_ref_refuses(spark: ReparkSession) -> None:
    """A parent ref past a rename that drops its name raises, never binds (insensitive)."""
    frame = spark.createDataFrame([(1, 10)], ["id", "v"])
    renamed = frame.withColumnRenamed("v", "V2")
    with pytest.raises(AnalysisException, match="No field named"):
        renamed.select(frame.v).collect()


def test_s4_case_only_rename_parent_ref_binds(spark: ReparkSession) -> None:
    """A parent ref past a case-only rename binds under insensitive resolution."""
    frame = spark.createDataFrame([(1, 10)], ["id", "v"])
    renamed = frame.withColumnRenamed("v", "V")
    assert [row[0] for row in renamed.select(frame.v).collect()] == [10]


def test_s4_replaced_output_parent_ref_reads_new_value(spark: ReparkSession) -> None:
    """A parent ref past a same-name replacement reads the new value (insensitive)."""
    frame = spark.createDataFrame([(1, 10)], ["id", "v"])
    replaced = frame.withColumn("v", frame.v + 1)
    assert [row[0] for row in replaced.select(frame.v).collect()] == [11]


def test_s4_sort_marker_same_frame_twins_refuses(spark: ReparkSession) -> None:
    """A marked sort key on its own twin frame stays written and unresolved (insensitive)."""
    frame = spark.createDataFrame([(2, 20), (1, 10)], ["id", "ID"])
    with pytest.raises(AnalysisException, match="UNRESOLVED_COLUMN"):
        frame.orderBy(frame["id"].desc()).collect()
    with pytest.raises(AnalysisException, match="UNRESOLVED_COLUMN"):
        frame.orderBy(frame["id"].asc()).collect()


def test_s4_sql_twins_same_frame_refuses(spark: ReparkSession) -> None:
    """A same-frame ref on SQL twins stays written and ambiguous (insensitive)."""
    frame = spark.sql("SELECT 1 AS id, 2 AS ID")
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        frame.select(frame["id"]).collect()


def test_s4_compound_past_dup_output_refuses(spark: ReparkSession) -> None:
    """A compound over a duplicate output stays written and ambiguous (insensitive)."""
    frame = spark.createDataFrame([(1, 10)], ["id", "v"])
    projected = frame.select(frame.v, frame.v.alias("V"))
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        projected.select(projected.v + 1).collect()
