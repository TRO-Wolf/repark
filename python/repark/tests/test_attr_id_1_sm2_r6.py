from __future__ import annotations

from pathlib import Path
from typing import Any

import _sm2_shared as sm2
import pytest

from repark import _native
from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark import functions as spark_functions
from repark.spark.qualified_names import _using_mark

_LEFT_KEYS: dict[str, list[tuple[Any, ...]]] = {
    "inner": [(2,), (3,)],
    "left": [(1,), (2,), (3,)],
    "right": [(2,), (3,), (None,)],
    "full": [(1,), (2,), (3,), (None,)],
}
_RIGHT_KEYS: dict[str, list[tuple[Any, ...]]] = {
    "inner": [(2,), (3,)],
    "left": [(2,), (3,), (None,)],
    "right": [(2,), (3,), (4,)],
    "full": [(2,), (3,), (4,), (None,)],
}
_SHOWN_KEYS: dict[str, list[tuple[Any, ...]]] = {
    "inner": [(2,), (3,)],
    "left": [(1,), (2,), (3,)],
    "right": [(2,), (3,), (4,)],
    "full": [(1,), (2,), (3,), (4,)],
}
_STAR: dict[str, list[tuple[Any, ...]]] = {
    "inner": [(2, "b", "x"), (3, "c", "y")],
    "left": [(1, "a", None), (2, "b", "x"), (3, "c", "y")],
    "right": [(2, "b", "x"), (3, "c", "y"), (4, None, "z")],
    "full": [(1, "a", None), (2, "b", "x"), (3, "c", "y"), (4, None, "z")],
}
_RIGHT_OVER_TWO: dict[str, list[tuple[Any, ...]]] = {
    "inner": [(3,)],
    "left": [(3,)],
    "right": [(3,), (4,)],
    "full": [(3,), (4,)],
}
_SORTED_BY_LEFT: dict[str, list[Any]] = {
    "inner": [2, 3],
    "left": [1, 2, 3],
    "right": [4, 2, 3],
    "full": [4, 1, 2, 3],
}
_SORTED_BY_RIGHT: dict[str, list[Any]] = {
    "inner": [2, 3],
    "left": [1, 2, 3],
    "right": [2, 3, 4],
    "full": [1, 2, 3, 4],
}
_JOINED_ON_RIGHT: dict[str, list[tuple[Any, ...]]] = {
    "left": [(2, "b", "x", 2, "x"), (3, "c", "y", 3, "y")],
    "right": [(2, "b", "x", 2, "x"), (3, "c", "y", 3, "y"), (4, None, "z", 4, "z")],
    "full": [(2, "b", "x", 2, "x"), (3, "c", "y", 3, "y"), (4, None, "z", 4, "z")],
}
_RIGHT_STAR: dict[str, list[tuple[Any, ...]]] = {
    "left": [(2, "x"), (3, "y"), (None, None)],
    "right": [(2, "x"), (3, "y"), (4, "z")],
    "full": [(2, "x"), (3, "y"), (4, "z"), (None, None)],
}
_LEFT_STAR: dict[str, list[tuple[Any, ...]]] = {
    "left": [(1, "a"), (2, "b"), (3, "c")],
    "right": [(2, "b"), (3, "c"), (None, None)],
    "full": [(1, "a"), (2, "b"), (3, "c"), (None, None)],
}
_SIDE_PAIRS: dict[str, list[tuple[Any, ...]]] = {
    "inner": [(2, 2), (3, 3)],
    "left": [(1, None), (2, 2), (3, 3)],
    "right": [(2, 2), (3, 3), (None, 4)],
    "full": [(1, None), (2, 2), (3, 3), (None, 4)],
}
_OUTER = ["left", "right", "full"]
_EMITTING = ["inner", "left", "right", "full"]


def _using_frames(session: Any) -> tuple[Any, Any]:
    left = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, s STRING")
    right = session.createDataFrame([(2, "x"), (3, "y"), (4, "z")], "id INT, t STRING")
    return left, right


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted(
        (tuple(row) for row in frame.collect()),
        key=lambda row: tuple(str(value) for value in row),
    )


def _shown(frame: Any) -> list[Any]:
    return [row[0] for row in frame.collect()]


def _assert_using_refusal(action: Any, ref: str) -> None:
    refused = sm2._refusal_of(action)
    assert isinstance(refused, UnsupportedOperationException)
    assert f"qualified reference {ref} to a USING join key" in str(refused)
    assert "not supported in repark v1" in str(refused)


@pytest.mark.parametrize("how", _EMITTING)
def test_using_star_and_unqualified_key_show_the_merged_key(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-star")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    assert frame.columns == ["id", "s", "t"]
    assert _rows(frame) == _STAR[how]
    assert _rows(frame.select("*")) == _STAR[how]
    assert _rows(frame.select("id")) == _SHOWN_KEYS[how]
    assert _rows(frame.select(frame["id"])) == _SHOWN_KEYS[how]
    assert _rows(frame.select(spark_functions.col("id"))) == _SHOWN_KEYS[how]
    session.stop()


@pytest.mark.parametrize("how", _EMITTING)
def test_using_unaliased_star_shows_the_merged_key(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-plainstar")
    left, right = _using_frames(session)
    frame = left.join(right, "id", how)
    assert _rows(frame.select("*")) == _STAR[how]
    assert _rows(frame.filter("id > 2").select("id")) == _RIGHT_OVER_TWO[how]
    session.stop()


@pytest.mark.parametrize("how", _EMITTING)
def test_using_per_side_keys_select(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-sides")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    assert _rows(frame.select("l.id")) == _LEFT_KEYS[how]
    assert _rows(frame.select("r.id")) == _RIGHT_KEYS[how]
    assert _rows(frame.select(spark_functions.col("l.id"))) == _LEFT_KEYS[how]
    assert _rows(frame.select(spark_functions.col("r.id"))) == _RIGHT_KEYS[how]
    assert _rows(frame.select(frame["l.id"])) == _LEFT_KEYS[how]
    assert _rows(frame.select(frame["r.id"])) == _RIGHT_KEYS[how]
    assert _rows(frame.selectExpr("l.id + 0")) == _LEFT_KEYS[how]
    assert _rows(frame.selectExpr("r.id + 0")) == _RIGHT_KEYS[how]
    assert frame.selectExpr("r.id + 0").columns == ["(id + 0)"]
    assert _rows(frame.select("l.id", "r.id")) == _SIDE_PAIRS[how]
    assert frame.select("l.id", "r.id").columns == ["id", "id"]
    session.stop()


@pytest.mark.parametrize("how", _OUTER)
def test_outer_using_compound_over_a_side_key(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-compound")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    plus_one = [(None if key is None else key + 1,) for (key,) in _RIGHT_KEYS[how]]
    assert _rows(frame.select(spark_functions.col("r.id") + 1)) == sorted(
        plus_one, key=lambda row: str(row[0])
    )
    session.stop()


@pytest.mark.parametrize("how", _EMITTING)
def test_using_per_side_keys_filter_and_sort(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-fs")
    left, right = _using_frames(session)
    aliased_left = left.alias("l")
    aliased_right = right.alias("r")
    frame = aliased_left.join(aliased_right, "id", how)
    assert _rows(frame.filter("r.id > 2").select("id")) == _RIGHT_OVER_TWO[how]
    assert _rows(frame.filter(aliased_right["id"] > 2).select("id")) == _RIGHT_OVER_TWO[how]
    assert _rows(frame.filter(frame["r.id"] > 2).select("id")) == _RIGHT_OVER_TWO[how]
    assert _rows(frame.filter("l.id > 2").select("id")) == [(3,)]
    assert _rows(frame.filter(aliased_left["id"] > 2).select("id")) == [(3,)]
    assert _shown(frame.sort("l.id")) == _SORTED_BY_LEFT[how]
    assert _shown(frame.sort(aliased_left["id"])) == _SORTED_BY_LEFT[how]
    assert _shown(frame.sort(spark_functions.col("l.id"))) == _SORTED_BY_LEFT[how]
    assert _shown(frame.sort("r.id")) == _SORTED_BY_RIGHT[how]
    assert _shown(frame.sort(aliased_right["id"])) == _SORTED_BY_RIGHT[how]
    assert _shown(frame.sort(spark_functions.col("r.id"))) == _SORTED_BY_RIGHT[how]
    assert frame.filter("r.id > 2").columns == ["id", "s", "t"]
    assert frame.sort("r.id").columns == ["id", "s", "t"]
    session.stop()


@pytest.mark.parametrize("how", _OUTER)
def test_outer_using_stale_keys_answer_per_side(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-stale")
    left, right = _using_frames(session)
    aliased_left = left.alias("l")
    aliased_right = right.alias("r")
    frame = aliased_left.join(aliased_right, "id", how)
    assert _rows(frame.select(aliased_left["id"])) == _LEFT_KEYS[how]
    assert _rows(frame.select(aliased_right["id"])) == _RIGHT_KEYS[how]
    assert _rows(frame.select(left["id"])) == _LEFT_KEYS[how]
    assert _rows(frame.select(right["id"])) == _RIGHT_KEYS[how]
    plain = left.join(right, "id", how)
    assert _rows(plain.select(left["id"])) == _LEFT_KEYS[how]
    assert _rows(plain.select(right["id"])) == _RIGHT_KEYS[how]
    assert _rows(plain.filter(right["id"] > 2).select("id")) == _RIGHT_OVER_TWO[how]
    assert _shown(plain.sort(right["id"])) == _SORTED_BY_RIGHT[how]
    assert _shown(plain.sort(left["id"])) == _SORTED_BY_LEFT[how]
    session.stop()


@pytest.mark.parametrize("how", _OUTER)
def test_outer_using_side_keys_in_a_join_condition(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-cond")
    left, right = _using_frames(session)
    aliased_left = left.alias("l")
    aliased_right = right.alias("r")
    frame = aliased_left.join(aliased_right, "id", how)
    other = right.alias("q")
    q_key = spark_functions.col("q.id")
    two = [(2, "b", "x", 2, "x"), (3, "c", "y", 3, "y")]
    assert _rows(frame.join(other, spark_functions.col("r.id") == q_key)) == _JOINED_ON_RIGHT[how]
    assert _rows(frame.join(other, aliased_right["id"] == q_key)) == _JOINED_ON_RIGHT[how]
    assert _rows(frame.join(other, frame["r.id"] == q_key)) == _JOINED_ON_RIGHT[how]
    assert _rows(frame.join(other, spark_functions.col("l.id") == q_key)) == two
    assert _rows(frame.join(other, aliased_left["id"] == q_key)) == two
    shown = two if how == "left" else _JOINED_ON_RIGHT[how]
    assert _rows(frame.join(other, frame["id"] == q_key)) == shown
    assert frame.join(other, aliased_right["id"] == q_key).columns == ["id", "s", "t", "id", "t"]
    session.stop()


@pytest.mark.parametrize("how", _OUTER)
def test_outer_using_qualified_stars_carry_the_side_key(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-qstar")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    assert frame.select("r.*").columns == ["id", "t"]
    assert _rows(frame.select("r.*")) == _RIGHT_STAR[how]
    assert frame.select("l.*").columns == ["id", "s"]
    assert _rows(frame.select("l.*")) == _LEFT_STAR[how]
    session.stop()


@pytest.mark.parametrize("how", _OUTER)
def test_hidden_keys_never_reach_columns_schema_or_exports(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-hidden")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    assert len(_native.using_hidden_key_fields(frame._plan())) == (2 if how == "full" else 1)
    for derived in (frame, frame.filter("r.id > 0"), frame.sort("l.id"), frame.sort("r.id")):
        assert derived.columns == ["id", "s", "t"]
        assert derived.schema.simpleString() == "struct<id:int,s:string,t:string>"
        assert list(_native.logical_column_names(derived._plan())) == ["id", "s", "t"]
        assert list(derived.toPandas().columns) == ["id", "s", "t"]
        assert derived.to_arrow().schema.names == ["id", "s", "t"]
    target = tmp_path / f"out-{how}"
    frame.filter("r.id > 0").write.parquet(str(target))
    sm2._assert_no_twin_bytes(target)
    assert session.read.parquet(str(target)).columns == ["id", "s", "t"]
    session.stop()


@pytest.mark.parametrize("how", _EMITTING)
def test_shown_key_attribute_id_follows_the_join_type(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-ids")
    left, right = _using_frames(session)
    aliased_left = left.alias("l")
    aliased_right = right.alias("r")
    left_id = _native.attribute_ids(_native.stamp_attribute_ids(aliased_left._inner))[0]
    right_id = _native.attribute_ids(_native.stamp_attribute_ids(aliased_right._inner))[0]
    frame = aliased_left.join(aliased_right, "id", how)
    shown = _native.attribute_ids(frame._plan())[0]
    if how in ("inner", "left"):
        assert shown == left_id
    elif how == "right":
        assert shown == right_id
    else:
        assert shown not in (left_id, right_id, None)
    session.stop()


@pytest.mark.parametrize("how", ["right", "full"])
def test_chained_using_join_matches_on_the_merged_key(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-chain")
    left, right = _using_frames(session)
    other = session.createDataFrame([(2, "p"), (3, "q"), (4, "r")], "id INT, u STRING")
    frame = left.alias("l").join(right.alias("r"), "id", how)
    chained = frame.join(other.alias("q"), "id", "full")
    expected = [(2, "b", "x", "p"), (3, "c", "y", "q"), (4, None, "z", "r")]
    if how == "full":
        expected = [(1, "a", None, None), *expected]
    assert _rows(chained) == expected
    assert _rows(left.join(right, "id", how).join(other, "id", "full")) == expected
    inner = [(2, "b", "x", "p"), (3, "c", "y", "q"), (4, None, "z", "r")]
    assert _rows(frame.join(other.alias("q"), "id")) == inner
    session.stop()


@pytest.mark.parametrize("how", ["right", "full"])
def test_merged_key_feeds_later_operations(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-later")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    keys = _SHOWN_KEYS[how]
    assert _rows(frame.groupBy("id").count()) == [(key, 1) for (key,) in keys]
    assert _rows(frame.withColumn("k", spark_functions.col("id") + 1).select("k")) == [
        (key + 1,) for (key,) in keys
    ]
    assert _rows(frame.distinct()) == _STAR[how]
    assert _rows(frame.alias("x").select("x.id")) == keys
    assert _rows(frame.drop("id")) == sorted(
        (row[1:] for row in _STAR[how]), key=lambda row: tuple(str(value) for value in row)
    )
    session.stop()


@pytest.mark.parametrize("how", _OUTER)
def test_side_key_past_a_narrowing_select_or_alias_refuses(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-narrow")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    hidden = "`l`.`id`" if how == "right" else "`r`.`id`"
    name = hidden.replace("`", "")
    _assert_using_refusal(lambda: frame.select("id", "s").select(name).collect(), hidden)
    _assert_using_refusal(lambda: frame.alias("x").select(name).collect(), hidden)
    _assert_using_refusal(lambda: frame.groupBy(spark_functions.col(name)).count(), hidden)
    _assert_using_refusal(
        lambda: frame.withColumn("k", spark_functions.col(name)).collect(), hidden
    )
    session.stop()


def test_semi_using_binds_left_key(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-semi")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "left_semi")
    assert frame.columns == ["id", "s"]
    assert _rows(frame.select("l.id")) == [(2,), (3,)]
    session.stop()


def test_anti_using_binds_left_key(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-anti")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "left_anti")
    assert frame.columns == ["id", "s"]
    assert _rows(frame.select("l.id")) == [(1,)]
    session.stop()


@pytest.mark.parametrize("how", ["left_semi", "left_anti"])
def test_semi_anti_using_refuse_right_key_unresolved(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-r")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    refused = sm2._refusal_of(lambda: frame.select("r.id").collect())
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert sm2._sql_state_of(refused) == "42703"
    assert "`r`.`id`" in str(refused)
    assert _native.using_hidden_key_fields(frame._plan()) == []
    session.stop()


@pytest.mark.parametrize(
    ("how", "star", "right_keys"),
    [
        ("left", [(1,), (2,)], [("2",), (None,)]),
        ("right", [(2,), (3,)], [("2",), ("3",)]),
        ("full", [(1,), (2,), (3,)], [("2",), ("3",), (None,)]),
    ],
)
def test_mixed_type_using_keeps_the_left_key_type(
    tmp_path: Path, how: str, star: list[tuple[Any, ...]], right_keys: list[tuple[Any, ...]]
) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-mixed-{how}")
    left = session.createDataFrame([(1,), (2,)], "id INT")
    right = session.createDataFrame([("2",), ("3",)], "id STRING")
    frame = left.alias("l").join(right.alias("r"), "id", how)
    assert frame.schema.simpleString() == "struct<id:int>"
    assert _rows(frame.select("*")) == star
    assert _rows(frame.select("r.id")) == right_keys
    session.stop()


@pytest.mark.parametrize("how", _OUTER)
def test_two_key_using_coalesces_each_key(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-two-{how}")
    left = session.createDataFrame([(1, 1, "a"), (2, 2, "b")], "a INT, b INT, s STRING")
    right = session.createDataFrame([(2, 2, "x"), (3, 3, "y")], "a INT, b INT, t STRING")
    frame = left.alias("l").join(right.alias("r"), ["a", "b"], how)
    expected = {
        "left": [(1, 1, "a", None), (2, 2, "b", "x")],
        "right": [(2, 2, "b", "x"), (3, 3, None, "y")],
        "full": [(1, 1, "a", None), (2, 2, "b", "x"), (3, 3, None, "y")],
    }
    sides = {
        "left": [(1, None), (2, 2)],
        "right": [(2, 2), (None, 3)],
        "full": [(1, None), (2, 2), (None, 3)],
    }
    assert frame.columns == ["a", "b", "s", "t"]
    assert _rows(frame) == expected[how]
    assert _rows(frame.select("l.a", "r.b")) == sides[how]
    session.stop()


def test_using_key_guards_literals_and_unqualified(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-guards")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "full")
    assert _rows(frame.select(spark_functions.lit("l.id"))) == [("l.id",)] * 4
    assert _rows(frame.filter("t == 'r.id'")) == []
    _assert_using_refusal(lambda: frame.filter("`r`.`id` > 2").collect(), "`r`.`id`")
    session.stop()


def test_no_alias_using_binds_no_qualifier(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-noalias")
    left, right = _using_frames(session)
    frame = left.join(right, "id", "full")
    mark = _using_mark(frame)
    assert mark is not None
    assert all(not quals for quals, _display, _alias in mark[2])
    refused = sm2._refusal_of(lambda: frame.select("l.id").collect())
    assert isinstance(refused, AnalysisException)
    session.stop()


@pytest.mark.parametrize("how", _OUTER)
def test_on_expression_join_is_unchanged(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-on-{how}")
    left, right = _using_frames(session)
    frame = left.alias("l").join(
        right.alias("r"), spark_functions.col("l.id") == spark_functions.col("r.id"), how
    )
    expected = {
        "left": [(1, "a", None, None), (2, "b", 2, "x"), (3, "c", 3, "y")],
        "right": [(2, "b", 2, "x"), (3, "c", 3, "y"), (None, None, 4, "z")],
        "full": [
            (1, "a", None, None),
            (2, "b", 2, "x"),
            (3, "c", 3, "y"),
            (None, None, 4, "z"),
        ],
    }
    assert frame.columns == ["id", "s", "id", "t"]
    assert _rows(frame) == expected[how]
    assert _using_mark(frame) is None
    assert _native.using_hidden_key_fields(frame._plan()) == []
    renamed = right.withColumnRenamed("id", "rid")
    named = left.join(renamed, spark_functions.col("id") == spark_functions.col("rid"), how)
    assert named.columns == ["id", "s", "rid", "t"]
    assert _rows(named) == expected[how]
    session.stop()
