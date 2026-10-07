from __future__ import annotations

from pathlib import Path
from typing import Any

import _sm2_shared as sm2
import pytest

from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark import functions as spark_functions
from repark.spark.qualified_names import _using_mark


def _using_frames(session: Any) -> tuple[Any, Any]:
    left = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, s STRING")
    right = session.createDataFrame([(2, "x"), (3, "y"), (4, "z")], "id INT, t STRING")
    return left, right


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted(
        (tuple(row) for row in frame.collect()),
        key=lambda row: tuple(str(value) for value in row),
    )


def _assert_using_refusal(action: Any, ref: str) -> None:
    refused = sm2._refusal_of(action)
    assert isinstance(refused, UnsupportedOperationException)
    assert f"qualified reference {ref} to a USING join key" in str(refused)
    assert "not supported in repark v1" in str(refused)


def test_inner_using_binds_both_side_keys(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-inner")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "inner")
    assert frame.columns == ["id", "s", "t"]
    assert _rows(frame.select("l.id", "r.id")) == [(2, 2), (3, 3)]
    assert _rows(frame.select("r.id", "l.id")) == [(2, 2), (3, 3)]
    session.stop()


def test_inner_using_compound_over_qualified_key(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-inner-compound")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "inner")
    assert _rows(frame.select(spark_functions.col("r.id") + 1)) == [(3,), (4,)]
    session.stop()


def test_inner_using_stale_keys_answer(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-inner-stale")
    left, right = _using_frames(session)
    aliased_left = left.alias("l")
    aliased_right = right.alias("r")
    frame = aliased_left.join(aliased_right, "id", "inner")
    assert _rows(frame.select(aliased_left["id"])) == [(2,), (3,)]
    assert _rows(frame.select(aliased_right["id"])) == [(2,), (3,)]
    assert _rows(frame.filter(aliased_right["id"] > 2).select("id")) == [(3,)]
    assert _rows(frame.sort(aliased_right["id"]).select("id")) == [(2,), (3,)]
    session.stop()


def test_left_using_binds_left_key(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-left")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "left")
    assert frame.columns == ["id", "s", "t"]
    assert _rows(frame.select("l.id")) == [(1,), (2,), (3,)]
    assert _rows(frame.select("*")) == [
        (1, "a", None),
        (2, "b", "x"),
        (3, "c", "y"),
    ]
    session.stop()


def test_right_using_binds_left_key_star_and_select(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-right")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "right")
    assert frame.columns == ["id", "s", "t"]
    assert _rows(frame.select("l.id")) == [(2,), (3,), (None,)]
    assert _rows(frame.select("*")) == [
        (2, "b", "x"),
        (3, "c", "y"),
        (None, None, "z"),
    ]
    assert _rows(frame.select("id")) == [(2,), (3,), (None,)]
    session.stop()


def test_full_using_binds_left_key_star_and_select(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-full")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "full")
    assert frame.columns == ["id", "s", "t"]
    assert _rows(frame.select("*")) == [
        (1, "a", None),
        (2, "b", "x"),
        (3, "c", "y"),
        (None, None, "z"),
    ]
    assert _rows(frame.select("id")) == [(1,), (2,), (3,), (None,)]
    assert _rows(frame.select("l.id")) == [(1,), (2,), (3,), (None,)]
    assert _rows(frame.select(frame["l.id"])) == [(1,), (2,), (3,), (None,)]
    session.stop()


_LEFT_ROWS: dict[str, dict[str, list[tuple[Any, ...]]]] = {
    "left": {
        "keys": [(1,), (2,), (3,)],
        "filtered": [(2,), (3,)],
        "plus_one": [(2,), (3,), (4,)],
    },
    "right": {
        "keys": [(2,), (3,), (None,)],
        "filtered": [(2,), (3,)],
        "plus_one": [(3,), (4,), (None,)],
    },
    "full": {
        "keys": [(1,), (2,), (3,), (None,)],
        "filtered": [(2,), (3,)],
        "plus_one": [(2,), (3,), (4,), (None,)],
    },
}


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_using_left_side_answers(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-left")
    left, right = _using_frames(session)
    aliased_left = left.alias("l")
    aliased_right = right.alias("r")
    frame = aliased_left.join(aliased_right, "id", how)
    expected = _LEFT_ROWS[how]
    assert _rows(frame.select(aliased_left["id"])) == expected["keys"]
    assert _rows(frame.select("l.id")) == expected["keys"]
    assert _rows(frame.select(frame["l.id"])) == expected["keys"]
    assert _rows(frame.filter(aliased_left["id"] > 1).select("id")) == expected["filtered"]
    assert _rows(frame.filter("l.id > 1").select("id")) == expected["filtered"]
    assert _rows(frame.sort(aliased_left["id"]).select("id")) == expected["keys"]
    assert _rows(frame.sort("l.id").select("id")) == expected["keys"]
    assert _rows(frame.select(spark_functions.col("l.id") + 1)) == expected["plus_one"]
    assert _rows(frame.selectExpr("l.id + 0")) == expected["keys"]
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_using_stale_left_key_joins_again(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-lcond")
    left, right = _using_frames(session)
    aliased_left = left.alias("l")
    frame = aliased_left.join(right.alias("r"), "id", how)
    again = frame.join(right.alias("q"), aliased_left["id"] == spark_functions.col("q.id"))
    assert _rows(again) == [(2, "b", "x", 2, "x"), (3, "c", "y", 3, "y")]
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
    session.stop()


def test_mixed_type_using_follows_left_key(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-mixed")
    left = session.createDataFrame([(1,), (2,)], "id INT")
    right = session.createDataFrame([("2",), ("3",)], "id STRING")
    frame = left.alias("l").join(right.alias("r"), "id", "full")
    assert _rows(frame.select("*")) == [(1,), (2,), (None,)]
    assert _rows(frame.select("l.id")) == [(1,), (2,), (None,)]
    session.stop()


def test_fresh_unqualified_key_joins_again(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-fresh")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "full")
    again = frame.join(right.alias("q"), frame["id"] == spark_functions.col("q.id"))
    assert _rows(again) == [(2, "b", "x", 2, "x"), (3, "c", "y", 3, "y")]
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_using_refuses_right_key_select(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-rsel")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    _assert_using_refusal(lambda: frame.select("r.id").collect(), "`r`.`id`")
    _assert_using_refusal(lambda: frame["r.id"], "`r`.`id`")
    _assert_using_refusal(lambda: frame.select("l.id", "r.id").collect(), "`r`.`id`")
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_using_refuses_right_key_sort_filter_expr(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-rsfe")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    _assert_using_refusal(lambda: frame.sort("r.id").collect(), "`r`.`id`")
    _assert_using_refusal(lambda: frame.filter("r.id > 1").collect(), "`r`.`id`")
    _assert_using_refusal(lambda: frame.selectExpr("r.id + 0").collect(), "`r`.`id`")
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_using_refuses_right_key_compound(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-rcompound")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    _assert_using_refusal(
        lambda: frame.select(spark_functions.col("r.id") + 1).collect(), "`r`.`id`"
    )
    _assert_using_refusal(
        lambda: frame.filter(spark_functions.col("r.id") > 1).collect(), "`r`.`id`"
    )
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_using_refuses_right_key_join_condition(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-rcond")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    _assert_using_refusal(
        lambda: frame.join(
            right.alias("q"),
            spark_functions.col("r.id") == spark_functions.col("q.id"),
        ).collect(),
        "`r`.`id`",
    )
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_using_refuses_stale_right_key_select_filter_sort(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-rstale")
    left, right = _using_frames(session)
    aliased_right = right.alias("r")
    frame = left.alias("l").join(aliased_right, "id", how)
    side_key = aliased_right["id"]
    _assert_using_refusal(lambda: frame.select(side_key).collect(), "`id`")
    _assert_using_refusal(lambda: frame.filter(side_key > 1).collect(), "`id`")
    _assert_using_refusal(lambda: frame.sort(side_key).collect(), "`id`")
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_using_refuses_stale_right_key_join_condition(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"sm2-r6-{how}-rstalec")
    left, right = _using_frames(session)
    aliased_right = right.alias("r")
    frame = left.alias("l").join(aliased_right, "id", how)
    side_key = aliased_right["id"]
    _assert_using_refusal(
        lambda: frame.join(right.alias("q"), side_key == spark_functions.col("q.id")).collect(),
        "`id`",
    )
    session.stop()


def test_using_key_guards_literals_and_unqualified(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-guards")
    left, right = _using_frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", "full")
    assert _rows(frame.select(spark_functions.lit("l.id"))) == [
        ("l.id",),
        ("l.id",),
        ("l.id",),
        ("l.id",),
    ]
    assert _rows(frame.filter("t == 'l.id'")) == []
    assert _rows(frame.select("id")) == [(1,), (2,), (3,), (None,)]
    session.stop()


def test_no_alias_using_carries_no_marker(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r6-noalias")
    left, right = _using_frames(session)
    frame = left.join(right, "id", "full")
    assert _using_mark(frame) is None
    assert _rows(frame.select("*")) == [
        (1, "a", None),
        (2, "b", "x"),
        (3, "c", "y"),
        (None, None, "z"),
    ]
    refused = sm2._refusal_of(lambda: frame.select("l.id").collect())
    assert isinstance(refused, AnalysisException)
    session.stop()
