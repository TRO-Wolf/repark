from __future__ import annotations

from pathlib import Path

import _sm2_shared as sm2

from repark.spark import functions as spark_functions


def test_qualified_getitem_repr_keeps_qualifier_self_join(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r7-self")
    frame = sm2._self_join(session)
    assert repr(frame["r.v"]) == "Column<'r.v'>"
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    sided = left.alias("a").join(
        left.alias("b"), spark_functions.col("a.id") == spark_functions.col("b.id")
    )
    assert repr(sided["b.v"]) == "Column<'b.v'>"
    session.stop()


def test_qualified_getitem_repr_keeps_qualifier_mixed_join(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r7-mixed")
    frame = sm2._mixed_join(session)
    assert repr(frame["r.t"]) == "Column<'r.t'>"
    session.stop()


def test_qualified_getitem_repr_keeps_alias_qualifier(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r7-alias")
    left = session.createDataFrame([(1, "a", 10)], "id INT, s STRING, v INT")
    assert repr(left.alias("x")["x.id"]) == "Column<'x.id'>"
    session.stop()


def test_qualified_getitem_repr_folds_qualifier_keeps_name_spelling(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-r7-folded")
    frame = sm2._mixed_join(session)
    assert repr(frame["R.T"]) == "Column<'r.T'>"
    session.stop()


def test_qualified_getitem_repr_keeps_using_side_qualifier(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r7-using")
    left = session.createDataFrame([(1, "a", 10)], "id INT, s STRING, v INT")
    right = session.createDataFrame([(1, "x")], "id INT, t STRING")
    frame = left.alias("l").join(right.alias("r"), "id")
    assert repr(frame["l.id"]) == "Column<'l.id'>"
    session.stop()


def test_qualified_getitem_projection_and_compound_stay_bare(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-r7-bare")
    frame = sm2._mixed_join(session)
    assert frame.select(frame["r.t"]).columns == ["t"]
    assert frame.select(frame["r.id"] + 1).columns == ["(id + 1)"]
    session.stop()


def test_free_column_repr_unchanged(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-r7-controls")
    sm2._mixed_join(session)
    assert repr(spark_functions.col("r.t")) == "Column<'r.t'>"
    assert repr(spark_functions.col("t")) == "Column<'t'>"
    session.stop()
