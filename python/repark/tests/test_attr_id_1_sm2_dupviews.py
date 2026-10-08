from __future__ import annotations

from pathlib import Path
from typing import Any

import _sm2_shared as sm2

from repark.errors import UnsupportedOperationException
from repark.spark import functions as spark_functions


def test_create_or_replace_temp_view_over_self_join_registers_display_names(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-crotv-self")
    frame = sm2._self_join(session)
    frame.createOrReplaceTempView("vj")
    assert session.catalog.tableExists("vj") is True
    assert session.sql("SELECT * FROM vj").columns == ["id", "s", "v", "id", "s", "v"]
    session.stop()


def test_create_temp_view_over_mixed_join_registers_display_names(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-ctv-mixed")
    frame = sm2._mixed_join(session)
    frame.createTempView("vj")
    assert session.catalog.tableExists("vj") is True
    assert session.table("vj").columns == ["id", "s", "v", "id", "t"]
    session.stop()


def test_create_global_temp_view_over_self_join_is_unsupported_like_any_frame(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-cgtv-self")
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: frame.createGlobalTempView("vj"))
    assert isinstance(refused, UnsupportedOperationException)
    session.stop()


def test_create_or_replace_global_temp_view_over_self_join_is_unsupported(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-crogtv-self")
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: frame.createOrReplaceGlobalTempView("vj"))
    assert isinstance(refused, UnsupportedOperationException)
    session.stop()


def test_temp_view_over_using_join_registers_and_repeats_the_shared_columns(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-using")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    frame = left.alias("l").join(left.alias("r"), "id", "inner")
    frame.createOrReplaceTempView("vj")
    assert session.catalog.tableExists("vj") is True
    assert session.sql("SELECT * FROM vj").columns == ["id", "s", "v", "s", "v"]
    session.stop()


def test_temp_view_over_folded_join_names_registers_and_answers(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-fold")
    upper = session.createDataFrame([(1, "x"), (3, "y")], ["ID", "T"])
    lower = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    frame = upper.alias("r").join(
        lower.alias("l"),
        spark_functions.col("r.ID") == spark_functions.col("l.id"),
    )
    assert frame.columns == ["ID", "T", "id", "s", "v"]
    frame.createOrReplaceTempView("vj")
    assert session.catalog.tableExists("vj") is True
    table = session.sql("SELECT * FROM vj").to_arrow()
    assert table.column_names == ["ID", "T", "id", "s", "v"]
    assert table.num_rows == 1
    session.stop()


def test_temp_view_over_plain_frame_registers_and_answers(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-plain")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    left.createOrReplaceTempView("vj")
    assert session.catalog.tableExists("vj") is True
    assert session.sql("SELECT * FROM vj ORDER BY id").collect() == left.orderBy("id").collect()
    assert session.table("vj").columns == ["id", "s", "v"]
    described = session.sql("DESCRIBE vj").collect()
    assert [row["col_name"] for row in described] == ["id", "s", "v"]
    session.stop()


def test_temp_view_replace_over_plain_frame_still_works(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-replace")
    left = session.createDataFrame([(1, "a", 10)], ["id", "s", "v"])
    right = session.createDataFrame([(2, "b", 20)], ["id", "s", "v"])
    left.createOrReplaceTempView("vj")
    right.createOrReplaceTempView("vj")
    assert [tuple(row) for row in session.table("vj").collect()] == [(2, "b", 20)]
    session.stop()


def test_explain_over_self_join_still_runs(tmp_path: Path, capsys: Any) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-explain")
    frame = sm2._self_join(session)
    assert frame.explain() is None
    assert len(capsys.readouterr().out) > 0
    session.stop()


def test_global_temp_view_over_plain_frame_stays_unsupported(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-global-plain")
    left = session.createDataFrame([(1, "a", 10)], ["id", "s", "v"])
    refused = sm2._refusal_of(lambda: left.createGlobalTempView("vj"))
    assert isinstance(refused, UnsupportedOperationException)
    session.stop()


def test_registered_duplicate_view_shows_no_internal_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupviews-clean")
    frame = sm2._self_join(session)
    frame.createOrReplaceTempView("vj")
    names = [table.name for table in session.catalog.listTables()]
    assert not any("__repark_" in name for name in names)
    assert not any("__repark_" in name for name in session.table("vj").columns)
    described = session.sql("DESCRIBE vj").collect()
    assert not any("__repark_" in row["col_name"] for row in described)
    session.stop()
