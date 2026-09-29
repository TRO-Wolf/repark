"""POLARS-IS-DUPLICATED-1 — Column.is_duplicated() answers as real polars on both doors.

pins: polars-is-duplicated-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import datetime
import hashlib
import json
import time
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812
from repark import polars as rp
from repark.errors import UnsupportedOperationException
from repark.spark.window import Window

_FIXTURE = json.loads(
    (Path(__file__).parent / "polars_is_duplicated_1_polars_oracle.json").read_text()
)

INT_VALS = [1, 2, 2, None, None, 3, 1]
BIGINT_VALS = [10, 10, 10, 20, 30]
DOUBLE_VALS = [1.0, float("nan"), float("nan"), 0.0, -0.0, None, 2.0]
DOUBLE_SINGLE_VALS = [1.0, float("nan"), None, 2.0]
STRING_VALS = ["A", "a", "b", "b", None]
DATE_VALS = [
    datetime.date(2024, 1, 1),
    datetime.date(2024, 1, 2),
    datetime.date(2024, 1, 1),
    None,
    None,
]
DECIMAL_VALS = [Decimal("1.10"), Decimal("1.1"), Decimal("2.50"), None, None]
BOOLEAN_VALS = [True, False, True, None, None, False]
ONE_ROW_VALS = [42]
BIG_VALS = [*range(9997), 0, 1, 2]

_FRAME_DEFS: tuple[tuple[str, list[Any], str], ...] = (
    ("int", INT_VALS, "c INT"),
    ("bigint", BIGINT_VALS, "c BIGINT"),
    ("double", DOUBLE_VALS, "c DOUBLE"),
    ("double_single", DOUBLE_SINGLE_VALS, "c DOUBLE"),
    ("string", STRING_VALS, "c STRING"),
    ("date", DATE_VALS, "c DATE"),
    ("decimal", DECIMAL_VALS, "c DECIMAL(10, 2)"),
    ("boolean", BOOLEAN_VALS, "c BOOLEAN"),
    ("one_row", ONE_ROW_VALS, "c INT"),
)

_WINDOW_FILTER_PREFIX = (
    "This feature is not implemented: Physical plan does not support "
    "logical expression WindowFunction("
)


@pytest.fixture
def session() -> Iterator[ReparkSession]:
    live = ReparkSession.builder.appName("pytest-polars-is-duplicated-1").getOrCreate()
    yield live
    live.stop()


def _encode(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return {"$float": repr(value)}
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, datetime.datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"$date": value.isoformat()}
    return {"$str": str(value)}


def _dicts(frame: Any) -> list[dict[str, Any]]:
    cols = list(frame.columns)
    return [{c: _encode(v) for c, v in zip(cols, row, strict=True)} for row in frame.collect()]


def _make_frame(session: ReparkSession, name: str) -> Any:
    if name == "empty":
        return session.createDataFrame([], "c INT")
    if name == "big":
        return session.createDataFrame([(v,) for v in BIG_VALS], "c INT")
    for frame_name, vals, ddl in _FRAME_DEFS:
        if frame_name == name:
            return session.createDataFrame([(v,) for v in vals], ddl)
    raise AssertionError(f"unknown frame {name}")


def _colfn(door: str) -> Any:
    return F.col if door == "F" else rp.col


def _widen(frame: Any, door: str, mask: Any) -> Any:
    if door == "F":
        return frame.withColumn("d", mask)
    return frame.pl.with_columns(mask.alias("d")).spark


def test_masks_match_polars_on_both_doors(session: ReparkSession) -> None:
    for name, _vals, _ddl in _FRAME_DEFS:
        frame = _make_frame(session, name)
        expected = _FIXTURE["frames"][name]["mask"]
        for door in ("F", "rp"):
            mask = _colfn(door)("c").is_duplicated().alias("d")
            assert _dicts(frame.select(mask)) == expected


def test_filter_matches_polars_on_both_doors(session: ReparkSession) -> None:
    names = [name for name, _v, _d in _FRAME_DEFS] + ["empty"]
    for name in names:
        frame = _make_frame(session, name)
        expected = _FIXTURE["frames"][name]["filter"]
        assert _dicts(frame.filter(F.col("c").is_duplicated())) == expected
        assert _dicts(frame.pl.filter(rp.col("c").is_duplicated()).spark) == expected


def test_select_matches_polars_on_both_doors(session: ReparkSession) -> None:
    names = [name for name, _v, _d in _FRAME_DEFS] + ["empty"]
    for name in names:
        frame = _make_frame(session, name)
        expected = _FIXTURE["frames"][name]["select"]
        for door in ("F", "rp"):
            mask = _colfn(door)("c").is_duplicated().alias("d")
            assert _dicts(frame.select("c", mask)) == expected


def test_with_column_matches_polars_on_both_doors(session: ReparkSession) -> None:
    names = [name for name, _v, _d in _FRAME_DEFS] + ["empty"]
    for name in names:
        frame = _make_frame(session, name)
        expected = _FIXTURE["frames"][name]["with_columns"]
        for door in ("F", "rp"):
            assert _dicts(_widen(frame, door, _colfn(door)("c").is_duplicated())) == expected


def test_negation_and_conjunction_in_filter(session: ReparkSession) -> None:
    frame = session.createDataFrame([(1, 0), (2, 5), (2, 0), (3, 5)], "c INT, k INT")
    assert _dicts(frame.filter(~F.col("c").is_duplicated())) == _FIXTURE["combo"]["not.filter"]
    assert (
        _dicts(frame.pl.filter(~rp.col("c").is_duplicated()).spark)
        == _FIXTURE["combo"]["not.filter"]
    )
    assert (
        _dicts(frame.filter(F.col("c").is_duplicated() & (F.col("k") > 1)))
        == _FIXTURE["combo"]["and.filter"]
    )
    assert (
        _dicts(frame.pl.filter(rp.col("c").is_duplicated() & (rp.col("k") > 1)).spark)
        == _FIXTURE["combo"]["and.filter"]
    )


def test_expression_receiver_and_prefilter(session: ReparkSession) -> None:
    frame = session.createDataFrame([(1,), (2,), (3,), (4,), (5,)], "a INT")
    for door in ("F", "rp"):
        colfn = _colfn(door)
        assert (
            _dicts(frame.filter((colfn("a") % 2).is_duplicated()))
            == _FIXTURE["combo"]["expr.filter"]
        )
        mask = ((colfn("a") % 2).is_duplicated()).alias("d")
        expected = [{"d": row["d"]} for row in _FIXTURE["combo"]["expr.select"]]
        assert _dicts(frame.select(mask)) == expected
    pre = session.createDataFrame([(1,), (1,), (2,), (2,), (3,)], "c INT")
    assert (
        _dicts(pre.filter(F.col("c") > 1).filter(F.col("c").is_duplicated()))
        == _FIXTURE["combo"]["prefilter.filter"]
    )
    assert (
        _dicts(pre.pl.filter(rp.col("c") > 1).filter(rp.col("c").is_duplicated()).spark)
        == _FIXTURE["combo"]["prefilter.filter"]
    )


def test_big_frame_summary_and_timing(session: ReparkSession) -> None:
    frame = _make_frame(session, "big")
    expected = _FIXTURE["big_summary"]
    started = time.perf_counter()
    filtered = _dicts(frame.filter(F.col("c").is_duplicated()))
    elapsed = time.perf_counter() - started
    assert len(filtered) == expected["filter_count"] == expected["true_count"]
    assert sorted({row["c"] for row in filtered}) == expected["duplicated_values"]
    mask_rows = _dicts(frame.select(F.col("c").is_duplicated().alias("d")))
    bits = "".join("1" if row["d"] else "0" for row in mask_rows)
    assert hashlib.sha256(bits.encode()).hexdigest() == expected["mask_sha256"]
    assert sum(1 for row in mask_rows if row["d"]) == expected["true_count"]
    assert elapsed < 2.0


def test_window_in_filter_still_refuses(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    predicate = F.count("*").over(Window.partitionBy("c")) > 1
    with pytest.raises(UnsupportedOperationException, match="WindowFunction"):
        frame.filter(predicate).collect()
    try:
        frame.filter(predicate).collect()
        raise AssertionError("window filter answered")
    except UnsupportedOperationException as error:
        assert str(error).startswith(_WINDOW_FILTER_PREFIX)
    ranked = F.row_number().over(Window.partitionBy("c").orderBy("c")) > 1
    with pytest.raises(UnsupportedOperationException, match="WindowFunction"):
        frame.filter(ranked).collect()


def test_window_select_answers_unchanged(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    predicate = F.count("*").over(Window.partitionBy("c")) > 1
    expected = _FIXTURE["keep"]["keep.window_select"]
    selected = frame.select(frame["c"], predicate.alias("d"))
    got = {
        "columns": selected.columns,
        "rows": [[_encode(v) for v in r] for r in selected.collect()],
    }
    assert got == expected


def test_null_and_membership_cells_unchanged(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    assert _dicts(frame.filter(F.col("c").isNull())) == [
        {"c": row[0]} for row in _FIXTURE["keep"]["keep.isnull_filter"]["rows"]
    ]
    assert _dicts(frame.select(F.col("c").isNull().alias("d"))) == [
        {"d": row[0]} for row in _FIXTURE["keep"]["keep.isnull_select"]["rows"]
    ]
    assert _dicts(frame.filter(F.col("c").isin(1, 2))) == [
        {"c": row[0]} for row in _FIXTURE["keep"]["keep.isin_filter"]["rows"]
    ]
    assert _dicts(frame.filter(F.col("c").isNotNull())) == [
        {"c": row[0]} for row in _FIXTURE["keep"]["keep.notnull_filter"]["rows"]
    ]
    assert _dicts(frame.withColumn("d", F.col("c").isNull())) == [
        {"c": row[0], "d": row[1]} for row in _FIXTURE["keep"]["keep.withcol_isnull"]["rows"]
    ]


def test_row_number_and_drop_duplicates_unchanged(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    ranked = frame.select(
        frame["c"], F.row_number().over(Window.partitionBy("c").orderBy("c")).alias("d")
    )
    got = {"columns": ranked.columns, "rows": [list(r) for r in ranked.collect()]}
    assert got == _FIXTURE["keep"]["keep.row_number_select"]
    for key, ddl_vals in (("keep.dropdup_int", "int"), ("keep.dropdup_double", "double")):
        source = _make_frame(session, ddl_vals)
        dropped = source.dropDuplicates(["c"])
        rows = sorted(json.dumps([_encode(v) for v in r]) for r in dropped.collect())
        expected = sorted(json.dumps(r) for r in _FIXTURE["keep"][key]["rows"])
        assert rows == expected


def test_plain_rp_columns_unchanged(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    assert _dicts(frame.pl.filter(rp.col("c") > 1).spark) == [
        {"c": r[0]} for r in _FIXTURE["keep"]["keep.rp_filter"]["rows"]
    ]
    assert _dicts(frame.pl.select(rp.col("c")).spark) == [
        {"c": r[0]} for r in _FIXTURE["keep"]["keep.rp_select"]["rows"]
    ]
    assert _dicts(frame.pl.with_columns((rp.col("c") + 1).alias("d")).spark) == [
        {"c": r[0], "d": r[1]} for r in _FIXTURE["keep"]["keep.rp_with_columns"]["rows"]
    ]
    assert _dicts(frame.pl.select((rp.col("c") * 2).alias("d")).spark) == [
        {"d": r[0]} for r in _FIXTURE["keep"]["keep.rp_select_expr"]["rows"]
    ]
    assert _dicts(frame.pl.filter(rp.col("c").isNull()).spark) == [
        {"c": r[0]} for r in _FIXTURE["keep"]["keep.rp_filter_null"]["rows"]
    ]


def test_dir_gains_only_is_duplicated() -> None:
    names = set(dir(F.col("c")))
    assert "is_duplicated" in names
    assert sorted(names - {"is_duplicated"}) == _FIXTURE["keep"]["keep.dir_names"]
