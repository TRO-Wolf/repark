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


def test_sorted_input_order_survives_filter_on_both_doors(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    asc = [row["c"] for row in _dicts(frame.orderBy("c").filter(F.col("c").is_duplicated()))]
    assert asc == [None, None, 1, 1, 2, 2]
    desc = [
        row["c"]
        for row in _dicts(frame.orderBy(F.col("c").desc()).filter(F.col("c").is_duplicated()))
    ]
    assert desc == [2, 2, 1, 1, None, None]
    rp_asc = [
        row["c"] for row in _dicts(frame.pl.sort("c").filter(rp.col("c").is_duplicated()).spark)
    ]
    assert rp_asc == [None, None, 1, 1, 2, 2]
    rp_desc = [
        row["c"]
        for row in _dicts(
            frame.pl.sort("c", descending=True).filter(rp.col("c").is_duplicated()).spark
        )
    ]
    assert rp_desc == [None, None, 2, 2, 1, 1]


def test_sorted_input_order_survives_select_on_both_doors(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    mask = F.col("c").is_duplicated().alias("d")
    asc = _dicts(frame.orderBy("c").select("c", mask))
    assert [(row["c"], row["d"]) for row in asc] == [
        (None, True),
        (None, True),
        (1, True),
        (1, True),
        (2, True),
        (2, True),
        (3, False),
    ]
    desc = _dicts(frame.orderBy(F.col("c").desc()).select("c", mask))
    assert [(row["c"], row["d"]) for row in desc] == [
        (3, False),
        (2, True),
        (2, True),
        (1, True),
        (1, True),
        (None, True),
        (None, True),
    ]
    rp_mask = rp.col("c").is_duplicated().alias("d")
    rp_asc = _dicts(frame.pl.sort("c").select("c", rp_mask).spark)
    assert [(row["c"], row["d"]) for row in rp_asc] == [
        (None, True),
        (None, True),
        (1, True),
        (1, True),
        (2, True),
        (2, True),
        (3, False),
    ]
    rp_desc = _dicts(frame.pl.sort("c", descending=True).select("c", rp_mask).spark)
    assert [(row["c"], row["d"]) for row in rp_desc] == [
        (None, True),
        (None, True),
        (3, False),
        (2, True),
        (2, True),
        (1, True),
        (1, True),
    ]


def test_post_mask_sort_matches_plain_column(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    expected = [(3, False), (None, True), (None, True), (1, True), (1, True), (2, True), (2, True)]
    masked = frame.select("c", F.col("c").is_duplicated().alias("d")).orderBy("d", "c")
    assert [(row["c"], row["d"]) for row in _dicts(masked)] == expected
    widened = frame.withColumn("d", F.col("c").is_duplicated()).orderBy("d", "c")
    assert [(row["c"], row["d"]) for row in _dicts(widened)] == expected
    plain = session.createDataFrame(
        [(1, True), (2, True), (2, True), (None, True), (None, True), (3, False), (1, True)],
        "c INT, d BOOLEAN",
    ).orderBy("d", "c")
    assert [(row["c"], row["d"]) for row in _dicts(plain)] == expected


def test_post_mask_sort_constant_mask_keeps_second_key(session: ReparkSession) -> None:
    frame = session.createDataFrame([(2,), (None,), (1,)], "k INT")
    ordered = frame.select("k", F.col("k").is_duplicated().alias("d")).orderBy("d", "k")
    assert [(row["k"], row["d"]) for row in _dicts(ordered)] == [
        (None, False),
        (1, False),
        (2, False),
    ]


def test_post_mask_sort_second_key_survives(session: ReparkSession) -> None:
    frame = session.createDataFrame(
        [(1, "a"), (2, "b"), (2, "c"), (None, "d"), (1, "e"), (3, None)], "k INT, v STRING"
    )
    ordered = frame.select("k", "v", F.col("k").is_duplicated().alias("dk")).orderBy("dk", "v")
    assert [(row["k"], row["v"], row["dk"]) for row in _dicts(ordered)] == [
        (3, None, False),
        (None, "d", False),
        (1, "a", True),
        (2, "b", True),
        (2, "c", True),
        (1, "e", True),
    ]


def test_sorted_mask_with_limit_and_desc_nulls_last(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    limited = frame.orderBy("c").filter(F.col("c").is_duplicated()).limit(3)
    assert [row["c"] for row in _dicts(limited)] == [None, None, 1]
    nulls_last = frame.orderBy(F.col("c").desc_nulls_last()).filter(F.col("c").is_duplicated())
    assert [row["c"] for row in _dicts(nulls_last)] == [2, 2, 1, 1, None, None]


def test_sorted_mask_three_keys_and_sort_within_partitions(session: ReparkSession) -> None:
    frame = session.createDataFrame(
        [(1, "a", 5), (1, "a", 4), (2, "b", 1), (None, "a", 2), (1, "b", 3), (None, None, 6)],
        "a INT, b STRING, c INT",
    )
    tri = frame.orderBy("a", "b", "c").filter(F.col("a").is_duplicated())
    assert [(row["a"], row["b"], row["c"]) for row in _dicts(tri)] == [
        (None, None, 6),
        (None, "a", 2),
        (1, "a", 4),
        (1, "a", 5),
        (1, "b", 3),
    ]
    ints = _make_frame(session, "int")
    within = ints.sortWithinPartitions("c").filter(F.col("c").is_duplicated())
    assert [row["c"] for row in _dicts(within)] == [None, None, 1, 1, 2, 2]


def test_mask_after_repartition_matches_set_and_groupagg(session: ReparkSession) -> None:
    frame = _make_frame(session, "int")
    repart = [row["c"] for row in _dicts(frame.repartition(4).filter(F.col("c").is_duplicated()))]
    assert sorted(repart, key=repr) == sorted([1, 2, 2, None, None, 1], key=repr)
    grouped = frame.select("c", F.col("c").is_duplicated().alias("d")).groupBy("d").count()
    assert {(row["d"], row["count"]) for row in _dicts(grouped)} == {(True, 6), (False, 1)}
