"""DFCORE-4b show golden pins: byte-identical show output across the display move.

Every expected string below was recorded on the pre-slice tree; the move-only
relocation of ``show`` and its helpers into ``display.py`` must reproduce each
one byte-identically, with the same ``count()`` tallies.
"""

from __future__ import annotations

import io
import warnings
from collections.abc import Iterator
from contextlib import redirect_stdout

import pytest

from repark import ReparkSession
from repark.spark.dataframe import DataFrame

_MIXED_ROWS: list[tuple[int, str | None, float | None, bool | None]] = [
    (1, "alpha", 1.5, True),
    (2, "beta-longer-than-twenty-chars", 2.25, False),
    (3, None, None, None),
    (4, "delta", -0.5, True),
    (5, "echo", 100.125, False),
]

_MIXED_SCHEMA = "id INT, name STRING, score DOUBLE, flag BOOLEAN"

_MIXED_DEFAULT = """\
+----+----------------------+---------+-------+
| id | name                 | score   | flag  |
+----+----------------------+---------+-------+
| 1  | alpha                | 1.5     | true  |
| 2  | beta-longer-than-... | 2.25    | false |
| 3  | NULL                 | NULL    | NULL  |
| 4  | delta                | -0.5    | true  |
| 5  | echo                 | 100.125 | false |
+----+----------------------+---------+-------+"""

_MIXED_TRUNC_FALSE = """\
+----+-------------------------------+---------+-------+
| id | name                          | score   | flag  |
+----+-------------------------------+---------+-------+
| 1  | alpha                         | 1.5     | true  |
| 2  | beta-longer-than-twenty-chars | 2.25    | false |
| 3  | NULL                          | NULL    | NULL  |
| 4  | delta                         | -0.5    | true  |
| 5  | echo                          | 100.125 | false |
+----+-------------------------------+---------+-------+"""

_MIXED_TRUNC_5 = """\
+----+-------+-------+-------+
| id | name  | score | flag  |
+----+-------+-------+-------+
| 1  | alpha | 1.5   | true  |
| 2  | be... | 2.25  | false |
| 3  | NULL  | NULL  | NULL  |
| 4  | delta | -0.5  | true  |
| 5  | echo  | 10... | false |
+----+-------+-------+-------+"""

_MIXED_VERTICAL = """\
-RECORD 0---------------------
 id    | 1
 name  | alpha
 score | 1.5
 flag  | true
-RECORD 1---------------------
 id    | 2
 name  | beta-longer-than-...
 score | 2.25
 flag  | false
-RECORD 2---------------------
 id    | 3
 name  | NULL
 score | NULL
 flag  | NULL
-RECORD 3---------------------
 id    | 4
 name  | delta
 score | -0.5
 flag  | true
-RECORD 4---------------------
 id    | 5
 name  | echo
 score | 100.125
 flag  | false"""

_MIXED_N0 = """\
+----+------+-------+------+
| id | name | score | flag |
+----+------+-------+------+
+----+------+-------+------+"""

_MIXED_N1 = """\
+----+-------+-------+------+
| id | name  | score | flag |
+----+-------+-------+------+
| 1  | alpha | 1.5   | true |
+----+-------+-------+------+"""

_MIXED_N5 = """\
+----+----------------------+---------+-------+
| id | name                 | score   | flag  |
+----+----------------------+---------+-------+
| 1  | alpha                | 1.5     | true  |
| 2  | beta-longer-than-... | 2.25    | false |
| 3  | NULL                 | NULL    | NULL  |
| 4  | delta                | -0.5    | true  |
| 5  | echo                 | 100.125 | false |
+----+----------------------+---------+-------+"""

_MIXED_N6 = """\
+----+----------------------+---------+-------+
| id | name                 | score   | flag  |
+----+----------------------+---------+-------+
| 1  | alpha                | 1.5     | true  |
| 2  | beta-longer-than-... | 2.25    | false |
| 3  | NULL                 | NULL    | NULL  |
| 4  | delta                | -0.5    | true  |
| 5  | echo                 | 100.125 | false |
+----+----------------------+---------+-------+"""

_MIXED_VERTICAL_N1 = """\
-RECORD 0------
 id    | 1
 name  | alpha
 score | 1.5
 flag  | true
only showing top 1 row"""

_EMPTY_SHOW = """\
+----+------+
| id | name |
+----+------+
+----+------+"""

_EMPTY_VERTICAL = ""

_UNI_SHOW = """\
+----+--------------+
| id | text         |
+----+--------------+
| 1  | héllo→world✓ |
| 2  | NULL         |
| 3  | 日本語          |
+----+--------------+"""

_UNI_VERTICAL = """\
-RECORD 0------------
 id   | 1
 text | héllo→world✓
-RECORD 1------------
 id   | 2
 text | NULL
-RECORD 2------------
 id   | 3
 text | 日本語"""

_STYLED_POLARS_12 = """\
shape: (12, 2)
┌─────┬─────┐
│ id  ┆ v   │
│ --- ┆ --- │
│ i32 ┆ str │
╞═════╪═════╡
│ 1   ┆ v1  │
│ 2   ┆ v2  │
│ 3   ┆ v3  │
│ 4   ┆ v4  │
│ 5   ┆ v5  │
│ …   ┆ …   │
│ 8   ┆ v8  │
│ 9   ┆ v9  │
│ 10  ┆ v10 │
│ 11  ┆ v11 │
│ 12  ┆ v12 │
└─────┴─────┘"""

_STYLED_POLARS_N3 = """\
shape: (12, 2)
┌─────┬─────┐
│ id  ┆ v   │
│ --- ┆ --- │
│ i32 ┆ str │
╞═════╪═════╡
│ 1   ┆ v1  │
│ 2   ┆ v2  │
│ …   ┆ …   │
│ 12  ┆ v12 │
└─────┴─────┘"""

_STYLED_POLARS_N0 = """\
shape: (12, 2)
┌─────┬─────┐
│ id  ┆ v   │
│ --- ┆ --- │
│ i32 ┆ str │
╞═════╪═════╡
└─────┴─────┘"""

_STYLED_DUCKDB_12 = """\
┌───────┬─────────┐
│   id  │    v    │
│ int32 │ varchar │
├───────┼─────────┤
│     1 │ v1      │
│     2 │ v2      │
│     3 │ v3      │
│     4 │ v4      │
│     5 │ v5      │
│     6 │ v6      │
│     7 │ v7      │
│     8 │ v8      │
│     9 │ v9      │
│    10 │ v10     │
│    11 │ v11     │
│    12 │ v12     │
├───────┴─────────┤
│     12 rows     │
└─────────────────┘"""

_STYLED_DUCKDB_N3 = """\
┌───────┬─────────┐
│   id  │    v    │
│ int32 │ varchar │
├───────┼─────────┤
│     1 │ v1      │
│   ·   │    ·    │
│   ·   │    ·    │
│   ·   │    ·    │
│    11 │ v11     │
│    12 │ v12     │
├───────┴─────────┤
│     12 rows     │
│    (3 shown)    │
└─────────────────┘"""

_STYLED_DUCKDB_N0 = """\
┌───────┬─────────┐
│   id  │    v    │
│ int32 │ varchar │
├───────┼─────────┤
│     12 rows     │
│    (0 shown)    │
└─────────────────┘"""


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Fresh session per test (default spark display style)."""
    session = ReparkSession.builder.appName("dfcore4b-show").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _capture_show(frame: DataFrame, *args: object, **kwargs: object) -> str:
    """Run ``frame.show(...)`` and return stdout without the trailing newline."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        frame.show(*args, **kwargs)  # type: ignore[arg-type]
    return buffer.getvalue().removesuffix("\n")


def test_show_mixed_truncate_shapes(spark: ReparkSession) -> None:
    """Default, full-width, and width-5 truncation match the pre-slice output."""
    frame = spark.createDataFrame(_MIXED_ROWS, _MIXED_SCHEMA)
    assert _capture_show(frame) == _MIXED_DEFAULT
    assert _capture_show(frame, truncate=False) == _MIXED_TRUNC_FALSE
    assert _capture_show(frame, truncate=5) == _MIXED_TRUNC_5


def test_show_mixed_n_shapes(spark: ReparkSession) -> None:
    """Zero, one, exact-N, and over-N row windows match the pre-slice output."""
    frame = spark.createDataFrame(_MIXED_ROWS, _MIXED_SCHEMA)
    assert _capture_show(frame, 0) == _MIXED_N0
    assert _capture_show(frame, 1) == _MIXED_N1
    assert _capture_show(frame, 5) == _MIXED_N5
    assert _capture_show(frame, 6) == _MIXED_N6


def test_show_vertical_shapes(spark: ReparkSession) -> None:
    """Vertical record layout matches the pre-slice output, full and n=1."""
    frame = spark.createDataFrame(_MIXED_ROWS, _MIXED_SCHEMA)
    assert _capture_show(frame, vertical=True) == _MIXED_VERTICAL
    assert _capture_show(frame, 1, vertical=True) == _MIXED_VERTICAL_N1


def test_show_empty_and_unicode_frames(spark: ReparkSession) -> None:
    """Empty frames and unicode/NULL cells match the pre-slice output."""
    empty = spark.createDataFrame([], "id INT, name STRING")
    assert _capture_show(empty) == _EMPTY_SHOW
    assert _capture_show(empty, vertical=True) == _EMPTY_VERTICAL
    uni = spark.createDataFrame(
        [(1, "héllo→world✓"), (2, None), (3, "日本語")],
        "id INT, text STRING",
    )
    assert _capture_show(uni) == _UNI_SHOW
    assert _capture_show(uni, vertical=True) == _UNI_VERTICAL


def test_show_styled_shapes(spark: ReparkSession) -> None:
    """Polars and duckdb styles match the pre-slice output at full, 3, and 0 rows."""
    for style in ("polars", "duckdb"):
        spark.display_style = style
        frame = spark.createDataFrame(
            [(index, f"v{index}") for index in range(1, 13)], "id INT, v STRING"
        )
        if style == "polars":
            assert _capture_show(frame) == _STYLED_POLARS_12
            assert _capture_show(frame, 3) == _STYLED_POLARS_N3
            assert _capture_show(frame, 0) == _STYLED_POLARS_N0
        else:
            assert _capture_show(frame) == _STYLED_DUCKDB_12
            assert _capture_show(frame, 3) == _STYLED_DUCKDB_N3
            assert _capture_show(frame, 0) == _STYLED_DUCKDB_N0


def test_show_count_tallies(
    spark: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Vertical-full show counts once; plain show never counts (pre-slice tallies)."""
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    calls: list[str] = []
    original = DataFrame.count

    def counting(self: DataFrame) -> int:
        calls.append("count")
        return original(self)

    monkeypatch.setattr(DataFrame, "count", counting)
    _capture_show(frame, 2, vertical=True)
    assert len(calls) == 1
    calls.clear()
    _capture_show(frame, 2)
    assert calls == []


def test_show_styled_vertical_warning_attributes_to_caller(spark: ReparkSession) -> None:
    """The styled-vertical warning names the calling file, not the display module."""
    ReparkSession.builder.config("repark.display.style", "polars").getOrCreate()
    frame = spark.createDataFrame(_MIXED_ROWS, _MIXED_SCHEMA)
    try:
        with warnings.catch_warnings(record=True) as caught, redirect_stdout(io.StringIO()):
            warnings.simplefilter("always", UserWarning)
            frame.show(vertical=True)
    finally:
        ReparkSession.builder.config("repark.display.style", "spark").getOrCreate()
    vertical = [item for item in caught if "stay horizontal" in str(item.message)]
    assert len(vertical) == 1
    assert vertical[0].filename == __file__
