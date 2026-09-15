"""Row.count / Row.index tuple-protocol pins against the committed row oracle. pins: row-tuple-1."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark.spark.row import Row

_ORACLE_DOCUMENT: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_row_oracle.json").read_text(encoding="utf-8")
)
_ORACLE_CELLS: dict[str, Any] = _ORACLE_DOCUMENT["cells"]
_ROW_CELL_IDS: tuple[str, ...] = (
    "count",
    "count_missing",
    "positional_count",
    "count_nan",
    "count_factory",
    "index",
    "index_start",
    "index_missing",
    "index_factory",
)


def _result(cell_id: str) -> Any:
    return _ORACLE_CELLS[cell_id]["result"]["value"]


def _error(cell_id: str) -> dict[str, Any]:
    return _ORACLE_CELLS[cell_id]["error"]


def test_count_reproduces_row_count() -> None:
    """Reproduces row/count: Row(a=1, b=2, c=1).count(1). pins: row-tuple-1/C-001."""
    assert Row(a=1, b=2, c=1).count(1) == _result("count")


def test_count_reproduces_row_count_missing() -> None:
    """Reproduces row/count_missing: Row(a=1, b=2).count(9). pins: row-tuple-1/C-001."""
    assert Row(a=1, b=2).count(9) == _result("count_missing")


def test_count_reproduces_row_positional_count() -> None:
    """Reproduces row/positional_count: Row(1, 1, None).count(None). pins: row-tuple-1/C-001."""
    assert Row(1, 1, None).count(None) == _result("positional_count")


def test_count_reproduces_row_count_nan() -> None:
    """Reproduces row/count_nan: distinct NaN objects never match. pins: row-tuple-1/C-001."""
    assert Row(a=float("nan")).count(float("nan")) == _result("count_nan")


def test_count_reproduces_row_count_factory() -> None:
    """Reproduces row/count_factory: factories count field names. pins: row-tuple-1/C-001."""
    assert Row("a", "b").count("a") == _result("count_factory")


def test_index_reproduces_row_index() -> None:
    """Reproduces row/index: Row(a=1, b=2, c=1).index(1). pins: row-tuple-1/C-002."""
    assert Row(a=1, b=2, c=1).index(1) == _result("index")


def test_index_reproduces_row_index_start() -> None:
    """Reproduces row/index_start: Row(a=1, b=2, c=1).index(1, 1). pins: row-tuple-1/C-002."""
    assert Row(a=1, b=2, c=1).index(1, 1) == _result("index_start")


def test_index_reproduces_row_index_factory() -> None:
    """Reproduces row/index_factory: factories index field names. pins: row-tuple-1/C-002."""
    assert Row("a", "b").index("b") == _result("index_factory")


def test_index_reproduces_row_index_missing() -> None:
    """Reproduces row/index_missing: the exact ValueError message. pins: row-tuple-1/C-002."""
    recorded = _error("index_missing")
    with pytest.raises(ValueError) as caught:
        Row(a=1, b=2).index(9)
    assert type(caught.value) is ValueError
    assert str(caught.value) == recorded["message"]


def test_oracle_fixture_carries_the_row_cells() -> None:
    """The committed oracle holds the nine row cells. pins: row-tuple-1/C-003."""
    assert set(_ORACLE_CELLS) >= set(_ROW_CELL_IDS)
    assert "PySpark 4.1.2" in _ORACLE_DOCUMENT["oracle"]


def test_index_stop_and_negative_start_match_tuple() -> None:
    """L-001: index stop/negative-start tuple bounds. pins: row-tuple-1/C-004."""
    with pytest.raises(ValueError) as caught:
        Row(a=1, b=2, c=1).index(1, 0, 0)
    assert str(caught.value) == _error("index_missing")["message"]
    assert Row(a=1, b=2, c=1).index(1, 0, 1) == 0
    assert Row(a=1, b=2, c=1).index(1, -1) == 2


def test_names_and_values_are_never_searched_together() -> None:
    """L-002: names+values are never searched together. pins: row-tuple-1/C-004."""
    assert Row(a="a", b=1).count("a") == 1
    with pytest.raises(ValueError) as caught:
        Row(x="a").index("x")
    assert str(caught.value) == _error("index_missing")["message"]


def test_method_wins_attribute_access_item_answers_the_field() -> None:
    """L-003: the method wins attribute access; item access answers. pins: row-tuple-1/C-004."""
    assert callable(Row(count=3).count)
    assert Row(count=3)["count"] == 3
    assert callable(Row(index=7).index)
    assert Row(index=7)["index"] == 7


def test_collected_count_column_method_wins_and_item_answers() -> None:
    """L-003: a collected count column: item access + callable method. pins: row-tuple-1/C-004."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("pytest-row-tuple-1-groupby-count").getOrCreate()
    try:
        row = spark.createDataFrame([("a", 1)], ["g", "n"]).groupBy("g").count().collect()[0]
        assert row["count"] == 1
        assert callable(row.count)
    finally:
        spark.stop()


def test_collected_nested_struct_counts_as_dict_not_row() -> None:
    """L-004: EX-ROW-1 — the collected struct cell is a dict. pins: row-tuple-1/C-004."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("pytest-row-tuple-1-nested-struct").getOrCreate()
    try:
        row = spark.sql("SELECT named_struct('x', 1, 'y', 2) AS st").collect()[0]
        assert row.count({"x": 1, "y": 2}) == 1
        assert row.count(Row(x=1, y=2)) == 0
    finally:
        spark.stop()


def test_index_and_count_reject_keyword_arguments() -> None:
    """L-005 / R-3: keyword calls raise TypeError like tuple. pins: row-tuple-1/C-004."""
    with pytest.raises(TypeError):
        Row(a=1).index(1, start=0)
    with pytest.raises(TypeError):
        Row(a=1).count(value=1)
