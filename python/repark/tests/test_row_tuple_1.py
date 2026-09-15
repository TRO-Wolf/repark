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
