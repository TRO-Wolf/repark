"""DISPLAY-BRIDGE-1 pins: a bridged frame's show() follows the display style (R-13).

pins: display-bridge-1/C-001, C-002, C-003
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import redirect_stdout
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark.dataframe import DataFrame
from repark.spark.session import _DISPLAY_STYLE_KEY

_SPARK_GRID_MIA = """\
+---+
| x |
+---+
| 2 |
| 4 |
| 6 |
+---+"""

_SPARK_GRID_MIA_VERTICAL = """\
-RECORD 0-
 x | 2
-RECORD 1-
 x | 4
-RECORD 2-
 x | 6"""


@pytest.fixture
def polars_session() -> Iterator[ReparkSession]:
    """Fresh session pinned to the polars display style via builder config."""
    session = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "polars").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def duckdb_session() -> Iterator[ReparkSession]:
    """Fresh session pinned to the duckdb display style via builder config."""
    session = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "duckdb").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def spark_session() -> Iterator[ReparkSession]:
    """Fresh session pinned to the spark display style via builder config."""
    session = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "spark").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _double_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """Double the ``x`` column of every batch (mapInArrow test bridge)."""
    for batch in batches:
        values = [None if item is None else int(item) * 2 for item in batch.column(0).to_pylist()]
        yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])


def _bridged_frame(session: ReparkSession, size: int) -> DataFrame:
    """Build an uncached mapInArrow frame of ``size`` doubled rows (the peek trigger)."""
    base = session.createDataFrame([(index,) for index in range(1, size + 1)], "x INT")
    return base.mapInArrow(_double_batches, "x INT")


def _capture_show(frame: object, *args: object, **kwargs: object) -> str:
    """Run ``frame.show(...)`` and return stdout without the trailing newline."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        cast(Any, frame).show(*args, **kwargs)
    return buffer.getvalue().removesuffix("\n")


def test_bridged_show_spark_grid_unchanged(spark_session: ReparkSession) -> None:
    """Under spark the bridged peek keeps the exact grid, horizontal and vertical."""
    frame = _bridged_frame(spark_session, 3)
    assert _capture_show(frame) == _SPARK_GRID_MIA
    assert _capture_show(frame, vertical=True) == _SPARK_GRID_MIA_VERTICAL


def test_bridged_show_matches_repr_polars(polars_session: ReparkSession) -> None:
    """A 12-row bridged frame's show() prints exactly the styled text repr returns."""
    frame = _bridged_frame(polars_session, 12)
    shown = _capture_show(frame)
    assert shown.startswith("shape: (12, 1)")
    assert "│ 2   │" in shown
    assert "│ 24  │" in shown
    assert "│ …   │" in shown
    assert repr(frame) == shown


def test_bridged_show_matches_repr_duckdb(duckdb_session: ReparkSession) -> None:
    """A 12-row bridged frame's show() prints exactly the styled box repr returns."""
    frame = _bridged_frame(duckdb_session, 12)
    shown = _capture_show(frame)
    assert "12 rows" in shown
    assert "(12 shown)" not in shown
    assert repr(frame) == shown


def test_bridged_small_peek_shape_exact(polars_session: ReparkSession) -> None:
    """A 3-row bridged frame's peek is the whole frame: exact shape, all rows, no ellipsis."""
    frame = _bridged_frame(polars_session, 3)
    shown = _capture_show(frame)
    assert shown.startswith("shape: (3, 1)")
    assert "│ 2   │" in shown
    assert "│ 4   │" in shown
    assert "│ 6   │" in shown
    assert "│ …   │" not in shown


def test_bridged_small_peek_shape_exact_duckdb(duckdb_session: ReparkSession) -> None:
    """A 3-row bridged frame renders the whole duckdb box exactly as repr does."""
    frame = _bridged_frame(duckdb_session, 3)
    shown = _capture_show(frame)
    assert "3 rows" in shown
    assert "(3 shown)" not in shown
    assert repr(frame) == shown


def test_bridged_show_pays_one_count_when_peek_full(polars_session: ReparkSession) -> None:
    """A 25-row bridged frame's peek fills: the renderer takes exactly one count."""
    frame = _bridged_frame(polars_session, 25)
    with patch.object(DataFrame, "count", autospec=True, side_effect=DataFrame.count) as spy:
        shown = _capture_show(frame)
    assert spy.call_count == 1
    assert shown.startswith("shape: (25, 1)")
    assert "│ 48  │" in shown
    assert "│ …   │" in shown


def test_bridged_small_peek_never_counts(polars_session: ReparkSession) -> None:
    """A 12-row bridged frame's peek (limit 20) is short: exact shape, zero count calls."""
    frame = _bridged_frame(polars_session, 12)
    with patch.object(DataFrame, "count", autospec=True, side_effect=DataFrame.count) as spy:
        shown = _capture_show(frame)
    assert spy.call_count == 0
    assert shown.startswith("shape: (12, 1)")


def test_bridged_styled_show_peeks_once(polars_session: ReparkSession) -> None:
    """A styled bridged show() peeks the bridge exactly once."""
    frame = _bridged_frame(polars_session, 12)
    with patch.object(
        DataFrame,
        "_consume_map_in_arrow_batches",
        autospec=True,
        side_effect=DataFrame._consume_map_in_arrow_batches,
    ) as spy:
        _capture_show(frame)
    assert spy.call_count == 1


def test_bridged_styled_vertical_warns(polars_session: ReparkSession) -> None:
    """vertical=True on a styled bridged frame keeps the warning and stays horizontal."""
    frame = _bridged_frame(polars_session, 3)
    with pytest.warns(UserWarning, match="stay horizontal"):
        shown = _capture_show(frame, vertical=True)
    assert "-RECORD" not in shown
    assert shown.startswith("shape: (3, 1)")
