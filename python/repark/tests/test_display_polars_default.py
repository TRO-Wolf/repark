"""DISPLAY-POLARS-1 steps 2 and 3 pins: styled show fetch discipline and styled repr doors.

pins: display-polars-1/C-003, display-polars-1/C-004
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import redirect_stdout
from typing import Any, cast

import pytest

from repark import ReparkSession
from repark.spark.session import _DISPLAY_STYLE_KEY

_ORDERED_7_SQL = """\
SELECT id FROM (VALUES (1), (2), (3), (4), (5), (6), (7)) AS t(id) ORDER BY id
"""

_ORDERED_12_SQL = """\
SELECT id FROM (VALUES
 (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)
) AS t(id) ORDER BY id
"""

_SPARK_REPR_SCHEMA = "DataFrame[a: int, b: string]"

_SPARK_REPR_EAGER_GRID = """\
+-+-+
|a|b|
+-+-+
|1|x|
+-+-+"""

_SPARK_REPR_EAGER_HTML = """\
<table border='1'>
<tr><th>a</th><th>b</th></tr>
<tr><td>1</td><td>x</td></tr>
</table>"""


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


def _session_with_style(style: str) -> ReparkSession:
    """Build a fresh session with the given display style via builder config."""
    return ReparkSession.builder.config(_DISPLAY_STYLE_KEY, style).getOrCreate()


def _capture_show(frame: object, *args: object, **kwargs: object) -> str:
    """Run ``frame.show(...)`` and return stdout without the trailing newline."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        cast(Any, frame).show(*args, **kwargs)
    return buffer.getvalue().removesuffix("\n")


def _install_count_spy(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Patch ``DataFrame.count`` to append one entry per call and return the call log."""
    from repark import dataframe as dataframe_module

    calls: list[int] = []
    original = dataframe_module.DataFrame.count

    def counting_count(frame_arg: Any) -> int:
        calls.append(1)
        return cast(int, original(frame_arg))

    monkeypatch.setattr(dataframe_module.DataFrame, "count", counting_count)
    return calls


def test_small_frame_renders_without_count(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 7-row frame renders whole through the probe fetch with no count and no tail fetch."""
    calls = _install_count_spy(monkeypatch)
    frame = polars_session.sql(_ORDERED_7_SQL)
    out = _capture_show(frame, truncate=False)
    assert calls == []
    assert out.startswith("shape: (7, 1)")
    assert "│ 1   │" in out
    assert "│ 7   │" in out
    assert "│ …   │" not in out


def test_large_frame_counts_once(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 12-row frame pays exactly one count, one tail fetch, and keeps the ellipsis shape."""
    calls = _install_count_spy(monkeypatch)
    frame = polars_session.sql(_ORDERED_12_SQL)
    out = _capture_show(frame, truncate=False)
    assert calls == [1]
    assert out.startswith("shape: (12, 1)")
    assert "│ 1   │" in out
    assert "│ 12  │" in out
    assert "│ …   │" in out
    assert "│ 6   │" not in out
    assert "│ 7   │" not in out


def test_polars_repr_renders_table_without_eager_eval(polars_session: ReparkSession) -> None:
    """repr under polars returns exactly the table show() prints, eager eval unset."""
    frame = polars_session.sql(_ORDERED_7_SQL)
    shown = _capture_show(frame)
    assert shown.startswith("shape: (7, 1)")
    assert repr(frame) == shown


def test_duckdb_repr_renders_table(duckdb_session: ReparkSession) -> None:
    """repr under duckdb returns exactly the styled box show() prints, eager eval unset."""
    frame = duckdb_session.sql(_ORDERED_7_SQL)
    shown = _capture_show(frame)
    assert "7 rows" in shown
    assert repr(frame) == shown


def test_spark_repr_unchanged(spark_session: ReparkSession) -> None:
    """repr under spark keeps the schema form without eager eval and the grid with it."""
    frame = spark_session.sql("SELECT 1 AS a, 'x' AS b")
    assert repr(frame) == _SPARK_REPR_SCHEMA
    spark_session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    try:
        assert repr(frame) == _SPARK_REPR_EAGER_GRID
        assert frame._repr_html_() == _SPARK_REPR_EAGER_HTML
    finally:
        spark_session.conf.set("spark.sql.repl.eagerEval.enabled", "false")


def test_styled_repr_html_is_none() -> None:
    """_repr_html_ returns None under polars and duckdb with eager eval on and off."""
    for style in ("polars", "duckdb"):
        session = _session_with_style(style)
        try:
            frame = session.sql(_ORDERED_7_SQL)
            session.conf.set("spark.sql.repl.eagerEval.enabled", "false")
            assert frame._repr_html_() is None
            session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
            assert frame._repr_html_() is None
        finally:
            session.stop()
    session = _session_with_style("spark")
    try:
        frame = session.sql("SELECT 1 AS a, 'x' AS b")
        session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
        html = frame._repr_html_()
        assert html is not None
        assert html.startswith("<table border='1'>")
    finally:
        session.stop()


def test_small_frame_repr_does_not_count(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 7-row frame's repr renders whole through the probe fetch with zero count calls."""
    calls = _install_count_spy(monkeypatch)
    frame = polars_session.sql(_ORDERED_7_SQL)
    out = repr(frame)
    assert calls == []
    assert out.startswith("shape: (7, 1)")
    assert "│ 1   │" in out
    assert "│ 7   │" in out
    assert "│ …   │" not in out
