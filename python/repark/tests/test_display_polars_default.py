"""DISPLAY-POLARS-1 steps 2 and 3 pins: styled show fetch discipline and styled repr doors.

pins: display-polars-1/C-003, display-polars-1/C-004
pins: display-lazy-1/C-001, C-002
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import redirect_stdout
from typing import Any, cast

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException
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
    """Lazy repr is the schema header; eager repr is exactly the table show() prints."""
    frame = polars_session.sql(_ORDERED_7_SQL)
    shown = _capture_show(frame)
    assert shown.startswith("shape: (7, 1)")
    lazy = repr(frame)
    assert lazy.splitlines()[0].startswith("lazy: ")
    assert "│ 1   │" not in lazy
    assert repr(frame.eager()) == shown


def test_duckdb_repr_renders_table(duckdb_session: ReparkSession) -> None:
    """Lazy repr is the schema header; eager repr is exactly the box show() prints."""
    frame = duckdb_session.sql(_ORDERED_7_SQL)
    shown = _capture_show(frame)
    assert "7 rows" in shown
    lazy = repr(frame)
    assert lazy.splitlines()[0].startswith("lazy: ")
    assert "7 rows" not in lazy
    assert repr(frame.eager()) == shown


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
    """A lazy 7-row frame's repr is the schema header with zero count calls."""
    calls = _install_count_spy(monkeypatch)
    frame = polars_session.sql(_ORDERED_7_SQL)
    out = repr(frame)
    assert calls == []
    assert out.splitlines()[0].startswith("lazy: ")
    assert "shape:" not in out
    assert "│ 1   │" not in out
    assert "│ 7   │" not in out


_ORACLE_SQL_INTS_STRINGS_NULLS = """\
SELECT ord, i, b, s, u, long_s, st, li FROM (VALUES
 (0, CAST(1 AS INT), CAST(200000 AS BIGINT), 'a', CAST(NULL AS VARCHAR),
  'abcdefghijklmnopqrstuvwxyz0123456789', struct(1 AS a, 'x' AS b), array(1, 2, 3)),
 (1, CAST(NULL AS INT), CAST(NULL AS BIGINT), 'bb', 'hi',
  'short', struct(2 AS a, 'yy' AS b), CAST(NULL AS ARRAY<INT>)),
 (2, CAST(-7 AS INT), CAST(3 AS BIGINT), CAST(NULL AS VARCHAR), 'zz',
  'tiny', CAST(NULL AS STRUCT<a INT, b VARCHAR>), array(9))
) AS t(ord, i, b, s, u, long_s, st, li) ORDER BY ord
"""

_ORACLE_SQL_FLOATS_BOOLS_DATES = """\
SELECT ord, f, b, d FROM (VALUES
 (0, CAST(1.5 AS DOUBLE), true, CAST('2026-01-02' AS DATE)),
 (1, CAST(-2.5 AS DOUBLE), false, CAST('2026-12-31' AS DATE)),
 (2, CAST(0.1 AS DOUBLE), CAST(NULL AS BOOLEAN), CAST(NULL AS DATE)),
 (3, CAST(100.5 AS DOUBLE), true, CAST('2026-06-15' AS DATE)),
 (4, CAST(3.14159 AS DOUBLE), false, CAST('2026-01-02' AS DATE)),
 (5, CAST('NaN' AS DOUBLE), true, CAST('2026-03-04' AS DATE)),
 (6, CAST(NULL AS DOUBLE), false, CAST('2026-05-06' AS DATE)),
 (7, CAST(1234567890.0 AS DOUBLE), true, CAST('2026-07-08' AS DATE))
) AS t(ord, f, b, d) ORDER BY ord
"""

_ORACLE_WIDE_SQL = """\
SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8 FROM (VALUES
 (1, 2, 3, 4, 5, 6, 7, 8, 9),
 (10, 20, 30, 40, 50, 60, 70, 80, 90)
) AS t(c0, c1, c2, c3, c4, c5, c6, c7, c8)
"""


def _polars_text_of(frame: Any, monkeypatch: pytest.MonkeyPatch) -> str:
    """Render the frame's own Arrow output through polars with default config."""
    polars = pytest.importorskip("polars")
    monkeypatch.delenv("POLARS_FMT_STR_LEN", raising=False)
    monkeypatch.delenv("POLARS_FMT_MAX_COLS", raising=False)
    return str(polars.from_arrow(frame.to_arrow()))


def test_polars_oracle_ints_strings_nulls(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lazy repr is the header; eager repr matches polars itself on nested cells."""
    frame = polars_session.sql(_ORACLE_SQL_INTS_STRINGS_NULLS)
    assert repr(frame).splitlines()[0].startswith("lazy: ")
    assert "{" not in repr(frame)
    assert repr(frame.eager()) == _polars_text_of(frame, monkeypatch)


def test_polars_oracle_floats_bools_dates(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lazy repr is the header; eager repr matches polars itself on dates."""
    frame = polars_session.sql(_ORACLE_SQL_FLOATS_BOOLS_DATES)
    assert repr(frame).splitlines()[0].startswith("lazy: ")
    assert repr(frame.eager()) == _polars_text_of(frame, monkeypatch)


def test_polars_oracle_wide_frame_col_ellipsis(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 9-column lazy frame elides the header; eager matches polars itself."""
    frame = polars_session.sql(_ORACLE_WIDE_SQL)
    lazy = repr(frame)
    assert lazy.splitlines()[0].startswith("lazy: ")
    assert "c4" not in lazy
    assert repr(frame.eager()) == _polars_text_of(frame, monkeypatch)


def test_str_len_cuts_with_ellipsis(polars_session: ReparkSession) -> None:
    """Eager cells longer than str_len keep str_len characters plus one ellipsis."""
    assert polars_session.conf.get("repark.display.str_len") == "30"
    frame = polars_session.sql("SELECT 'abcdefghijklmnopqrstuvwxyz0123456789' AS s")
    eager = frame.eager()
    assert "abcdefghijklmnopqrstuvwxyz0123…" in repr(eager)
    assert "abcdefghijklmnopqrstuvwxyz0123456789" not in repr(eager)
    assert "…" not in repr(frame)
    shown = _capture_show(frame, truncate=False)
    assert "abcdefghijklmnopqrstuvwxyz0123456789" in shown
    shown_ten = _capture_show(frame, truncate=10)
    assert "abcdefghij…" in shown_ten
    assert "abcdefghijk" not in shown_ten
    polars_session.conf.set("repark.display.str_len", "4")
    try:
        assert "abcd…" in repr(eager)
        assert "abcde" not in repr(eager)
    finally:
        polars_session.conf.unset("repark.display.str_len")
    assert polars_session.conf.get("repark.display.str_len") == "30"


def test_display_keys_conf_get_set(polars_session: ReparkSession) -> None:
    """The four display keys read defaults, round-trip set/unset, and refuse bad values."""
    assert polars_session.conf.get("repark.display.style") == "polars"
    assert polars_session.conf.get("repark.display.max_rows") == "10"
    assert polars_session.conf.get("repark.display.max_cols") == "8"
    assert polars_session.conf.get("repark.display.str_len") == "30"
    frame = polars_session.sql(_ORDERED_7_SQL)
    polars_session.conf.set("repark.display.max_rows", "4")
    try:
        out = _capture_show(frame, truncate=False)
        assert out.startswith("shape: (7, 1)")
        assert "│ 1   │" in out
        assert "│ 2   │" in out
        assert "│ 3   │" not in out
        assert "│ 6   │" in out
        assert "│ 7   │" in out
        assert "│ …   │" in out
    finally:
        polars_session.conf.unset("repark.display.max_rows")
    assert polars_session.conf.get("repark.display.max_rows") == "10"
    wide = polars_session.sql("SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d, 5 AS e, 6 AS f")
    polars_session.conf.set("repark.display.max_cols", 4)
    try:
        wide_out = repr(wide.eager())
        assert wide_out.startswith("shape: (1, 6)")
        assert "│ a   ┆ b   ┆ … ┆ e   ┆ f   │" in wide_out
        assert "│ 1   ┆ 2   ┆ … ┆ 5   ┆ 6   │" in wide_out
        assert "┆ c " not in wide_out
        assert "┆ d " not in wide_out
        lazy_out = repr(wide)
        assert lazy_out.splitlines()[0].startswith("lazy: ")
        assert "│ a   ┆ b   ┆ … ┆ e   ┆ f   │" in lazy_out
        assert "│ 1   ┆ 2   ┆ … ┆ 5   ┆ 6   │" not in lazy_out
        assert "┆ c " not in lazy_out
        assert "┆ d " not in lazy_out
    finally:
        polars_session.conf.unset("repark.display.max_cols")
    for bad in ("0", "-1", "abc", "10.5", ""):
        with pytest.raises(IllegalArgumentException, match=r"repark\.display"):
            polars_session.conf.set("repark.display.max_rows", bad)
    with pytest.raises(IllegalArgumentException, match=r"repark\.display"):
        polars_session.conf.set("repark.display.max_cols", True)
    with pytest.raises(IllegalArgumentException, match=r"repark\.display"):
        ReparkSession.builder.config("repark.display.str_len", "bogus").getOrCreate()
