"""R-DISPLAY: opt-in ``DataFrame.show()`` styles (``spark`` / ``polars`` / ``duckdb``).

Default remains PySpark-parity (byte-identical to the pre-style ASCII grid). Styles are selected
via ``repark.display.style`` builder config or the runtime ``session.display_style`` attribute.

Oracle notes (measured 2026-07-28, not from memory):

* **spark (repark pre-change / this unit default):** ``+---+`` grid, ``NULL`` for nulls, cell
  truncate with ``...`` — existing ``test_session.py::test_show_*`` pins stay green.
* **polars 1.32.3:** ``shape: (rows, cols)`` + box with ``┆`` separators, ``---`` underline,
  dtype row (``i64``/``str``/…), first 5 / last 5 with ``…`` when rows > 10, ``null`` / ``NaN``.
* **duckdb 1.3.1 ``Relation.show()``:** box-drawing with type row under header, numeric right-
  align, ``NULL`` / ``nan``, footer ``N rows`` and ``(K shown)`` when truncated with middle ``·``.

Head+tail styles call ``count()`` (extra scan, disclosed on ``show`` docstring), ``limit`` for
the head, and ``_preview_tail_rows`` (engine-side skip+fetch) for the tail — never a full-table
collect for the preview path.

``n`` is a keep-set cap on every style: duckdb ``show(1)`` keeps the first row (not last-only);
polars ``show(0)``/``show(k)`` must not over-show (edges still never enlarge past 5).

MUTATION: force default style to polars → ``test_default_show_byte_identical_spark_grid`` reds.
MUTATION: drop head+tail on polars → ``test_polars_style_head_tail_golden`` reds (rows 6/7 appear).
MUTATION: full materialize+slice / skip limit_with_skip / root ``pa.table(self)`` /
  ``pa.table(self._inner)`` native stream → ``test_styled_show_does_not_full_collect`` +
  ``test_preview_tail_rows_uses_limit_with_skip`` red (incl. decoy ``limit_with_skip`` +
  root-native export; C5-Q-001).
MUTATION: drop reuse-path style apply → ``test_get_or_create_reuse_applies_display_style`` reds.
MUTATION: warn on pure display-style reuse / skip ``_builder_config`` sync →
  ``test_get_or_create_reuse_display_style_no_false_config_warning`` red (C6-Q-001).
MUTATION: delete case-insensitive key loop / reuse ``key.lower()`` →
  ``test_builder_display_style_key_case_insensitive`` +
  ``test_get_or_create_reuse_display_style_key_case_insensitive`` red (C6-Q-002).
MUTATION: dual-cased display.style first/exact-wins (not last-wins) →
  ``test_builder_display_style_dual_case_last_wins`` red; skip-validate later invalid alias →
  ``test_builder_display_style_dual_case_invalid_last_refuses`` red (C7-Q-001 / C7-L-001).
MUTATION: drop ``total <= fetch`` short-circuit in ``_preview_tail_rows`` →
  ``test_preview_tail_rows_total_le_fetch_uses_limit_not_negative_skip`` red (C7-Q-002).
MUTATION: int ``truncate<=0`` passed through as cap →
  ``test_show_truncate_non_positive_means_no_truncation`` red (C6-L-001).
MUTATION: ``str(bool)`` cells → ``test_styled_show_boolean_lowercase`` reds.
MUTATION: logical_schema collapsed types → ``test_styled_show_narrow_arrow_type_labels`` reds.
MUTATION: duckdb show(0) footer uses total without ``(0 shown)`` →
  ``test_duckdb_style_show_zero_footer_reports_zero_shown`` red.
MUTATION: duckdb show(1) ``use_ellipsis=True`` with ``tail_n=0`` →
  ``test_duckdb_style_show_one_keeps_first_row`` reds on body ``·`` (C8-Q-001).
MUTATION: ``int(bool)`` coerce for ``show(n)`` → ``test_show_rejects_bool_n`` red (C8-L-001).
MUTATION: public ``DataFrame.tail`` → ``test_no_public_dataframe_tail`` red.
"""

from __future__ import annotations

import io
import warnings
from collections.abc import Iterator
from contextlib import redirect_stdout

import pytest

from repark import ReparkSession
from repark.dataframe import DataFrame
from repark.errors import IllegalArgumentException, PySparkTypeError
from repark.session import (
    _DEFAULT_DISPLAY_STYLE,
    _DISPLAY_STYLE_KEY,
    normalize_display_style,
)

# =============================================================================================
# Fixtures + live-captured goldens (repark 2026-07-28, pin what we ship)
# =============================================================================================

_ORDERED_12_SQL = """\
SELECT id, label FROM (VALUES
 (1, 'x1'), (2, 'x2'), (3, 'x3'), (4, 'x4'), (5, 'x5'), (6, 'x6'),
 (7, 'x7'), (8, 'x8'), (9, 'x9'), (10, 'x10'), (11, 'x11'), (12, 'x12')
) AS t(id, label) ORDER BY id
"""

_SPARK_BASIC_GOLDEN = """\
+---+---+
| a | b |
+---+---+
| 1 | x |
+---+---+"""

_SPARK_NULL_GOLDEN = """\
+------+----+
| n    | s  |
+------+----+
| NULL | hi |
+------+----+"""

_POLARS_12_GOLDEN = """\
shape: (12, 2)
┌─────┬───────┐
│ id  ┆ label │
│ --- ┆ ---   │
│ i64 ┆ str   │
╞═════╪═══════╡
│ 1   ┆ x1    │
│ 2   ┆ x2    │
│ 3   ┆ x3    │
│ 4   ┆ x4    │
│ 5   ┆ x5    │
│ …   ┆ …     │
│ 8   ┆ x8    │
│ 9   ┆ x9    │
│ 10  ┆ x10   │
│ 11  ┆ x11   │
│ 12  ┆ x12   │
└─────┴───────┘"""

_POLARS_EMPTY_GOLDEN = """\
shape: (0, 1)
┌─────┐
│ id  │
│ --- │
│ i64 │
╞═════╡
└─────┘"""

_POLARS_ONE_ROW_GOLDEN = """\
shape: (1, 1)
┌─────┐
│ id  │
│ --- │
│ i64 │
╞═════╡
│ 1   │
└─────┘"""

_POLARS_NULL_NAN_GOLDEN = """\
shape: (3, 3)
┌──────┬─────┬──────┐
│ i    ┆ f   ┆ s    │
│ ---  ┆ --- ┆ ---  │
│ i64  ┆ f64 ┆ str  │
╞══════╪═════╪══════╡
│ 1    ┆ 1.0 ┆ a    │
│ null ┆ NaN ┆ null │
│ 3    ┆ 2.5 ┆ c    │
└──────┴─────┴──────┘"""

_DUCKDB_SMALL_GOLDEN = """\
┌───────┬─────────┐
│   id  │  label  │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ x1      │
│     2 │ x2      │
├───────┴─────────┤
│      2 rows     │
└─────────────────┘"""

_DUCKDB_ELLIPSIS_GOLDEN = """\
┌───────┬─────────┐
│   id  │  label  │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ x1      │
│     2 │ x2      │
│   ·   │    ·    │
│   ·   │    ·    │
│   ·   │    ·    │
│    11 │ x11     │
│    12 │ x12     │
├───────┴─────────┤
│     12 rows     │
│    (4 shown)    │
└─────────────────┘"""

_DUCKDB_ONE_ROW_GOLDEN = """\
┌──────────┐
│    id    │
│  int64   │
├──────────┤
│        1 │
├──────────┤
│  1 rows  │
└──────────┘"""

_DUCKDB_EMPTY_GOLDEN = """\
┌──────────┐
│    id    │
│  int64   │
├──────────┤
│  0 rows  │
└──────────┘"""

_DUCKDB_NULL_GOLDEN = """\
┌───────┬────────┐
│   n   │   f    │
│ int32 │ double │
├───────┼────────┤
│ NULL  │ nan    │
├───────┴────────┤
│     1 rows     │
└────────────────┘"""


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Fresh session per test (default spark display style)."""
    session = ReparkSession.builder.getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _capture_show(frame: object, *args: object, **kwargs: object) -> str:
    """Run ``frame.show(...)`` and return stdout without the print trailing newline."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        frame.show(*args, **kwargs)  # type: ignore[attr-defined]
    return buffer.getvalue().removesuffix("\n")


def _session_with_style(style: str) -> ReparkSession:
    """Build a fresh session with the given display style via builder config."""
    return ReparkSession.builder.config(_DISPLAY_STYLE_KEY, style).getOrCreate()


# =============================================================================================
# Config knob
# =============================================================================================


def test_default_display_style_is_spark(spark: ReparkSession) -> None:
    assert spark.display_style == _DEFAULT_DISPLAY_STYLE == "spark"


def test_builder_config_sets_display_style() -> None:
    session = _session_with_style("polars")
    try:
        assert session.display_style == "polars"
    finally:
        session.stop()


def test_runtime_display_style_attribute(spark: ReparkSession) -> None:
    assert spark.display_style == "spark"
    spark.display_style = "duckdb"
    assert spark.display_style == "duckdb"
    spark.display_style = "SPARK"  # case-insensitive
    assert spark.display_style == "spark"


def test_invalid_display_style_refuses_loud() -> None:
    with pytest.raises(IllegalArgumentException, match=r"repark\.display\.style"):
        normalize_display_style("pandas")
    with pytest.raises(IllegalArgumentException, match=r"repark\.display\.style"):
        ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "bogus").getOrCreate()
    spark = ReparkSession.builder.getOrCreate()
    try:
        with pytest.raises(IllegalArgumentException, match=r"repark\.display\.style"):
            spark.display_style = "bogus"
    finally:
        spark.stop()


def test_display_style_key_is_repark_prefixed_not_spark() -> None:
    # Stay out of the spark.* namespace so no PySpark config collides.
    assert _DISPLAY_STYLE_KEY.startswith("repark.")
    assert not _DISPLAY_STYLE_KEY.startswith("spark.")


# =============================================================================================
# Default spark style — byte-identical regression (near-drop-in mandate)
# =============================================================================================


def test_default_show_byte_identical_spark_grid(spark: ReparkSession) -> None:
    """Default ``df.show()`` must stay the PySpark-style ASCII grid (no shape header / no box)."""
    frame = spark.sql("SELECT 1 AS a, 'x' AS b")
    out = _capture_show(frame)
    assert out == _SPARK_BASIC_GOLDEN
    spark.display_style = "spark"
    assert _capture_show(frame) == _SPARK_BASIC_GOLDEN


def test_default_show_truncate_and_n_unchanged(spark: ReparkSession) -> None:
    """Existing truncate / n behavior is preserved on the spark path."""
    frame = spark.sql("SELECT 'abcdefghijklmnopqrstuvwxyz' AS long_col")
    out = _capture_show(frame, 1, truncate=10)
    assert "..." in out
    assert "abcdefghijklmnopqrstuvwxyz" not in out
    assert out.startswith("+")
    assert "shape:" not in out


def test_default_show_null_spelling(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT CAST(NULL AS INT) AS n, 'hi' AS s")
    out = _capture_show(frame, truncate=False)
    assert out == _SPARK_NULL_GOLDEN


def test_spark_style_does_not_call_count_for_show(
    spark: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default path must not pay for an extra count() query."""
    from repark import dataframe as dataframe_module

    calls = {"count": 0}
    original = dataframe_module.DataFrame.count

    def counting_count(self: object) -> int:
        calls["count"] += 1
        return original(self)  # type: ignore[arg-type]

    monkeypatch.setattr(dataframe_module.DataFrame, "count", counting_count)
    frame = spark.sql("SELECT 1 AS a")
    _capture_show(frame)
    assert calls["count"] == 0


# =============================================================================================
# polars style goldens
# =============================================================================================


def test_polars_style_head_tail_golden() -> None:
    session = _session_with_style("polars")
    try:
        frame = session.sql(_ORDERED_12_SQL)
        out = _capture_show(frame, truncate=False)
        assert out == _POLARS_12_GOLDEN
        assert "│ 6   ┆ x6    │" not in out
        assert "│ 7   ┆ x7    │" not in out
    finally:
        session.stop()


def test_polars_style_empty_frame() -> None:
    session = _session_with_style("polars")
    try:
        frame = session.sql("SELECT id FROM (VALUES (1)) AS t(id) WHERE id > 99")
        out = _capture_show(frame, truncate=False)
        assert out == _POLARS_EMPTY_GOLDEN
    finally:
        session.stop()


def test_polars_style_one_row() -> None:
    session = _session_with_style("polars")
    try:
        frame = session.sql("SELECT 1 AS id")
        out = _capture_show(frame, truncate=False)
        assert out == _POLARS_ONE_ROW_GOLDEN
    finally:
        session.stop()


def test_polars_style_null_and_nan() -> None:
    session = _session_with_style("polars")
    try:
        # Explicit ord column so null sits in the middle (ORDER BY i NULLS LAST puts null last).
        frame = session.sql(
            "SELECT i, f, s FROM (VALUES "
            "(0, CAST(1 AS BIGINT), CAST(1.0 AS DOUBLE), 'a'), "
            "(1, CAST(NULL AS BIGINT), CAST('NaN' AS DOUBLE), CAST(NULL AS VARCHAR)), "
            "(2, CAST(3 AS BIGINT), CAST(2.5 AS DOUBLE), 'c')"
            ") AS t(ord, i, f, s) ORDER BY ord"
        )
        out = _capture_show(frame, truncate=False)
        assert out == _POLARS_NULL_NAN_GOLDEN
    finally:
        session.stop()


def test_polars_style_truncation() -> None:
    session = _session_with_style("polars")
    try:
        frame = session.sql("SELECT 'abcdefghijklmnopqrstuvwxyz' AS s")
        out = _capture_show(frame, truncate=10)
        assert "..." in out
        assert "abcdefghijklmnopqrstuvwxyz" not in out
        assert out.startswith("shape: (1, 1)")
    finally:
        session.stop()


def test_polars_style_uses_count(monkeypatch: pytest.MonkeyPatch) -> None:
    from repark import dataframe as dataframe_module

    session = _session_with_style("polars")
    try:
        calls = {"count": 0}
        original = dataframe_module.DataFrame.count

        def counting_count(self: object) -> int:
            calls["count"] += 1
            return original(self)  # type: ignore[arg-type]

        monkeypatch.setattr(dataframe_module.DataFrame, "count", counting_count)
        frame = session.sql("SELECT 1 AS a")
        _capture_show(frame)
        assert calls["count"] == 1
    finally:
        session.stop()


def test_polars_style_honors_n_keep_set() -> None:
    """``show(n)`` must not over-show under polars (n caps; edges still never enlarge past 5).

    MUTATION: ignore ``n`` again → ``show(0)`` still prints body rows / ``show(1)`` prints >1.
    MUTATION: ``use_ellipsis=True`` with ``tail_n=0`` → ``show(1)`` bare ``…`` row (C8-Q-001 class).
    """
    session = _session_with_style("polars")
    try:
        frame = session.sql(_ORDERED_12_SQL)
        out0 = _capture_show(frame, 0, truncate=False)
        assert out0.startswith("shape: (12, 2)")
        assert "│ 1   ┆ x1    │" not in out0
        assert "│ 12  ┆ x12   │" not in out0

        out1 = _capture_show(frame, 1, truncate=False)
        assert "│ 1   ┆ x1    │" in out1
        assert "│ 2   ┆ x2    │" not in out1
        assert "│ 12  ┆ x12   │" not in out1
        # Head-only keep-set: no bare middle ellipsis without a following tail (C8-Q-001 class).
        assert "│ …   ┆ …     │" not in out1

        # When both head and tail exist, the middle ellipsis row is present.
        out2 = _capture_show(frame, 2, truncate=False)
        assert "│ 1   ┆ x1    │" in out2
        assert "│ 12  ┆ x12   │" in out2
        assert "│ …   ┆ …     │" in out2

        # n does not enlarge past the 5+5 polars edges (default n=20 already pinned elsewhere).
        out20 = _capture_show(frame, 20, truncate=False)
        assert out20 == _POLARS_12_GOLDEN
        assert "│ 6   ┆ x6    │" not in out20
    finally:
        session.stop()


def test_polars_style_n_caps_small_frame() -> None:
    """On frames ≤10, polars still honors ``show(n)`` as a keep-set (not always all rows)."""
    session = _session_with_style("polars")
    try:
        frame = session.sql("SELECT id FROM (VALUES (1), (2), (3)) AS t(id) ORDER BY id")
        out = _capture_show(frame, 1, truncate=False)
        assert out.startswith("shape: (3, 1)")
        assert "│ 1   │" in out
        assert "│ 2   │" not in out
        assert "│ 3   │" not in out
    finally:
        session.stop()


# =============================================================================================
# duckdb style goldens
# =============================================================================================


def test_duckdb_style_small_frame_golden() -> None:
    session = _session_with_style("duckdb")
    try:
        frame = session.sql(
            "SELECT id, label FROM (VALUES (1, 'x1'), (2, 'x2')) AS t(id, label) ORDER BY id"
        )
        out = _capture_show(frame, truncate=False)
        assert out == _DUCKDB_SMALL_GOLDEN
    finally:
        session.stop()


def test_duckdb_style_ellipsis_and_footer() -> None:
    session = _session_with_style("duckdb")
    try:
        frame = session.sql(_ORDERED_12_SQL)
        out = _capture_show(frame, 4, truncate=False)
        assert out == _DUCKDB_ELLIPSIS_GOLDEN
    finally:
        session.stop()


def test_duckdb_style_empty_frame() -> None:
    session = _session_with_style("duckdb")
    try:
        frame = session.sql("SELECT id FROM (VALUES (1)) AS t(id) WHERE id > 99")
        out = _capture_show(frame, truncate=False)
        assert out == _DUCKDB_EMPTY_GOLDEN
    finally:
        session.stop()


def test_duckdb_style_one_row() -> None:
    session = _session_with_style("duckdb")
    try:
        frame = session.sql("SELECT 1 AS id")
        out = _capture_show(frame, truncate=False)
        assert out == _DUCKDB_ONE_ROW_GOLDEN
    finally:
        session.stop()


def test_duckdb_style_null_spelling() -> None:
    session = _session_with_style("duckdb")
    try:
        frame = session.sql("SELECT CAST(NULL AS INT) AS n, CAST('NaN' AS DOUBLE) AS f")
        out = _capture_show(frame, truncate=False)
        assert out == _DUCKDB_NULL_GOLDEN
    finally:
        session.stop()


def test_duckdb_style_show_one_keeps_first_row() -> None:
    """``show(1)`` on a multi-row frame keeps the first row only — no middle ``·``, no tail.

    MUTATION: ``head_n = n // 2`` without the zero-head guard → id 12 appears, id 1 absent.
    MUTATION: ``use_ellipsis = True`` when ``tail_n == 0`` → body emits three ``·`` rows while
    footer already says ``(1 shown)`` (C8-Q-001).
    """
    session = _session_with_style("duckdb")
    try:
        frame = session.sql(_ORDERED_12_SQL)
        out = _capture_show(frame, 1, truncate=False)
        # Exact first-row keep-set: id=1 / label=x1 present; id=12 / x12 absent (no vacuous
        # ``"1" in out`` that also matches ``12 rows`` / ``(1 shown)``).
        assert "x1" in out
        assert "x12" not in out
        # Body cell for id 1 (right-aligned in the id column).
        assert "│     1 │ x1      │" in out or "│        1 │" in out
        assert "12 rows" in out
        assert "(1 shown)" in out
        # C8-Q-001: no middle-ellipsis body when the keep-set is a single head row.
        body_lines = [
            line
            for line in out.splitlines()
            if line.startswith("│") and "rows" not in line and "shown" not in line
        ]
        middle_dot_lines = [line for line in body_lines if "·" in line]
        assert middle_dot_lines == [], (
            f"show(1) must not render middle · rows when tail_n=0; got {middle_dot_lines!r}"
        )
        # Header + type row + one data row (no · / tail rows).
        assert len(body_lines) == 3, body_lines
    finally:
        session.stop()


def test_duckdb_style_show_zero_footer_reports_zero_shown() -> None:
    """duckdb ``show(0)`` on a non-empty frame must footer ``(0 shown)``, not full-N only.

    Keep-set is empty (no body rows) while cardinality stays in the main footer. MUTATION:
    ``shown_rows=total_rows`` when ``use_ellipsis`` is False, or gate ``(K shown)`` on
    ``show_ellipsis`` alone → footer claims ``12 rows`` with no ``(0 shown)`` (C4-L-001).
    """
    session = _session_with_style("duckdb")
    try:
        frame = session.sql(_ORDERED_12_SQL)
        out = _capture_show(frame, 0, truncate=False)
        assert "12 rows" in out
        assert "(0 shown)" in out
        # Empty keep-set: no data cells for id 1 / id 12 (label cells are unambiguous).
        assert "x1" not in out
        assert "x12" not in out
        # No middle-ellipsis body for a zero keep-set (footer-only truncation signal).
        body_lines = [
            line
            for line in out.splitlines()
            if line.startswith("│") and "rows" not in line and "shown" not in line
        ]
        # Header + type row only (no data / · rows).
        assert len(body_lines) == 2, body_lines
    finally:
        session.stop()


def test_styled_show_boolean_lowercase() -> None:
    """Boolean cells must spell lowercase true/false (not Python True/False).

    MUTATION: drop the ``isinstance(value, bool)`` branch in ``_cell_text`` → ``True`` appears.
    """
    for style in ("polars", "duckdb", "spark"):
        session = _session_with_style(style)
        try:
            frame = session.sql("SELECT true AS t, false AS f")
            out = _capture_show(frame, truncate=False)
            assert "True" not in out
            assert "False" not in out
            assert "true" in out
            assert "false" in out
        finally:
            session.stop()


def test_styled_show_narrow_arrow_type_labels() -> None:
    """TINYINT/SMALLINT/FLOAT labels use precise Arrow widths (not collapsed i32/f64).

    MUTATION: restore ``logical_schema_fields`` + coarse ``arrow_type_key`` for the dtype row →
    ``i32``/``int32``/``f64``/``double`` for narrow casts.
    """
    session = _session_with_style("polars")
    try:
        frame = session.sql(
            "SELECT CAST(1 AS TINYINT) AS t, CAST(2 AS SMALLINT) AS s, "
            "CAST(1.5 AS FLOAT) AS f, CAST(2.5 AS DOUBLE) AS d"
        )
        out = _capture_show(frame, truncate=False)
        # Type row must name precise widths (order: t, s, f, d).
        assert "│ i8  ┆ i16 ┆ f32 ┆ f64 │" in out or (
            "i8" in out and "i16" in out and "f32" in out and "f64" in out
        )
        # Collapsed labels from logical_schema_fields must not win.
        type_line = next(line for line in out.splitlines() if "i8" in line or "i32" in line)
        assert "i8" in type_line and "i16" in type_line and "f32" in type_line
        # Ensure we did not label tinyint/smallint as plain i32-only.
        assert type_line.count("i32") == 0
    finally:
        session.stop()

    session = _session_with_style("duckdb")
    try:
        frame = session.sql(
            "SELECT CAST(1 AS TINYINT) AS t, CAST(2 AS SMALLINT) AS s, CAST(1.5 AS FLOAT) AS f"
        )
        out = _capture_show(frame, truncate=False)
        assert "int8" in out
        assert "int16" in out
        # duckdb float32 label is "float" (not "double").
        type_lines = [line for line in out.splitlines() if "int8" in line or "int32" in line]
        assert type_lines, out
        joined = "\n".join(type_lines)
        assert "int8" in joined and "int16" in joined
        assert "float" in out
        # Narrow int columns must not be labeled int32.
        assert "int32" not in joined
    finally:
        session.stop()


def test_polars_show_zero_logs_zero_shown_rows(
    spark: ReparkSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``show(0)`` must log keep-set size 0, not total_rows (C2-Q-003)."""
    import logging

    spark.display_style = "polars"
    frame = spark.sql(_ORDERED_12_SQL)
    with caplog.at_level(logging.INFO, logger="repark.dataframe"):
        _capture_show(frame, 0, truncate=False)
    info_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.INFO
    ]
    assert any("show(0 rows)" in message for message in info_messages), info_messages
    assert not any("show(12 rows)" in message for message in info_messages), info_messages


# =============================================================================================
# Private tail preview path + no full-table collect discipline
# =============================================================================================


def test_preview_tail_rows_returns_last_n(spark: ReparkSession) -> None:
    frame = spark.sql(_ORDERED_12_SQL)
    total = frame.count()
    assert total == 12
    table = frame._preview_tail_rows(3, total_rows=total)
    assert table.num_rows == 3
    assert table.column("id").to_pylist() == [10, 11, 12]


def test_preview_tail_rows_uses_limit_with_skip(
    spark: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tail preview must engine-skip: ``limit_with_skip(skip=total-fetch, fetch=n)``.

    MUTATION: full ``to_arrow``/``pa.table``/``collect`` then slice, or
    ``limit_with_skip(0, total)``, keeps last-n values green but fails this pin (C2-Q-001).

    Hollow decoy (C5-Q-001): call ``limit_with_skip(skip, fetch)`` then discard it and
    ``pa.table(self._inner).slice(...)`` — facade ``to_arrow`` / ``__arrow_c_stream__`` spies
    stay quiet while the unlimited plan fully materializes via the *native* root stream.
    This pin also forbids root-native ``__arrow_c_stream__`` and requires the
    ``limit_with_skip`` return value to be the plan that crosses the Arrow boundary.
    """
    frame = spark.sql(_ORDERED_12_SQL)
    root_inner = frame._inner
    inner_type = type(root_inner)
    original_lws = inner_type.limit_with_skip
    original_native_stream = inner_type.__arrow_c_stream__
    skip_calls: list[tuple[int, int]] = []
    limited_plans: list[object] = []
    streamed_inners: list[object] = []
    forbidden_calls: list[str] = []

    def tracking_limit_with_skip(self: object, skip: int, fetch: int) -> object:
        skip_calls.append((int(skip), int(fetch)))
        plan = original_lws(self, skip, fetch)
        limited_plans.append(plan)
        return plan

    def tracking_native_stream(
        self: object,
        *args: object,
        **kwargs: object,
    ) -> object:
        streamed_inners.append(self)
        if self is root_inner:
            # Unlimited-plan export (``pa.table(self._inner)`` / root collect attack).
            forbidden_calls.append("native __arrow_c_stream__(root_inner)")
        return original_native_stream(self, *args, **kwargs)

    monkeypatch.setattr(inner_type, "limit_with_skip", tracking_limit_with_skip)
    monkeypatch.setattr(inner_type, "__arrow_c_stream__", tracking_native_stream)
    table = frame._preview_tail_rows(3, total_rows=12)
    assert table.num_rows == 3
    assert table.column("id").to_pylist() == [10, 11, 12]
    assert skip_calls == [(9, 3)], f"expected limit_with_skip(9, 3), got {skip_calls}"
    assert not forbidden_calls, (
        f"tail preview must not fully export the unlimited root plan: {forbidden_calls}"
    )
    assert limited_plans, "limit_with_skip must run for a non-trivial tail"
    # Decoy ``limit_with_skip`` + root ``pa.table`` leaves limited_plans unused for export.
    assert any(streamed is limited_plans[0] for streamed in streamed_inners), (
        "limit_with_skip return must be the plan exported across the Arrow boundary "
        f"(streamed={len(streamed_inners)}, limited={len(limited_plans)})"
    )


def test_preview_tail_rows_empty_and_zero(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT id FROM (VALUES (1)) AS t(id) WHERE id > 99")
    table = frame._preview_tail_rows(5, total_rows=0)
    assert table.num_rows == 0
    frame2 = spark.sql("SELECT 1 AS id")
    table0 = frame2._preview_tail_rows(0, total_rows=1)
    assert table0.num_rows == 0


def test_preview_tail_rows_total_le_fetch_uses_limit_not_negative_skip(
    spark: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``total <= fetch``, preview the whole window via ``limit`` — never negative skip.

    C7-Q-002: deleting ``if total <= fetch: return self.limit(total)...`` stays green under the
    prior suite (only exercised ``total > fetch``). With ``total < fetch``, ``skip = total -
    fetch`` is negative and the native ``limit_with_skip(skip: usize, …)`` path is wrong (Python
    → Rust ``usize`` refuses negatives). This pin requires:

    * values = all ``total`` rows when ``n > total``;
    * ``limit_with_skip`` is **not** called on the short-frame path;
    * the same holds for the ``total == fetch`` boundary (skip would be 0 — still prefer limit).
    """
    frame = spark.sql("SELECT id FROM (VALUES (1), (2), (3)) AS t(id) ORDER BY id")
    assert frame.count() == 3
    root_inner = frame._inner
    inner_type = type(root_inner)
    original_lws = inner_type.limit_with_skip
    skip_calls: list[tuple[int, int]] = []

    def tracking_limit_with_skip(self: object, skip: int, fetch: int) -> object:
        skip_calls.append((int(skip), int(fetch)))
        return original_lws(self, skip, fetch)

    monkeypatch.setattr(inner_type, "limit_with_skip", tracking_limit_with_skip)

    # total < fetch — the mutation-critical branch (would compute skip = 3 - 5 = -2).
    table_lt = frame._preview_tail_rows(5, total_rows=3)
    assert table_lt.num_rows == 3
    assert table_lt.column("id").to_pylist() == [1, 2, 3]
    assert skip_calls == [], (
        f"total < fetch must use limit(total), not limit_with_skip; got {skip_calls}"
    )

    # total == fetch — still the short-circuit (skip would be 0).
    table_eq = frame._preview_tail_rows(3, total_rows=3)
    assert table_eq.num_rows == 3
    assert table_eq.column("id").to_pylist() == [1, 2, 3]
    assert skip_calls == [], (
        f"total == fetch must use limit(total), not limit_with_skip; got {skip_calls}"
    )


def test_runtime_style_switch_affects_show(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS a, 'x' AS b")
    spark_out = _capture_show(frame)
    assert spark_out == _SPARK_BASIC_GOLDEN
    spark.display_style = "polars"
    polars_out = _capture_show(frame, truncate=False)
    assert polars_out.startswith("shape: (1, 2)")
    spark.display_style = "spark"
    assert _capture_show(frame) == spark_out


def test_styled_show_does_not_full_collect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Styled head+tail must not fully materialize the unlimited plan.

    Hollow pin history:

    * C2-Q-001 — only ``to_arrow`` row counts + forced ``_preview_tail_rows``;
    * C4-Q-001 — facade root ``to_arrow`` / ``__arrow_c_stream__`` (``pa.table(self)``);
    * C5-Q-001 — still green under decoy ``limit_with_skip`` + ``pa.table(self._inner)``
      (native root stream bypasses the facade dunder).

    This pin also:

    * asserts native ``limit_with_skip(skip=total-fetch, fetch=tail_n)``;
    * forbids ``DataFrame.collect`` / ``to_polars`` during styled show;
    * forbids unlimited export on the root *facade* via ``to_arrow`` / ``__arrow_c_stream__``;
    * forbids unlimited export on the root *native* via ``__arrow_c_stream__``
      (covers ``pa.table(self._inner)``);
    * requires the ``limit_with_skip`` return value to be the plan that streams;
    * keeps per-``to_arrow`` row counts strictly below the full result size.
    """
    from repark import dataframe as dataframe_module

    original_to_arrow = dataframe_module.DataFrame.to_arrow
    original_preview = dataframe_module.DataFrame._preview_tail_rows
    original_collect = dataframe_module.DataFrame.collect
    original_to_polars = dataframe_module.DataFrame.to_polars
    original_arrow_c_stream = dataframe_module.DataFrame.__arrow_c_stream__
    rows_per_call: list[int] = []
    preview_calls = {"n": 0}
    skip_calls: list[tuple[int, int]] = []
    limited_plans: list[object] = []
    streamed_native_inners: list[object] = []
    forbidden_calls: list[str] = []
    native_spies_installed = {"done": False}
    # Root frame under show() — identity-checked so limited/spawned children stay free.
    root_frame_box: list[object] = []
    # Root native plan (``frame._inner``) — ``pa.table(self._inner)`` hits this, not facade.
    root_inner_box: list[object] = []

    def tracking_to_arrow(self: object) -> object:
        if root_frame_box and self is root_frame_box[0]:
            # Unlimited plan export (full materialize+slice attack path).
            forbidden_calls.append("to_arrow(root)")
        table = original_to_arrow(self)  # type: ignore[arg-type]
        rows_per_call.append(table.num_rows)
        return table

    def tracking_arrow_c_stream(self: object, requested_schema: object | None = None) -> object:
        # ``pa.table(self)`` / ``pyarrow`` stream consumers hit this dunder without
        # going through ``to_arrow`` — spy must catch root unlimited stream export.
        if root_frame_box and self is root_frame_box[0]:
            forbidden_calls.append("__arrow_c_stream__(root)")
        return original_arrow_c_stream(self, requested_schema)  # type: ignore[arg-type]

    def tracking_preview(self: object, n: int, *, total_rows: int) -> object:
        preview_calls["n"] += 1
        return original_preview(self, n, total_rows=total_rows)  # type: ignore[arg-type]

    def forbidden_collect(self: object) -> object:
        forbidden_calls.append("collect")
        return original_collect(self)  # type: ignore[arg-type]

    def forbidden_to_polars(self: object) -> object:
        forbidden_calls.append("to_polars")
        return original_to_polars(self)  # type: ignore[arg-type]

    monkeypatch.setattr(dataframe_module.DataFrame, "to_arrow", tracking_to_arrow)
    monkeypatch.setattr(dataframe_module.DataFrame, "__arrow_c_stream__", tracking_arrow_c_stream)
    monkeypatch.setattr(dataframe_module.DataFrame, "_preview_tail_rows", tracking_preview)
    monkeypatch.setattr(dataframe_module.DataFrame, "collect", forbidden_collect)
    monkeypatch.setattr(dataframe_module.DataFrame, "to_polars", forbidden_to_polars)

    def _ensure_native_partial_collect_spies(frame: object) -> None:
        """Patch native limit_with_skip + __arrow_c_stream__ once (same PyO3 type)."""
        if native_spies_installed["done"]:
            return
        inner_type = type(frame._inner)  # type: ignore[attr-defined]
        original_lws = inner_type.limit_with_skip
        original_native_stream = inner_type.__arrow_c_stream__

        def tracking_lws(self: object, skip: int, fetch: int) -> object:
            skip_calls.append((int(skip), int(fetch)))
            plan = original_lws(self, skip, fetch)
            limited_plans.append(plan)
            return plan

        def tracking_native_stream(
            self: object,
            *args: object,
            **kwargs: object,
        ) -> object:
            streamed_native_inners.append(self)
            if root_inner_box and self is root_inner_box[0]:
                forbidden_calls.append("native __arrow_c_stream__(root_inner)")
            return original_native_stream(self, *args, **kwargs)

        monkeypatch.setattr(inner_type, "limit_with_skip", tracking_lws)
        monkeypatch.setattr(inner_type, "__arrow_c_stream__", tracking_native_stream)
        native_spies_installed["done"] = True

    def _assert_partial_collect_discipline(
        *,
        expected_skip: tuple[int, int],
        max_rows_per_export: int,
    ) -> None:
        assert preview_calls["n"] == 1
        assert not forbidden_calls, f"styled show must not call {forbidden_calls}"
        # No single materialization may return the full 12-row result (full collect + slice).
        assert rows_per_call, "styled show must collect head and/or tail via to_arrow"
        assert all(row_count < 12 for row_count in rows_per_call)
        assert max(rows_per_call) <= max_rows_per_export
        assert skip_calls == [expected_skip], (
            f"expected limit_with_skip{expected_skip}, got {skip_calls}"
        )
        assert limited_plans, "limit_with_skip must run for head+tail styled show"
        assert any(streamed is limited_plans[0] for streamed in streamed_native_inners), (
            "limit_with_skip return must be exported (decoy call + root pa.table is forbidden)"
        )

    session = _session_with_style("polars")
    try:
        frame = session.sql(_ORDERED_12_SQL)
        root_frame_box.clear()
        root_frame_box.append(frame)
        root_inner_box.clear()
        root_inner_box.append(frame._inner)
        _ensure_native_partial_collect_spies(frame)
        out = _capture_show(frame, truncate=False)
        assert out == _POLARS_12_GOLDEN
        # polars 12-row default: head 5 + tail 5 → skip=7, fetch=5
        _assert_partial_collect_discipline(expected_skip=(7, 5), max_rows_per_export=5)
    finally:
        session.stop()

    rows_per_call.clear()
    preview_calls["n"] = 0
    skip_calls.clear()
    limited_plans.clear()
    streamed_native_inners.clear()
    forbidden_calls.clear()
    root_frame_box.clear()
    root_inner_box.clear()
    session = _session_with_style("duckdb")
    try:
        frame = session.sql(_ORDERED_12_SQL)
        root_frame_box.append(frame)
        root_inner_box.append(frame._inner)
        _ensure_native_partial_collect_spies(frame)
        out = _capture_show(frame, 4, truncate=False)
        assert out == _DUCKDB_ELLIPSIS_GOLDEN
        # duckdb n=4 on 12 rows: head 2 + tail 2 → skip=10, fetch=2
        _assert_partial_collect_discipline(expected_skip=(10, 2), max_rows_per_export=2)
    finally:
        session.stop()


def test_get_or_create_reuse_applies_display_style() -> None:
    """Explicit ``repark.display.style`` on getOrCreate reuse updates the live session.

    MUTATION: delete the reuse-path apply in ``Builder.get_or_create`` → style stays ``spark``.
    """
    first = ReparkSession.builder.getOrCreate()
    try:
        assert first.display_style == "spark"
        reused = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "polars").getOrCreate()
        assert reused is first
        assert first.display_style == "polars"
        # Show must actually follow the reused session's new style (not only the attribute).
        frame = first.sql("SELECT 1 AS a, 'x' AS b")
        out = _capture_show(frame, truncate=False)
        assert out.startswith("shape: (1, 2)")
    finally:
        first.stop()


def test_get_or_create_reuse_display_style_no_false_config_warning() -> None:
    """Pure ``repark.display.style`` reuse must apply style without the engine-knob warning.

    Display style is always applied on reuse (facade-only). Emitting
    "some configuration may not apply" for a pure style delta is a false positive (C6-Q-001).
    Engine-knob reuse still warns (see ``test_session_config_knobs``).

    MUTATION: compare full ``_config`` (incl. display style) for the warn → first reuse warns.
    MUTATION: apply style but skip ``_sync_display_style_into_builder_config`` and reinstate
    full-dict warn compare → second pure-style reuse re-warns.
    """
    first = ReparkSession.builder.getOrCreate()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", UserWarning)
            reused = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "polars").getOrCreate()
        assert reused is first
        assert first.display_style == "polars"
        config_warns = [
            item
            for item in caught
            if issubclass(item.category, UserWarning)
            and "some configuration may not apply" in str(item.message)
        ]
        assert config_warns == [], (
            "pure display-style reuse must not claim configuration may not apply; "
            f"got {[str(w.message) for w in config_warns]}"
        )
        # Repeat pure-style reuse (same value): still silent after _builder_config sync.
        with warnings.catch_warnings(record=True) as caught_again:
            warnings.simplefilter("always", UserWarning)
            again = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "polars").getOrCreate()
        assert again is first
        config_warns_again = [
            item
            for item in caught_again
            if issubclass(item.category, UserWarning)
            and "some configuration may not apply" in str(item.message)
        ]
        assert config_warns_again == [], (
            "repeat pure display-style reuse must stay silent after builder snapshot sync; "
            f"got {[str(w.message) for w in config_warns_again]}"
        )
        # Engine knob still differs → warn (regression fence for the exclusion helper).
        with pytest.warns(UserWarning, match=r"some configuration may not apply"):
            knobby = (
                ReparkSession.builder.config("spark.sql.shuffle.partitions", "4")
                .config(_DISPLAY_STYLE_KEY, "duckdb")
                .getOrCreate()
            )
        assert knobby is first
        assert first.display_style == "duckdb"
    finally:
        first.stop()


def test_builder_display_style_key_case_insensitive() -> None:
    """Builder accepts mixed-case ``repark.display.style`` keys (C6-Q-002).

    MUTATION: drop the case-insensitive key loop in ``Builder._resolve_display_style`` →
    ``Repark.Display.Style`` is ignored and style stays default ``spark``.
    """
    session = ReparkSession.builder.config("Repark.Display.Style", "POLARS").getOrCreate()
    try:
        assert session.display_style == "polars"
        frame = session.sql("SELECT 1 AS a, 'x' AS b")
        out = _capture_show(frame, truncate=False)
        assert out.startswith("shape: (1, 2)")
    finally:
        session.stop()


def test_get_or_create_reuse_display_style_key_case_insensitive() -> None:
    """Reuse path honors mixed-case display-style keys (C6-Q-002).

    MUTATION: reuse apply uses exact-key match only (drop ``key.lower()``) → style stays spark.
    """
    first = ReparkSession.builder.getOrCreate()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", UserWarning)
            reused = ReparkSession.builder.config("REPARK.DISPLAY.STYLE", "duckdb").getOrCreate()
        assert reused is first
        assert first.display_style == "duckdb"
        config_warns = [
            item
            for item in caught
            if issubclass(item.category, UserWarning)
            and "some configuration may not apply" in str(item.message)
        ]
        assert config_warns == []
        frame = first.sql("SELECT 1 AS a")
        out = _capture_show(frame, truncate=False)
        assert "┌" in out  # duckdb box, not spark +---+
        assert out.startswith("+") is False
    finally:
        first.stop()


def test_builder_display_style_dual_case_last_wins() -> None:
    """Dual-cased ``repark.display.style`` keys are last-write-wins (C7-Q-001 / C7-L-001).

    MUTATION: prefer exact canonical key / first case-insensitive hit → later mixed-case
    ``polars`` override is ignored and style stays ``spark`` (silent wrong show() style).
    Also pins that ``Builder.config`` collapses aliases onto the canonical key so the map
    does not retain both casings.
    """
    builder = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "spark").config(
        "Repark.Display.Style", "polars"
    )
    # Canonicalize-on-write: only the last value under the canonical key remains.
    assert _DISPLAY_STYLE_KEY in builder._config
    assert not any(
        key != _DISPLAY_STYLE_KEY and key.lower() == _DISPLAY_STYLE_KEY for key in builder._config
    )
    assert builder._config[_DISPLAY_STYLE_KEY] == "polars"
    session = builder.getOrCreate()
    try:
        assert session.display_style == "polars"
        frame = session.sql("SELECT 1 AS a, 'x' AS b")
        out = _capture_show(frame, truncate=False)
        assert out.startswith("shape: (1, 2)"), f"last-wins polars expected; got:\n{out}"
    finally:
        session.stop()

    # Opposite order: mixed-case first, canonical last → last (canonical) wins.
    builder2 = ReparkSession.builder.config("REPARK.DISPLAY.STYLE", "duckdb").config(
        _DISPLAY_STYLE_KEY, "polars"
    )
    assert builder2._config.get(_DISPLAY_STYLE_KEY) == "polars"
    session2 = builder2.getOrCreate()
    try:
        assert session2.display_style == "polars"
    finally:
        session2.stop()


def test_builder_display_style_dual_case_invalid_last_refuses() -> None:
    """Later invalid case-alias of display.style must fail loud (C7-Q-001 / C7-L-001).

    MUTATION: resolve only the first/exact key and skip ``normalize_display_style`` on later
    aliases → ``.config(canonical, 'polars').config(mixed, 'pandas')`` silently accepts polars.
    """
    with pytest.raises(IllegalArgumentException, match=r"repark\.display\.style"):
        (
            ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "polars")
            .config("Repark.Display.Style", "pandas")
            .getOrCreate()
        )


def test_resolve_display_style_last_wins_when_dual_aliases_present() -> None:
    """Defensive last-wins when the builder map already holds dual casings (C7-Q-001).

    MUTATION: ``raw = self._config.get(canonical)`` then break on first CI hit → exact
    ``spark`` shadows a later mixed-case ``polars`` that a notebook-style re-config left in
    the map (e.g. before canonicalize-on-write, or after direct map mutation).
    """
    builder = ReparkSession.builder
    # Simulate a dual-cased map (bypass config() collapse to pin resolve-time last-wins).
    builder._config = {
        _DISPLAY_STYLE_KEY: "spark",
        "Repark.Display.Style": "polars",
    }
    assert builder._resolve_display_style() == "polars"
    # Invalid last alias still refuses (not shadowed by earlier valid exact key).
    builder._config = {
        _DISPLAY_STYLE_KEY: "polars",
        "REPARK.DISPLAY.STYLE": "pandas",
    }
    with pytest.raises(IllegalArgumentException, match=r"repark\.display\.style"):
        builder._resolve_display_style()


def test_show_truncate_non_positive_means_no_truncation(spark: ReparkSession) -> None:
    """``truncate=0`` / negative ints mean full cells (Spark parity), not blank/chop (C6-L-001).

    MUTATION: ``cap = int(truncate)`` for all ints → ``truncate=0`` yields empty body cells
    (``text[:0]``); ``truncate=-1`` chops the last character (``text[:-1]``).
    """
    long_value = "abcdefghijklmnopqrstuvwxyz"
    frame = spark.sql(f"SELECT '{long_value}' AS long_col")
    for truncate_arg in (0, -1, -20):
        out = _capture_show(frame, 1, truncate=truncate_arg)
        assert long_value in out, (
            f"truncate={truncate_arg!r} must show the full cell (Spark truncate>0 only); "
            f"got:\n{out}"
        )
        assert "..." not in out
    # Positive int still truncates (fence against "always disable").
    out_pos = _capture_show(frame, 1, truncate=10)
    assert "..." in out_pos
    assert long_value not in out_pos
    # Styled paths share the same cap resolution.
    spark.display_style = "polars"
    try:
        out_polars = _capture_show(frame, 1, truncate=0)
        assert long_value in out_polars
        assert "..." not in out_polars
    finally:
        spark.display_style = "spark"


def test_get_or_create_reuse_without_style_key_leaves_style() -> None:
    """Reuse without an explicit display-style config must not clobber a runtime-set style."""
    first = ReparkSession.builder.getOrCreate()
    try:
        first.display_style = "duckdb"
        reused = ReparkSession.builder.appName("reuse-no-style").getOrCreate()
        assert reused is first
        assert first.display_style == "duckdb"
    finally:
        first.stop()


def test_show_rejects_bool_n(spark: ReparkSession) -> None:
    """``show(True)`` / ``show(False)`` must raise — bool is not a row count (C8-L-001).

    ``bool`` is an ``int`` subclass, so ``max(0, int(False))`` → 0 and ``int(True)`` → 1 would
    silently empty or shrink the keep-set. PySpark 3.5+ refuses a bool ``n``; use
    ``truncate=False`` as a keyword for the truncate flag.

    MUTATION: drop the ``isinstance(n, bool)`` guard (or only ``isinstance(n, int)``) →
    ``show(False)`` prints an empty keep-set and ``show(True)`` prints one row.
    """
    # ORDER BY keeps show(1) deterministic (unordered UNION ALL may surface either row).
    frame = spark.sql(
        "SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a) ordered_rows ORDER BY a"
    )
    for bad_n in (True, False):
        with pytest.raises(PySparkTypeError, match="NOT_INT"):
            frame.show(bad_n)
        with pytest.raises(PySparkTypeError, match="NOT_INT"):
            frame.show(bad_n, truncate=False)
    # Non-bool int still accepted (fence against over-rejecting).
    out = _capture_show(frame, 1, truncate=False)
    assert "| 1 |" in out
    # Keyword truncate=bool remains valid (positional bool is the hazard, not the flag type).
    out_full = _capture_show(frame, 2, truncate=False)
    assert "| 1 |" in out_full and "| 2 |" in out_full


def test_public_tail_and_preview_tail_coexist_and_agree() -> None:
    """The combined R-TAIL x R-DISPLAY contract (supersedes the pre-combine ownership pin
    ``test_no_public_dataframe_tail``, whose premise died when the branches merged).

    Public ``tail`` is PySpark-parity (full collect, then trailing slice — PySpark's own
    documented driver-memory caveat). Private ``_preview_tail_rows`` stays the DISPLAY path:
    engine-side ``limit_with_skip``, so ``show()`` never pays a full collect for a preview.
    They coexist deliberately and must agree on the rows they return.
    """
    assert hasattr(DataFrame, "tail")
    assert hasattr(DataFrame, "_preview_tail_rows")
    spark = ReparkSession.builder.getOrCreate()
    try:
        df = spark.createDataFrame([(i, f"r{i}") for i in range(10)], ["id", "v"])
        tail_ids = [row["id"] for row in df.tail(3)]
        preview_ids = df._preview_tail_rows(3, total_rows=10).column("id").to_pylist()
        assert tail_ids == preview_ids == [7, 8, 9], (
            f"parity tail and display preview must agree: {tail_ids} vs {preview_ids}"
        )
    finally:
        spark.stop()
    assert callable(DataFrame._preview_tail_rows)
