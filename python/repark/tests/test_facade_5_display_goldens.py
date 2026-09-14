"""FACADE-5 display-renderer goldens against the committed JSON file.

Every unbound renderer x truncation-rule pair in the FACADE-5 step-0 census
records one golden case here from the base tree; each case maps door labels to
``{"rendered": str}`` or ``{"refused": {"class", "message"}}`` payloads.
"""

from __future__ import annotations

import datetime
import io
import json
import os
from collections.abc import Callable, Iterator
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

RECORD_ENV = "REPARK_FACADE_5_RECORD_GOLDENS"
GOLDEN_PATH = Path(__file__).with_name("facade_5_display_goldens.json")
BASE_DATE = datetime.date(2024, 1, 2)
BASE_TS = datetime.datetime(2024, 1, 2, 3, 4, 5)

Case = Callable[[ReparkSession], dict[str, Any]]

MIXED_SCHEMA = "id INT, name STRING, score DOUBLE, flag BOOLEAN"
NESTED_SCHEMA = "id INT, st STRUCT<a: INT, b: STRING>, li ARRAY<INT>, m MAP<STRING, INT>"
SCALAR_SCHEMA = (
    "i INT, f DOUBLE, s STRING, b BOOLEAN, d DATE, t TIMESTAMP, dc DECIMAL(38, 18), bin BINARY"
)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Session for FACADE-5 display goldens (UTC, spark display style)."""
    session = (
        ReparkSession.builder.appName("facade-5-display")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _capture_show(frame: Any, *args: Any, **kwargs: Any) -> str:
    """Run ``frame.show(...)`` and return stdout without the trailing newline."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        frame.show(*args, **kwargs)
    return buffer.getvalue().removesuffix("\n")


def _door(label: str, thunk: Callable[[], str]) -> tuple[str, dict[str, Any]]:
    """Render one door, recording either its bytes or its refusal."""
    try:
        return label, {"rendered": thunk()}
    except Exception as error:
        return label, {"refused": {"class": type(error).__name__, "message": str(error)}}


def _under_style(
    session: ReparkSession, style: str, thunk: Callable[[], dict[str, Any]]
) -> dict[str, Any]:
    """Run ``thunk`` while the session display style is ``style``."""
    previous = session.display_style
    session.display_style = style
    try:
        return thunk()
    finally:
        session.display_style = previous


def _under_eager(
    session: ReparkSession,
    max_rows: int,
    truncate: int,
    thunk: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Run ``thunk`` under ``spark.sql.repl.eagerEval`` confs, then unset them."""
    session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    session.conf.set("spark.sql.repl.eagerEval.maxNumRows", str(max_rows))
    session.conf.set("spark.sql.repl.eagerEval.truncate", str(truncate))
    try:
        return thunk()
    finally:
        session.conf.unset("spark.sql.repl.eagerEval.enabled")
        session.conf.unset("spark.sql.repl.eagerEval.maxNumRows")
        session.conf.unset("spark.sql.repl.eagerEval.truncate")


def _mixed_frame(session: ReparkSession) -> Any:
    """The four-column mixed frame with a cell past every truncate cap."""
    return session.createDataFrame(
        [
            (1, "alpha", 1.5, True),
            (2, "quick-brown-fox-jumps-over-the-lazy-dog-twice-over", 2.25, False),
            (3, None, None, None),
            (4, "delta", -0.5, True),
            (5, "echo", 100.125, False),
        ],
        MIXED_SCHEMA,
    )


def _nested_frame(session: ReparkSession) -> Any:
    """The struct/array/map frame with truncatable nested strings."""
    return session.createDataFrame(
        [
            (1, (1, "s1-quite-a-long-nested-string"), [1, 2, 3, 4], {"k1": 1, "k2": 2}),
            (2, None, [], {}),
            (3, (3, "s3"), None, None),
        ],
        NESTED_SCHEMA,
    )


def _scalar_frame(session: ReparkSession) -> Any:
    """The seven-type-plus-binary frame covering scalar spellings."""
    return session.createDataFrame(
        [
            (1, 1.5, "alpha", True, BASE_DATE, BASE_TS, Decimal("1.50"), b"ab"),
            (2, float("nan"), None, False, None, None, Decimal("2.25"), None),
            (3, -0.125, "héllo→world✓", True, BASE_DATE, BASE_TS, Decimal("-0.75"), b"\x00\xff"),
            (4, 1e15, "日本語", None, None, None, None, b""),
        ],
        SCALAR_SCHEMA,
    )


def _inf_frame(session: ReparkSession) -> Any:
    """An inf/-inf/nan float frame through the SQL door (infinite floats refuse CDF)."""
    return session.sql(
        "SELECT CAST('inf' AS DOUBLE) AS f, CAST('-inf' AS DOUBLE) AS g, "
        "CAST('nan' AS DOUBLE) AS h, CAST(1.5 AS DOUBLE) AS v"
    )


def _escape_frame(session: ReparkSession) -> Any:
    """The frame whose column names and cells carry HTML-escapable characters."""
    return session.createDataFrame(
        [(1, "a<b>&\"'"), (2, "<script>x</script>")],
        ["id", "a<b>&\"'"],
    )


def _wide_frame(session: ReparkSession) -> Any:
    """The 50-column frame with two rows."""
    ddl = ", ".join(
        f"c{index:02d} {'BIGINT' if index % 2 == 0 else 'STRING'}" for index in range(50)
    )
    return session.createDataFrame(
        [
            tuple(index if index % 2 == 0 else f"v{index}" for index in range(50)),
            tuple(
                index * 10 if index % 2 == 0 else f"w{index}-longish-string-value"
                for index in range(50)
            ),
        ],
        ddl,
    )


def _wide12_frame(session: ReparkSession) -> Any:
    """A lazy 12-column frame built through the SQL door."""
    return session.sql(
        "SELECT " + ", ".join(f"CAST({index} AS INT) AS c{index}" for index in range(12))
    )


def _zero_col_frame(session: ReparkSession) -> Any:
    """A one-row zero-column lazy frame."""
    return session.sql("SELECT 1 AS x").drop("x")


def _zero_col_empty_frame(session: ReparkSession) -> Any:
    """A zero-row zero-column lazy frame."""
    return session.sql("SELECT 1 AS x WHERE 1 = 0").drop("x")


def _case_ascii_trunc2(session: ReparkSession) -> dict[str, Any]:
    """ASCII ``show(3, truncate=2)``: sub-cap hard cut, no ellipsis."""
    frame = _mixed_frame(session)
    return dict([_door("show_n3_trunc2", lambda: _capture_show(frame, 3, truncate=2))])


def _case_ascii_trunc_str7(session: ReparkSession) -> dict[str, Any]:
    """ASCII ``show(3, truncate="7")``: digit-string truncate argument."""
    frame = _mixed_frame(session)
    return dict([_door("show_n3_trunc_str7", lambda: _capture_show(frame, 3, truncate="7"))])


def _case_ascii_nested(session: ReparkSession) -> dict[str, Any]:
    """ASCII ``show(4, truncate=20)`` on the nested frame."""
    frame = _nested_frame(session)
    return dict([_door("show_trunc20", lambda: _capture_show(frame, 4, truncate=20))])


def _case_ascii_nested_off(session: ReparkSession) -> dict[str, Any]:
    """ASCII ``show(4, truncate=False)`` on the nested frame."""
    frame = _nested_frame(session)
    return dict([_door("show_trunc_off", lambda: _capture_show(frame, 4, truncate=False))])


def _case_ascii_scalars(session: ReparkSession) -> dict[str, Any]:
    """ASCII ``show`` on the scalar frame plus the SQL-door inf frame."""
    frame = _scalar_frame(session)
    inf = _inf_frame(session)
    return dict(
        [
            _door("show_default", lambda: _capture_show(frame, 4)),
            _door("show_inf", lambda: _capture_show(inf, 4)),
        ]
    )


def _case_vertical_trunc_off(session: ReparkSession) -> dict[str, Any]:
    """Vertical ``show(3, truncate=False)`` on the mixed frame."""
    frame = _mixed_frame(session)
    return dict(
        [_door("show_v_trunc_off", lambda: _capture_show(frame, 3, truncate=False, vertical=True))]
    )


def _case_vertical_trunc5(session: ReparkSession) -> dict[str, Any]:
    """Vertical ``show(3, truncate=5)`` on the mixed frame."""
    frame = _mixed_frame(session)
    return dict(
        [_door("show_v_trunc5", lambda: _capture_show(frame, 3, truncate=5, vertical=True))]
    )


def _case_vertical_trunc2(session: ReparkSession) -> dict[str, Any]:
    """Vertical ``show(3, truncate=2)`` on the mixed frame."""
    frame = _mixed_frame(session)
    return dict(
        [_door("show_v_trunc2", lambda: _capture_show(frame, 3, truncate=2, vertical=True))]
    )


def _case_vertical_nested(session: ReparkSession) -> dict[str, Any]:
    """Vertical ``show(4)`` on the nested frame."""
    frame = _nested_frame(session)
    return dict([_door("show_v_trunc20", lambda: _capture_show(frame, 4, vertical=True))])


def _case_eager_trunc_default(session: ReparkSession) -> dict[str, Any]:
    """Eager ``repr`` at conf truncate 20 on the mixed frame."""
    frame = _mixed_frame(session)
    return _under_eager(session, 20, 20, lambda: dict([_door("repr", lambda: repr(frame))]))


def _case_eager_trunc_conf5(session: ReparkSession) -> dict[str, Any]:
    """Eager ``repr`` at conf truncate 5 on the mixed frame."""
    frame = _mixed_frame(session)
    return _under_eager(session, 20, 5, lambda: dict([_door("repr", lambda: repr(frame))]))


def _case_eager_trunc_off(session: ReparkSession) -> dict[str, Any]:
    """Eager ``repr`` at conf truncate 0 on the mixed frame."""
    frame = _mixed_frame(session)
    return _under_eager(session, 20, 0, lambda: dict([_door("repr", lambda: repr(frame))]))


def _case_eager_nested(session: ReparkSession) -> dict[str, Any]:
    """Eager ``repr`` on the nested frame."""
    frame = _nested_frame(session)
    return _under_eager(session, 20, 20, lambda: dict([_door("repr", lambda: repr(frame))]))


def _case_eager_wide(session: ReparkSession) -> dict[str, Any]:
    """Eager ``repr`` on the 50-column frame."""
    frame = _wide_frame(session)
    return _under_eager(session, 20, 20, lambda: dict([_door("repr", lambda: repr(frame))]))


def _case_html_escape(session: ReparkSession) -> dict[str, Any]:
    """``_repr_html_`` on names and cells carrying ``&<>"'`` characters."""
    frame = _escape_frame(session)
    return _under_eager(session, 20, 20, lambda: dict([_door("html", lambda: frame._repr_html_())]))


def _case_html_trunc5(session: ReparkSession) -> dict[str, Any]:
    """``_repr_html_`` at conf truncate 5 on the mixed frame."""
    frame = _mixed_frame(session)
    return _under_eager(session, 20, 5, lambda: dict([_door("html", lambda: frame._repr_html_())]))


def _case_html_trunc_off(session: ReparkSession) -> dict[str, Any]:
    """``_repr_html_`` at conf truncate 0 on the mixed frame."""
    frame = _mixed_frame(session)
    return _under_eager(session, 20, 0, lambda: dict([_door("html", lambda: frame._repr_html_())]))


def _case_html_nested(session: ReparkSession) -> dict[str, Any]:
    """``_repr_html_`` on the nested frame."""
    frame = _nested_frame(session)
    return _under_eager(session, 20, 20, lambda: dict([_door("html", lambda: frame._repr_html_())]))


def _case_polars_trunc_true(session: ReparkSession) -> dict[str, Any]:
    """Polars ``show(3, truncate=True)``: the ``str_len`` 30 cap with ``…``."""
    frame = _mixed_frame(session)
    return _under_style(
        session,
        "polars",
        lambda: dict([_door("show_trunc_true", lambda: _capture_show(frame, 3, truncate=True))]),
    )


def _case_polars_trunc10(session: ReparkSession) -> dict[str, Any]:
    """Polars ``show(3, truncate=10)``."""
    frame = _mixed_frame(session)
    return _under_style(
        session,
        "polars",
        lambda: dict([_door("show_trunc10", lambda: _capture_show(frame, 3, truncate=10))]),
    )


def _case_polars_trunc2(session: ReparkSession) -> dict[str, Any]:
    """Polars ``show(3, truncate=2)``: ``…`` still appended below cap 3."""
    frame = _mixed_frame(session)
    return _under_style(
        session,
        "polars",
        lambda: dict([_door("show_trunc2", lambda: _capture_show(frame, 3, truncate=2))]),
    )


def _case_polars_unicode(session: ReparkSession) -> dict[str, Any]:
    """Polars ``show`` on unicode cells under the cap."""
    frame = session.createDataFrame(
        [(1, "héllo→world✓"), (2, None), (3, "日本語")], "id INT, text STRING"
    )
    return _under_style(
        session,
        "polars",
        lambda: dict([_door("show_default", lambda: _capture_show(frame, 3))]),
    )


def _case_duckdb_trunc_true(session: ReparkSession) -> dict[str, Any]:
    """DuckDB ``show(4, truncate=True)``: the ``str_len`` 30 cap with ``...``."""
    frame = _mixed_frame(session)
    return _under_style(
        session,
        "duckdb",
        lambda: dict([_door("show_trunc_true", lambda: _capture_show(frame, 4, truncate=True))]),
    )


def _case_duckdb_trunc10(session: ReparkSession) -> dict[str, Any]:
    """DuckDB ``show(4, truncate=10)``."""
    frame = _mixed_frame(session)
    return _under_style(
        session,
        "duckdb",
        lambda: dict([_door("show_trunc10", lambda: _capture_show(frame, 4, truncate=10))]),
    )


def _case_duckdb_trunc2(session: ReparkSession) -> dict[str, Any]:
    """DuckDB ``show(4, truncate=2)``: sub-cap hard cut, no ellipsis."""
    frame = _mixed_frame(session)
    return _under_style(
        session,
        "duckdb",
        lambda: dict([_door("show_trunc2", lambda: _capture_show(frame, 4, truncate=2))]),
    )


def _case_duckdb_nested(session: ReparkSession) -> dict[str, Any]:
    """DuckDB ``show(4)`` on the nested frame."""
    frame = _nested_frame(session)
    return _under_style(
        session,
        "duckdb",
        lambda: dict([_door("show_default", lambda: _capture_show(frame, 4))]),
    )


def _case_duckdb_scalars(session: ReparkSession) -> dict[str, Any]:
    """DuckDB ``show`` on the scalar frame plus the SQL-door inf frame."""
    frame = _scalar_frame(session)
    inf = _inf_frame(session)
    return _under_style(
        session,
        "duckdb",
        lambda: dict(
            [
                _door("show_default", lambda: _capture_show(frame, 4)),
                _door("show_inf", lambda: _capture_show(inf, 4)),
            ]
        ),
    )


def _case_duckdb_lazy_wide(session: ReparkSession) -> dict[str, Any]:
    """DuckDB lazy ``repr`` on the 12-column frame (no max_cols gap rule)."""
    frame = _wide12_frame(session)
    return _under_style(session, "duckdb", lambda: dict([_door("repr", lambda: repr(frame))]))


def _styled_zero_col_doors(session: ReparkSession, frame: Any, style: str) -> dict[str, Any]:
    """The show and repr doors of one style on the zero-column frame."""
    return dict(
        [
            _door(f"show_{style}", lambda: _capture_show(frame, 5)),
            _door(f"repr_{style}", lambda: repr(frame)),
        ]
    )


def _case_styled_zero_cols(session: ReparkSession) -> dict[str, Any]:
    """Polars and duckdb ``show``/``repr`` on the zero-column frame."""
    frame = _zero_col_frame(session)
    results: dict[str, Any] = {}
    for style in ("polars", "duckdb"):
        results.update(
            _under_style(
                session, style, lambda bound=style: _styled_zero_col_doors(session, frame, bound)
            )
        )
    return results


def _spark_zero_col_doors(session: ReparkSession, frame: Any) -> dict[str, Any]:
    """Every spark door on one zero-column frame, eager eval on for HTML."""
    results = dict(
        [
            _door("show", lambda: _capture_show(frame, 5)),
            _door("show_vertical", lambda: _capture_show(frame, 5, vertical=True)),
            _door("repr_lazy", lambda: repr(frame)),
        ]
    )
    results.update(
        _under_eager(
            session,
            20,
            20,
            lambda: dict(
                [
                    _door("repr_eager", lambda: repr(frame)),
                    _door("html", lambda: frame._repr_html_()),
                ]
            ),
        )
    )
    return results


def _case_spark_zero_cols(session: ReparkSession) -> dict[str, Any]:
    """Spark doors on the one-row zero-column frame (slice-pad rows pinned)."""
    return _spark_zero_col_doors(session, _zero_col_frame(session))


def _case_spark_zero_cols_empty(session: ReparkSession) -> dict[str, Any]:
    """Spark doors on the zero-row zero-column frame (all slice-pad rows)."""
    return _spark_zero_col_doors(session, _zero_col_empty_frame(session))


def _put(out: dict[str, Case], case_id: str, thunk: Case) -> None:
    """Add one case; duplicate ids fail loudly."""
    if case_id in out:
        raise AssertionError(f"duplicate case id {case_id}")
    out[case_id] = thunk


def _all_cases() -> dict[str, Case]:
    """Every FACADE-5 display golden case."""
    out: dict[str, Case] = {}
    _put(out, "ascii_trunc2", _case_ascii_trunc2)
    _put(out, "ascii_trunc_str7", _case_ascii_trunc_str7)
    _put(out, "ascii_nested", _case_ascii_nested)
    _put(out, "ascii_nested_off", _case_ascii_nested_off)
    _put(out, "ascii_scalars", _case_ascii_scalars)
    _put(out, "vertical_trunc_off", _case_vertical_trunc_off)
    _put(out, "vertical_trunc5", _case_vertical_trunc5)
    _put(out, "vertical_trunc2", _case_vertical_trunc2)
    _put(out, "vertical_nested", _case_vertical_nested)
    _put(out, "eager_trunc_default", _case_eager_trunc_default)
    _put(out, "eager_trunc_conf5", _case_eager_trunc_conf5)
    _put(out, "eager_trunc_off", _case_eager_trunc_off)
    _put(out, "eager_nested", _case_eager_nested)
    _put(out, "eager_wide", _case_eager_wide)
    _put(out, "html_escape", _case_html_escape)
    _put(out, "html_trunc5", _case_html_trunc5)
    _put(out, "html_trunc_off", _case_html_trunc_off)
    _put(out, "html_nested", _case_html_nested)
    _put(out, "polars_trunc_true", _case_polars_trunc_true)
    _put(out, "polars_trunc10", _case_polars_trunc10)
    _put(out, "polars_trunc2", _case_polars_trunc2)
    _put(out, "polars_unicode", _case_polars_unicode)
    _put(out, "duckdb_trunc_true", _case_duckdb_trunc_true)
    _put(out, "duckdb_trunc10", _case_duckdb_trunc10)
    _put(out, "duckdb_trunc2", _case_duckdb_trunc2)
    _put(out, "duckdb_nested", _case_duckdb_nested)
    _put(out, "duckdb_scalars", _case_duckdb_scalars)
    _put(out, "duckdb_lazy_wide", _case_duckdb_lazy_wide)
    _put(out, "styled_zero_cols", _case_styled_zero_cols)
    _put(out, "spark_zero_cols", _case_spark_zero_cols)
    _put(out, "spark_zero_cols_empty", _case_spark_zero_cols_empty)
    return out


def _running_in_ci() -> bool:
    """True when GitHub Actions or generic CI is set."""
    return os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true"


def _record_requested() -> bool:
    """True when the explicit record environment variable is `1`."""
    return os.environ.get(RECORD_ENV, "") == "1"


def _assert_record_mode_allowed() -> None:
    """Refuse record mode when CI is set so a golden cannot be rewritten in CI."""
    if _record_requested() and _running_in_ci():
        raise AssertionError(f"{RECORD_ENV} is set while CI is set; refusing to rewrite goldens")


def _canonical_json(payload: dict[str, dict[str, object]]) -> str:
    """Stable JSON bytes used both to record and to compare."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _build_payload(spark: ReparkSession) -> dict[str, dict[str, object]]:
    """Run every case against one session."""
    return {case_id: case(spark) for case_id, case in _all_cases().items()}


def test_display_goldens_match_committed_bytes(spark: ReparkSession) -> None:
    """Byte-identical display-renderer goldens. pins: facade-5/C-004"""
    _assert_record_mode_allowed()
    payload = _build_payload(spark)
    assert len(payload) >= 30, f"need 30+ cases, got {len(payload)}"
    encoded = _canonical_json(payload)
    if _record_requested():
        GOLDEN_PATH.write_text(encoded, encoding="utf-8")
    if not GOLDEN_PATH.is_file():
        raise AssertionError(f"missing golden {GOLDEN_PATH.name}; set {RECORD_ENV}=1 to record")
    expected = GOLDEN_PATH.read_text(encoding="utf-8")
    if encoded != expected:
        actual_ids = set(payload)
        want = json.loads(expected)
        want_ids = set(want)
        missing = sorted(want_ids - actual_ids)
        extra = sorted(actual_ids - want_ids)
        changed = sorted(
            case_id
            for case_id in sorted(actual_ids & want_ids)
            if payload[case_id] != want[case_id]
        )
        raise AssertionError(
            f"golden byte mismatch missing={missing[:12]} extra={extra[:12]} changed={changed[:12]}"
        )


def test_record_mode_fails_when_ci_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Record env is refused in CI. pins: facade-5/C-004"""
    monkeypatch.setenv(RECORD_ENV, "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
    monkeypatch.delenv("CI")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
