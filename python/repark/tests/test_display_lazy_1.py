"""Lazy-repr pins. pins: display-lazy-1/C-001, C-002, C-003, C-004, C-005"""

from __future__ import annotations

import functools
import io
from collections.abc import Iterator
from contextlib import redirect_stdout
from typing import Any, cast

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.session import _DISPLAY_STYLE_KEY

_LAZY_FIRST_LINE_5 = (
    "lazy: 5 columns, not yet materialized — .eager(), .show() or .collect() run the plan"
)
_LAZY_FIRST_LINE_12 = (
    "lazy: 12 columns, not yet materialized — .eager(), .show() or .collect() run the plan"
)

_LAZY_FIRST_LINE_1 = (
    "lazy: 1 columns, not yet materialized — .eager(), .show() or .collect() run the plan"
)

_POLARS_LAZY_5 = """\
lazy: 5 columns, not yet materialized — .eager(), .show() or .collect() run the plan
┌─────┬─────┬─────────┬──────┬───────┐
│ id  ┆ v   ┆ new_col ┆ id_2 ┆ spied │
│ --- ┆ --- ┆ ---     ┆ ---  ┆ ---   │
│ i64 ┆ f64 ┆ i32     ┆ i32  ┆ i64   │
└─────┴─────┴─────────┴──────┴───────┘"""

_DUCKDB_LAZY_5 = """\
lazy: 5 columns, not yet materialized — .eager(), .show() or .collect() run the plan
┌───────┬────────┬─────────┬───────┬───────┐
│   id  │   v    │ new_col │  id_2 │ spied │
│ int64 │ double │  int32  │ int32 │ int64 │
└───────┴────────┴─────────┴───────┴───────┘"""

_POLARS_LAZY_12 = """\
lazy: 12 columns, not yet materialized — .eager(), .show() or .collect() run the plan
┌─────┬─────┬─────┬─────┬───┬─────┬─────┬─────┬─────┐
│ c0  ┆ c1  ┆ c2  ┆ c3  ┆ … ┆ c8  ┆ c9  ┆ c10 ┆ c11 │
│ --- ┆ --- ┆ --- ┆ --- ┆   ┆ --- ┆ --- ┆ --- ┆ --- │
│ i32 ┆ i32 ┆ i32 ┆ i32 ┆   ┆ i32 ┆ i32 ┆ i32 ┆ i32 │
└─────┴─────┴─────┴─────┴───┴─────┴─────┴─────┴─────┘"""


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


def _session_with_style(style: str) -> ReparkSession:
    """Build a fresh session with the given display style via builder config."""
    return ReparkSession.builder.config(_DISPLAY_STYLE_KEY, style).getOrCreate()


def _values_list(size: int) -> str:
    """Render a one-column VALUES list with exactly ``size`` ordered rows."""
    return ",".join(f"({index})" for index in range(1, size + 1))


def _spied_frame(session: ReparkSession, calls: list[int], size: int = 25) -> Any:
    """Build the 5-column frame whose ``spied`` column counts one UDF call per row."""

    def spy(value: int | None) -> int | None:
        calls.append(1)
        return None if value is None else int(value) * 2

    base = session.sql(
        "SELECT CAST(id AS BIGINT) AS id, CAST(id * 1.5 AS DOUBLE) AS v, "
        f"CAST(id % 7 AS INT) AS new_col, CAST(id + 100 AS INT) AS id_2 "
        f"FROM (VALUES {_values_list(size)}) AS t(id) ORDER BY id"
    )
    return base.withColumn("spied", F.udf(spy, "long")(base["id"]))


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


def _install_to_arrow_spy(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Patch ``DataFrame.to_arrow`` to append one entry per call and return the call log."""
    from repark import dataframe as dataframe_module

    calls: list[int] = []
    original = dataframe_module.DataFrame.to_arrow

    def counting_to_arrow(frame_arg: Any) -> Any:
        calls.append(1)
        return original(frame_arg)

    monkeypatch.setattr(dataframe_module.DataFrame, "to_arrow", counting_to_arrow)
    return calls


class _RecordingBridge:
    """Doubling bridge counting every yielded output batch."""

    def __init__(self) -> None:
        """Start with zero yielded batches."""
        self.batches: int = 0

    def __call__(self, batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Double the input column, recording one entry per yielded batch."""
        for batch in batches:
            self.batches += 1
            values = [
                None if item is None else int(item) * 2 for item in batch.column(0).to_pylist()
            ]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])


def test_lazy_repr_polars_exact_bytes(polars_session: ReparkSession) -> None:
    """A lazy 25-row 5-column frame renders the D-1 schema header under polars."""
    calls: list[int] = []
    frame = _spied_frame(polars_session, calls)
    assert calls == []
    assert repr(frame) == _POLARS_LAZY_5


def test_lazy_repr_duckdb_exact_bytes(duckdb_session: ReparkSession) -> None:
    """A lazy 25-row 5-column frame renders the D-1 schema header under duckdb."""
    calls: list[int] = []
    frame = _spied_frame(duckdb_session, calls)
    assert calls == []
    assert repr(frame) == _DUCKDB_LAZY_5


def test_lazy_repr_max_cols_elision(polars_session: ReparkSession) -> None:
    """A lazy 12-column frame hides the middle columns behind one gap column."""
    frame = polars_session.sql(
        "SELECT " + ", ".join(f"CAST({index} AS INT) AS c{index}" for index in range(12))
    )
    assert repr(frame) == _POLARS_LAZY_12


def test_lazy_repr_empty_columns() -> None:
    """A zero-column lazy frame headers with the minimal box under both styles."""
    for style in ("polars", "duckdb"):
        session = _session_with_style(style)
        try:
            frame = session.sql("SELECT 1 AS x").drop("x")
            assert frame.columns == []
            rendered = repr(frame)
            assert rendered.splitlines()[0].startswith("lazy: 0 columns, ")
            assert rendered.endswith("┌┐\n└┘")
        finally:
            session.stop()


def test_lazy_repr_zero_engine_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repr of a lazy frame costs zero UDF calls, zero counts, and zero exports."""
    count_calls = _install_count_spy(monkeypatch)
    export_calls = _install_to_arrow_spy(monkeypatch)
    for style in ("polars", "duckdb"):
        session = _session_with_style(style)
        try:
            count_calls.clear()
            export_calls.clear()
            udf_calls: list[int] = []
            frame = _spied_frame(session, udf_calls)
            assert udf_calls == []
            rendered = repr(frame)
            assert udf_calls == []
            assert count_calls == []
            assert export_calls == []
            assert rendered.splitlines()[0] == _LAZY_FIRST_LINE_5
            assert "shape:" not in rendered
        finally:
            session.stop()


def _twelve_id_frame(session: ReparkSession) -> Any:
    """Build the ordered 12-row single-column frame the D-2 pins share."""
    return session.sql(f"SELECT id FROM (VALUES {_values_list(12)}) AS t(id) ORDER BY id")


def test_eager_repr_renders_data_with_zero_counts(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repr of an eager frame renders the data table over the lazy header box."""
    count_calls = _install_count_spy(monkeypatch)
    frame = _twelve_id_frame(polars_session)
    lazy_lines = repr(frame).splitlines()
    assert count_calls == []
    eager = frame.eager()
    count_calls.clear()
    eager_rendered = repr(eager)
    eager_lines = eager_rendered.splitlines()
    assert count_calls == []
    assert eager_lines[0] == "shape: (12, 1)"
    assert "│ 1   │" in eager_rendered
    assert "│ 12  │" in eager_rendered
    assert eager_lines[1:5] == lazy_lines[1:5]
    assert eager_lines[-1] == lazy_lines[-1]


def test_cached_materialised_repr_renders_data_with_one_count(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repr of a materialised cache renders rows with exactly one count over the view."""
    count_calls = _install_count_spy(monkeypatch)
    export_calls = _install_to_arrow_spy(monkeypatch)
    udf_calls: list[int] = []
    frame = _spied_frame(polars_session, udf_calls)
    frame.cache()
    assert repr(frame).splitlines()[0] == _LAZY_FIRST_LINE_5
    assert frame.count() == 25
    assert frame._cache_view is not None
    count_calls.clear()
    export_calls.clear()
    udf_before = len(udf_calls)
    rendered = repr(frame)
    assert count_calls == [1]
    assert len(udf_calls) == udf_before
    assert rendered.splitlines()[0] == "shape: (25, 5)"
    assert "│ 1   ┆ 1.5" in rendered
    assert "│ 25  ┆ 37.5" in rendered


def test_persist_materialised_repr_renders_data_with_one_count(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repr of a materialised persist renders rows with exactly one count over the view."""
    count_calls = _install_count_spy(monkeypatch)
    frame = _twelve_id_frame(polars_session)
    frame.persist()
    assert frame.count() == 12
    assert frame._cache_view is not None
    count_calls.clear()
    rendered = repr(frame)
    assert count_calls == [1]
    assert rendered.splitlines()[0] == "shape: (12, 1)"
    assert "│ 12  │" in rendered


def test_checkpointed_repr_renders_data_with_zero_counts(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repr of an eager-plus-checkpoint frame renders rows reusing the stored shape."""
    count_calls = _install_count_spy(monkeypatch)
    frame = _twelve_id_frame(polars_session).eager().localCheckpoint()
    assert frame._eager_shape == (12, 1)
    assert frame._cache_view is None
    count_calls.clear()
    rendered = repr(frame)
    assert count_calls == []
    assert rendered.splitlines()[0] == "shape: (12, 1)"
    assert "│ 1   │" in rendered
    assert "│ 12  │" in rendered


def test_pending_cache_repr_stays_lazy(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repr of a cache-marked but unmaterialised frame stays schema-only with zero actions."""
    count_calls = _install_count_spy(monkeypatch)
    export_calls = _install_to_arrow_spy(monkeypatch)
    udf_calls: list[int] = []
    frame = _spied_frame(polars_session, udf_calls)
    frame.cache()
    assert frame._cache_view is None
    rendered = repr(frame)
    assert rendered == _POLARS_LAZY_5
    assert udf_calls == []
    assert count_calls == []
    assert export_calls == []


def test_action_does_not_flip_lazy(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Collect, count, and show leave the frame lazy; repr afterwards is still the header."""
    count_calls = _install_count_spy(monkeypatch)
    frame = _twelve_id_frame(polars_session)
    assert frame.count() == 12
    assert [row.id for row in frame.collect()] == list(range(1, 13))
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        frame.show()
    assert frame._eager_shape is None
    count_calls.clear()
    assert repr(frame).splitlines()[0] == _LAZY_FIRST_LINE_1
    assert count_calls == []


def test_eager_eval_lazy_repr_renders_rows_with_one_plan_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With eagerEval on, a lazy frame renders rows running the plan exactly once."""
    count_calls = _install_count_spy(monkeypatch)
    export_calls = _install_to_arrow_spy(monkeypatch)
    for style in ("polars", "duckdb"):
        session = _session_with_style(style)
        try:
            session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
            try:
                count_calls.clear()
                export_calls.clear()
                frame = session.sql(
                    f"SELECT id FROM (VALUES {_values_list(7)}) AS t(id) ORDER BY id"
                )
                rendered = repr(frame)
                if style == "polars":
                    assert "shape: (7, 1)" in rendered
                    assert "│ 7   │" in rendered
                else:
                    assert "7 rows" in rendered
                    assert "(7 shown)" not in rendered
                assert export_calls == [1]
                assert count_calls == []
            finally:
                session.conf.set("spark.sql.repl.eagerEval.enabled", "false")
        finally:
            session.stop()


def test_spark_door_and_str_and_html_unchanged() -> None:
    """Spark repr, str, and styled HTML keep their shipped bytes under every style."""
    calls: list[int] = []
    schema_form = "DataFrame[id: bigint, v: double, new_col: int, id_2: int, spied: bigint]"
    for style in ("spark", "polars", "duckdb"):
        session = _session_with_style(style)
        try:
            frame = _spied_frame(session, calls)
            assert str(frame) == schema_form
            assert frame._repr_html_() is None
            if style == "spark":
                assert repr(frame) == schema_form
        finally:
            session.stop()
    assert calls == []


def test_lazy_bridged_repr_renders_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lazy mapInArrow frame renders the D-1 header running the bridge zero times."""
    count_calls = _install_count_spy(monkeypatch)
    for style, label in (("polars", "i32"), ("duckdb", "int32")):
        session = _session_with_style(style)
        try:
            bridge = _RecordingBridge()
            count_calls.clear()
            base = session.sql(f"SELECT id FROM (VALUES {_values_list(5)}) AS t(id) ORDER BY id")
            frame = base.mapInArrow(bridge, "x INT")
            rendered = repr(frame)
            assert bridge.batches == 0
            assert count_calls == []
            assert rendered.splitlines()[0] == (
                "lazy: 1 columns, not yet materialized — .eager(), "
                ".show() or .collect() run the plan"
            )
            assert label in rendered
            assert "shape:" not in rendered
            assert " rows" not in rendered
        finally:
            session.stop()


def _spied(value: int | None, calls: list[int]) -> int | None:
    """Double one value while recording the call (module scope for the D-5 chains)."""
    calls.append(1)
    return None if value is None else int(value) * 2


def _pure_frame(session: ReparkSession, size: int = 25) -> Any:
    """Build the ordered pure-SQL frame the D-5 chains share (no UDF anywhere)."""
    return session.sql(
        "SELECT CAST(id AS BIGINT) AS id, CAST(id * 1.5 AS DOUBLE) AS v, "
        f"CAST(id % 7 AS INT) AS new_col, CAST(id + 100 AS INT) AS id_2 "
        f"FROM (VALUES {_values_list(size)}) AS t(id) ORDER BY id"
    )


def test_transformations_stay_lazy_until_action(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Eight transformation chains cost zero UDF and count calls through repr."""
    count_calls = _install_count_spy(monkeypatch)
    udf_calls: list[int] = []
    spy = F.udf(functools.partial(_spied, calls=udf_calls), "long")
    base = _pure_frame(polars_session)
    other = _pure_frame(polars_session)
    filtered = base.filter(base["id"] > 3)
    grouped = base.groupBy("new_col").agg(F.count("id").alias("n"))
    ordered = base.orderBy("id")
    joined = base.join(other.select("id").withColumn("flag", F.lit(1)), on="id")
    unioned = base.union(other)
    chains = [
        base.withColumns({"spied": spy(base["id"]), "extra": base["id"] + 1}),
        base.withColumn("spied", spy(base["id"])),
        base.select("id", "v", spy(base["id"]).alias("spied")),
        filtered.withColumn("spied", spy(filtered["id"])),
        grouped.withColumn("spied", spy(grouped["new_col"])),
        ordered.withColumn("spied", spy(ordered["id"])),
        joined.withColumn("spied", spy(joined["id"])),
        unioned.withColumn("spied", spy(unioned["id"])),
    ]
    assert udf_calls == []
    assert count_calls == []
    for frame in chains:
        rendered = repr(frame)
        assert rendered.splitlines()[0].startswith("lazy: ")
        assert "shape:" not in rendered
    assert udf_calls == []
    assert count_calls == []
    for frame in chains:
        before = len(udf_calls)
        rows = frame.collect()
        assert len(rows) == len(udf_calls) - before > 0
