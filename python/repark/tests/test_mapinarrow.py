"""U-SPIKE-MAPINARROW: DataFrame.mapInArrow facade streaming bridge pins."""

from __future__ import annotations

import gc
from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException, PySparkTypeError
from repark.types import IntegerType, StructField, StructType


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("mapinarrow").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _double_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        values = [None if v is None else int(v) * 2 for v in batch.column(0).to_pylist()]
        yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])


def test_mapinarrow_values_and_arrow_type(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.mapInArrow(_double_batches, "x INT")
    # Laziness: schema/columns without executing user func.
    assert out.columns == ["x"]
    table = out.to_arrow()
    assert table.column_names == ["x"]
    assert table.schema.field("x").type == pa.int32()
    assert sorted(table.column("x").to_pylist()) == [2, 4, 6]


def test_mapinarrow_structtype_schema(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,)], schema=StructType([StructField("x", IntegerType())]))
    out = frame.mapInArrow(_double_batches, StructType([StructField("x", IntegerType())]))
    assert out.collect()[0].x == 2


def test_mapinarrow_empty_input(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([], "x INT")
    calls = {"n": 0}

    def track(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from batches

    out = frame.mapInArrow(track, "x INT")
    assert out.collect() == []
    assert calls["n"] == 1  # action ran


def test_mapinarrow_empty_iterator_from_func(spark: ReparkSession) -> None:
    def drop_all(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for _ in batches:
            pass
        return iter(())

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.mapInArrow(drop_all, "x INT")
    assert out.collect() == []
    assert out.to_arrow().num_rows == 0
    assert out.to_arrow().schema.field("x").type == pa.int32()


def test_mapinarrow_schema_mismatch_names(spark: ReparkSession) -> None:
    def wrong_name(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            yield pa.record_batch([batch.column(0)], names=["y"])

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInArrow(wrong_name, "x INT")
    with pytest.raises(
        PySparkException, match=r"schema mismatch: field names.*\['x'\].*\['y'\]"
    ) as caught:
        out.collect()
    message = str(caught.value)
    assert "expected" in message and "got" in message
    assert "x" in message and "y" in message


def test_mapinarrow_schema_mismatch_types(spark: ReparkSession) -> None:
    def wrong_type(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            as_str = pa.array([str(v) for v in batch.column(0).to_pylist()], type=pa.string())
            yield pa.record_batch([as_str], names=["x"])

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInArrow(wrong_type, "x INT")
    with pytest.raises(PySparkException, match=r"schema mismatch on field 'x'") as caught:
        out.collect()
    message = str(caught.value)
    # Loud name + type detail (octo C1-Q-006) — not a bare "schema mismatch" substring.
    assert "int32" in message
    assert "string" in message


def test_mapinarrow_user_func_exception_surfaces(spark: ReparkSession) -> None:
    def boom(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for _ in batches:
            raise RuntimeError("user boom")
        return iter(())

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInArrow(boom, "x INT")
    with pytest.raises(PySparkException, match="user boom") as caught:
        out.collect()
    message = str(caught.value)
    assert "RuntimeError" in message
    # Both traceback text AND the original message (octo C2-Q-004 — not a hollow or-clause).
    assert "Traceback" in message
    assert "user boom" in message


def test_mapinarrow_lazy_until_action(spark: ReparkSession) -> None:
    calls = {"n": 0}

    def counted(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from _double_batches(batches)

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInArrow(counted, "x INT")
    assert calls["n"] == 0
    _ = out.columns
    _ = out.schema
    assert calls["n"] == 0
    assert out.count() == 1
    assert calls["n"] == 1


def test_mapinarrow_repeated_actions_rerun(spark: ReparkSession) -> None:
    calls = {"n": 0}

    def counted(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from _double_batches(batches)

    frame = spark.createDataFrame([(3,)], "x INT")
    out = frame.mapInArrow(counted, "x INT")
    assert out.count() == 1
    assert out.collect()[0].x == 6
    assert calls["n"] == 2


def test_mapinarrow_cache_pins_single_run(spark: ReparkSession) -> None:
    calls = {"n": 0}

    def counted(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from _double_batches(batches)

    frame = spark.createDataFrame([(4,)], "x INT")
    out = frame.mapInArrow(counted, "x INT").cache()
    assert out.count() == 1
    assert out.collect()[0].x == 8
    # cache materializes once on first action; second action scans MemTable.
    assert calls["n"] == 1


def test_mapinarrow_cache_unpersist_reruns_func(spark: ReparkSession) -> None:
    """After unpersist, mapInArrow must re-run func (octo C1-Q-003 / C1-L-003)."""
    calls = {"n": 0}

    def counted(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from _double_batches(batches)

    frame = spark.createDataFrame([(5,)], "x INT")
    out = frame.mapInArrow(counted, "x INT").cache()
    assert out.count() == 1
    assert calls["n"] == 1
    out.unpersist()
    assert out.is_cached is False
    assert out._map_bridge is not None
    assert out.collect()[0].x == 10
    assert calls["n"] == 2


def test_mapinarrow_multi_batch_input_row_multiset(spark: ReparkSession) -> None:
    # createDataFrame path may be one batch; still pin multiset identity.
    rows = [(i,) for i in range(30)]
    frame = spark.createDataFrame(rows, "x INT")
    out = frame.mapInArrow(_double_batches, "x INT")
    got = sorted(out.to_arrow().column("x").to_pylist())
    assert got == [i * 2 for i in range(30)]


def test_mapinarrow_requires_schema(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,)], "x INT")
    with pytest.raises(PySparkTypeError):
        frame.mapInArrow(_double_batches, None)  # type: ignore[arg-type]


def test_mapinarrow_func_must_be_callable(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,)], "x INT")
    with pytest.raises(PySparkTypeError, match="callable"):
        frame.mapInArrow("not-callable", "x INT")  # type: ignore[arg-type]


def test_mapinarrow_filter_after_materializes(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.mapInArrow(_double_batches, "x INT").filter("x > 3")
    assert sorted(row.x for row in out.collect()) == [4, 6]


def test_mapinarrow_smallint_tinyint_float_arrow_widths(spark: ReparkSession) -> None:
    """DDL widths match session createDataFrame (octo C1-Q-001 / C1-L-001 / C1-L-002)."""

    def identity(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        yield from batches

    small = spark.createDataFrame([(1,), (2,)], "x SMALLINT")
    small_out = small.mapInArrow(identity, "x SMALLINT").to_arrow()
    assert small_out.schema.field("x").type == pa.int16()
    assert sorted(small_out.column("x").to_pylist()) == [1, 2]

    tiny = spark.createDataFrame([(3,)], "x TINYINT")
    tiny_out = tiny.mapInArrow(identity, "x TINYINT").to_arrow()
    assert tiny_out.schema.field("x").type == pa.int8()
    assert tiny_out.column("x").to_pylist() == [3]

    flt = spark.createDataFrame([(1.5,)], "x FLOAT")
    flt_out = flt.mapInArrow(identity, "x FLOAT").to_arrow()
    assert flt_out.schema.field("x").type == pa.float32()
    assert flt_out.column("x").to_pylist() == pytest.approx([1.5])


def test_mapinarrow_incremental_not_collect_all_then_udf(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Output IPC is written as each batch yields — not after exhausting func (C1-Q-004).

    I4 primary path uses C-stream register (no IPC). This pin exercises the **IPC fallback**
    path (native symbol hidden) so C1-Q-004 stays load-bearing for version-skew.
    """
    import pyarrow.ipc as pa_ipc

    write_events: list[str] = []
    real_new_stream = pa_ipc.new_stream
    tracking_enabled = {"on": False}

    def tracking_new_stream(
        sink: object, schema: object, *args: object, **kwargs: object
    ) -> object:
        writer = real_new_stream(sink, schema, *args, **kwargs)
        if not tracking_enabled["on"]:
            return writer
        original_write = writer.write_batch

        def write_batch(batch: pa.RecordBatch, **write_kwargs: object) -> None:
            write_events.append(f"write:{batch.num_rows}")
            original_write(batch, **write_kwargs)

        writer.write_batch = write_batch  # type: ignore[method-assign]
        return writer

    monkeypatch.setattr(pa_ipc, "new_stream", tracking_new_stream)

    def multi_yield(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        # Two synthetic output batches independent of parent chunking.
        yield pa.record_batch([pa.array([1], type=pa.int32())], names=["x"])
        write_events.append("after_first_yield")
        yield pa.record_batch([pa.array([2], type=pa.int32())], names=["x"])
        write_events.append("after_second_yield")

    frame = spark.createDataFrame([(0,)], "x INT")
    out = frame.mapInArrow(multi_yield, "x INT")
    # Force IPC fallback so new_stream write tracking observes incremental encode.
    real_session = out._session

    class _SessionIpcOnly:
        def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
            real_session.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

        def __getattr__(self, name: str) -> object:
            if name == "register_arrow_stream_as_temp_view":
                raise AttributeError(name)
            return getattr(real_session, name)

    out._session = _SessionIpcOnly()  # type: ignore[assignment]
    write_events.clear()
    tracking_enabled["on"] = True
    table = out.to_arrow()
    assert sorted(table.column("x").to_pylist()) == [1, 2]
    # Incremental: first write lands before the generator resumes after first yield.
    assert write_events[0] == "write:1"
    assert write_events[1] == "after_first_yield"
    assert write_events[2] == "write:1"
    assert write_events[3] == "after_second_yield"


def test_mapinarrow_upstream_closed_on_func_raise(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Upstream RecordBatchReader.close runs when func raises early (C1-Q-002 / C1-SAF-002)."""
    closed = {"n": 0}
    real_from_stream = pa.RecordBatchReader.from_stream

    class _TrackingReader:
        def __init__(self, inner: pa.RecordBatchReader) -> None:
            self._inner = inner

        def __iter__(self) -> Iterator[pa.RecordBatch]:
            yield from self._inner

        def close(self) -> None:
            closed["n"] += 1
            close = getattr(self._inner, "close", None)
            if callable(close):
                close()

    def tracking_from_stream(source: object, *args: object, **kwargs: object) -> _TrackingReader:
        return _TrackingReader(real_from_stream(source, *args, **kwargs))

    class _PatchedRecordBatchReader:
        from_stream = staticmethod(tracking_from_stream)

    monkeypatch.setattr(pa, "RecordBatchReader", _PatchedRecordBatchReader)

    def boom_before_consume(
        batches: Iterator[pa.RecordBatch],
    ) -> Iterator[pa.RecordBatch]:
        raise RuntimeError("early boom")
        yield  # pragma: no cover

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInArrow(boom_before_consume, "x INT")
    with pytest.raises(PySparkException, match="early boom"):
        out.collect()
    assert closed["n"] >= 1

    def return_none(batches: Iterator[pa.RecordBatch]) -> None:
        return None

    out_none = frame.mapInArrow(return_none, "x INT")  # type: ignore[arg-type]
    with pytest.raises(PySparkException, match="got None"):
        out_none.collect()
    assert closed["n"] >= 2


def test_mapinarrow_mia_views_hidden_and_gc_bounded(spark: ReparkSession) -> None:
    """``__repark_mia_*`` hidden from listTables; GC drops views (C1-SAF-001)."""
    spark._ensure_information_schema()

    def _raw_mia_count() -> int:
        rows = (
            spark.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE '__repark_mia_%'"
            )
            .to_arrow()
            .to_pylist()
        )
        return len(rows)

    def identity(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        yield from batches

    baseline = _raw_mia_count()
    holders = []
    for index in range(8):
        frame = spark.createDataFrame([(index,)], "x INT")
        holders.append(frame.mapInArrow(identity, "x INT"))
    mid = _raw_mia_count()
    assert mid > baseline
    listed = [table.name for table in spark.catalog.listTables()]
    assert not any(name.startswith("__repark_mia_") for name in listed)
    # Run a few actions (ephemeral action views).
    for holder in holders[:3]:
        assert holder.count() == 1
    del holders
    gc.collect()
    gc.collect()
    after = _raw_mia_count()
    assert after <= baseline + 3, (baseline, mid, after)


def test_mapinarrow_register_tracks_before_sql_fail(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failed sql after register must not leave an untracked MIA view (C3-SAF-001).

    Native PyO3 methods are read-only; wrap ``DataFrame._session`` (Python attr) so we can
    inject sql failure after register_ipc and assert track/drop ordering.
    """
    from repark.dataframe import DataFrame as _DataFrame

    spark._ensure_information_schema()

    def identity(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        yield from batches

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInArrow(identity, "x INT")
    events: list[str] = []
    real = out._session
    original_track = _DataFrame._track_mia_view

    def tracking_track(self: object, view_name: str, *, replace_ephemeral: bool) -> None:
        events.append(f"track:{view_name}")
        original_track(self, view_name, replace_ephemeral=replace_ephemeral)

    monkeypatch.setattr(_DataFrame, "_track_mia_view", tracking_track)

    class _SessionProxy:
        def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
            events.append(f"register:{view_name}")
            real.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

        def register_arrow_stream_as_temp_view(self, view_name: str, stream_obj: object) -> None:
            # I4 primary path — action re-ingest uses C-stream register (IPC is fallback).
            events.append(f"register:{view_name}")
            real.register_arrow_stream_as_temp_view(view_name, stream_obj)

        def sql(self, query: str) -> object:
            events.append(f"sql:{query}")
            if "__repark_mia_" in query:
                raise RuntimeError("injected register-path failure")
            return real.sql(query)

        def drop_temp_view(self, view_name: str) -> object:
            events.append(f"drop:{view_name}")
            return real.drop_temp_view(view_name)

        def __getattr__(self, name: str) -> object:
            return getattr(real, name)

    out._session = _SessionProxy()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="injected register-path failure"):
        out.collect()

    # Action path: register → track → sql fails; view stays on finalize list (tracked).
    reg_i = next(i for i, event in enumerate(events) if event.startswith("register:"))
    track_i = next(i for i, event in enumerate(events) if event.startswith("track:"))
    sql_i = next(i for i, event in enumerate(events) if event.startswith("sql:"))
    assert reg_i < track_i < sql_i, events
    registered_name = events[reg_i].split(":", 1)[1]
    assert registered_name in out._mia_temp_views

    # Placeholder-construction path: sql fail after register must eager-drop.
    events.clear()
    real_frame_session = frame._session
    frame._session = _SessionProxy()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="injected register-path failure"):
        frame.mapInArrow(identity, "x INT")
    reg_i = next(i for i, event in enumerate(events) if event.startswith("register:"))
    sql_i = next(i for i, event in enumerate(events) if event.startswith("sql:"))
    drop_i = next(i for i, event in enumerate(events) if event.startswith("drop:"))
    assert reg_i < sql_i < drop_i, events
    frame._session = real_frame_session


def test_mapinarrow_isempty_take_peek_bounded(spark: ReparkSession) -> None:
    """isEmpty/take do not require full output materialize (C1-SAF-003)."""
    yields = {"n": 0}

    def many_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        # Drain parent, then emit many singleton batches.
        for _ in batches:
            pass
        for value in range(50):
            yields["n"] += 1
            yield pa.record_batch([pa.array([value], type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(0,)], "x INT")
    out = frame.mapInArrow(many_batches, "x INT")
    assert out.isEmpty() is False
    # isEmpty only needs the first output row — should stop well before 50 yields.
    assert yields["n"] == 1

    yields["n"] = 0
    rows = out.take(3)
    assert [row.x for row in rows] == [0, 1, 2]
    assert yields["n"] == 3


def test_mapinarrow_show_peek_bounded(spark: ReparkSession) -> None:
    """show() MIA path bounds output yields via max_output_rows (octo C4-Q-002)."""
    import contextlib
    import io

    yields = {"n": 0}

    def many_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for _ in batches:
            pass
        for value in range(50):
            yields["n"] += 1
            yield pa.record_batch([pa.array([value], type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(0,)], "x INT")
    out = frame.mapInArrow(many_batches, "x INT")
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        out.show(4)
    assert yields["n"] == 4
    rendered = buffer.getvalue()
    assert "0" in rendered and "3" in rendered
    # Peek must not one-shot-clear the bridge (same re-run contract as take/isEmpty).
    assert out._map_bridge is not None
    yields["n"] = 0
    assert out.take(2)[0].x == 0
    assert yields["n"] == 2


def test_mapinarrow_parent_rerun_after_plan_child(spark: ReparkSession) -> None:
    """filter/select must not clear parent re-run (octo C4-Q-001 / C4-L-001)."""
    calls = {"n": 0}

    def counted(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from _double_batches(batches)

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.mapInArrow(counted, "x INT")
    filtered = out.filter("x > 0")
    selected = out.select("x")
    assert out._map_bridge is not None
    # Child plans are one-shot snapshots (v1); they still materialize real rows.
    assert sorted(row.x for row in filtered.collect()) == [2, 4]
    assert sorted(row.x for row in selected.collect()) == [2, 4]
    after_children = calls["n"]
    assert after_children >= 1
    # Parent actions re-run unless cache (charter + _action_inner contract).
    assert out.count() == 2
    assert out.count() == 2
    assert calls["n"] == after_children + 2
    assert out._map_bridge is not None
    # Child plan-stable MemTable survives parent action re-runs.
    assert sorted(row.x for row in filtered.collect()) == [2, 4]


def test_mapinarrow_set_op_right_fail_drops_left_staging(spark: ReparkSession) -> None:
    """crossJoin/set-ops must drop left staging if right materialize fails (C4-SAF-001)."""

    def identity(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        yield from batches

    def boom(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        raise RuntimeError("right staging boom")
        yield  # pragma: no cover

    spark._ensure_information_schema()

    def _staging_count() -> int:
        rows = (
            spark.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE '__repark_set_%' "
                "OR table_name LIKE '__repark_x_%'"
            )
            .to_arrow()
            .to_pylist()
        )
        return len(rows)

    left = spark.createDataFrame([(1,), (2,)], "x INT").mapInArrow(identity, "x INT")
    right = spark.createDataFrame([(3,)], "x INT").mapInArrow(boom, "x INT")
    before = _staging_count()
    with pytest.raises(PySparkException, match="right staging boom"):
        _ = left.intersect(right)
    assert _staging_count() == before
    with pytest.raises(PySparkException, match="right staging boom"):
        _ = left.crossJoin(right)
    assert _staging_count() == before
    # Left handle still re-runs after failed dual registration.
    assert sorted(row.x for row in left.collect()) == [1, 2]


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("pandas") is None,
    reason="pandas optional extra not installed",
)
def test_mapinarrow_mapinpandas_values(spark: ReparkSession) -> None:
    """mapInPandas thin wrapper (octo C1-Q-005)."""

    def double_pdfs(pdfs: Iterator[object]) -> Iterator[object]:
        for pdf in pdfs:
            out = pdf.copy()
            out["x"] = out["x"] * 2
            yield out

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    result = frame.mapInPandas(double_pdfs, "x INT")
    assert sorted(row.x for row in result.collect()) == [2, 4, 6]
    assert result.to_arrow().schema.field("x").type == pa.int32()


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("pandas") is None,
    reason="pandas optional extra not installed",
)
def test_mapinarrow_mapinpandas_none_is_loud(spark: ReparkSession) -> None:
    """mapInPandas must not treat None as empty iterator (octo C3-L-002)."""

    def return_none(pdfs: Iterator[object]) -> None:
        for _ in pdfs:
            pass
        return None

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInPandas(return_none, "x INT")  # type: ignore[arg-type]
    with pytest.raises(PySparkException, match=r"got None"):
        out.collect()


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("pandas") is None,
    reason="pandas optional extra not installed",
)
def test_mapinarrow_mapinpandas_empty_wrong_name_is_loud(spark: ReparkSession) -> None:
    """Empty wrong-name pandas yield must not silent-empty under declared schema (C6-L-001).

    ``pa.Table.from_pandas(...).to_batches()`` returns ``[]`` for zero-row frames, which used
    to skip ``_validate_map_in_arrow_batch``. Mutation: drop the 0-row RecordBatch emit in
    ``mapInPandas`` and this pin fails (``collect() == []`` instead of loud mismatch).
    """
    import pandas as pd

    def wrong_empty_name(pdfs: Iterator[object]) -> Iterator[object]:
        for _ in pdfs:
            pass
        yield pd.DataFrame(columns=["y"])

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInPandas(wrong_empty_name, "x INT")
    with pytest.raises(PySparkException, match=r"schema mismatch: field names.*\['x'\].*\['y'\]"):
        out.collect()


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("pandas") is None,
    reason="pandas optional extra not installed",
)
def test_mapinarrow_mapinpandas_empty_wrong_type_is_loud(spark: ReparkSession) -> None:
    """Empty wrong-type pandas yield must be loud like mapInArrow empty batches (C6-L-001)."""
    import pandas as pd

    def wrong_empty_type(pdfs: Iterator[object]) -> Iterator[object]:
        for _ in pdfs:
            pass
        yield pd.DataFrame({"x": pd.Series([], dtype="float64")})

    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.mapInPandas(wrong_empty_type, "x INT")
    with pytest.raises(PySparkException, match=r"schema mismatch on field 'x'") as caught:
        out.collect()
    message = str(caught.value)
    assert "int32" in message
    assert "double" in message or "float" in message


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("pandas") is None,
    reason="pandas optional extra not installed",
)
def test_mapinarrow_mapinpandas_empty_correct_schema_ok(spark: ReparkSession) -> None:
    """Empty pandas with matching schema remains a valid empty multiset (C6-L-001)."""
    import pandas as pd

    def empty_ok(pdfs: Iterator[object]) -> Iterator[object]:
        for _ in pdfs:
            pass
        yield pd.DataFrame({"x": pd.Series([], dtype="int32")})

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.mapInPandas(empty_ok, "x INT")
    assert out.collect() == []
    assert out.to_arrow().schema.field("x").type == pa.int32()


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("pandas") is None,
    reason="pandas optional extra not installed",
)
def test_mapinarrow_mapinpandas_yield_before_consume_preserves_input(
    spark: ReparkSession,
) -> None:
    """Yield-before-consume must still iterate the real input pdfs (octo C8-L-001).

    ``mapInPandas``'s ``_arrow_func`` closes ``_pdf_iter`` over the input batch stream.
    Rebinding that free name to each yield's ``table.to_batches()`` made a UDF that
    yields a prefix row then walks ``pdfs`` pull the *output* (or ``[]``) as input —
    e.g. prefix ``0`` + input ``[1,2,3]`` → ``[0,0]`` instead of ``[0,1,2,3]``, or an
    empty prefix silently dropped the whole multiset. Mutation: reassign the closed-over
    input name on yield and this pin fails.
    """
    import pandas as pd

    def yield_then_consume(pdfs: Iterator[object]) -> Iterator[object]:
        yield pd.DataFrame({"x": pd.Series([0], dtype="int32")})
        yield from pdfs

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.mapInPandas(yield_then_consume, "x INT")
    assert sorted(row.x for row in out.collect()) == [0, 1, 2, 3]
    assert out.to_arrow().schema.field("x").type == pa.int32()


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("pandas") is None,
    reason="pandas optional extra not installed",
)
def test_mapinarrow_mapinpandas_empty_prefix_then_consume_preserves_input(
    spark: ReparkSession,
) -> None:
    """Empty yield-before-consume must not drop the input multiset (octo C8-L-001)."""
    import pandas as pd

    def empty_prefix_then_consume(pdfs: Iterator[object]) -> Iterator[object]:
        yield pd.DataFrame({"x": pd.Series([], dtype="int32")})
        yield from pdfs

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.mapInPandas(empty_prefix_then_consume, "x INT")
    assert sorted(row.x for row in out.collect()) == [1, 2, 3]


def test_mapinarrow_identity_noops_preserve_bridge(spark: ReparkSession) -> None:
    """repartition/coalesce/hint/offset(0)/toDF() must not drop MIA rows (C2-Q-002 / C2-L-002)."""
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    base = frame.mapInArrow(_double_batches, "x INT")
    for child in (
        base.repartition(4),
        base.coalesce(1),
        base.hint("broadcast"),
        base.offset(0),
        base.toDF(),
    ):
        assert child._map_bridge is not None
        assert sorted(row.x for row in child.collect()) == [2, 4, 6]


def test_mapinarrow_selectexpr_alias_sample_union_not_empty(spark: ReparkSession) -> None:
    """SQL-lowering paths must materialize bridge before binding _inner (C2-Q-003 / C2-L-003)."""
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    mapped = frame.mapInArrow(_double_batches, "x INT")
    assert sorted(row.x for row in mapped.selectExpr("x").collect()) == [2, 4, 6]
    assert sorted(row.x for row in mapped.alias("mia_alias").collect()) == [2, 4, 6]
    assert sorted(row.x for row in mapped.sample(1.0).collect()) == [2, 4, 6]
    left = spark.createDataFrame([(10,)], "x INT").mapInArrow(_double_batches, "x INT")
    right = spark.createDataFrame([(20,)], "x INT").mapInArrow(_double_batches, "x INT")
    assert sorted(row.x for row in left.union(right).collect()) == [20, 40]
    crossed = mapped.crossJoin(spark.createDataFrame([(0,)], "y INT"))
    assert sorted(row.y for row in crossed.collect()) == [0, 0, 0]
    # summary on transformed values (count of non-null after double).
    summary_rows = {row.summary: row.x for row in mapped.summary("count", "min", "max").collect()}
    assert summary_rows["count"] == "3"
    assert summary_rows["min"] == "2"
    assert summary_rows["max"] == "6"


def test_mapinarrow_parquet_write_runs_udf(spark: ReparkSession, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Writer registers real MIA rows — not empty placeholder wipe (C2-Q-001 / C2-SAF-001).

    Also pins re-run after write: uncached write must not clear ``_map_bridge`` so later
    collect / write invoke ``func`` again (octo C3-Q-001 / C3-Q-002 / C3-L-001).
    """
    calls = {"n": 0}

    def counted(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from _double_batches(batches)

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.mapInArrow(counted, "x INT")
    path = tmp_path / "mia_parquet"
    out.write.mode("overwrite").parquet(str(path))
    assert calls["n"] == 1
    assert out._map_bridge is not None
    back = spark.read.parquet(str(path))
    assert sorted(row.x for row in back.collect()) == [2, 4, 6]
    # Post-write action re-runs the UDF (not a silent one-shot pin).
    assert sorted(row.x for row in out.collect()) == [2, 4, 6]
    assert calls["n"] == 2
    path2 = tmp_path / "mia_parquet_2"
    out.write.mode("overwrite").parquet(str(path2))
    assert calls["n"] == 3
    back2 = spark.read.parquet(str(path2))
    assert sorted(row.x for row in back2.collect()) == [2, 4, 6]


def test_mapinarrow_cache_filter_unpersist_reruns(spark: ReparkSession) -> None:
    """cache + plan child must not clear bridge; unpersist re-runs (C2-Q-005)."""
    calls = {"n": 0}

    def counted(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from _double_batches(batches)

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.mapInArrow(counted, "x INT").cache()
    # First action pins cache once.
    assert out.count() == 3
    assert calls["n"] == 1
    # Child plan must use MemTable rows without clearing parent bridge.
    filtered = out.filter("x > 2")
    assert sorted(row.x for row in filtered.collect()) == [4, 6]
    assert out._map_bridge is not None
    out.unpersist()
    assert out.is_cached is False
    assert out._map_bridge is not None
    assert sorted(row.x for row in out.collect()) == [2, 4, 6]
    assert calls["n"] == 2


def test_mapinarrow_unpersist_resets_plan_ready_for_children(spark: ReparkSession) -> None:
    """After plan-stable + cache + unpersist, plan children re-run bridge (C7-L-001).

    Sticky ``_mia_plan_ready`` would keep cache-era ``_inner`` for filter/select while
    parent actions re-run — non-idempotent funcs diverge (child stale vs parent fresh).
    """
    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [None if v is None else int(v) * 2 + tag for v in batch.column(0).to_pylist()]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.mapInArrow(tag_double, "x INT")
    # Plan child before cache pins plan-stable snapshot (tag=1) and sets _mia_plan_ready.
    pre = out.filter("x > 0")
    assert sorted(row.x for row in pre.collect()) == [3, 5]  # 2*x + 1
    assert out._mia_plan_ready is True
    assert calls["n"] == 1
    cached = out.cache()
    assert cached.count() == 2
    # cache materialize re-runs once (tag=2); plan-ready still True pre-fix.
    assert calls["n"] == 2
    out.unpersist()
    assert out.is_cached is False
    assert out._map_bridge is not None
    assert out._mia_plan_ready is False
    # Post-unpersist plan child must re-run (tag=3), not reuse cache-era rows (tag=1/2).
    child = out.filter("x > 0")
    child_vals = sorted(row.x for row in child.collect())
    assert child_vals == [5, 7]  # 2*x + 3
    assert calls["n"] == 3
    # Parent action also re-runs (tag=4); multiset must not lag child on a sticky snapshot.
    parent_vals = sorted(row.x for row in out.collect())
    assert parent_vals == [6, 8]  # 2*x + 4
    assert calls["n"] == 4


def test_mapinarrow_unpersist_action_then_plan_child(spark: ReparkSession) -> None:
    """Plan-stable + cache + unpersist + parent action must not leave dangling _inner (C7-Q-001).

    Action replace_ephemeral drops the restored lineage view; without rebind + plan-ready
    reset, later filter/select/groupBy short-circuit onto a dead MemTable.
    """
    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [None if v is None else int(v) * 2 + tag for v in batch.column(0).to_pylist()]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.mapInArrow(tag_double, "x INT")
    # Establish plan-stable + ready before cache.
    assert sorted(row.x for row in out.filter("x > 0").collect()) == [3, 5]
    assert out._mia_plan_ready is True
    assert calls["n"] == 1
    out.cache()
    assert out.count() == 2
    assert calls["n"] == 2
    out.unpersist()
    assert out._mia_plan_ready is False
    # Parent action drops restored action-ephemeral lineage; must rebind _inner.
    assert sorted(row.x for row in out.collect()) == [5, 7]  # 2*x + 3
    assert calls["n"] == 3
    # Plan child after that action must rematerialize (not dangling ready shortcut).
    child_vals = sorted(row.x for row in out.filter("x > 0").collect())
    assert child_vals == [6, 8]  # 2*x + 4
    assert calls["n"] == 4
    # Direct _inner readers (schema/type path) stay live after the sequence.
    assert out.na._type_keys() == {"x": "int"}
    # groupBy/agg also goes through prepare — must not silently zero on dead handle.
    # ready is True after the filter above, so this reuses the plan-stable snapshot (no 5th run).
    grouped = out.groupBy().count().collect()
    assert grouped[0][0] == 2
    assert calls["n"] == 4


def test_mapinarrow_temp_view_registration_keeps_bridge(spark: ReparkSession) -> None:
    """createOrReplaceTempView must not one-shot-clear uncached MIA (C3-Q-001 / C3-L-001)."""
    calls = {"n": 0}

    def counted(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        yield from _double_batches(batches)

    frame = spark.createDataFrame([(7,)], "x INT")
    out = frame.mapInArrow(counted, "x INT")
    out.createOrReplaceTempView("mia_reg_c3")
    assert calls["n"] == 1
    assert out._map_bridge is not None
    # Named view holds that materialization; DF handle still re-runs.
    assert spark.sql("SELECT * FROM mia_reg_c3").collect()[0].x == 14
    assert out.collect()[0].x == 14
    assert calls["n"] == 2


def test_mapinarrow_upstream_input_streamed_not_collect_all(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Upstream batches are pulled lazily into func — not list()/collect-all first (C5-Q-001).

    A mutation that does ``list(input_reader)`` (or otherwise exhausts the reader) before
    calling ``func`` stays green under the multi-batch multiset pin alone; this pin fails that
    class of regression.
    """
    pulls = {"n": 0}
    events: list[str] = []

    class _MultiBatchReader:
        def __init__(self) -> None:
            self._batches = [
                pa.record_batch([pa.array([1, 2], type=pa.int32())], names=["x"]),
                pa.record_batch([pa.array([3, 4], type=pa.int32())], names=["x"]),
            ]
            self._index = 0

        def __iter__(self) -> Iterator[pa.RecordBatch]:
            while self._index < len(self._batches):
                pulls["n"] += 1
                batch = self._batches[self._index]
                self._index += 1
                yield batch

        def close(self) -> None:
            return None

    def tracking_from_stream(source: object, *args: object, **kwargs: object) -> _MultiBatchReader:
        del source, args, kwargs
        return _MultiBatchReader()

    class _PatchedRecordBatchReader:
        from_stream = staticmethod(tracking_from_stream)

    monkeypatch.setattr(pa, "RecordBatchReader", _PatchedRecordBatchReader)

    def streamed_func(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        events.append(f"enter:{pulls['n']}")
        first = next(batches)
        events.append(f"after_first:{pulls['n']}")
        yield first
        for batch in batches:
            events.append(f"after_next:{pulls['n']}")
            yield batch

    frame = spark.createDataFrame([(0,)], "x INT")
    out = frame.mapInArrow(streamed_func, "x INT")
    table = out.to_arrow()
    assert sorted(table.column("x").to_pylist()) == [1, 2, 3, 4]
    # Streaming: zero pulls at func entry; first pull only after next(batches).
    assert events[0] == "enter:0"
    assert events[1] == "after_first:1"
    assert events[2] == "after_next:2"
    assert pulls["n"] == 2


def test_mapinarrow_groupby_agg_values(spark: ReparkSession) -> None:
    """groupBy/agg must use prepared MIA rows, not empty placeholder (octo C5-Q-002)."""
    frame = spark.createDataFrame([(1,), (1,), (2,)], "x INT")
    # After double: keys 2, 2, 4 → counts {2: 2, 4: 1}
    mapped = frame.mapInArrow(_double_batches, "x INT")
    grouped = mapped.groupBy("x").count()
    got = sorted((int(row.x), int(row["count"])) for row in grouped.collect())
    assert got == [(2, 2), (4, 1)]
    # Dict-form agg also binds against real rows (not silent empty multiset).
    summed = mapped.groupBy("x").agg({"x": "sum"})
    sum_got = sorted((int(row.x), int(row["sum(x)"])) for row in summed.collect())
    assert sum_got == [(2, 4), (4, 4)]


def test_mapinarrow_plan_children_do_not_accumulate_stable_views(
    spark: ReparkSession,
) -> None:
    """Repeated plan children on one parent reuse one plan-stable MIA (octo C5-SAF-001)."""
    spark._ensure_information_schema()

    def _raw_mia_count() -> int:
        rows = (
            spark.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE '__repark_mia_%'"
            )
            .to_arrow()
            .to_pylist()
        )
        return len(rows)

    def identity(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        yield from batches

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    mapped = frame.mapInArrow(identity, "x INT")
    before = _raw_mia_count()
    # Holding the parent while building many short-lived plan children must not retain N
    # full bridge outputs (pre-fix: each _prepare_for_plan appended a plan-stable view).
    for _ in range(12):
        assert mapped.filter("x > 0").count() == 3
    after = _raw_mia_count()
    # Placeholder at mapInArrow + at most one plan-stable snapshot (reuse).
    assert after - before <= 1
    assert mapped._mia_plan_ready is True
    # Values still correct after reuse; parent actions still re-run (bridge kept).
    assert sorted(row.x for row in mapped.collect()) == [1, 2, 3]
    assert mapped._map_bridge is not None


def test_mapinarrow_select_explode_materializes_bridge(spark: ReparkSession) -> None:
    """mapInArrow → select(explode) unnests UDF rows + single plan-stable prepare (C6-Q-001).

    Combine octo C1-Q-001 / C1-SAF-001 / C1-L-001: ``_select_with_generator`` mid-projected
    via raw ``_inner`` (empty MIA MemTable) so explode/withColumn(explode) silently
    returned zero rows while ordinary select/filter already used ``_plan()``.

    Combine C6-Q-001: peers (global-agg / pivot / selectExpr) pin non-idempotent
    ``calls["n"] == 1``; idempotent ``_double_batches`` alone left double-prepare residual
    green. Tag-stamped values prove first-run snapshot (tag=1 → x*2+1), not a second prepare.

    Array via ``F.array_repeat`` over MIA columns (DDL has no ARRAY token; ``F.array`` is
    still loud-unsupported) so the generator attaches to the uncached mapInArrow parent.
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(0).to_pylist()
            ]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(10,), (20,)], "x INT")
    mapped = frame.mapInArrow(tag_double, "x INT")
    # Laziness: schema only — bridge not yet run.
    assert mapped.columns == ["x"]
    assert mapped._map_bridge is not None

    # tag=1 → 21, 41; explode array_repeat(x, 2) → two rows per value.
    out = mapped.select(
        mapped.x,
        F.explode(F.array_repeat(mapped.x, 2)).alias("e"),
    ).orderBy("x", "e")
    table = out.to_arrow()
    assert calls["n"] == 1
    assert mapped._mia_plan_ready is True
    assert table.column_names == ["x", "e"]
    assert [(r["x"], r["e"]) for r in table.to_pylist()] == [
        (21, 21),
        (21, 21),
        (41, 41),
        (41, 41),
    ]
    # Parent still re-runs on action (bridge kept after plan-child construction).
    assert mapped._map_bridge is not None
    parent_n = calls["n"]
    parent_rows = sorted(row.x for row in mapped.collect())
    assert len(parent_rows) == 2
    assert calls["n"] == parent_n + 1  # action re-run (not one-shot pin)


def test_mapinarrow_with_column_explode_materializes_bridge(spark: ReparkSession) -> None:
    """mapInArrow → withColumn(explode) multiplies UDF rows + single prepare (C6-Q-001)."""
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(0).to_pylist()
            ]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(7,), (8,)], "x INT")
    mapped = frame.mapInArrow(tag_double, "x INT")
    out = mapped.withColumn("e", F.explode(F.array_repeat(mapped.x, 1))).orderBy("x", "e")
    rows = [(r.x, r.e) for r in out.collect()]
    assert calls["n"] == 1
    assert mapped._mia_plan_ready is True
    # tag=1 → 15, 17; explode single-element array keeps one row each
    assert rows == [(15, 15), (17, 17)]


def test_mapinarrow_select_global_agg_sql_values_and_single_prepare(
    spark: ReparkSession,
) -> None:
    """mapInArrow → select(sum, lit) / cast(sum) / sum+1 share pure-AF snapshot (C2-Q/L-001).

    Combine octo C2-Q-001 / C2-L-001: ``_select_global_aggregate_sql`` used
    ``create_or_replace_temp_view`` (action re-run) then ``group_by()`` (plan-stable
    second run). Non-idempotent mapInArrow made SQL global-agg disagree with pure AF
    ``select(sum)`` / ``groupBy().agg``, and registering empty ``_inner`` stayed residual
    green without Arrow value pins.

    Mutation that reverts to action+second-prepare (or empty-placeholder register) fails
    the call-count and/or sum value pins here.
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(0).to_pylist()
            ]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    # tag=1 → rows 3, 5 → sum 8 (same as pure AF groupBy().agg path).
    expected_sum = 8

    # Pure AF baseline (one plan-stable prepare).
    calls["n"] = 0
    pure_mapped = frame.mapInArrow(tag_double, "x INT")
    pure_table = pure_mapped.select(F.sum("x")).to_arrow()
    assert pure_table.to_pylist() == [{"sum(x)": expected_sum}]
    assert calls["n"] == 1
    assert pure_mapped._mia_plan_ready is True

    # SQL path: sum + foldable lit companion — one prepare only, value matches pure AF.
    calls["n"] = 0
    lit_mapped = frame.mapInArrow(tag_double, "x INT")
    lit_table = lit_mapped.select(F.sum("x"), F.lit(1).alias("one")).to_arrow()
    assert lit_table.column_names == ["sum(x)", "one"]
    assert lit_table.to_pylist() == [{"sum(x)": expected_sum, "one": 1}]
    assert calls["n"] == 1
    assert lit_mapped._mia_plan_ready is True

    # SQL path: cast(sum) — value + type pin (Arrow path, not show-only).
    calls["n"] = 0
    cast_mapped = frame.mapInArrow(tag_double, "x INT")
    cast_table = cast_mapped.select(F.sum("x").cast("double").alias("total")).to_arrow()
    assert cast_table.to_pylist() == [{"total": float(expected_sum)}]
    assert pa.types.is_floating(cast_table.schema.field("total").type)
    assert calls["n"] == 1

    # SQL path: sum+1 composed aggregate — same first-run snapshot as pure AF (+1).
    calls["n"] = 0
    plus_mapped = frame.mapInArrow(tag_double, "x INT")
    plus_table = plus_mapped.select((F.sum("x") + 1).alias("s1")).to_arrow()
    assert plus_table.to_pylist() == [{"s1": expected_sum + 1}]
    assert calls["n"] == 1

    # Double-path agreement on one handle family: pure vs SQL must not be 8 vs 10.
    calls["n"] = 0
    agree_mapped = frame.mapInArrow(tag_double, "x INT")
    via_af = agree_mapped.select(F.sum("x")).to_arrow().to_pylist()[0]["sum(x)"]
    # Fresh handle for SQL (pure AF already prepared its parent).
    calls["n"] = 0
    agree_sql = frame.mapInArrow(tag_double, "x INT")
    via_sql = agree_sql.select(F.sum("x"), F.lit(0).alias("z")).to_arrow().to_pylist()[0]["sum(x)"]
    assert via_af == via_sql == expected_sum


def test_mapinarrow_group_by_pivot_sum_values_and_single_prepare(
    spark: ReparkSession,
) -> None:
    """mapInArrow → groupBy.pivot.sum Arrow values + single prepare (combine C3-Q-002).

    Cross-unit F2xS1 pin: residual-green ``groupBy.agg`` alone does not cover pivot's
    prepare / CASE-agg path on a mapInArrow parent. Non-idempotent UDF + double prepare
    (or empty-placeholder residual green) fails call-count and/or Arrow values here.
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_rows(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            g_values = list(batch.column(0).to_pylist())
            p_values = list(batch.column(1).to_pylist())
            x_values = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(2).to_pylist()
            ]
            yield pa.record_batch(
                [
                    pa.array(g_values, type=pa.string()),
                    pa.array(p_values, type=pa.int32()),
                    pa.array(x_values, type=pa.int32()),
                ],
                names=["g", "p", "x"],
            )

    frame = spark.createDataFrame(
        [
            ("a", 1, 10),
            ("a", 2, 20),
            ("b", 1, 30),
        ],
        "g STRING, p INT, x INT",
    )
    # tag=1 → x becomes 21, 41, 61
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_rows, "g STRING, p INT, x INT")
    table = mapped.groupBy("g").pivot("p", [1, 2]).sum("x").orderBy("g").to_arrow()
    assert calls["n"] == 1
    assert mapped._mia_plan_ready is True
    rows = table.to_pylist()
    assert table.column_names == ["g", "1", "2"]
    a_row = next(row for row in rows if row["g"] == "a")
    b_row = next(row for row in rows if row["g"] == "b")
    assert a_row["1"] == 21 and a_row["2"] == 41
    assert b_row["1"] == 61 and b_row["2"] is None
    # Control: groupBy.agg sum on same UDF shape still one prepare (regression guard).
    calls["n"] = 0
    mapped_agg = frame.mapInArrow(tag_rows, "g STRING, p INT, x INT")
    agg_table = mapped_agg.groupBy("g").agg(F.sum("x").alias("total")).orderBy("g").to_arrow()
    assert calls["n"] == 1
    assert agg_table.to_pylist() == [
        {"g": "a", "total": 62},
        {"g": "b", "total": 61},
    ]


def test_mapinarrow_selectexpr_plan_stable_after_prepare(spark: ReparkSession) -> None:
    """selectExpr registers ``_plan()`` snapshot, not action re-run (combine C4-L-001).

    After ``select(F.sum)`` / ``filter`` prepare, non-idempotent mapInArrow must not re-run
    on ``selectExpr(\"sum(x)\")`` / ``selectExpr(\"x\")`` — same class as fixed C2 global-agg
    SQL. Residual-green without this pin: selectExpr only checked non-empty on a fresh
    idempotent double. Mutation that reverts to ``_native_for_registration`` fails call-count
    and/or value agreement with plan-stable ``select``.
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(0).to_pylist()
            ]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    # tag=1 → rows 3, 5 → sum 8 (same as pure AF / plan-stable select).
    expected_sum = 8

    # Fresh selectExpr alone — one plan-stable prepare, correct values.
    calls["n"] = 0
    mapped_fresh = frame.mapInArrow(tag_double, "x INT")
    fresh_table = mapped_fresh.selectExpr("sum(x) AS total").to_arrow()
    assert fresh_table.to_pylist() == [{"total": expected_sum}]
    assert calls["n"] == 1
    assert mapped_fresh._mia_plan_ready is True

    # After pure AF select prepare on same handle, selectExpr must reuse snapshot.
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "x INT")
    via_af = mapped.select(F.sum("x")).to_arrow().to_pylist()[0]["sum(x)"]
    assert via_af == expected_sum
    assert calls["n"] == 1
    assert mapped._mia_plan_ready is True
    via_expr = mapped.selectExpr("sum(x) AS total").to_arrow().to_pylist()[0]["total"]
    # Action re-run would bump calls to 2 and tag=2 sum to 10.
    assert calls["n"] == 1
    assert via_expr == expected_sum

    # selectExpr("x") after filter prepare agrees with select("x") on same snapshot.
    calls["n"] = 0
    mapped_x = frame.mapInArrow(tag_double, "x INT")
    filtered = mapped_x.filter("x IS NOT NULL")
    assert calls["n"] == 1
    via_select = sorted(row.x for row in filtered.select("x").collect())
    via_select_expr = sorted(row.x for row in filtered.selectExpr("x").collect())
    assert via_select == via_select_expr == [3, 5]
    # Parent prepared once; filter child has plan snapshot (no second UDF for selectExpr).
    assert calls["n"] == 1

    # Post-prepare selectExpr("x") on the mapInArrow parent itself (not only the child).
    calls["n"] = 0
    mapped_parent = frame.mapInArrow(tag_double, "x INT")
    _ = mapped_parent.select("x").collect()
    assert calls["n"] == 1
    parent_expr = sorted(row.x for row in mapped_parent.selectExpr("x").collect())
    assert parent_expr == [3, 5]
    assert calls["n"] == 1


def test_mapinarrow_sql_lowering_plan_stable_after_prepare(spark: ReparkSession) -> None:
    """alias/sample/summary/set-ops/crossJoin/unpivot register ``_plan()`` (combine C5-Q-001).

    Residual-green hollow pin only checked non-empty on a fresh idempotent double. After
    prepare, action re-run via ``_native_for_registration`` would bump the non-idempotent
    UDF and disagree with ``select`` / ``selectExpr`` peers. Mutation that reverts any
    cited path fails call-count and/or value agreement with tag=1 rows [3, 5].
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(0).to_pylist()
            ]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    expected_rows = [3, 5]  # tag=1 → 2*x + 1
    expected_sum = 8

    def _prepare(mapped: object) -> None:
        via = mapped.select(F.sum("x")).to_arrow().to_pylist()[0]["sum(x)"]
        assert via == expected_sum
        assert calls["n"] == 1
        assert mapped._mia_plan_ready is True

    # alias
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "x INT")
    _prepare(mapped)
    aliased = sorted(row.x for row in mapped.alias("mia_alias").collect())
    assert aliased == expected_rows
    assert calls["n"] == 1

    # sample(1.0) — identity fraction still plan-stable
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "x INT")
    _prepare(mapped)
    sampled = sorted(row.x for row in mapped.sample(1.0).collect())
    assert sampled == expected_rows
    assert calls["n"] == 1

    # randomSplit single weight — full frame
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "x INT")
    _prepare(mapped)
    split = mapped.randomSplit([1.0], seed=42)
    assert len(split) == 1
    assert sorted(row.x for row in split[0].collect()) == expected_rows
    assert calls["n"] == 1

    # summary stats on tag=1 values
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "x INT")
    _prepare(mapped)
    summary_rows = {row.summary: row.x for row in mapped.summary("count", "min", "max").collect()}
    assert summary_rows["count"] == "2"
    assert summary_rows["min"] == "3"
    assert summary_rows["max"] == "5"
    assert calls["n"] == 1

    # crossJoin
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "x INT")
    _prepare(mapped)
    other = spark.createDataFrame([(0,)], "y INT")
    crossed = sorted(row.x for row in mapped.crossJoin(other).collect())
    assert crossed == expected_rows
    assert calls["n"] == 1

    # intersect with self-derived empty-diff peer (plan-stable both sides)
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "x INT")
    _prepare(mapped)
    peer = spark.createDataFrame([(3,), (5,)], "x INT")
    intersected = sorted(row.x for row in mapped.intersect(peer).collect())
    assert intersected == expected_rows
    assert calls["n"] == 1

    # subtract empty other
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "x INT")
    _prepare(mapped)
    empty_other = spark.createDataFrame([], "x INT")
    subtracted = sorted(row.x for row in mapped.subtract(empty_other).collect())
    assert subtracted == expected_rows
    assert calls["n"] == 1

    # unpivot plan-stable (wide schema from dual-column MIA)
    def tag_dual(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            xs = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(0).to_pylist()
            ]
            ys = [
                None if value is None else int(value) + tag for value in batch.column(1).to_pylist()
            ]
            yield pa.record_batch(
                [pa.array(xs, type=pa.int32()), pa.array(ys, type=pa.int32())],
                names=["a", "b"],
            )

    calls["n"] = 0
    wide = spark.createDataFrame([(1, 10), (2, 20)], "a INT, b INT")
    mapped_wide = wide.mapInArrow(tag_dual, "a INT, b INT")
    # prepare via select sum
    via_sum = mapped_wide.select(F.sum("a")).to_arrow().to_pylist()[0]["sum(a)"]
    assert via_sum == 8  # tag=1: 3+5
    assert calls["n"] == 1
    melted = mapped_wide.unpivot(None, ["a", "b"], "variable", "value").to_arrow().to_pylist()
    assert calls["n"] == 1
    by_var: dict[str, list[int]] = {}
    for row in melted:
        by_var.setdefault(row["variable"], []).append(row["value"])
    assert sorted(by_var["a"]) == [3, 5]
    assert sorted(by_var["b"]) == [11, 21]  # tag=1: 10+1, 20+1


def test_mapinarrow_cube_agg_plan_stable_and_alias(spark: ReparkSession) -> None:
    """cube/rollup SQL agg uses ``_plan()`` + AS alias names (combine C5-L-001 / C5-L-002).

    Pre-fix: ``_agg_via_sql_group`` action-registered via createOrReplaceTempView (second
    UDF run after prepare) and omitted ``AS`` so ``.alias('c')`` / Spark default names
    were dropped. Mutation that reverts registration fails call-count/value; mutation that
    drops AS fails column ``c`` presence.
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            gs = batch.column(0).to_pylist()
            xs = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(1).to_pylist()
            ]
            yield pa.record_batch(
                [pa.array(gs, type=pa.string()), pa.array(xs, type=pa.int32())],
                names=["g", "x"],
            )

    frame = spark.createDataFrame([("a", 1), ("a", 2), ("b", 3)], "g STRING, x INT")
    # tag=1 → x values 3,5,7; group a sum=8, b sum=7, grand total=15
    calls["n"] = 0
    mapped = frame.mapInArrow(tag_double, "g STRING, x INT")
    via_select = mapped.select(F.sum("x")).to_arrow().to_pylist()[0]["sum(x)"]
    assert via_select == 15
    assert calls["n"] == 1
    assert mapped._mia_plan_ready is True

    cube_table = mapped.cube("g").agg(F.sum("x").alias("total")).to_arrow()
    assert calls["n"] == 1  # plan-stable — action re-run would be 2 and sums shift
    assert "total" in cube_table.column_names
    by_g = {row["g"]: row["total"] for row in cube_table.to_pylist()}
    assert by_g["a"] == 8
    assert by_g["b"] == 7
    assert by_g[None] == 15

    # rollup + count alias column name (C5-L-002) on a fresh prepare
    calls["n"] = 0
    mapped2 = frame.mapInArrow(tag_double, "g STRING, x INT")
    _ = mapped2.select(F.sum("x")).collect()
    assert calls["n"] == 1
    rollup_table = mapped2.rollup("g").agg(F.count("*").alias("c")).to_arrow()
    assert calls["n"] == 1
    assert "c" in rollup_table.column_names
    # Three grouping rows for rollup(g): a, b, grand total — each with counts.
    counts = {row["g"]: row["c"] for row in rollup_table.to_pylist()}
    assert counts["a"] == 2
    assert counts["b"] == 1
    assert counts[None] == 3


def test_mapinarrow_identity_child_reuses_plan_ready_after_prepare(
    spark: ReparkSession,
) -> None:
    """Identity no-ops copy ``_mia_plan_ready`` so post-prepare peers do not re-run (C7-Q-001).

    Residual-green hollow pin: ``test_mapinarrow_identity_noops_preserve_bridge`` only
    checks pre-prepare collect on an idempotent double. After ``select(F.sum)`` prepare,
    ``repartition``/``coalesce``/``hint``/``offset(0)``/``toDF()`` must keep
    ``_mia_plan_ready`` and share the plan-stable snapshot — next select/filter/explode/agg
    on the identity child must not bump a non-idempotent UDF or diverge from the parent.
    Mutation that drops ``child._mia_plan_ready = self._mia_plan_ready`` fails call-count
    and/or tag=1 values.
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(0).to_pylist()
            ]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    expected_rows = [3, 5]  # tag=1 → 2*x + 1
    expected_sum = 8

    for make_child in (
        lambda mapped: mapped.repartition(4),
        lambda mapped: mapped.coalesce(1),
        lambda mapped: mapped.hint("broadcast"),
        lambda mapped: mapped.offset(0),
        lambda mapped: mapped.toDF(),
    ):
        calls["n"] = 0
        mapped = frame.mapInArrow(tag_double, "x INT")
        via_parent = mapped.select(F.sum("x")).to_arrow().to_pylist()[0]["sum(x)"]
        assert via_parent == expected_sum
        assert calls["n"] == 1
        assert mapped._mia_plan_ready is True

        child = make_child(mapped)
        assert child._map_bridge is not None
        assert child._mia_plan_ready is True
        # select / filter / explode / pure AF on identity child — still one snapshot.
        via_select = sorted(row.x for row in child.select("x").collect())
        assert via_select == expected_rows
        assert calls["n"] == 1
        via_filter = sorted(row.x for row in child.filter("x IS NOT NULL").collect())
        assert via_filter == expected_rows
        assert calls["n"] == 1
        via_agg = child.select(F.sum("x")).to_arrow().to_pylist()[0]["sum(x)"]
        assert via_agg == expected_sum
        assert calls["n"] == 1
        via_explode = sorted(
            row.e
            for row in child.select(F.explode(F.array_repeat(F.col("x"), 1)).alias("e")).collect()
        )
        assert via_explode == expected_rows
        assert calls["n"] == 1


def test_mapinarrow_polars_join_plan_stable_after_prepare(spark: ReparkSession) -> None:
    """``pl.join`` registers ``_plan()`` snapshots, not action re-run (combine C7-Q-002).

    DataFrame.join uses ``_plan()``; polars.join previously used
    ``create_or_replace_temp_view`` → ``_native_for_registration`` (action-like). After
    prepare, a non-idempotent mapInArrow left side would re-run on pl.join and disagree
    with DataFrame.join / select peers. Mutation that reverts to action registration
    fails call-count and/or tag=1 join values.
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom

    calls = {"n": 0}

    def tag_double(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        calls["n"] += 1
        tag = calls["n"]
        for batch in batches:
            values = [
                None if value is None else int(value) * 2 + tag
                for value in batch.column(0).to_pylist()
            ]
            yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])

    left_src = spark.createDataFrame([(1,), (2,)], "x INT")
    right = spark.createDataFrame([(3, "a"), (5, "b")], "x INT, label STRING")
    expected_rows = [3, 5]
    expected_sum = 8

    calls["n"] = 0
    mapped = left_src.mapInArrow(tag_double, "x INT")
    via_prepare = mapped.select(F.sum("x")).to_arrow().to_pylist()[0]["sum(x)"]
    assert via_prepare == expected_sum
    assert calls["n"] == 1
    assert mapped._mia_plan_ready is True

    # DataFrame.join control — plan-stable (existing C5 peer class).
    df_joined = mapped.join(right, on="x", how="inner")
    df_xs = sorted(row.x for row in df_joined.collect())
    assert df_xs == expected_rows
    assert calls["n"] == 1

    # polars.join must match DF join values and not re-run the UDF.
    # Use ``_frame.collect()`` (no real-polars import) so this pin stays in the MIA suite.
    pl_joined = mapped.pl.join(right.pl, on="x", how="inner")
    pl_xs = sorted(row.x for row in pl_joined._frame.collect())
    assert pl_xs == expected_rows
    assert calls["n"] == 1
    assert sorted(row.label for row in pl_joined._frame.collect()) == ["a", "b"]
    # Action re-run would bump calls to 2 and tag=2 → x values 4, 6 (no join hits).
    assert pl_xs == df_xs
