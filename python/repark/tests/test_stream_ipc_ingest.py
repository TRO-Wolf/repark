"""I4 R-STREAM-IPC-INGEST: Arrow C Stream register + mapInArrow bridge switch pins.

Named oracle for unit I4. Companion: untouched-green ``test_mapinarrow.py`` battery.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("stream-ipc-ingest").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _double_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        values = [None if v is None else int(v) * 2 for v in batch.column(0).to_pylist()]
        yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])


def test_register_arrow_stream_as_temp_view_native_symbol_present(spark: ReparkSession) -> None:
    """Native session exposes the I4 C-stream register seam."""
    native = spark._ensure_alive()
    assert callable(getattr(native, "register_arrow_stream_as_temp_view", None))


def test_register_arrow_stream_round_trip_values_and_types(spark: ReparkSession) -> None:
    """RecordBatchReader → register_arrow_stream → SQL scan preserves values + Arrow types."""
    native = spark._ensure_alive()
    schema = pa.schema([("x", pa.int32()), ("s", pa.string())])
    batches = [
        pa.record_batch(
            [pa.array([1, 2], type=pa.int32()), pa.array(["a", "b"], type=pa.string())],
            schema=schema,
        ),
        pa.record_batch(
            [pa.array([3], type=pa.int32()), pa.array(["c"], type=pa.string())],
            schema=schema,
        ),
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    view_name = "__repark_stream_ipc_rt"
    try:
        native.register_arrow_stream_as_temp_view(view_name, reader)
        table = spark.sql(f"SELECT * FROM {view_name}").to_arrow()
        assert table.schema.field("x").type == pa.int32()
        assert table.schema.field("s").type == pa.string()
        assert sorted(table.column("x").to_pylist()) == [1, 2, 3]
        assert sorted(table.column("s").to_pylist()) == ["a", "b", "c"]
    finally:
        native.drop_temp_view(view_name)


def test_register_arrow_stream_empty_schema_only(spark: ReparkSession) -> None:
    """Empty stream still registers a zero-row MemTable with the declared schema."""
    native = spark._ensure_alive()
    schema = pa.schema([("x", pa.int32())])
    reader = pa.RecordBatchReader.from_batches(schema, [])
    view_name = "__repark_stream_ipc_empty"
    try:
        native.register_arrow_stream_as_temp_view(view_name, reader)
        table = spark.sql(f"SELECT * FROM {view_name}").to_arrow()
        assert table.num_rows == 0
        assert table.schema.field("x").type == pa.int32()
    finally:
        native.drop_temp_view(view_name)


def test_register_arrow_stream_rejects_non_exporter(spark: ReparkSession) -> None:
    native = spark._ensure_alive()
    with pytest.raises(TypeError, match="Arrow C Stream exporter"):
        native.register_arrow_stream_as_temp_view("__repark_stream_ipc_bad", object())


def test_register_arrow_stream_bare_capsule_round_trip(spark: ReparkSession) -> None:
    """Bare ``arrow_array_stream`` PyCapsule (not only protocol exporters) registers correctly.

    Mutation: drop the ``is_instance_of::<PyCapsule>`` branch → this pin fails.
    """
    native = spark._ensure_alive()
    table = pa.table({"x": pa.array([7, 8], type=pa.int32())})
    capsule = table.__arrow_c_stream__()
    assert type(capsule).__name__ == "PyCapsule"
    view_name = "__repark_stream_ipc_capsule"
    try:
        native.register_arrow_stream_as_temp_view(view_name, capsule)
        out = spark.sql(f"SELECT * FROM {view_name}").to_arrow()
        assert out.schema.field("x").type == pa.int32()
        assert sorted(out.column("x").to_pylist()) == [7, 8]
    finally:
        native.drop_temp_view(view_name)


def test_register_arrow_stream_exporter_raises_preserves_exception(
    spark: ReparkSession,
) -> None:
    """``__arrow_c_stream__`` raising propagates original exception type (octo C1-Q-001)."""
    native = spark._ensure_alive()

    class _BoomExporter:
        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            raise RuntimeError("injected exporter boom")

    with pytest.raises(RuntimeError, match="injected exporter boom") as info:
        native.register_arrow_stream_as_temp_view("__repark_stream_ipc_boom_exp", _BoomExporter())
    # Must not be remapped to TypeError (pre-fix swallowed the original type).
    assert type(info.value) is RuntimeError


def test_register_arrow_stream_mid_stream_error_no_partial_view(
    spark: ReparkSession,
) -> None:
    """Mid-stream C-stream pull failure does not leave a partial temp view (octo C1-L-001)."""
    native = spark._ensure_alive()
    schema = pa.schema([("x", pa.int32())])
    first = pa.record_batch([pa.array([1], type=pa.int32())], schema=schema)

    def boom_batches() -> Iterator[pa.RecordBatch]:
        yield first
        raise RuntimeError("injected mid-stream cstream batch failure")

    reader = pa.RecordBatchReader.from_batches(schema, boom_batches())
    view_name = "__repark_stream_ipc_midfail"
    with pytest.raises(PySparkException, match="injected mid-stream cstream batch failure"):
        native.register_arrow_stream_as_temp_view(view_name, reader)

    # No partial registration — name must not resolve as a usable temp view.
    with pytest.raises(PySparkException):
        spark.sql(f"SELECT * FROM {view_name}").collect()

    # Session not poisoned.
    assert spark.createDataFrame([(1,)], "z INT").collect()[0].z == 1


def test_register_arrow_stream_nested_repark_stream_no_abort(
    spark: ReparkSession,
) -> None:
    """Generator that re-enters a repark C-stream must not process-abort (octo C1-SAF-001).

    Pre-fix: ``register_arrow_stream`` draining ``from_batches(gen)`` where ``gen`` iterates
    ``from_stream(repark_df)`` aborted with ``PyEval_SaveThread`` / thread-state-NULL.
    """
    native = spark._ensure_alive()
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    # Capture schema from a one-shot stream, then re-open for the nested generator.
    probe = pa.RecordBatchReader.from_stream(frame)
    schema = probe.schema
    del probe
    frame2 = spark.createDataFrame([(1,), (2,), (3,)], "x INT")

    def reenter_repark() -> Iterator[pa.RecordBatch]:
        nested = pa.RecordBatchReader.from_stream(frame2)
        yield from nested

    reader = pa.RecordBatchReader.from_batches(schema, reenter_repark())
    view_name = "__repark_stream_ipc_nested"
    try:
        native.register_arrow_stream_as_temp_view(view_name, reader)
        table = spark.sql(f"SELECT * FROM {view_name}").to_arrow()
        assert sorted(table.column("x").to_pylist()) == [1, 2, 3]
    finally:
        native.drop_temp_view(view_name)


def test_mapinarrow_cstream_path_values_and_types(spark: ReparkSession) -> None:
    """mapInArrow primary path uses C-stream register (row/schema pin)."""
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.mapInArrow(_double_batches, "x INT")
    table = out.to_arrow()
    assert table.schema.field("x").type == pa.int32()
    assert sorted(table.column("x").to_pylist()) == [2, 4, 6]


def test_mapinarrow_cstream_vs_ipc_fallback_row_schema_equivalence(
    spark: ReparkSession,
) -> None:
    """C-stream path and IPC fallback agree on values + Arrow types for the same UDF."""
    frame = spark.createDataFrame([(10,), (20,), (None,)], "x INT")
    out = frame.mapInArrow(_double_batches, "x INT")

    # Primary (C-stream) path.
    cstream_table = out.to_arrow()

    # Force IPC fallback by hiding the native C-stream symbol on this DF's session.
    real_session = out._session
    ipc_calls = {"n": 0}

    class _SessionIpcOnly:
        def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
            ipc_calls["n"] += 1
            real_session.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

        def __getattr__(self, name: str) -> Any:
            if name == "register_arrow_stream_as_temp_view":
                raise AttributeError(name)
            return getattr(real_session, name)

    out._session = _SessionIpcOnly()  # type: ignore[assignment]
    ipc_table = out.to_arrow()

    assert ipc_calls["n"] >= 1, "IPC fallback must call register_ipc_stream_as_temp_view"
    assert ipc_table.schema.field("x").type == pa.int32()
    assert cstream_table.schema.field("x").type == pa.int32()
    # Null-aware multiset equality (order non-contractual for mapInArrow).
    assert sorted(
        [(v,) for v in ipc_table.column("x").to_pylist()],
        key=lambda row: (row[0] is None, row[0]),
    ) == sorted(
        [(v,) for v in cstream_table.column("x").to_pylist()],
        key=lambda row: (row[0] is None, row[0]),
    )


def test_mapinarrow_mid_stream_exception_is_pyspark_and_session_usable(
    spark: ReparkSession,
) -> None:
    """User-func exception mid-stream surfaces PySparkException; a fresh action works."""
    calls = {"n": 0}

    def boom_after_one(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            calls["n"] += 1
            if calls["n"] == 1:
                yield batch
            else:
                raise RuntimeError("injected mid-stream user failure")

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")

    # Force multi-batch upstream so the func can fail after first yield when input has >1 batch.
    # createDataFrame may be single-batch; raise on second *output* pull instead:
    def boom_on_second_output(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        yielded = 0
        for batch in batches:
            for index in range(batch.num_rows):
                yielded += 1
                row = batch.slice(index, 1)
                if yielded >= 2:
                    raise RuntimeError("injected mid-stream user failure")
                yield row

    out = frame.mapInArrow(boom_on_second_output, "x INT")
    with pytest.raises(PySparkException, match="injected mid-stream user failure") as info:
        out.collect()
    assert isinstance(info.value, PySparkException)
    assert isinstance(info.value, RuntimeError)

    # Session not poisoned — a fresh unrelated action works.
    assert spark.createDataFrame([(9,)], "y INT").collect()[0].y == 9

    # Same mapInArrow frame can be re-attempted (bridge re-runs); still raises.
    with pytest.raises(PySparkException, match="injected mid-stream user failure"):
        out.collect()


def test_mapinarrow_uses_cstream_register_not_ipc_when_present(spark: ReparkSession) -> None:
    """Structural pin: action path prefers register_arrow_stream over IPC encode."""

    def identity(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        yield from batches

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.mapInArrow(identity, "x INT")
    real = out._session
    events: list[str] = []

    class _SessionProxy:
        def register_arrow_stream_as_temp_view(self, view_name: str, stream_obj: object) -> None:
            events.append(f"cstream:{view_name}")
            assert hasattr(stream_obj, "__arrow_c_stream__") or type(stream_obj).__name__ == (
                "RecordBatchReader"
            )
            real.register_arrow_stream_as_temp_view(view_name, stream_obj)

        def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
            events.append(f"ipc:{view_name}:{len(ipc_bytes)}")
            real.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

        def __getattr__(self, name: str) -> Any:
            return getattr(real, name)

    out._session = _SessionProxy()  # type: ignore[assignment]
    table = out.to_arrow()
    assert sorted(table.column("x").to_pylist()) == [1, 2]
    assert any(event.startswith("cstream:") for event in events), events
    assert not any(event.startswith("ipc:") for event in events), events
