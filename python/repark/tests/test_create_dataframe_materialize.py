"""R-PERF-VALUES — createDataFrame materializes once (MemTable), not re-plan VALUES.

N4 measured ~22s per action at 100k rows because every ``collect``/``count`` re-executed the
VALUES body. After materialization, a second action on the same frame is a table scan.

Covers list/Row, pandas, polars, and schema= forms so every ingestion path changes together.
"""

from __future__ import annotations

import time

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark.types import IntegerType, StringType, StructField, StructType


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("x1-values-mat").getOrCreate()
    yield session
    session.stop()


def test_create_dataframe_list_second_action_is_cheap(spark: ReparkSession) -> None:
    rows = [(index, f"r{index}") for index in range(5_000)]
    frame = spark.createDataFrame(rows, schema=["id", "label"])
    # Warm + correctness
    assert frame.count() == 5_000
    t0 = time.perf_counter()
    assert frame.count() == 5_000
    second = time.perf_counter() - t0
    # Pre-fix: 5k VALUES re-plan is still tens of ms; post-fix MemTable is << 50ms typical.
    # Generous CI margin: second action must complete well under 2s (was multi-second at 100k).
    assert second < 2.0, f"second count took {second:.3f}s — VALUES re-plan still live?"


def test_create_dataframe_schema_int32_preserved(spark: ReparkSession) -> None:
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("label", StringType(), True),
        ]
    )
    frame = spark.createDataFrame([(1, "a"), (2, "b")], schema=schema)
    table = frame.to_arrow()
    assert table.schema.field("id").type == pa.int32()
    assert table.column("id").to_pylist() == [1, 2]


def test_create_dataframe_row_and_count(spark: ReparkSession) -> None:
    from repark import Row

    frame = spark.createDataFrame([Row(id=1, v="x"), Row(id=2, v="y")])
    assert frame.count() == 2
    assert frame.collect()[0][0] == 1


def test_create_dataframe_pandas_roundtrip(spark: ReparkSession) -> None:
    pd = pytest.importorskip("pandas")
    pdf = pd.DataFrame({"id": [10, 20], "v": [1.5, 2.5]})
    frame = spark.createDataFrame(pdf)
    assert frame.count() == 2
    table = frame.to_arrow()
    assert table.column("id").to_pylist() == [10, 20]


def test_create_dataframe_polars_roundtrip(spark: ReparkSession) -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [3, 4], "label": ["a", "b"]})
    frame = spark.createDataFrame(pdf)
    assert frame.count() == 2
    assert sorted(frame.to_arrow().column("id").to_pylist()) == [3, 4]


def test_create_dataframe_100k_construction_under_five_seconds(spark: ReparkSession) -> None:
    """R-PERF-ARROW-CDF: 100k-row list createDataFrame completes in < 5s (was ~22s VALUES)."""
    rows = [(index, f"r{index % 1000}") for index in range(100_000)]
    t0 = time.perf_counter()
    frame = spark.createDataFrame(rows, schema=["id", "label"])
    # Force materialization (view registration completes at create).
    assert frame.count() == 100_000
    elapsed = time.perf_counter() - t0
    assert elapsed < 5.0, f"100k createDataFrame took {elapsed:.3f}s (budget 5s; VALUES path ~22s)"


class _NativeRegisterProxy:
    """Count C-stream / IPC / VALUES MemTable registers on the native session handle.

    Shared by P1a structural pins so probes stay consistent (octo C1 Q-003).

    ``register_arrow_stream_as_temp_view`` is **not** a real attribute when
    ``hide_c_stream`` is true so production ``getattr(..., None)`` version-skew
    detection still works (a class method would short-circuit that path).
    """

    def __init__(
        self,
        real: object,
        *,
        hide_c_stream: bool = False,
        require_arrow_c_stream: bool = False,
        fail_after_register_on_sql: bool = False,
    ) -> None:
        self._real = real
        self.hide_c_stream = hide_c_stream
        self.require_arrow_c_stream = require_arrow_c_stream
        self.fail_after_register_on_sql = fail_after_register_on_sql
        self.stream_views: list[str] = []
        self.ipc_byte_lens: list[int] = []
        self.materialize_views: list[str] = []
        self.dropped_views: list[str] = []

    def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
        self.ipc_byte_lens.append(len(ipc_bytes))
        self._real.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

    def materialize_as_temp_view(self, view_name: str, frame: object) -> None:
        self.materialize_views.append(view_name)
        self._real.materialize_as_temp_view(view_name, frame)

    def drop_temp_view(self, name: str) -> bool:
        self.dropped_views.append(name)
        return bool(self._real.drop_temp_view(name))

    def sql(self, query: str) -> object:
        registered = self.stream_views or self.ipc_byte_lens or self.materialize_views
        if self.fail_after_register_on_sql and registered:
            raise RuntimeError("injected sql failure after MemTable register (P1a SAF-001)")
        return self._real.sql(query)

    def _register_arrow_stream_as_temp_view(self, view_name: str, stream_obj: object) -> None:
        if self.require_arrow_c_stream:
            assert hasattr(stream_obj, "__arrow_c_stream__")
        self.stream_views.append(view_name)
        self._real.register_arrow_stream_as_temp_view(view_name, stream_obj)

    def __getattr__(self, name: str) -> object:
        if name == "register_arrow_stream_as_temp_view":
            if self.hide_c_stream:
                raise AttributeError(name)
            return self._register_arrow_stream_as_temp_view
        return getattr(self._real, name)


def test_create_dataframe_prefers_arrow_c_stream_not_ipc(spark: ReparkSession) -> None:
    """P1a: createDataFrame registers via C Stream, not IPC encode/to_vec.

    Mutation: restore ``register_ipc_stream_as_temp_view`` as the only path → this pin fails.
    """
    real = spark._ensure_alive()
    proxy = _NativeRegisterProxy(real, require_arrow_c_stream=True)
    spark._inner = proxy  # type: ignore[assignment]
    try:
        frame = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])
        assert frame.count() == 2
        assert frame.to_arrow().column("id").to_pylist() == [1, 2]
    finally:
        spark._inner = real  # type: ignore[assignment]

    assert len(proxy.stream_views) == 1, (
        f"expected one C-stream register, got {proxy.stream_views!r}"
    )
    assert proxy.ipc_byte_lens == [], (
        f"IPC must not run when C-stream is present; got {proxy.ipc_byte_lens!r}"
    )


def test_create_dataframe_c_stream_covers_tuples_pandas_polars(spark: ReparkSession) -> None:
    """All three primary inputs ride C-stream transport (same MemTable register seam)."""
    real = spark._ensure_alive()
    proxy = _NativeRegisterProxy(real)
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    spark._inner = proxy  # type: ignore[assignment]
    try:
        assert spark.createDataFrame([(1,)], ["x"]).count() == 1
        assert spark.createDataFrame(pd.DataFrame({"x": [2]})).count() == 1
        assert spark.createDataFrame(pl.DataFrame({"x": [3]})).count() == 1
    finally:
        spark._inner = real  # type: ignore[assignment]

    assert len(proxy.stream_views) == 3, (
        f"expected 3 C-stream registers, got {len(proxy.stream_views)}"
    )
    assert proxy.ipc_byte_lens == []


def test_create_dataframe_empty_typed_prefers_c_stream(spark: ReparkSession) -> None:
    """Typed empty frames also ride C-stream (not IPC) via _empty_typed_arrow_frame."""
    real = spark._ensure_alive()
    proxy = _NativeRegisterProxy(real)
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("label", StringType(), True),
        ]
    )
    spark._inner = proxy  # type: ignore[assignment]
    try:
        frame = spark.createDataFrame([], schema=schema)
        assert frame.count() == 0
        assert frame.to_arrow().schema.field("id").type == pa.int32()
    finally:
        spark._inner = real  # type: ignore[assignment]

    assert len(proxy.stream_views) == 1, (
        f"empty typed must C-stream register, got {proxy.stream_views!r}"
    )
    assert proxy.ipc_byte_lens == []


def test_create_dataframe_ipc_fallback_when_c_stream_absent(spark: ReparkSession) -> None:
    """Version-skew: missing C-stream symbol falls back to IPC register."""
    real = spark._ensure_alive()
    proxy = _NativeRegisterProxy(real, hide_c_stream=True)
    spark._inner = proxy  # type: ignore[assignment]
    try:
        frame = spark.createDataFrame([(7, "z")], ["id", "label"])
        assert frame.count() == 1
        assert frame.collect()[0][0] == 7
    finally:
        spark._inner = real  # type: ignore[assignment]

    assert len(proxy.ipc_byte_lens) == 1 and proxy.ipc_byte_lens[0] > 0
    assert proxy.stream_views == []


def test_create_dataframe_drops_view_when_sql_after_register_fails(spark: ReparkSession) -> None:
    """P1a SAF-001: sql() failure after C-stream register must drop the orphan MemTable.

    Mutation: remove the try/except drop in ``_materialize_arrow_as_memtable_frame`` →
    ``dropped_views`` stays empty while ``stream_views`` is non-empty.
    """
    real = spark._ensure_alive()
    proxy = _NativeRegisterProxy(real, fail_after_register_on_sql=True)
    spark._inner = proxy  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="injected sql failure"):
            spark.createDataFrame([(1, "a")], ["id", "label"])
    finally:
        spark._inner = real  # type: ignore[assignment]

    assert len(proxy.stream_views) == 1
    assert proxy.dropped_views == proxy.stream_views
    # Orphan must not remain as a resolvable temp view.
    assert not real.table_exists(proxy.stream_views[0])


def test_create_dataframe_drops_view_when_sql_after_ipc_register_fails(
    spark: ReparkSession,
) -> None:
    """P1a SAF-001 on IPC version-skew branch (octo C2 Q-004)."""
    real = spark._ensure_alive()
    proxy = _NativeRegisterProxy(real, hide_c_stream=True, fail_after_register_on_sql=True)
    spark._inner = proxy  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="injected sql failure"):
            spark.createDataFrame([(1, "a")], ["id", "label"])
    finally:
        spark._inner = real  # type: ignore[assignment]

    assert len(proxy.ipc_byte_lens) == 1 and proxy.ipc_byte_lens[0] > 0
    assert len(proxy.dropped_views) == 1
    assert proxy.stream_views == []
    assert not real.table_exists(proxy.dropped_views[0])


def test_create_dataframe_drops_view_when_sql_after_values_materialize_fails(
    spark: ReparkSession,
) -> None:
    """P1a SAF-001 on untyped empty VALUES materialize path (octo C2 Q-005)."""
    real = spark._ensure_alive()
    proxy = _NativeRegisterProxy(real, fail_after_register_on_sql=True)
    spark._inner = proxy  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="injected sql failure"):
            # Untyped empty → WHERE 1=0 VALUES seed → materialize_as_temp_view.
            spark.createDataFrame([], schema=["id", "label"])
    finally:
        spark._inner = real  # type: ignore[assignment]

    assert len(proxy.materialize_views) == 1
    assert proxy.dropped_views == proxy.materialize_views
    assert proxy.stream_views == []
    assert proxy.ipc_byte_lens == []
    assert not real.table_exists(proxy.materialize_views[0])


def test_create_dataframe_c_stream_error_does_not_fall_back_to_ipc(spark: ReparkSession) -> None:
    """Charter: C-stream runtime failure fails loud — IPC is version-skew only (octo C3).

    Mutation: wrap C-stream call in try/except and fall back to IPC → this pin fails.
    """
    real = spark._ensure_alive()
    proxy = _NativeRegisterProxy(real)

    def _boom(view_name: str, stream_obj: object) -> None:
        proxy.stream_views.append(view_name)
        raise RuntimeError("injected C-stream register failure (P1a C3)")

    # Install boom as the resolved C-stream symbol (still present → no version-skew path).
    proxy._register_arrow_stream_as_temp_view = _boom  # type: ignore[method-assign]
    spark._inner = proxy  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="injected C-stream register failure"):
            spark.createDataFrame([(1, "a")], ["id", "label"])
    finally:
        spark._inner = real  # type: ignore[assignment]

    assert len(proxy.stream_views) == 1
    assert proxy.ipc_byte_lens == [], "IPC must not run when C-stream symbol exists but errors"
    assert proxy.dropped_views == []  # register never completed → nothing to drop


def test_create_dataframe_pandas_uses_native_arrow_not_row_loop(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2a: pandas path uses ``_arrow_table_from_pandas`` (native), not the row extractor.

    Mutation: restore ``_rows_from_pandas`` as the only path → this pin fails on the spy.
    (``pa.Table.from_pandas`` is a C-extension slot and cannot be monkeypatched; the
    builder vs rows split is the mutation seam — critic-octo C5.)
    """
    import repark.spark.session as session_mod

    pd = pytest.importorskip("pandas")
    calls: list[str] = []
    real_from_pandas = session_mod._arrow_table_from_pandas
    real_rows = session_mod._rows_from_pandas

    def spy_from_pandas(*args: object, **kwargs: object) -> object:
        calls.append("from_pandas")
        return real_from_pandas(*args, **kwargs)

    def spy_rows(*args: object, **kwargs: object) -> object:
        calls.append("rows")
        return real_rows(*args, **kwargs)

    monkeypatch.setattr(session_mod, "_arrow_table_from_pandas", spy_from_pandas)
    monkeypatch.setattr(session_mod, "_rows_from_pandas", spy_rows)
    frame = spark.createDataFrame(pd.DataFrame({"id": [1, 2], "label": ["a", "b"]}))
    table = frame.to_arrow()
    assert table.column("id").to_pylist() == [1, 2]
    assert table.schema.field("id").type == pa.int64()
    assert "from_pandas" in calls
    assert "rows" not in calls


def test_create_dataframe_polars_uses_native_arrow_not_row_loop(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2a: polars path uses native ``.to_arrow()`` builder, not the row extractor."""
    import repark.spark.session as session_mod

    pl = pytest.importorskip("polars")
    calls: list[str] = []
    real_from_polars = session_mod._arrow_table_from_polars
    real_rows = session_mod._rows_from_polars

    def spy_from_polars(*args: object, **kwargs: object) -> object:
        calls.append("from_polars")
        return real_from_polars(*args, **kwargs)

    def spy_rows(*args: object, **kwargs: object) -> object:
        calls.append("rows")
        return real_rows(*args, **kwargs)

    monkeypatch.setattr(session_mod, "_arrow_table_from_polars", spy_from_polars)
    monkeypatch.setattr(session_mod, "_rows_from_polars", spy_rows)
    frame = spark.createDataFrame(pl.DataFrame({"id": [3, 4], "label": ["x", "y"]}))
    assert sorted(frame.to_arrow().column("id").to_pylist()) == [3, 4]
    assert "from_polars" in calls
    assert "rows" not in calls


def test_create_dataframe_native_typed_schema_refuses_inf(spark: ReparkSession) -> None:
    """P2a critic-octo C1: StructType Double/Float must refuse ±inf on native pandas/polars.

    Mutation: early-return on ``engine_type`` before ``is_inf`` → this pin goes red while
    untyped ``createDataFrame(pd.DataFrame({...inf}))`` still refuses.
    """
    from repark.errors import PySparkTypeError
    from repark.spark.types import DoubleType, FloatType, StructField, StructType

    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    double_schema = StructType([StructField("x", DoubleType(), True)])
    float_schema = StructType([StructField("x", FloatType(), True)])
    with pytest.raises(PySparkTypeError, match="infinite float"):
        spark.createDataFrame(pd.DataFrame({"x": [1.0, float("inf")]}), schema=double_schema)
    with pytest.raises(PySparkTypeError, match="infinite float"):
        spark.createDataFrame(pd.DataFrame({"x": [float("-inf")]}), schema=float_schema)
    with pytest.raises(PySparkTypeError, match="infinite float"):
        spark.createDataFrame(pl.DataFrame({"x": [1.0, float("inf")]}), schema=double_schema)


def test_create_dataframe_native_decimal_envelope_matches_list(spark: ReparkSession) -> None:
    """P2a critic-octo C1: pandas/polars Decimal refuse is PySparkValueError (not ArrowInvalid).

    List path already pins envelope; native from_pandas/to_arrow must not leak rescale errors.
    """
    from decimal import Decimal

    from repark.errors import PySparkValueError

    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    too_fine = Decimal("1E-19")
    with pytest.raises(PySparkValueError, match="DECIMAL\\(38, 18\\)"):
        spark.createDataFrame([(too_fine,)], ["d"])
    with pytest.raises(PySparkValueError, match="DECIMAL\\(38, 18\\)"):
        spark.createDataFrame(pd.DataFrame({"d": [too_fine]}))
    with pytest.raises(PySparkValueError, match="DECIMAL\\(38, 18\\)"):
        spark.createDataFrame(pl.DataFrame({"d": [too_fine]}))
    too_wide = Decimal(10) ** 25
    with pytest.raises(PySparkValueError, match="DECIMAL\\(38, 18\\)"):
        spark.createDataFrame(pd.DataFrame({"d": [too_wide]}))


def test_create_dataframe_pandas_duplicate_columns_fail_loud(spark: ReparkSession) -> None:
    """P2a critic-octo C2: duplicate pandas labels → PySparkValueError (not AttributeError)."""
    from repark.errors import PySparkValueError

    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame([[1, 2]], columns=["a", "a"])
    with pytest.raises(PySparkValueError, match="duplicate column names"):
        spark.createDataFrame(frame)


def test_create_dataframe_object_null_schema_cast_is_pyspark_type_error(
    spark: ReparkSession,
) -> None:
    """P2a critic-octo C2: object all-NaT + DoubleType schema fails as PySparkTypeError."""
    import pandas as pd

    from repark.errors import PySparkTypeError
    from repark.spark.types import DoubleType, StructField, StructType

    schema = StructType([StructField("x", DoubleType(), True)])
    frame = pd.DataFrame({"x": pd.Series([pd.NaT, pd.NaT], dtype=object)})
    with pytest.raises(PySparkTypeError, match="cannot cast inferred null type"):
        spark.createDataFrame(frame, schema=schema)


def test_create_dataframe_empty_pandas_polars_structtype_keeps_types(
    spark: ReparkSession,
) -> None:
    """P2a critic-octo C4: empty pandas/polars + StructType → 0-row frame with declared types.

    Name-only schema still refuses (interchange pin). List empty+StructType already worked;
    native frame builders must match.
    """
    from repark.spark.types import IntegerType, StringType, StructField, StructType

    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    schema = StructType(
        [StructField("id", IntegerType(), True), StructField("label", StringType(), True)]
    )
    pandas_table = spark.createDataFrame(
        pd.DataFrame({"id": pd.Series([], dtype="int32"), "label": pd.Series([], dtype="string")}),
        schema=schema,
    ).to_arrow()
    assert pandas_table.num_rows == 0
    assert pandas_table.schema.field("id").type == pa.int32()
    assert pandas_table.schema.field("label").type == pa.string()
    polars_table = spark.createDataFrame(
        pl.DataFrame(schema={"id": pl.Int32, "label": pl.String}),
        schema=schema,
    ).to_arrow()
    assert polars_table.num_rows == 0
    assert polars_table.schema.field("id").type == pa.int32()
    assert polars_table.schema.field("label").type == pa.string()
