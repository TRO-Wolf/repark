"""Group F — production-job gaps from the 2026-07-21 dogfood run.

Closes the six shims that `process_silver.py` needed on RePark (recorded in the dogfood
report for that run). Every behavioral pin is
recorded from live PySpark 4.1.2 (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`SPARK_LOCAL_IP=127.0.0.1`); oracle values live in the test docstrings.
"""

from __future__ import annotations

import inspect
import warnings
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession, __version__
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.dataframe import DataFrame
from repark.errors import AnalysisException
from repark.session import SparkContext


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-dogfood-gaps").getOrCreate()
    session.register_memory_catalog("cat", tmp_path / "warehouse")
    session.sql("CREATE NAMESPACE cat.ns")
    yield session
    session.stop()


# ==================================================================================================
# F1 — current_timestamp() microsecond UTC (engine bug that broke Iceberg v2 CTAS)
# ==================================================================================================


def test_current_timestamp_arrow_type_is_microsecond_utc(spark: ReparkSession) -> None:
    """Oracle (live PySpark 4.1.2): ``timestamp[us, tz=UTC]``.

    Recording::

        spark.range(1).select(F.current_timestamp().alias("ts")).toArrow().schema
        # ts: timestamp[us, tz=UTC] not null
    """
    table = spark.sql("SELECT 1 AS a").withColumn("ts", F.current_timestamp()).to_arrow()
    field_type = table.schema.field("ts").type
    assert pa.types.is_timestamp(field_type), field_type
    assert field_type.unit == "us", f"expected us precision, got {field_type}"
    assert str(field_type.tz).upper() in {"UTC", "+00:00"}, f"expected UTC tz, got {field_type}"
    # Oracle (PySpark 4.1.2): field is not null on the Arrow path.
    assert not table.schema.field("ts").nullable, (
        "Spark/oracle current_timestamp Arrow field is not null"
    )


def test_sql_current_timestamp_still_nanosecond_residual(spark: ReparkSession) -> None:
    """Octo C1-Q-001 residual: SQL ``current_timestamp()`` is still DataFusion ns.

    Group F fixed the **functions** path only. This pin fails closed if SQL is later
    aligned without updating the ledger — and documents the known gap for Iceberg CTAS
    via pure SQL.
    """
    table = spark.sql("SELECT current_timestamp() AS ts").to_arrow()
    field_type = table.schema.field("ts").type
    assert pa.types.is_timestamp(field_type)
    assert field_type.unit == "ns", (
        f"SQL current_timestamp residual expected ns until a SQL-shim unit; got {field_type}. "
        "If this is us, update task/todo.md Group F follow-up and this pin."
    )


def test_current_timestamp_ctas_into_iceberg_v2_succeeds(spark: ReparkSession) -> None:
    """Dogfood GAP-6 regression: CTAS of a raw ``F.current_timestamp()`` column into Iceberg.

    The production job uses the DataFrame/functions path
    (``.withColumn("ingestion_timestamp", current_timestamp())``), not SQL
    ``current_timestamp()``. Before the µs cast this failed with
    ``timestamp_ns is not supported until v3``. Mutation proof: reverting the
    binding cast re-breaks this test.
    """
    source = spark.sql("SELECT 1 AS id").withColumn("ingestion_timestamp", F.current_timestamp())
    source.createOrReplaceTempView("src_ts")
    spark.sql("CREATE TABLE cat.ns.ts_ctas AS SELECT * FROM src_ts")
    rows = spark.sql("SELECT id FROM cat.ns.ts_ctas").to_arrow()
    assert rows.column("id").to_pylist() == [1]
    # Schema of the written column must not be nanosecond.
    written = spark.table("cat.ns.ts_ctas").to_arrow()
    ts_type = written.schema.field("ingestion_timestamp").type
    assert pa.types.is_timestamp(ts_type)
    assert ts_type.unit == "us", f"Iceberg v2 path must persist microsecond timestamps: {ts_type}"
    # Written column should retain UTC/offset tz (Spark oracle / F1 cast), not strip to naive.
    assert str(ts_type.tz).upper() in {"UTC", "+00:00"}, (
        f"expected UTC/+00:00 tz after CTAS, got {ts_type}"
    )
    # C4-Q-001: written value near-now (type-only pin was hollow).
    import datetime

    value = written.column("ingestion_timestamp")[0].as_py()
    assert value is not None
    if getattr(value, "tzinfo", None) is not None:
        now = datetime.datetime.now(datetime.UTC)
        value_naive = value.astimezone(datetime.UTC).replace(tzinfo=None)
        now_naive = now.replace(tzinfo=None)
    else:
        now_naive = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
        value_naive = value
    delta = abs((now_naive - value_naive).total_seconds())
    assert delta < 120, f"CTAS current_timestamp value not near now: {value!r} delta={delta}s"


# ==================================================================================================
# F2 / F3 — sparkContext + version
# ==================================================================================================


def test_spark_context_set_log_level_is_silent_noop(spark: ReparkSession) -> None:
    """``setLogLevel`` accepted for source compatibility; silent no-op (OTH-010)."""
    docstring = SparkContext.setLogLevel.__doc__ or ""
    assert "no-op" in docstring or "tracing" in docstring
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert spark.sparkContext.setLogLevel("ERROR") is None


def test_spark_context_application_id_stable_per_session(spark: ReparkSession) -> None:
    first = spark.sparkContext.applicationId
    second = spark.sparkContext.applicationId
    assert first == second
    assert first.startswith("local-repark-")
    assert len(first) > len("local-repark-")


def test_spark_context_master_defaults_and_records_builder(tmp_path: Path) -> None:
    from repark import session as session_module

    session_module._reset_dropin_warnings_for_tests()
    session_module._reset_active_session_for_tests()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # master warn-once is separate disclosure
        session = ReparkSession.builder.appName("master-probe").master("local[4]").getOrCreate()
    try:
        assert session.sparkContext.master == "local[4]"
    finally:
        session.stop()
        session_module._reset_active_session_for_tests()


def test_spark_context_master_default_local_repark(tmp_path: Path) -> None:
    """Octo C1-L-002: default master is local[repark] when builder omits .master()."""
    from repark import session as session_module

    session_module._reset_dropin_warnings_for_tests()
    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("default-master").getOrCreate()
    try:
        assert session.sparkContext.master == "local[repark]"
    finally:
        session.stop()
        session_module._reset_active_session_for_tests()


def test_spark_context_unknown_attr_raises(spark: ReparkSession) -> None:
    with pytest.raises(AttributeError, match=r"out of scope|no attribute"):
        _ = spark.sparkContext.parallelize  # type: ignore[attr-defined]


def test_stopped_session_blocks_spark_context_and_version(tmp_path: Path) -> None:
    """Octo C5: stop() makes sparkContext/version raise RuntimeError."""
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("stop-sc").getOrCreate()
    session.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        _ = session.sparkContext
    with pytest.raises(RuntimeError, match="stopped"):
        _ = session.version
    session_module._reset_active_session_for_tests()


def test_spark_version_is_repark_prefixed(spark: ReparkSession) -> None:
    """Oracle: live Spark returns ``"4.1.2"``; repark returns ``repark-<dist version>``.

    Scripts log the string; they must not parse it as a Spark release (disclosed).
    """
    assert spark.version == f"repark-{__version__}"
    assert spark.version != "4.1.2"


# ==================================================================================================
# F4 — withColumns / withColumnsRenamed (oracle-driven atomicity)
# ==================================================================================================


def test_with_columns_atomic_cross_dependency(spark: ReparkSession) -> None:
    """Oracle (PySpark 4.1.2): both exprs see the ORIGINAL frame.

    ``df.withColumns({"a": col("b")+1, "b": col("a")+100})`` on ``(a=1,b=10)`` → ``(11, 101)``.
    A sequential fold would yield ``b=111``.
    """
    frame = spark.createDataFrame([(1, 10)], ["a", "b"])
    out = frame.withColumns({"a": F.col("b") + 1, "b": F.col("a") + 100})
    assert out.columns == ["a", "b"]
    rows = out.to_arrow()
    assert rows.column("a").to_pylist() == [11]
    assert rows.column("b").to_pylist() == [101]


def test_with_columns_swap_atomic(spark: ReparkSession) -> None:
    """Oracle: ``withColumns({"a": col("b"), "b": col("a")})`` on ``(1,10)`` → ``(10, 1)``."""
    frame = spark.createDataFrame([(1, 10)], ["a", "b"])
    out = frame.withColumns({"a": F.col("b"), "b": F.col("a")})
    rows = out.to_arrow()
    assert rows.column("a").to_pylist() == [10]
    assert rows.column("b").to_pylist() == [1]


def test_with_columns_appends_new_names_in_dict_order(spark: ReparkSession) -> None:
    """Oracle: new columns append after existing ones in dict insertion order."""
    frame = spark.createDataFrame([(5,)], ["x"])
    out = frame.withColumns({"y": F.col("x") * 2, "z": F.col("x") + 1})
    assert out.columns == ["x", "y", "z"]
    rows = out.to_arrow()
    assert rows.column("y").to_pylist() == [10]
    assert rows.column("z").to_pylist() == [6]


def test_with_columns_renamed_simple_and_missing_noop(spark: ReparkSession) -> None:
    """Oracle: missing old name is no-op; present renames apply.

    ``withColumnsRenamed({"missing": "x", "a": "aa"})`` on ``[a,b]`` → cols ``[aa, b]``.
    """
    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    out = frame.withColumnsRenamed({"missing": "x", "a": "aa"})
    assert out.columns == ["aa", "b"]
    rows = out.to_arrow()
    assert rows.column("aa").to_pylist() == [1]
    assert rows.column("b").to_pylist() == [2]


def test_with_columns_renamed_chain_without_collision(spark: ReparkSession) -> None:
    """Sequential rename chain that stays unique: a→x, x→y on [a,b] → [y,b].

    Octo C1-L-001: intermediate name from a prior map entry is visible to later entries
    (running name-list), not a simultaneous original-only map.
    """
    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    out = frame.withColumnsRenamed({"a": "x", "x": "y"})
    # After a→x names are [x,b]; x→y rewrites the first only → [y,b]
    assert out.columns == ["y", "b"]
    assert out.to_arrow().column("y").to_pylist() == [1]


def test_with_columns_renamed_duplicate_final_names_fail_loud(spark: ReparkSession) -> None:
    """Spark probe ``{"a":"b","b":"c"}`` on ``[a,b]`` yields ``[c,c]`` (sequential).

    repark cannot materialize duplicate column names — raises AnalysisException (disclosed).
    """
    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    with pytest.raises(AnalysisException, match="duplicate column names"):
        frame.withColumnsRenamed({"a": "b", "b": "c"})


# ==================================================================================================
# F5 — DataFrame.transform
# ==================================================================================================


def test_with_columns_empty_map_is_identity(spark: ReparkSession) -> None:
    """Octo C3: empty colsMap projects the original frame unchanged."""
    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    out = frame.withColumns({})
    assert out.columns == ["a", "b"]
    assert out.to_arrow().column("a").to_pylist() == [1]


def test_transform_positional_args(spark: ReparkSession) -> None:
    """Octo C3-Q: transform forwards *args (not only kwargs)."""
    frame = spark.createDataFrame([(1,)], ["a"])

    def add_n(data_frame: DataFrame, n: int) -> DataFrame:
        return data_frame.withColumn("n", F.lit(n))

    out = frame.transform(add_n, 9)
    assert out.to_arrow().column("n").to_pylist() == [9]


def test_transform_signature_and_happy_path(spark: ReparkSession) -> None:
    """Oracle: ``inspect.signature(df.transform)`` is
    ``(func: Callable[..., DataFrame], *args: Any, **kwargs: Any) -> DataFrame``.
    """
    frame = spark.createDataFrame([(1, 10)], ["a", "b"])
    signature = inspect.signature(frame.transform)
    params = list(signature.parameters)
    assert params[0] == "func"
    assert "args" in params or any(
        p.kind == inspect.Parameter.VAR_POSITIONAL for p in signature.parameters.values()
    )

    def add_n(data_frame: DataFrame, n: int = 1) -> DataFrame:
        return data_frame.withColumn("n", F.lit(n))

    out = frame.transform(add_n, n=7)
    assert out.columns == ["a", "b", "n"]
    assert out.to_arrow().column("n").to_pylist() == [7]


def test_transform_non_dataframe_return_raises_assertion(spark: ReparkSession) -> None:
    """Oracle (PySpark 4.1.2): ``AssertionError`` —
    ``Func returned an instance of type [<class 'int'>], should have been DataFrame.``
    """
    frame = spark.createDataFrame([(1,)], ["a"])

    def bad(_data_frame: DataFrame) -> int:
        return 42

    with pytest.raises(AssertionError, match=r"should have been DataFrame"):
        frame.transform(bad)  # type: ignore[arg-type]


# ==================================================================================================
# F6 — DIVERGENCE-1 timestamp-LTZ collect (disclose only)
# ==================================================================================================


def test_divergence_timestamp_ltz_collect_passthrough(spark: ReparkSession, tmp_path: Path) -> None:
    """DIVERGENCE-1 (disclose, do not build session-tz machinery).

    Naive parquet timestamps cast via ``TimestampType()``:

    * **repark** passes the value through — Arrow int64 ticks after cast equal the source
      parquet column (host-TZ independent; ACC Q-001).
    * **PySpark** ``TimestampType`` is LTZ: on ``collect()`` it converts through the process
      local timezone. Recorded dogfood (EDT host): naive parquet ``09:00`` → collect ``05:00``.
      That Spark wall-clock half is recorded below as a constant so a future *EDT-shaped*
      conversion would still fail the tick-identity pin first.

    JVM-free: no live Spark in this test.
    """
    import datetime

    import pyarrow.parquet as pq

    naive = datetime.datetime(2026, 7, 20, 9, 0, 0)
    # Recorded Spark LTZ collect wall clock on EDT host (dogfood report 2026-07-21).
    spark_edt_collect_wall = datetime.datetime(2026, 7, 20, 5, 0, 0)

    path = tmp_path / "naive_ts.parquet"
    table = pa.table(
        {
            "id": [1],
            "ts": pa.array([naive], type=pa.timestamp("us")),
        }
    )
    pq.write_table(table, path)

    frame = spark.read.parquet(str(path))
    from repark.types import TimestampType

    source_arrow = frame.to_arrow()
    source_ticks = source_arrow.column("ts")[0].as_py()

    casted = frame.withColumn("ts2", F.col("ts").cast(TimestampType()))
    arrow = casted.to_arrow()
    cast_ticks = arrow.column("ts2")[0].as_py()

    # Host-TZ-independent: cast must not shift the Arrow value vs the parquet source.
    # An LTZ conversion (any zone) changes ticks; hour==9 alone is false-green on UTC hosts.
    assert cast_ticks == source_ticks, (
        f"repark must pass naive timestamp ticks through; source={source_ticks!r} "
        f"cast={cast_ticks!r}. If these differ, session-tz LTZ may have been introduced — "
        "update the DIVERGENCE-1 disclosure before changing this pin."
    )
    assert cast_ticks == naive or (
        hasattr(cast_ticks, "hour") and cast_ticks.hour == 9 and cast_ticks.day == 20
    )
    # Spark half (recorded): if repark ever returned the EDT-shifted wall clock, fail.
    if hasattr(cast_ticks, "hour"):
        assert cast_ticks != spark_edt_collect_wall, (
            "repark returned the recorded Spark-on-EDT collect wall clock; "
            "DIVERGENCE-1 may have silently converged — update disclosure"
        )


def test_with_columns_non_column_value_raises_type_error(spark: ReparkSession) -> None:
    """ACC Q-002: non-Column map values raise TypeError for replace *and* append keys."""
    frame = spark.createDataFrame([(1, 10)], ["a", "b"])
    with pytest.raises(TypeError, match="must be Column"):
        frame.withColumns({"a": 99})  # type: ignore[dict-item]
    with pytest.raises(TypeError, match="must be Column"):
        frame.withColumns({"new_col": "x"})  # type: ignore[dict-item]


# ==================================================================================================
# Octo r2 cycle 1 cheap S2 remediations
# ==================================================================================================


def test_config_spark_master_warns_once(tmp_path: Path) -> None:
    """C1-SEC-002: config spark.master must OTH-010-warn once (not only .master())."""
    from repark import session as session_module

    session_module._reset_dropin_warnings_for_tests()
    session_module._reset_active_session_for_tests()
    with pytest.warns(UserWarning, match="single-node"):
        session = (
            ReparkSession.builder.appName("cfg-master")
            .config("spark.master", "spark://prod:7077")
            .getOrCreate()
        )
    try:
        assert session.sparkContext.master == "spark://prod:7077"
        # Second build path: warn-once — no second warning.
        session.stop()
        session_module._reset_active_session_for_tests()
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            session2 = (
                ReparkSession.builder.appName("cfg-master-2")
                .config("spark.master", "local[2]")
                .getOrCreate()
            )
            session2.stop()
    finally:
        session_module._reset_active_session_for_tests()


def test_held_spark_context_raises_after_stop(tmp_path: Path) -> None:
    """C1-L-001: a held SparkContext must not outlive session.stop()."""
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("held-sc").getOrCreate()
    sc = session.sparkContext
    session.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        _ = sc.applicationId
    with pytest.raises(RuntimeError, match="stopped"):
        sc.setLogLevel("ERROR")
    with pytest.raises(RuntimeError, match="stopped"):
        _ = sc.master
    session_module._reset_active_session_for_tests()


def test_current_timestamp_value_is_near_now(spark: ReparkSession) -> None:
    """C1-Q-001: pin a live ``now()`` value, not only the Arrow type (mutation-proof)."""
    import datetime

    table = spark.sql("SELECT 1 AS a").withColumn("ts", F.current_timestamp()).to_arrow()
    value = table.column("ts")[0].as_py()
    assert value is not None
    # Arrow may return tz-aware datetime; compare as UTC-ish wall clock.
    if getattr(value, "tzinfo", None) is not None:
        now = datetime.datetime.now(datetime.UTC)
        value_utc = value.astimezone(datetime.UTC).replace(tzinfo=None)
        now_naive = now.replace(tzinfo=None)
    else:
        now_naive = datetime.datetime.utcnow()
        value_utc = value
    delta = abs((now_naive - value_utc).total_seconds())
    assert delta < 120, f"current_timestamp not near now: value={value!r} delta={delta}s"


def test_expr_current_timestamp_still_nanosecond_residual(spark: ReparkSession) -> None:
    """C1-Q-002: ``F.expr("current_timestamp()")`` still ns (SQL residual class, second entry)."""
    table = spark.sql("SELECT 1 AS a").withColumn("ts", F.expr("current_timestamp()")).to_arrow()
    field_type = table.schema.field("ts").type
    assert pa.types.is_timestamp(field_type)
    assert field_type.unit == "ns", (
        f"F.expr current_timestamp residual expected ns until SQL-shim unit; got {field_type}"
    )


def test_with_columns_non_str_key_raises_type_error(spark: ReparkSession) -> None:
    """C1-L-002: non-str map keys raise TypeError (not a late engine error)."""
    frame = spark.createDataFrame([(1,)], ["a"])
    with pytest.raises(TypeError, match="keys must be str"):
        frame.withColumns({1: F.lit(1)})  # type: ignore[dict-item]


def test_get_or_create_reuse_with_master_config_warns_once(tmp_path: Path) -> None:
    """C2-SEC-001: reuse getOrCreate with spark.master still OTH-010-warns once."""
    from repark import session as session_module

    session_module._reset_dropin_warnings_for_tests()
    session_module._reset_active_session_for_tests()
    # First session without master — no master warn.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        live = ReparkSession.builder.appName("reuse-base").getOrCreate()
    try:
        with pytest.warns(UserWarning, match="single-node"):
            again = (
                ReparkSession.builder.appName("reuse-master")
                .config("spark.master", "spark://cluster:7077")
                .getOrCreate()
            )
        assert again is live  # reuse, not rebuild
    finally:
        live.stop()
        session_module._reset_active_session_for_tests()


def test_with_columns_renamed_empty_map_is_identity(spark: ReparkSession) -> None:
    """C3-L-001: empty rename map is a no-op identity."""
    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    out = frame.withColumnsRenamed({})
    assert out.columns == ["a", "b"]
    assert out.to_arrow().column("a").to_pylist() == [1]


def test_sql_current_timestamp_ctas_still_rejects_ns(spark: ReparkSession) -> None:
    """C4-Q-001: SQL current_timestamp() still fails Iceberg v2 CTAS (residual).

    Mutation-proof for the residual: when SQL path is later fixed to µs, this pin must
    flip green-fail and the ledger follow-up closes.
    """
    with pytest.raises(Exception, match=r"timestamp_ns|not supported until v3"):
        spark.sql(
            "CREATE TABLE cat.ns.sql_ts_ctas AS "
            "SELECT 1 AS id, current_timestamp() AS ingestion_timestamp"
        )


def test_with_columns_snake_and_camel_are_identical(spark: ReparkSession) -> None:
    """C7-Q-001 / C8-Q-001: dual spellings are the same binding (identity + behavior)."""
    assert DataFrame.withColumns is DataFrame.with_columns
    assert DataFrame.withColumnsRenamed is DataFrame.with_columns_renamed
    frame = spark.createDataFrame([(1, 10)], ["a", "b"])
    via_camel = frame.withColumns({"a": F.col("b") + 1, "b": F.col("a") + 100})
    via_snake = frame.with_columns({"a": F.col("b") + 1, "b": F.col("a") + 100})
    assert via_camel.to_arrow().to_pydict() == via_snake.to_arrow().to_pydict()


# ==================================================================================================
# Octo r3 cycle 1 thorough remediations
# ==================================================================================================


def test_held_dataframe_raises_after_stop(tmp_path: Path) -> None:
    """C1-L-001: mint DF → stop session → count/collect/to_arrow must raise."""
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("held-df").getOrCreate()
    frame = session.sql("SELECT 1 AS a")
    session.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        frame.count()
    with pytest.raises(RuntimeError, match="stopped"):
        frame.collect()
    with pytest.raises(RuntimeError, match="stopped"):
        frame.to_arrow()
    with pytest.raises(RuntimeError, match="stopped"):
        frame.withColumn("b", F.lit(2))
    session_module._reset_active_session_for_tests()


def test_double_stop_is_idempotent(tmp_path: Path) -> None:
    """C1-L-002: second stop() is a no-op."""
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("dbl-stop").getOrCreate()
    session.stop()
    session.stop()  # must not raise
    session_module._reset_active_session_for_tests()


def test_empty_string_column_names_rejected(spark: ReparkSession) -> None:
    """C1-Q-002: empty-string column names fail loud (not materialize)."""
    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    with pytest.raises(AnalysisException, match="non-empty"):
        frame.withColumnsRenamed({"a": ""})
    with pytest.raises(AnalysisException, match="non-empty"):
        frame.withColumns({"": F.lit(1)})


def test_current_timestamp_cast_timestamptype_strips_tz(spark: ReparkSession) -> None:
    """C1-Q-001: cast(TimestampType()) strips F1 UTC → naive us (disclosed footgun)."""
    from repark.types import TimestampType

    table = (
        spark.sql("SELECT 1 AS a")
        .withColumn("ts", F.current_timestamp().cast(TimestampType()))
        .to_arrow()
    )
    field_type = table.schema.field("ts").type
    assert pa.types.is_timestamp(field_type)
    assert field_type.unit == "us"
    assert field_type.tz is None, f"cast(TimestampType) must be naive us, got {field_type}"


def test_expr_current_timestamp_ctas_still_rejects_ns(spark: ReparkSession) -> None:
    """C1-Q-003: F.expr current_timestamp residual still fails Iceberg v2 CTAS."""
    source = spark.sql("SELECT 1 AS id").withColumn("ts", F.expr("current_timestamp()"))
    source.createOrReplaceTempView("src_expr_ts")
    with pytest.raises(Exception, match=r"timestamp_ns|not supported until v3"):
        spark.sql("CREATE TABLE cat.ns.expr_ts_ctas AS SELECT * FROM src_expr_ts")


def test_config_spark_master_case_insensitive_warns(tmp_path: Path) -> None:
    """C1-SEC-001: Spark.Master (mixed case) still triggers OTH-010 warn."""
    from repark import session as session_module

    session_module._reset_dropin_warnings_for_tests()
    session_module._reset_active_session_for_tests()
    with pytest.warns(UserWarning, match="single-node"):
        session = (
            ReparkSession.builder.appName("case-master")
            .config("Spark.Master", "spark://host:7077")
            .getOrCreate()
        )
    try:
        assert session.sparkContext.master == "spark://host:7077"
    finally:
        session.stop()
        session_module._reset_active_session_for_tests()


def test_singular_empty_column_names_rejected(spark: ReparkSession) -> None:
    """C3-L-001: singular withColumn / withColumnRenamed reject empty/whitespace names."""
    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    with pytest.raises(AnalysisException, match="non-empty"):
        frame.withColumn("", F.lit(1))
    with pytest.raises(AnalysisException, match="non-empty"):
        frame.withColumn("   ", F.lit(1))
    with pytest.raises(AnalysisException, match="non-empty"):
        frame.withColumnRenamed("a", "")
    with pytest.raises(AnalysisException, match="non-empty"):
        frame.withColumnRenamed("a", "  ")


def test_held_writer_insert_into_raises_after_stop(spark: ReparkSession, tmp_path: Path) -> None:
    """C2-L-001: held DataFrameWriter.insertInto after session.stop must not commit."""
    from repark import session as session_module

    # Use fixture spark then stop it carefully — rebuild clean session for isolation.
    spark.stop()
    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("held-writer").getOrCreate()
    session.register_memory_catalog("catw", tmp_path / "wh_w")
    session.sql("CREATE NAMESPACE catw.ns")
    session.sql("CREATE TABLE catw.ns.t AS SELECT 1 AS id")
    frame = session.sql("SELECT 2 AS id")
    writer = frame.write.format("iceberg").mode("append")
    session.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        writer.insertInto("catw.ns.t")
    session_module._reset_active_session_for_tests()


def test_columns_schema_transform_raise_after_stop(tmp_path: Path) -> None:
    """C2-L-002/003: columns, schema, transform gate after stop."""
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("meta-stop").getOrCreate()
    frame = session.sql("SELECT 1 AS a")
    session.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        _ = frame.columns
    with pytest.raises(RuntimeError, match="stopped"):
        _ = frame.schema
    with pytest.raises(RuntimeError, match="stopped"):
        frame.transform(lambda d: d)
    session_module._reset_active_session_for_tests()


def test_held_writer_save_as_table_raises_after_stop(tmp_path: Path) -> None:
    """C6-S2: held saveAsTable after stop must raise (parity with insertInto)."""
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("held-sat").getOrCreate()
    session.register_memory_catalog("cats", tmp_path / "wh_s")
    session.sql("CREATE NAMESPACE cats.ns")
    frame = session.sql("SELECT 1 AS id")
    writer = frame.write.format("iceberg").mode("error")
    session.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        writer.saveAsTable("cats.ns.new_t")
    session_module._reset_active_session_for_tests()
