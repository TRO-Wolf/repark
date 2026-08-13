"""Facade tests for ``repark`` — import smoke, the builder chain, and an end-to-end SQL
round-trip through the native engine to Arrow / Polars.

These require the compiled wheel (``maturin develop``); they exercise the real boundary, not a
mock — a ``SELECT`` literal must round-trip with correct values and counts.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

import repark
from repark import ReparkSession
from repark.errors import (
    IllegalArgumentException,
    PySparkException,
    PySparkRuntimeError,
    PySparkTypeError,
)
from repark.session import _reset_active_session_for_tests


def test_import_smoke() -> None:
    # `import repark` and the documented entry point both resolve.
    assert hasattr(repark, "ReparkSession")
    assert hasattr(repark, "DataFrame")
    # `SparkSession` is kept as a byte-identical drop-in alias of ReparkSession.
    assert repark.SparkSession is repark.ReparkSession
    assert repark.ReparkSession is ReparkSession
    # The pre-rename capital-P casing stays importable (back-compat alias).
    assert repark.ReParkSession is repark.ReparkSession


def test_version_is_exposed() -> None:
    # The wheel's CI import smoke prints `repark.__version__`; it must exist and be a non-empty
    # string (a missing attribute is exactly the AttributeError that broke the smoke before).
    assert isinstance(repark.__version__, str)
    assert repark.__version__


def test_builder_get_or_create() -> None:
    # getOrCreate returns the identical active session on a second call (PySpark semantics).
    spark = ReparkSession.builder.appName("test").getOrCreate()
    assert isinstance(spark, ReparkSession)
    spark2 = ReparkSession.builder.app_name("test").get_or_create()
    assert spark is spark2


def test_get_or_create_differing_config_warns_and_returns_active() -> None:
    first = ReparkSession.builder.appName("a").getOrCreate()
    with pytest.warns(UserWarning, match="existing ReparkSession"):
        second = (
            ReparkSession.builder.appName("b").config("repark.memory.limit.gb", "2").getOrCreate()
        )
    assert second is first


def test_stop_then_get_or_create_builds_fresh() -> None:
    first = ReparkSession.builder.appName("first").getOrCreate()
    first.stop()
    second = ReparkSession.builder.appName("second").getOrCreate()
    assert second is not first
    # State pin: fresh session is alive (not stopped).
    assert second.sql("SELECT 1 AS n").count() == 1


def test_stopped_session_raises_named_error() -> None:
    spark = ReparkSession.builder.appName("stop-me").getOrCreate()
    spark.stop()
    with pytest.raises(RuntimeError, match="stopped ReparkSession"):
        spark.sql("SELECT 1")


def test_builder_is_fresh_per_access() -> None:
    # Independent `.builder` chains must not leak config into each other.
    b1 = ReparkSession.builder
    b2 = ReparkSession.builder
    assert b1 is not b2


def test_engine_knob_dual_spelling_identical_values_ok() -> None:
    # Dual spellings with the same integer collapse (repark-native first in the lookup order).
    spark = (
        ReparkSession.builder.config("repark.memory.limit.gb", "2")
        .config("spark.repark.memory.limit.gb", "2")
        .getOrCreate()
    )
    assert isinstance(spark, ReparkSession)


def test_engine_knob_conflicting_values_raise() -> None:
    # Conflicting dual spellings fail loud naming both keys (never frozenset-order pick).
    # Group X: the CLASS is now IllegalArgumentException — an invalid config value is a JVM
    # IllegalArgumentException in Spark (live pyspark 4.0.0 oracle), which is NOT a ValueError.
    # This pin used to assert `ValueError`, codifying the divergence; flipped in the same commit
    # as the fix (the Group S discipline). Same class the ENGINE raises for `Error::Config`.
    with pytest.raises(IllegalArgumentException, match=r"conflicting config") as raised:
        (
            ReparkSession.builder.config("repark.memory.limit.gb", "2")
            .config("spark.repark.memory.limit.gb", "4")
            .getOrCreate()
        )
    message = str(raised.value)
    assert "repark.memory.limit.gb" in message
    assert "spark.repark.memory.limit.gb" in message
    # Catch-compat all the way up the PySpark parents.
    assert isinstance(raised.value, PySparkException)
    assert isinstance(raised.value, RuntimeError)
    # ...and the deliberate break: it is no longer a ValueError (PySpark's isn't either).
    assert not isinstance(raised.value, ValueError)


def test_engine_knob_non_integer_raises() -> None:
    # Unparsable ints raise naming the key (never warn-and-default). Group X: the class is now
    # IllegalArgumentException (see the pin above) — MUTATION: revert the raise to ValueError → RED.
    with pytest.raises(
        IllegalArgumentException, match=r"repark\.memory\.limit\.gb.*must be an integer"
    ) as raised:
        ReparkSession.builder.config("repark.memory.limit.gb", "not-a-number").getOrCreate()
    assert isinstance(raised.value, PySparkException)


# ---------------------------------------------------------------------------
# C3 census expand — session active surface + RuntimeConfig additive blocks
# ---------------------------------------------------------------------------


def test_get_active_session_none_then_session() -> None:
    """C3: getActiveSession tracks process-wide active; None when stopped."""
    _reset_active_session_for_tests()
    assert ReparkSession.getActiveSession() is None
    spark = ReparkSession.builder.appName("c3-active").getOrCreate()
    assert ReparkSession.getActiveSession() is spark
    spark.stop()
    assert ReparkSession.getActiveSession() is None


def test_active_raises_when_no_session() -> None:
    """C3: active() → PySparkRuntimeError NO_ACTIVE_OR_DEFAULT_SESSION."""
    _reset_active_session_for_tests()
    with pytest.raises(PySparkRuntimeError) as raised:
        ReparkSession.active()
    assert raised.value.getCondition() == "NO_ACTIVE_OR_DEFAULT_SESSION"
    assert raised.value.getMessageParameters() == {}


def test_spark_session_alias_active_surface() -> None:
    """C3 octo C6: SparkSession alias exposes getActiveSession/active (drop-in)."""
    from repark import SparkSession

    _reset_active_session_for_tests()
    assert SparkSession is ReparkSession
    assert SparkSession.getActiveSession() is None
    with pytest.raises(PySparkRuntimeError) as raised:
        SparkSession.active()
    assert raised.value.getCondition() == "NO_ACTIVE_OR_DEFAULT_SESSION"
    spark = SparkSession.builder.appName("c3-alias").getOrCreate()
    try:
        assert SparkSession.getActiveSession() is spark
        assert SparkSession.active() is spark
    finally:
        spark.stop()
    assert SparkSession.getActiveSession() is None


def test_session_context_manager_stops() -> None:
    """C3: with session: … always stops (Apache test_create_new_session_with_statement)."""
    _reset_active_session_for_tests()
    with ReparkSession.builder.appName("c3-cm").getOrCreate() as spark:
        assert spark.sql("SELECT 1 AS n").count() == 1
    assert ReparkSession.getActiveSession() is None
    with pytest.raises(RuntimeError, match="stopped"):
        spark.sql("SELECT 1")


def test_context_manager_enter_does_not_promote_active() -> None:
    """C3 octo C7: __enter__ is not an active-session promotion point."""
    _reset_active_session_for_tests()
    first = ReparkSession.builder.appName("c3-cm-enter-a").getOrCreate()
    second = first.newSession()
    try:
        assert ReparkSession.getActiveSession() is first
        with second:
            # Enter alone must not steal active from first.
            assert ReparkSession.getActiveSession() is first
            second.sql("SELECT 1 AS n").count()
            assert ReparkSession.getActiveSession() is second
        # Exit stops second while it was active → process active clears.
        assert ReparkSession.getActiveSession() is None
    finally:
        first.stop()
        if second._inner is not None:
            second.stop()


def test_new_session_distinct_handle() -> None:
    """C3: newSession() returns a different live session (Apache test_new_session)."""
    _reset_active_session_for_tests()
    first = ReparkSession.builder.appName("c3-new").getOrCreate()
    second = first.newSession()
    try:
        assert second is not first
        # newSession must not steal active until an action promotes it.
        assert ReparkSession.getActiveSession() is first
        assert second.sql("SELECT 1 AS n").count() == 1
        assert ReparkSession.getActiveSession() is second
    finally:
        first.stop()
        second.stop()


def test_new_session_restores_active_on_base_exception() -> None:
    """C3 octo C1-Q-001: BaseException mid-newSession must not steal process active.

    ``newSession`` calls ``Builder.getOrCreate`` (camelCase alias). Patch that name so
    the decoy registers as active then raises ``KeyboardInterrupt``; ``try``/``finally``
    must restore the prior active even on BaseException (not only Exception).
    """
    _reset_active_session_for_tests()
    first = ReparkSession.builder.appName("c3-be").getOrCreate()
    assert ReparkSession.getActiveSession() is first
    real_get_or_create = ReparkSession.Builder.get_or_create
    decoy_holder: list[object] = []

    def _boom(self: object) -> object:
        decoy = real_get_or_create(self)  # type: ignore[arg-type]
        decoy_holder.append(decoy)
        import repark.session as session_mod

        session_mod._active_session = decoy  # type: ignore[attr-defined]
        raise KeyboardInterrupt("simulated mid-newSession")

    try:
        # newSession uses getOrCreate (class-body alias), not get_or_create by name.
        ReparkSession.Builder.getOrCreate = _boom  # type: ignore[method-assign,attr-defined]
        with pytest.raises(KeyboardInterrupt):
            first.newSession()
        assert ReparkSession.getActiveSession() is first
    finally:
        ReparkSession.Builder.getOrCreate = real_get_or_create  # type: ignore[method-assign,attr-defined]
        for decoy in decoy_holder:
            stop = getattr(decoy, "stop", None)
            if callable(stop):
                stop()
        first.stop()


def test_create_dataframe_promotes_active_session() -> None:
    """C3: createDataFrame on a newSession promotes active (Apache after_create_dataframe)."""
    _reset_active_session_for_tests()
    first = ReparkSession.builder.appName("c3-promote").getOrCreate()
    second = first.newSession()
    try:
        assert ReparkSession.getActiveSession() is first
        second.createDataFrame([(1, "a")], ["n", "s"])
        assert ReparkSession.getActiveSession() is second
        first.createDataFrame([(2, "b")], ["n", "s"])
        assert ReparkSession.getActiveSession() is first
    finally:
        first.stop()
        second.stop()


def test_new_session_preserves_foreign_active() -> None:
    """C3 octo C2-Q-002: newSession restores *current* active, not necessarily self."""
    _reset_active_session_for_tests()
    first = ReparkSession.builder.appName("c3-foreign-a").getOrCreate()
    second = first.newSession()
    third: ReparkSession | None = None
    try:
        second.sql("SELECT 1 AS n").count()
        assert ReparkSession.getActiveSession() is second
        # first is not active; newSession must keep second active.
        third = first.newSession()
        assert third is not first and third is not second
        assert ReparkSession.getActiveSession() is second
    finally:
        if third is not None:
            third.stop()
        second.stop()
        first.stop()


def test_get_or_create_reuse_skips_static_conf() -> None:
    """C3 octo C2-Q-001: soft-conf fold must not mutate static keys."""
    import warnings

    _reset_active_session_for_tests()
    first = ReparkSession.builder.appName("c3-static").getOrCreate()
    try:
        before = first.conf.getAll.get("spark.sql.warehouse.dir")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            second = ReparkSession.builder.config(
                "spark.sql.warehouse.dir", "/tmp/c3-static-evil"
            ).getOrCreate()
        assert second is first
        assert first.conf.getAll.get("spark.sql.warehouse.dir") == before
        assert not first.conf.isModifiable("spark.sql.warehouse.dir")
        # Static skip must not false-warn as unapplied (octo C3 C4-Q-002).
        static_warns = [
            w
            for w in caught
            if issubclass(w.category, UserWarning) and "spark.sql.warehouse.dir" in str(w.message)
        ]
        assert static_warns == []
    finally:
        first.stop()


def test_create_dataframe_type_error_still_promotes() -> None:
    """C3 octo C2 cheap: promote runs before validation (Spark method-entry parity)."""
    _reset_active_session_for_tests()
    first = ReparkSession.builder.appName("c3-promote-fail").getOrCreate()
    second = first.newSession()
    try:
        assert ReparkSession.getActiveSession() is first
        with pytest.raises(PySparkTypeError):
            second.createDataFrame(object())  # type: ignore[arg-type]
        assert ReparkSession.getActiveSession() is second
    finally:
        first.stop()
        second.stop()


def test_runtime_config_get_all_and_unset_raise() -> None:
    """C3: getAll / get-without-default / set(None) — Apache test_conf + test_get_all."""
    _reset_active_session_for_tests()
    spark = ReparkSession.builder.appName("c3-conf").getOrCreate()
    try:
        all_confs = spark.conf.getAll
        assert len(all_confs) > 0
        assert "foo" not in all_confs
        spark.conf.set("foo", "bar")
        assert len(spark.conf.getAll) == len(all_confs) + 1
        assert spark.conf.getAll["foo"] == "bar"
        spark.conf.unset("foo")
        with pytest.raises(Exception, match=r"not\.set"):
            spark.conf.get("not.set")
        assert spark.conf.get("missing", None) is None
        assert spark.conf.get("spark.sql.sources.partitionOverwriteMode") == "STATIC"
        assert spark.conf.get("spark.sql.sources.partitionOverwriteMode", None) is None
        with pytest.raises(IllegalArgumentException, match="None"):
            spark.conf.set("foo", None)
        spark.conf.set("flag", True)
        assert spark.conf.get("flag") == "true"
        assert spark.conf.isModifiable("spark.sql.execution.arrow.maxRecordsPerBatch")
        assert not spark.conf.isModifiable("spark.sql.warehouse.dir")
        # Static conf set must refuse (isModifiable=False is not advisory-only).
        with pytest.raises(Exception, match="static config"):
            spark.conf.set("spark.sql.warehouse.dir", "/tmp/evil")
        # getAll returns a copy — mutating the dump must not poison the store.
        dump = spark.conf.getAll
        dump["poison"] = "x"
        assert "poison" not in spark.conf.getAll
    finally:
        spark.stop()


def test_get_or_create_applies_facade_conf_on_reuse() -> None:
    """C3: soft conf keys fold into live session conf (Apache config_option_propagated)."""
    _reset_active_session_for_tests()
    first = ReparkSession.builder.config("spark-config1", "a").getOrCreate()
    try:
        assert first.conf.get("spark-config1") == "a"
        second = ReparkSession.builder.config("spark-config1", "b").getOrCreate()
        assert second is first
        assert first.conf.get("spark-config1") == "b"
    finally:
        first.stop()


def test_conf_unset_clears_builder_fallback() -> None:
    """C3 octo C3-Q-001: unset must not resurrect Builder.config snapshot values."""
    _reset_active_session_for_tests()
    spark = ReparkSession.builder.config("soft", "1").appName("c3-unset").getOrCreate()
    try:
        assert spark.conf.get("soft") == "1"
        spark.conf.unset("soft")
        with pytest.raises(Exception, match=r"soft"):
            spark.conf.get("soft")
        assert spark.conf.get("soft", "fallback") == "fallback"
        assert "soft" not in spark.conf.getAll
        # set after unset clears the tombstone.
        spark.conf.set("soft", "2")
        assert spark.conf.get("soft") == "2"
        assert spark.conf.getAll["soft"] == "2"
    finally:
        spark.stop()


def test_get_or_create_soft_fold_after_unset() -> None:
    """C3 octo C4: soft-fold after conf.unset re-applies and clears the tombstone."""
    _reset_active_session_for_tests()
    first = ReparkSession.builder.config("k", "1").appName("c3-refold").getOrCreate()
    try:
        first.conf.unset("k")
        with pytest.raises(Exception, match=r"\bk\b"):
            first.conf.get("k")
        second = ReparkSession.builder.config("k", "2").getOrCreate()
        assert second is first
        assert first.conf.get("k") == "2"
        assert first.conf.getAll["k"] == "2"
    finally:
        first.stop()


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest").getOrCreate()


def test_sql_round_trips_to_arrow(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a, 'x' AS b")
    table = df.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.column_names == ["a", "b"]
    assert table.column("a").to_pylist() == [1]
    assert table.column("b").to_pylist() == ["x"]


def test_collect_returns_list_of_rows(spark: ReparkSession) -> None:
    from repark import Row

    df = spark.sql("SELECT 1 AS a, 'x' AS b")
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert isinstance(rows[0], Row)
    assert rows[0].a == 1
    assert rows[0][1] == "x"
    assert rows[0].asDict() == {"a": 1, "b": "x"}


def test_count(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3")
    assert df.count() == 3


def test_pyarrow_table_consumes_dataframe_directly(spark: ReparkSession) -> None:
    # The Arrow PyCapsule dunder makes the DataFrame itself a valid Arrow stream source.
    df = spark.sql("SELECT 42 AS n")
    table = pa.table(df)
    assert table.column("n").to_pylist() == [42]


def test_to_polars(spark: ReparkSession) -> None:
    pl = pytest.importorskip("polars")
    df = spark.sql("SELECT 7 AS a, 'y' AS b")
    pdf = df.to_polars()
    assert isinstance(pdf, pl.DataFrame)
    assert pdf.to_dicts() == [{"a": 7, "b": "y"}]


def test_to_pandas(spark: ReparkSession) -> None:
    pd = pytest.importorskip("pandas")
    df = spark.sql("SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS t(a, b) ORDER BY a")
    pdf = df.to_pandas()
    assert isinstance(pdf, pd.DataFrame)
    assert list(pdf.columns) == ["a", "b"]
    assert pdf["a"].tolist() == [1, 2]
    assert pdf["b"].tolist() == ["x", "y"]


def test_to_pandas_camelcase_alias(spark: ReparkSession) -> None:
    # PySpark spells it `toPandas`; the byte-identical name must be the same method.
    pytest.importorskip("pandas")
    from repark import DataFrame

    assert DataFrame.toPandas is DataFrame.to_pandas
    df = spark.sql("SELECT 5 AS n")
    assert df.toPandas()["n"].tolist() == [5]


def test_to_numpy_numeric_matrix(spark: ReparkSession) -> None:
    # The ML-feature-matrix case: an all-numeric frame yields a single numeric 2-D array.
    # U2: VALUES (1.5, 2.5), (3.0, 4.0) are DECIMAL(2,1); Arrow decimal → object of Decimal.
    from decimal import Decimal

    import numpy as np

    df = spark.sql("SELECT * FROM (VALUES (1.5, 2.5), (3.0, 4.0)) AS t(a, b) ORDER BY a")
    matrix = df.to_numpy()
    assert matrix.shape == (2, 2)
    assert matrix.dtype == object
    expected = np.array(
        [
            [Decimal("1.5"), Decimal("2.5")],
            [Decimal("3.0"), Decimal("4.0")],
        ],
        dtype=object,
    )
    np.testing.assert_array_equal(matrix, expected)


def test_to_numpy_null_becomes_nan(spark: ReparkSession) -> None:
    # Nullable numeric columns convert to float64 with NaN (Arrow/pandas semantics).
    import numpy as np

    df = spark.sql("SELECT * FROM (VALUES (1, 10), (2, NULL)) AS t(a, b) ORDER BY a")
    matrix = df.to_numpy()
    assert matrix.shape == (2, 2)
    assert np.isnan(matrix[1, 1])
    assert matrix[0, 1] == 10.0


def test_to_numpy_mixed_types_promotes_to_object(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a, 'x' AS b")
    matrix = df.to_numpy()
    assert matrix.dtype == object
    assert matrix.tolist() == [[1, "x"]]


def test_to_numpy_zero_rows_keeps_column_shape(spark: ReparkSession) -> None:
    df = spark.sql("SELECT * FROM (VALUES (1)) AS t(a) WHERE a > 99")
    assert df.to_numpy().shape == (0, 1)


def test_show_prints_to_stdout(spark: ReparkSession, capsys: pytest.CaptureFixture[str]) -> None:
    df = spark.sql("SELECT 1 AS a, 'x' AS b")
    df.show()
    captured = capsys.readouterr().out
    assert "a" in captured and "x" in captured
    assert "|" in captured


def test_show_truncate_and_n(spark: ReparkSession, capsys: pytest.CaptureFixture[str]) -> None:
    df = spark.sql("SELECT 'abcdefghijklmnopqrstuvwxyz' AS long_col")
    df.show(1, truncate=10)
    captured = capsys.readouterr().out
    assert "..." in captured
    assert "abcdefghijklmnopqrstuvwxyz" not in captured


def test_columns_and_schema(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a, 'x' AS b")
    assert df.columns == ["a", "b"]
    assert df.schema.names == ["a", "b"]


def test_columns_and_schema_resolve_without_executing(spark: ReparkSession) -> None:
    """Metadata ops resolve the schema WITHOUT running the plan — pinned by an un-runnable plan.

    The plan casts a non-numeric string column to ``INT``: DataFusion resolves the output column
    and type at analysis time (so ``columns``/``schema`` succeed), but the Arrow cast kernel raises
    when the plan actually runs (so ``collect`` fails on the SAME df). ``columns``/``schema``
    succeeding proves they never materialized; ``collect`` raising proves the probe is real (a
    schema that also happened to execute cleanly would pin nothing).

    This replaces the former monkeypatch-``to_arrow`` pin, which was vacuous: the native accessors
    resolve the analyzed schema (``analyze_eagerly``) and never call the facade ``to_arrow``, so
    patching it protected nothing.
    """
    df = spark.sql("SELECT CAST(a AS INT) AS n FROM (VALUES ('abc')) AS t(a)")
    # Metadata resolves — the doomed cast is never evaluated.
    assert df.columns == ["n"]
    assert df.schema.names == ["n"]
    # The SAME plan raises when executed: the probe is a genuinely un-runnable plan, not a proxy.
    with pytest.raises(RuntimeError, match="Cast error"):
        df.collect()


def test_limit_and_show_cap_rows(spark: ReparkSession, capsys: pytest.CaptureFixture[str]) -> None:
    # ORDER BY makes limit/show deterministic, so "rows 3..10 absent" is a real pin, not luck.
    df = spark.sql(
        "SELECT id FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS t(id) "
        "ORDER BY id"
    )
    assert len(df.limit(3).collect()) == 3
    df.show(2)
    out = capsys.readouterr().out
    # Parse the PySpark-style ASCII grid (+sep+ / |header| / +sep+ / |row| … / +sep+): the
    # `|`-prefixed lines are the single header then the data rows. A vacuous `"1" in out` passed
    # even if show rendered all ten rows ("1" is a substring of "10"); count the data rows instead.
    bar_lines = [line for line in out.splitlines() if line.startswith("|")]
    header, *data_lines = bar_lines
    assert header.strip("| ").split() == ["id"]
    values = [line.strip("| ").strip() for line in data_lines]
    # show(2) must render EXACTLY two data rows — the first two after ORDER BY — and nothing else.
    assert values == ["1", "2"], f"show(2) must cap at two rows, rendered: {values}"
    # Belt-and-brace: rows 3..10 must be absent from the raw render (the cap really dropped them).
    for dropped in range(3, 11):
        assert f"| {dropped} " not in out, f"row {dropped} leaked past show(2): {out!r}"


def test_select_star(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a, 2 AS b")
    assert df.select("*").columns == ["a", "b"]


def test_create_dataframe_tuples_and_dicts(spark: ReparkSession) -> None:
    from_tuples = spark.createDataFrame([(1, "x"), (2, "y")], schema=["id", "name"])
    assert from_tuples.collect()[0].asDict() == {"id": 1, "name": "x"}
    from_dicts = spark.createDataFrame([{"id": 3, "name": "z"}])
    assert from_dicts.collect()[0].name == "z"


def test_read_parquet_via_reader(spark: ReparkSession, tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "t.parquet"
    pq.write_table(pa.table({"n": [1, 2]}), path)
    df = spark.read.parquet(str(path))
    assert df.collect()[0].n == 1


def test_is_null_and_when(spark: ReparkSession) -> None:
    import repark.functions as F  # noqa: N812 — PySpark idiomatic alias

    df = spark.sql("SELECT * FROM (VALUES (1), (CAST(NULL AS INT))) AS t(a)")
    flags = df.select(F.col("a").isNull().alias("n")).collect()
    assert [row.n for row in flags] == [False, True]
    # isNotNull is the complementary path (overnight WG2 requires both tested).
    not_null = df.select(F.col("a").isNotNull().alias("nn")).collect()
    assert [row.nn for row in not_null] == [True, False]
    labeled = df.select(
        F.when(F.col("a").isNull(), F.lit("missing")).otherwise(F.lit("ok")).alias("label")
    ).collect()
    assert [row.label for row in labeled] == ["ok", "missing"]


def test_f_expr_matches_spark_sql_on_substr_zero(spark: ReparkSession) -> None:
    # BUG-010: F.expr must use Spark semantics (substr pos 0 → 'hel'), not raw DataFusion.
    import repark.functions as F  # noqa: N812 — PySpark idiomatic alias

    via_expr = spark.sql("SELECT 1 AS dummy").select(F.expr("substr('hello', 0, 3)").alias("s"))
    via_sql = spark.sql("SELECT substr('hello', 0, 3) AS s")
    assert via_expr.collect()[0].s == via_sql.collect()[0].s == "hel"
