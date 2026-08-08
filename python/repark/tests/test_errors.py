"""Facade tests for the error taxonomy (WG-3; U4 added the unsupported/iceberg classes).

``repark.errors`` mirrors ``pyspark.errors``: a SQL/expression **syntax** error surfaces as
:class:`ParseException`, a **planning/analysis** error (an unresolved table or column, an iceberg
not-found / already-exists) as :class:`AnalysisException`, a deterministic **scope gate** or
unsupported iceberg feature as :class:`UnsupportedOperationException` (the class PySpark raises
for a JVM ``UnsupportedOperationException``), an invalid ``.config(...)`` value as
:class:`IllegalArgumentException` (Group X), and everything else (execution, iceberg commit/data
errors) as the base :class:`PySparkException`. All subclass :class:`RuntimeError`, so the
near-drop-in ``except RuntimeError`` path keeps working after migrating from PySpark.

Group X also adds the **Python-argument** wrappers the facade raises —
:class:`PySparkTypeError` / :class:`PySparkValueError` / :class:`PySparkAttributeError`, each
inheriting both :class:`PySparkException` and the builtin it wraps, exactly as ``pyspark.errors``
does.

Every case drives the *public* facade (``spark.sql``, ``F.expr``, DataFrame ops) end to end through
the built native module, asserting the exception **type** and that the original engine text
survives in ``str(exc)`` (the cause chain the taxonomy must not lose).
"""

from __future__ import annotations

from pathlib import Path

import pytest

import repark._native as _native
from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkAssertionError,
    PySparkAttributeError,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)


@pytest.fixture
def spark() -> ReparkSession:
    """A default session (PySpark ``SparkSession.builder.getOrCreate()``)."""
    return ReparkSession.builder.appName("pytest-errors").getOrCreate()


@pytest.fixture
def spark_with_catalog(tmp_path: Path) -> ReparkSession:
    """A session with the AWS-free memory catalog + a namespace, for the iceberg-path cases."""
    session = ReparkSession.builder.appName("pytest-errors-catalog").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.silver")
    return session


# ==================================================================================================
# The taxonomy shape: subclassing + re-export identity


def test_exception_hierarchy_subclasses_runtime_error() -> None:
    # Near-drop-in: existing `except RuntimeError` code must keep catching engine errors. The
    # PySpark-named leaves descend from the base, which descends from RuntimeError.
    assert issubclass(PySparkException, RuntimeError)
    assert issubclass(AnalysisException, PySparkException)
    assert issubclass(ParseException, PySparkException)
    assert issubclass(UnsupportedOperationException, PySparkException)
    assert issubclass(AnalysisException, RuntimeError)
    assert issubclass(ParseException, RuntimeError)
    assert issubclass(UnsupportedOperationException, RuntimeError)
    # C4 expand2: PySparkAssertionError under repark tree for check_error isinstance.
    assert issubclass(PySparkAssertionError, PySparkException)
    assert issubclass(PySparkAssertionError, AssertionError)
    assert issubclass(PySparkAssertionError, RuntimeError)
    # PySpark parity (Group S): ParseException IS-A AnalysisException — `pyspark.errors` defines
    # `ParseException(AnalysisException)` (base.py), so `except AnalysisException` catches a parse
    # error. Before Group S, ParseException subclassed PySparkException directly and a migrated
    # `except AnalysisException` silently missed syntax errors.
    assert issubclass(ParseException, AnalysisException)
    # ...but the relation is one-way, and the other leaves stay distinct.
    assert not issubclass(AnalysisException, ParseException)
    assert not issubclass(UnsupportedOperationException, AnalysisException)
    assert not issubclass(UnsupportedOperationException, ParseException)
    assert not issubclass(AnalysisException, UnsupportedOperationException)
    # Group X: IllegalArgumentException is a PySparkException leaf, distinct from the others.
    assert issubclass(IllegalArgumentException, PySparkException)
    assert issubclass(IllegalArgumentException, RuntimeError)
    assert not issubclass(IllegalArgumentException, AnalysisException)
    assert not issubclass(IllegalArgumentException, UnsupportedOperationException)
    assert not issubclass(AnalysisException, IllegalArgumentException)


def test_analysis_exception_catches_parse_errors_pyspark_parity(spark: ReparkSession) -> None:
    # The near-drop-in payoff of the reparenting: a SQL syntax error (raised as ParseException) is
    # caught by `except AnalysisException`, matching pyspark's `ParseException(AnalysisException)`.
    # MUTATION: revert the native `ParseException` base to `PySparkException` → this pin REDs (the
    # parse error is no longer an AnalysisException, so pytest.raises(AnalysisException) escapes).
    with pytest.raises(AnalysisException):
        spark.sql("SELECT * FROM")
    # Still catchable as ParseException, and still an AnalysisException / PySparkException /
    # RuntimeError all the way up — no broader catch regressed.
    try:
        spark.sql("SELECT * FROM")
    except ParseException as error:
        assert isinstance(error, AnalysisException)
        assert isinstance(error, PySparkException)
        assert isinstance(error, RuntimeError)
    else:
        pytest.fail("a syntax error must raise ParseException")


def test_errors_reexported_with_same_identity() -> None:
    # The facade re-exports the SAME class objects the engine raises — catching by identity works,
    # not by name coincidence. Break this and `except AnalysisException` silently stops catching.
    assert AnalysisException is _native.AnalysisException
    assert ParseException is _native.ParseException
    assert PySparkException is _native.PySparkException
    assert UnsupportedOperationException is _native.UnsupportedOperationException
    assert IllegalArgumentException is _native.IllegalArgumentException
    # The three Python-argument leaves are facade-defined (they need MULTIPLE bases, which
    # `pyo3::create_exception!` cannot express) — nothing in the engine raises them, so there is
    # deliberately NO native twin whose identity could drift.
    for facade_only in (PySparkValueError, PySparkTypeError, PySparkAttributeError):
        assert not hasattr(_native, facade_only.__name__)


# ==================================================================================================
# Entry point: spark.sql


def test_sql_syntax_error_raises_parse_exception(spark: ReparkSession) -> None:
    with pytest.raises(ParseException):
        spark.sql("SELECT * FROM")


def test_sql_unknown_table_raises_analysis_exception(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as raised:
        spark.sql("SELECT * FROM __no_such_table__")
    # Cause chain: the original engine diagnostic (the table name) survives in str(exc).
    assert "__no_such_table__" in str(raised.value)


def test_sql_execution_error_raises_base_exception(spark: ReparkSession) -> None:
    # A runtime cast failure is neither parse nor analysis — it must be the base type (NOT
    # AnalysisException/ParseException), still a RuntimeError. The plan is valid; execution fails.
    # KNOWN DIVERGENCE (parity backlog, F-BR-6): Spark non-ANSI returns NULL here. This pin
    # codifies today's raise-on-bad-CAST behavior, NOT a contract — a future CAST-parity unit
    # UPDATES it (assert NULL). See docs/spark-sql-iceberg-parity.md §7.
    df = spark.sql("SELECT CAST(a AS INT) AS n FROM (VALUES ('abc')) AS t(a)")
    with pytest.raises(PySparkException) as raised:
        df.collect()
    assert not isinstance(raised.value, (AnalysisException, ParseException))
    assert "Cast error" in str(raised.value)


# ==================================================================================================
# Entry point: F.expr


def test_expr_syntax_error_raises_parse_exception(spark: ReparkSession) -> None:
    with pytest.raises(ParseException):
        F.expr("1 +")


def test_expr_unresolved_column_raises_analysis_exception(spark: ReparkSession) -> None:
    # A column-referencing expr has no schema to bind to (F.expr plans against an empty schema);
    # PySpark raises AnalysisException here, and repark now matches (was ValueError pre-WG-3).
    with pytest.raises(AnalysisException):
        F.expr("a + 1")


# ==================================================================================================
# Entry point: DataFrame ops


def test_dataframe_filter_string_syntax_error_raises_parse_exception(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a")
    with pytest.raises(ParseException):
        df.filter("a +")  # filter(str) parses a SQL predicate — an incomplete one is a syntax error


def test_dataframe_select_unknown_column_raises_analysis_exception(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a")
    with pytest.raises(AnalysisException):
        df.select("__no_such_column__")


def test_dataframe_collect_execution_error_raises_base_exception(spark: ReparkSession) -> None:
    # A doomed cast built via DataFrame ops (not raw SQL), then executed: an execution error is the
    # base type, never analysis/parse.
    # KNOWN DIVERGENCE (parity backlog, F-BR-6): the DataFrame-ops twin of the CAST divergence;
    # Spark non-ANSI returns NULL. Update, don't obey, when CAST parity lands (parity doc §7).
    df = spark.sql("SELECT 'abc' AS v").select(F.col("v").cast("int"))
    with pytest.raises(PySparkException) as raised:
        df.collect()
    assert not isinstance(raised.value, (AnalysisException, ParseException))


# ==================================================================================================
# U4 (audit CQ-002/CQ-015, OTH-009): scope gates → UnsupportedOperationException; iceberg kinds
# classified. Each case drives the public `spark.sql` surface end to end.


def test_merge_partitioned_target_gate_retired_now_runs(
    spark_with_catalog: ReparkSession,
) -> None:
    # A4 gate RETIREMENT (was U4-P1). `MERGE INTO` an IDENTITY-partitioned table used to raise the
    # UnsupportedOperationException scope gate; it now RUNS — both arms route through the A1/U1
    # fanout. This pins the retirement at the facade: the exact statement that used to raise must
    # now commit and round-trip, and must NOT raise the scope gate. (The
    # UnsupportedOperationException taxonomy stays covered by the MoR-mode gate below +
    # test_runtime_error_still_catches...; the positive value-AND-type e2e lives in
    # test_catalog_flow.py::test_merge_partitioned_by_end_to_end.)
    spark = spark_with_catalog
    spark.sql(
        "CREATE TABLE mem.silver.part_t USING iceberg PARTITIONED BY (id) "
        "AS SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'"
    )
    spark.sql("SELECT 2 AS id, 'bee' AS name UNION ALL SELECT 3, 'c'").createOrReplaceTempView(
        "updates"
    )
    # No raise: the partitioned MERGE commits (the former gate is gone).
    spark.sql(
        "MERGE INTO mem.silver.part_t AS t USING updates AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET name = s.name "
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
    )
    rows = spark.sql("SELECT id, name FROM mem.silver.part_t ORDER BY id").to_arrow().to_pylist()
    assert rows == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "bee"},
        {"id": 3, "name": "c"},
    ]


def test_merge_mor_mode_gate_raises_unsupported_operation_exception(
    spark_with_catalog: ReparkSession,
) -> None:
    # U4-P2 — the second NotImplemented surface: the `write.merge.mode` gate. Risk: only one gate
    # rerouted (a special-cased fix) instead of the whole NotImplemented class.
    # Group T narrowed this gate (merge-on-read MERGE started RUNNING, leaving transform
    # partitioning as the probe); Group Y removed the transform limit too, so the probe moved once
    # more — to the UNRECOGNISED mode value, which is a permanent `NotImplemented` on the same
    # property rather than a scope boundary that a later group will retire.
    spark = spark_with_catalog
    spark.sql(
        "CREATE TABLE mem.silver.mor_t USING iceberg "
        "TBLPROPERTIES ('write.merge.mode' = 'merge-on-write') "
        "AS SELECT 1 AS id, 'a' AS name"
    )
    spark.sql("SELECT 2 AS id, 'b' AS name").createOrReplaceTempView("updates")
    with pytest.raises(UnsupportedOperationException) as raised:
        spark.sql(
            "MERGE INTO mem.silver.mor_t AS t USING updates AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET name = s.name"
        )
    assert "write.merge.mode" in str(raised.value)


def test_create_namespace_duplicate_raises_analysis_exception(
    spark_with_catalog: ReparkSession,
) -> None:
    # U4-P5 — an iceberg-origin error through the External route, end to end: a duplicate
    # CREATE NAMESPACE (no IF NOT EXISTS) carries iceberg's NamespaceAlreadyExists kind. Spark
    # raises NamespaceAlreadyExistsException, an AnalysisException subclass — pre-U4 repark raised
    # the bare base type here. The kind must be visible in str(exc) (the cause chain).
    spark = spark_with_catalog
    with pytest.raises(AnalysisException) as raised:
        spark.sql("CREATE NAMESPACE mem.silver")  # the fixture already created it
    assert "NamespaceAlreadyExists" in str(raised.value)


def test_drop_missing_table_raises_analysis_exception(
    spark_with_catalog: ReparkSession,
) -> None:
    # U4-P6 — DROP TABLE on a missing table (no IF EXISTS) carries iceberg's TableNotFound kind.
    # Spark raises NoSuchTableException, an AnalysisException subclass. Risk: catalog-shaped
    # errors staying in the base bucket, so `except AnalysisException` misses them post-migration.
    spark = spark_with_catalog
    with pytest.raises(AnalysisException) as raised:
        spark.sql("DROP TABLE mem.silver.__never_created__")
    assert "TableNotFound" in str(raised.value)


# ==================================================================================================
# Near-drop-in compatibility: the whole reason the typed exceptions subclass RuntimeError


def test_runtime_error_still_catches_the_typed_exceptions(
    spark_with_catalog: ReparkSession,
) -> None:
    # Pre-migration `except RuntimeError` on engine failures must keep working after the taxonomy.
    spark = spark_with_catalog
    with pytest.raises(RuntimeError):
        spark.sql("SELECT * FROM __no_such_table__")  # AnalysisException IS-A RuntimeError
    with pytest.raises(RuntimeError):
        spark.sql("SELECT * FROM")  # ParseException IS-A RuntimeError
    # U4: UnsupportedOperationException IS-A RuntimeError — the `write.merge.mode` gate as the
    # probe. Group Y moved it from "merge-on-read + a non-identity transform" (now supported) to an
    # UNRECOGNISED mode value, which stays a `NotImplemented` permanently. Setup (CTAS + temp view)
    # runs OUTSIDE the raises block so only the gate can satisfy it.
    spark.sql(
        "CREATE TABLE mem.silver.rt_t USING iceberg "
        "TBLPROPERTIES ('write.merge.mode' = 'merge-on-write') "
        "AS SELECT 1 AS id"
    )
    spark.sql("SELECT 1 AS id").createOrReplaceTempView("rt_updates")
    with pytest.raises(RuntimeError):
        spark.sql(
            "MERGE INTO mem.silver.rt_t AS t USING rt_updates AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET id = s.id"
        )


# ==================================================================================================
# GROUP X — the reachable PySpark exception leaf types.
#
# Method: a LIVE pyspark 4.0.0 oracle (JVM local[1], ANSI on — the shipped
# `iBergSpark/.venv` install) was probed for the class PySpark raises at each repark raise site;
# only types with >=1 reachable repark raise were wired (the Group S no-stubs rule). The oracle
# results that drive the pins below:
#
#   spark.conf invalid value          -> IllegalArgumentException  ("'-1' in
#                                        spark.sql.shuffle.partitions is invalid")
#   df.select(123) / df.filter(123)   -> PySparkTypeError(PySparkException, TypeError)
#   df.sort() / df.dropna(how=…)      -> PySparkValueError(PySparkException, ValueError)
#   df.nosuchattr                     -> PySparkAttributeError(PySparkException, AttributeError)


def test_invalid_catalog_config_raises_illegal_argument_exception() -> None:
    # ENGINE path: `repark_core::Error::Config` (an unmappable `spark.sql.catalog.<name>.*` block)
    # now classifies to IllegalArgumentException — what live pyspark 4.0.0 raises for an invalid
    # SQLConf value. Before Group X this landed in the base `PySparkException` bucket, so a
    # migrated `except IllegalArgumentException` silently missed it (the Group S failure mode).
    # MUTATION: route `Error::Config` back to `ErrorClass::Base` in repark-core → this REDs
    # (needs a maturin rebuild between mutation and test — the class identity is compiled in).
    with pytest.raises(IllegalArgumentException) as raised:
        ReparkSession.builder.appName("x-cfg").config(
            "spark.sql.catalog.badcat.type", "nosuchtype"
        ).getOrCreate()
    # Cause chain: the offending key AND value survive verbatim in str(exc).
    assert "spark.sql.catalog.badcat.type" in str(raised.value)
    assert "nosuchtype" in str(raised.value)
    # Parent catch-compat, and NOT the classes it must not be confused with.
    assert isinstance(raised.value, PySparkException)
    assert isinstance(raised.value, RuntimeError)
    assert not isinstance(raised.value, (AnalysisException, ParseException))
    assert not isinstance(raised.value, UnsupportedOperationException)


def test_engine_and_facade_config_errors_share_one_class() -> None:
    # The SAME misuse class reached two ways must raise the SAME type: the engine fold
    # (`Error::Config`, an empty catalog name) and the facade's own int-config parser
    # (`_lookup_int`). A split here is how a taxonomy rots — one path typed, the other left a
    # bare ValueError.
    with pytest.raises(IllegalArgumentException):
        ReparkSession.builder.appName("x-cfg-engine").config(
            "spark.sql.catalog..warehouse", "/tmp/x"
        ).getOrCreate()
    with pytest.raises(IllegalArgumentException):
        ReparkSession.builder.appName("x-cfg-facade").config(
            "repark.batch.size", "not-an-int"
        ).getOrCreate()


def test_python_type_misuse_raises_pyspark_type_error(spark: ReparkSession) -> None:
    # A wrong-typed argument to a facade method is PySpark's PySparkTypeError. The payoff: a
    # migrated `except PySparkException` (or `except PySparkTypeError`) now catches it — before
    # Group X repark raised a bare TypeError, which PySparkException never caught.
    # MUTATION: revert `raise PySparkTypeError` to `raise TypeError` in dataframe.py → RED.
    df = spark.sql("SELECT 1 AS a")
    for misuse in (lambda: df.select(123), lambda: df.filter(123), lambda: df.drop(123)):
        with pytest.raises(PySparkTypeError) as raised:
            misuse()
        # BOTH PySpark parents, so old `except TypeError` code AND migrated
        # `except PySparkException` code work.
        assert isinstance(raised.value, TypeError)
        assert isinstance(raised.value, PySparkException)
    # The functions surface too (not just DataFrame) — one entry point is not the claim.
    with pytest.raises(PySparkTypeError) as raised:
        F.sum(123)
    assert isinstance(raised.value, TypeError)
    assert isinstance(raised.value, PySparkException)


def test_python_value_misuse_raises_pyspark_value_error(spark: ReparkSession) -> None:
    # A bad VALUE (right type) is PySpark's PySparkValueError. Oracle: `df.sort()` ->
    # [CANNOT_BE_EMPTY], `df.dropna(how="bogus")` -> [VALUE_NOT_ANY_OR_ALL].
    # MUTATION: revert `raise PySparkValueError` to `raise ValueError` in dataframe.py → RED.
    df = spark.sql("SELECT 1 AS a")
    for misuse in (lambda: df.sort(), lambda: df.dropna(how="bogus")):
        with pytest.raises(PySparkValueError) as raised:
            misuse()
        assert isinstance(raised.value, ValueError)
        assert isinstance(raised.value, PySparkException)
    # The session surface too.
    with pytest.raises(PySparkValueError) as raised:
        spark.createDataFrame([])
    assert isinstance(raised.value, ValueError)
    assert isinstance(raised.value, PySparkException)


def test_unknown_column_attribute_raises_pyspark_attribute_error(spark: ReparkSession) -> None:
    # `df.nosuchattr` is PySpark's PySparkAttributeError. repark already emitted PySpark's exact
    # `[ATTRIBUTE_NOT_SUPPORTED]` message here — Group X gives it PySpark's CLASS too, so the
    # message was never the only oracle.
    # MUTATION: revert `raise PySparkAttributeError` to `raise AttributeError` → RED.
    df = spark.sql("SELECT 1 AS a")
    with pytest.raises(PySparkAttributeError) as raised:
        df.__no_such_attribute__  # noqa: B018 — attribute access IS the behavior under test
    assert "ATTRIBUTE_NOT_SUPPORTED" in str(raised.value)
    assert isinstance(raised.value, AttributeError)
    assert isinstance(raised.value, PySparkException)
    # `hasattr` must still work (it swallows AttributeError) — the widening must not break the
    # Python attribute protocol.
    assert not hasattr(df, "__no_such_attribute__")


def test_row_missing_key_and_bad_index_raise_pyspark_value_error() -> None:
    """G-ROW / E8 residual (Group X deferred): live PySpark 4.1.2 Row error classes.

    Oracle (2026-07-27, zulu-17):

    * ``Row(a=1)["zz"]`` → ``PySparkValueError`` (NOT ``KeyError``) — Group X archive
      named this a deliberate deferral; G-ROW closes it with the existing leaf.
    * ``Row(a=1)[object()]`` / ``[1.5]`` → ``PySparkValueError`` (NOT ``PySparkTypeError``)
      — same ``__fields__.index`` funnel as live ``pyspark.sql.types.Row``.
    * ``Row(a=1).missing`` → ``PySparkAttributeError`` ``[ATTRIBUTE_NOT_SUPPORTED]``.
    * ``Row(1, a=2)`` → ``PySparkValueError`` ``[CANNOT_SET_TOGETHER]``.

    No new exception leaves — reuses Group S/X ``errors.py`` types only. ``PySparkKeyError``
    stays deferred (malformed-Row IndexError branch unreachable while fields/values lock-step).
    """
    from repark.row import Row

    row = Row(a=1, b=2)
    with pytest.raises(PySparkValueError) as missing:
        _ = row["zz"]
    assert isinstance(missing.value, ValueError)
    assert isinstance(missing.value, PySparkException)
    assert not isinstance(missing.value, KeyError)

    with pytest.raises(PySparkValueError) as bad_type:
        _ = row[object()]  # type: ignore[index]
    assert isinstance(bad_type.value, ValueError)
    assert isinstance(bad_type.value, PySparkException)
    assert not isinstance(bad_type.value, TypeError)

    with pytest.raises(PySparkAttributeError, match=r"ATTRIBUTE_NOT_SUPPORTED") as attr:
        _ = row.missing  # type: ignore[attr-defined]
    assert isinstance(attr.value, AttributeError)
    assert isinstance(attr.value, PySparkException)

    with pytest.raises(PySparkValueError, match=r"CANNOT_SET_TOGETHER"):
        Row(1, a=2)  # type: ignore[misc]


def test_python_arg_errors_runtime_error_divergence_is_deliberate(spark: ReparkSession) -> None:
    # KNOWN DIVERGENCE, pinned so it is visible rather than accidental. In `pyspark.errors` the
    # three Python-argument wrappers are NOT RuntimeErrors (PySparkException subclasses Exception
    # there). In repark they are, because repark's PySparkException subclasses RuntimeError — the
    # deliberate near-drop-in decision that keeps `except RuntimeError` catching engine errors.
    # The consequence is a strict SUPERSET: everything PySpark catches, repark catches too; a
    # broad `except RuntimeError` additionally catches a facade arg error. Documented in
    # repark/errors.py. A future unit that decouples PySparkException from RuntimeError UPDATES
    # this pin — it is a record of today's shape, not a contract.
    df = spark.sql("SELECT 1 AS a")
    with pytest.raises(RuntimeError):
        df.select(123)
    assert issubclass(PySparkTypeError, RuntimeError)
    assert issubclass(PySparkValueError, RuntimeError)
    assert issubclass(PySparkAttributeError, RuntimeError)
    # The builtin bases are still exactly PySpark's, and the wrappers stay distinct from each
    # other (a single collapsed class would satisfy the isinstance checks above).
    assert issubclass(PySparkTypeError, TypeError)
    assert not issubclass(PySparkTypeError, ValueError)
    assert issubclass(PySparkValueError, ValueError)
    assert not issubclass(PySparkValueError, TypeError)
    assert issubclass(PySparkAttributeError, AttributeError)
    assert not issubclass(PySparkAttributeError, TypeError)
