"""Facade tests for ``DESCRIBE NAMESPACE [EXTENDED]`` and its ``DATABASE``/``SCHEMA`` synonyms.

The engine-side row set, ``EXTENDED`` ``Properties`` rendering and redaction are pinned in
``crates/repark-sql/src/lib.rs``; these tests drive the public facade end to end through the
built native module, pinning what a migrated PySpark caller actually sees:

- ``spark.sql(...).to_arrow()`` — column names, Arrow types and nullability, and the row VALUES
  (Z5: value AND type, on the Arrow path, never only ``show``);
- a missing namespace raises :class:`repark.errors.AnalysisException` by class identity (Z4) —
  the class live pyspark 4.0.0 raises (``AnalysisException`` / ``SCHEMA_NOT_FOUND`` / SQLSTATE
  42704);
- ``DESCRIBE <table>`` is not shadowed (Z6).

The output shape is pinned to a live pyspark 4.0.0 **DataSourceV2** oracle, which differs from
the v1 session catalog: v2 emits ``Comment``/``Location``/``Owner`` only when the namespace
metadata carries the key. Divergences are disclosed in ``execute_describe_namespace``'s Rust
doc block.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

CATALOG = "glue_catalog"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with a memory catalog holding one fully-populated namespace and one bare one."""
    session = ReparkSession.builder.appName("pytest-describe-namespace").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(
        f"CREATE NAMESPACE {CATALOG}.described COMMENT 'z full comment' "
        "LOCATION 's3://bucket/z/full' "
        "WITH DBPROPERTIES ('owner' = 'zowner', 'k2' = 'v2', 'k1' = 'v1', 'Amid' = 'vm')"
    )
    session.sql(f"CREATE NAMESPACE {CATALOG}.bare")
    return session


def _rows(spark: ReparkSession, sql: str) -> list[tuple[str, str]]:
    table = spark.sql(sql).to_arrow()
    return list(
        zip(
            table.column("info_name").to_pylist(),
            table.column("info_value").to_pylist(),
            strict=True,
        )
    )


def test_describe_namespace_arrow_schema_and_values(spark: ReparkSession) -> None:
    """Z5/Z1: the Arrow schema is Spark's exact shape and the values come from the namespace."""
    table = spark.sql(f"DESCRIBE NAMESPACE {CATALOG}.described").to_arrow()

    assert table.schema.names == ["info_name", "info_value"]
    assert table.schema.field("info_name").type == pa.string()
    assert table.schema.field("info_value").type == pa.string()
    assert not table.schema.field("info_name").nullable
    assert table.schema.field("info_value").nullable

    assert list(zip(table.column(0).to_pylist(), table.column(1).to_pylist(), strict=True)) == [
        ("Catalog Name", CATALOG),
        ("Namespace Name", "described"),
        ("Comment", "z full comment"),
        ("Location", "s3://bucket/z/full"),
        ("Owner", "zowner"),
    ]


def test_describe_namespace_extended_properties_row(spark: ReparkSession) -> None:
    """Z2: ``EXTENDED`` appends ``Properties`` in Spark's ``((k,v), …)`` form, sorted by key."""
    extended = _rows(spark, f"DESCRIBE NAMESPACE EXTENDED {CATALOG}.described")
    assert extended[-1] == ("Properties", "((Amid,vm), (k1,v1), (k2,v2))")

    plain = _rows(spark, f"DESCRIBE NAMESPACE {CATALOG}.described")
    assert not any(name == "Properties" for name, _ in plain)


def test_describe_namespace_redaction_truth_table(spark: ReparkSession) -> None:
    """Z2 (security): the key-OR-value redaction truth table, at the facade.

    Live pyspark 4.0.0 (v2 catalog). Spark folds ``(?i)secret|password|token|access[.]?key``
    and ``(?i)url`` over the key AND the value, replacing the value on either hit. ``innocent``
    and ``bare`` are the value-only hits a key-only predicate silently misses;
    ``access_key``/``ACCESS-KEY`` are shown by BOTH engines (Spark's separator is ``[.]?``),
    a named inherited gap rather than a repark choice.
    """
    spark.sql(
        f"CREATE NAMESPACE {CATALOG}.creds WITH DBPROPERTIES ("
        "'password' = 'p1', 'SeCrEt' = 'p2', 'my_token_2' = 'p3', 'accesskey' = 'p4', "
        "'access.key' = 'p5', 'ACCESS-KEY' = 'p6', 'plain' = 'p7', 'access_key' = 'p8', "
        "'innocent' = 'my password is hunter2', 'jdbc_url' = 'jdbc://u:pw@h/db', "
        "'urlish' = 'p9', 'valueurl' = 'http://x/URL', 'bare' = 'http://x/URL', "
        "'dashaccess-key' = 'p10')"
    )
    properties = dict(_rows(spark, f"DESCRIBE NAMESPACE EXTENDED {CATALOG}.creds"))["Properties"]
    assert properties == (
        "((ACCESS-KEY,p6), (SeCrEt,*********(redacted)), (access.key,*********(redacted)), "
        "(access_key,p8), (accesskey,*********(redacted)), (bare,*********(redacted)), "
        "(dashaccess-key,p10), (innocent,*********(redacted)), (jdbc_url,*********(redacted)), "
        "(my_token_2,*********(redacted)), (password,*********(redacted)), (plain,p7), "
        "(urlish,*********(redacted)), (valueurl,*********(redacted)))"
    )
    for secret in ["hunter2", "jdbc://u:pw@h/db", "http://x/URL"]:
        assert secret not in properties


def test_describe_namespace_omits_absent_rows(spark: ReparkSession) -> None:
    """Z1: v2 semantics — a row whose property is absent is omitted, not emitted as ``''``."""
    assert _rows(spark, f"DESCRIBE NAMESPACE EXTENDED {CATALOG}.bare") == [
        ("Catalog Name", CATALOG),
        ("Namespace Name", "bare"),
        ("Properties", ""),
    ]


@pytest.mark.parametrize(
    "statement",
    ["DESCRIBE DATABASE", "DESCRIBE SCHEMA", "DESC NAMESPACE", "DESC DATABASE", "DESC SCHEMA"],
)
def test_describe_namespace_synonyms_are_identical(spark: ReparkSession, statement: str) -> None:
    """Z3: every synonym spelling returns the same rows, with and without ``EXTENDED``."""
    assert _rows(spark, f"{statement} {CATALOG}.described") == _rows(
        spark, f"DESCRIBE NAMESPACE {CATALOG}.described"
    )
    assert _rows(spark, f"{statement} EXTENDED {CATALOG}.described") == _rows(
        spark, f"DESCRIBE NAMESPACE EXTENDED {CATALOG}.described"
    )


def test_describe_missing_namespace_raises_analysis_exception(spark: ReparkSession) -> None:
    """Z4: the missing-namespace class identity — live pyspark raises ``AnalysisException``."""
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE NAMESPACE {CATALOG}.no_such_ns")
    assert "[SCHEMA_NOT_FOUND]" in str(excinfo.value)
    assert "no_such_ns" in str(excinfo.value)

    with pytest.raises(AnalysisException):
        spark.sql(f"DESCRIBE NAMESPACE EXTENDED {CATALOG}.no_such_ns")


def test_describe_namespace_show_renders(
    spark: ReparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    """Z5: ``.show()`` renders the frame (the console path migrated jobs use)."""
    spark.sql(f"DESCRIBE NAMESPACE EXTENDED {CATALOG}.described").show(truncate=False)
    captured = capsys.readouterr().out
    assert "info_name" in captured
    assert "Namespace Name" in captured
    assert "((Amid,vm), (k1,v1), (k2,v2))" in captured
    assert "|" in captured


def test_describe_table_is_not_shadowed(spark: ReparkSession) -> None:
    """Z6: ``DESCRIBE <table>`` still describes a table, including one named ``namespace``."""
    spark.sql("SELECT 1 AS a, 'x' AS b").createOrReplaceTempView("namespace")
    spark.sql("SELECT 1 AS a, 'x' AS b").createOrReplaceTempView("database")
    for sql in ["DESCRIBE namespace", "DESC namespace", "DESCRIBE database"]:
        described = spark.sql(sql).to_arrow()
        assert described.schema.names[0] != "info_name", f"{sql} must not route to the namespace"
    assert spark.sql("DESCRIBE namespace").to_arrow().num_rows == 2
