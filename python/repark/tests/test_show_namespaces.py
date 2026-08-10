"""Facade tests for ``SHOW NAMESPACES`` and its ``SCHEMAS``/``DATABASES`` synonyms.

Group AB. The engine-side row rendering, ``LIKE``-pattern truth table and row order are pinned in
``crates/repark-sql/src/lib.rs``; these tests drive the *public* facade end to end through the
built native module, pinning what a migrated PySpark caller actually sees:

- ``spark.sql(...).to_arrow()`` — the column name, its Arrow **type** and nullability, and the row
  VALUES (AB5: value AND type, on the Arrow path, never only ``show``);
- ``.show()`` renders the frame;
- the ``SCHEMAS`` / ``DATABASES`` / ``FROM`` synonyms are identical (AB2);
- ``LIKE`` filters, and is Spark's ``filterPattern`` rather than SQL ``LIKE`` (AB3);
- an unknown catalog raises :class:`repark.errors.AnalysisException` **by class identity** (AB4) —
  the class live pyspark 4.0.0 raises for ``SHOW NAMESPACES IN nosuchcatalog``
  (``AnalysisException`` / condition ``SCHEMA_NOT_FOUND`` / SQLSTATE 42704);
- the two registry-rowed refusals
  ([NS-1](../../../docs/spark-sql-iceberg-parity.md#ns-1--show-namespaces-without-in-from-requires-an-explicit-catalog),
  [NS-2](../../../docs/spark-sql-iceberg-parity.md#ns-2--nested-show-namespaces-in-catalognamespace-is-refused))
  fail LOUD, and no other ``SHOW`` form is shadowed (AB6).

The output shape is pinned to a live pyspark 4.0.0 **DataSourceV2** oracle (2026-07-25) — the
catalog class repark ships, per the Group Z rule.
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
    """A session with a memory catalog holding three namespaces, incl. one needing backticks."""
    session = ReparkSession.builder.appName("pytest-show-namespaces").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {CATALOG}.sales")
    session.sql(f"CREATE NAMESPACE {CATALOG}.marketing")
    session.sql(f"CREATE NAMESPACE {CATALOG}.`ab space`")
    return session


def _names(spark: ReparkSession, sql: str) -> list[str]:
    return sorted(spark.sql(sql).to_arrow().column("namespace").to_pylist())


def test_show_namespaces_arrow_schema_and_values(spark: ReparkSession) -> None:
    """AB1/AB5: the Arrow schema is the live oracle's exact shape and the values are real.

    Oracle schema JSON, verbatim:
    ``{"fields":[{"metadata":{},"name":"namespace","nullable":false,"type":"string"}],
    "type":"struct"}``. The ``ab space`` row proves the values go through Spark's
    ``NamespaceHelper.quoted`` (oracle: a namespace ``my ns`` shows as ``` `my ns` ```).
    """
    table = spark.sql(f"SHOW NAMESPACES IN {CATALOG}").to_arrow()

    assert table.schema.names == ["namespace"]
    assert table.schema.field("namespace").type == pa.string()
    assert not table.schema.field("namespace").nullable
    assert not table.schema.field("namespace").metadata

    assert sorted(table.column("namespace").to_pylist()) == ["`ab space`", "marketing", "sales"]


@pytest.mark.parametrize(
    "statement",
    [
        "SHOW SCHEMAS IN",
        "SHOW DATABASES IN",
        "SHOW NAMESPACES FROM",
        "SHOW SCHEMAS FROM",
        "SHOW DATABASES FROM",
    ],
)
def test_show_namespaces_synonyms_are_identical(spark: ReparkSession, statement: str) -> None:
    """AB2: every synonym spelling returns the identical frame (all oracle-confirmed identical)."""
    assert _names(spark, f"{statement} {CATALOG}") == _names(spark, f"SHOW NAMESPACES IN {CATALOG}")


def test_show_namespaces_like_is_filter_pattern_not_sql_like(spark: ReparkSession) -> None:
    """AB3: ``LIKE`` is Spark's ``StringUtils.filterPattern`` — a case-insensitive, fully-matched
    regex per ``|``-alternative with ``*`` for ``.*``, NOT SQL ``LIKE``.

    The engine-side truth table (33 oracle-captured rows) lives in
    ``show_namespaces_like_truth_table``; these are the user-visible discriminators: full-match
    vs substring, case folding, ``|`` alternation, the SQL wildcards that are literals here, and
    the optional ``LIKE`` keyword.
    """
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE 'sales'") == ["sales"]
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE 'SALES'") == ["sales"]
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE 'ale'") == []
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE '*ale*'") == ["sales"]
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE 'sales|marketing'") == [
        "marketing",
        "sales",
    ]
    # SQL-`LIKE` wildcards are plain literals here (oracle: `al%` and `bet_` match nothing).
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE 'sale%'") == []
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE 'sale_'") == []
    # The pattern sees the QUOTED row, so the backticks are part of the string to match.
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE 'ab space'") == []
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE '*ab space*'") == ["`ab space`"]
    # The `LIKE` keyword itself is optional (oracle: `SHOW NAMESPACES IN cat 'al*'`).
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} 'sale*'") == ["sales"]
    # A non-matching pattern is an EMPTY frame, not an error.
    assert _names(spark, f"SHOW NAMESPACES IN {CATALOG} LIKE 'nope*'") == []


def test_show_namespaces_unknown_catalog_raises_analysis_exception(spark: ReparkSession) -> None:
    """AB4: the class identity — live pyspark raises ``AnalysisException`` (``SCHEMA_NOT_FOUND``).

    repark's message differs (divergence 3: Spark falls back to reading the unknown name as a
    NAMESPACE of the current catalog, repark has no fallback catalog); the CLASS is the pin.
    """
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql("SHOW NAMESPACES IN no_such_catalog")
    assert "unknown catalog `no_such_catalog`" in str(excinfo.value)

    with pytest.raises(AnalysisException):
        spark.sql("SHOW SCHEMAS IN no_such_catalog LIKE '*'")


def test_show_namespaces_disclosed_divergences_fail_loud(spark: ReparkSession) -> None:
    """AB6 pin for registry rows NS-1 and NS-2 — semantics live only there.

    See ``docs/spark-sql-iceberg-parity.md`` §2.4 rows
    [NS-1](../../../docs/spark-sql-iceberg-parity.md#ns-1--show-namespaces-without-in-from-requires-an-explicit-catalog)
    and
    [NS-2](../../../docs/spark-sql-iceberg-parity.md#ns-2--nested-show-namespaces-in-catalognamespace-is-refused).
    """
    with pytest.raises(AnalysisException) as no_catalog:
        spark.sql("SHOW NAMESPACES")
    assert "requires an explicit catalog" in str(no_catalog.value)

    with pytest.raises(AnalysisException) as nested:
        spark.sql(f"SHOW NAMESPACES IN {CATALOG}.sales")
    assert "one-part `IN <catalog>`" in str(nested.value)

    with pytest.raises(AnalysisException) as malformed:
        spark.sql(f"SHOW NAMESPACES IN {CATALOG} GARBAGE")
    assert "could not parse `SHOW NAMESPACES`" in str(malformed.value)


def test_show_namespaces_does_not_shadow_a_relation_named_namespaces(spark: ReparkSession) -> None:
    """AB6: Spark has no ``SHOW <relation>`` form, so a table named ``namespaces`` is untouched.

    The ``SHOW`` head is unambiguous (unlike ``DESCRIBE`` — the Z6 case), and a relation is reached
    through ``SELECT``/``DESCRIBE``, never ``SHOW``.
    """
    spark.sql("SELECT 1 AS a, 'x' AS b").createOrReplaceTempView("namespaces")
    spark.sql("SELECT 1 AS a, 'x' AS b").createOrReplaceTempView("schemas")

    assert spark.sql("SELECT * FROM namespaces").to_arrow().num_rows == 1
    assert spark.sql("DESCRIBE namespaces").to_arrow().num_rows == 2
    assert spark.sql("SELECT * FROM schemas").to_arrow().num_rows == 1


def test_show_namespaces_show_renders(
    spark: ReparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    """AB5: ``.show()`` renders the frame (the console path migrated jobs use)."""
    spark.sql(f"SHOW NAMESPACES IN {CATALOG}").show(truncate=False)
    captured = capsys.readouterr().out
    assert "namespace" in captured
    assert "sales" in captured
    assert "|" in captured
