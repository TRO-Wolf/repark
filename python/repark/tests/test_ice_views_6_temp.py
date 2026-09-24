"""IPI-40 views PR6a1 battery — SQL ``CREATE [OR REPLACE] TEMPORARY VIEW``.

A SQL temporary view is session-local and LOGICAL: it re-plans on every read,
shadows a catalog view of the same name for bare references, and never lands in
a catalog. Every expected value is Spark 4.1.2 + Iceberg 1.11.0 on the
``InMemoryCatalog`` ``sc`` (the ``D-TEMP-VIEW`` cell, packet facts A-8 and M-11,
and the PR6a1 probe for the refusal shapes the cell does not reach).

Spark's legacy parse refusals carry an internal ``_LEGACY_ERROR_TEMP_*``
condition and no SQLSTATE; RePark's native exceptions report no condition for a
message without a bracketed class, so those pins assert ``None`` as the
ALTER VIEW battery does. ``CREATE GLOBAL TEMPORARY VIEW`` succeeds on Spark and
refuses here by ruling: RePark has no cross-session ``global_temp`` schema.

pins: ice-views-1/C-018
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException, UnsupportedOperationException

UNSTRUCTURED_CONDITION: None = None
UNSTRUCTURED_SQLSTATE: None = None

CELL_ROWS = [[0, "d0", "a"], [1, "d1", "b"], [2, "d2", "a"]]

CELL_SCHEMA = [
    ("id", pa.int64(), True),
    ("data", pa.string(), True),
    ("cat", pa.string(), True),
]

TEMP_VIEW_V_EXISTS = (
    "[TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create the temporary view `v` because it "
    "already exists.\nChoose a different name, drop or replace the existing view. "
    "SQLSTATE: 42P07"
)

GLOBAL_TEMP_VIEW_REFUSAL = (
    "CREATE GLOBAL TEMPORARY VIEW is not supported: RePark has no cross-session "
    "`global_temp` schema, so a global temporary view is neither a durable catalog view "
    "nor a session-local temporary view. Use CREATE TEMPORARY VIEW for a session-local "
    "view or CREATE VIEW <catalog>.<namespace>.<view> for a durable one"
)


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog ``sc`` with namespace ``ns`` and the cell's table ``t``."""
    session = ReparkSession.builder.appName("pytest-ice-views-6-temp").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.t (id BIGINT, data STRING, cat STRING) USING iceberg")
    session.sql("INSERT INTO sc.ns.t VALUES (0, 'd0', 'a'), (1, 'd1', 'b'), (2, 'd2', 'a')")
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame to plain nested lists for golden comparison."""
    return [list(row) for row in frame.collect()]


def _schema(frame: Any) -> list[tuple[str, pa.DataType, bool]]:
    """Name, Arrow type and nullability of every column a frame answers."""
    return [(field.name, field.type, field.nullable) for field in frame.to_arrow().schema]


def _parse_refusal(spark: ReparkSession, statement: str) -> ParseException:
    """Run a statement that must refuse as a ParseException and return it."""
    with pytest.raises(ParseException) as caught:
        spark.sql(statement)
    assert type(caught.value) is ParseException
    return caught.value


def test_temp_view_from_sql(spark: ReparkSession) -> None:
    """D-TEMP-VIEW — the cell's rows and schema. pins: ice-views-1/C-018."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT * FROM sc.ns.t")
    frame = spark.sql("SELECT * FROM v ORDER BY id")
    assert _schema(frame) == CELL_SCHEMA
    assert _rows(frame) == CELL_ROWS


def test_temp_view_replans_after_insert(spark: ReparkSession) -> None:
    """A-8 — a later INSERT into the base table is visible through the view."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT * FROM sc.ns.t")
    spark.sql("INSERT INTO sc.ns.t VALUES (3, 'd3', 'c')")
    frame = spark.sql("SELECT * FROM v ORDER BY id")
    assert _schema(frame) == CELL_SCHEMA
    assert _rows(frame) == [*CELL_ROWS, [3, "d3", "c"]]


def test_plain_create_temp_view_serves(spark: ReparkSession) -> None:
    """A plain CREATE TEMPORARY VIEW on a fresh name serves its body."""
    spark.sql("CREATE TEMPORARY VIEW v AS SELECT * FROM sc.ns.t")
    assert _rows(spark.sql("SELECT * FROM v ORDER BY id")) == CELL_ROWS


def test_or_replace_temp_view_replaces(spark: ReparkSession) -> None:
    """OR REPLACE swaps the body of an existing temp view."""
    spark.sql("CREATE TEMPORARY VIEW v AS SELECT * FROM sc.ns.t")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT data FROM sc.ns.t WHERE id = 1")
    frame = spark.sql("SELECT * FROM v")
    assert _schema(frame) == [("data", pa.string(), True)]
    assert _rows(frame) == [["d1"]]


@pytest.mark.parametrize("keyword", ["TEMPORARY", "TEMP"])
def test_create_temp_view_on_existing_name_refuses(spark: ReparkSession, keyword: str) -> None:
    """A plain CREATE on an existing temp name refuses and keeps the old body."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT * FROM sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"CREATE {keyword} VIEW v AS SELECT 1 AS id")
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == f"Error during planning: {TEMP_VIEW_V_EXISTS}"
    assert caught.value.getCondition() == "TEMP_TABLE_OR_VIEW_ALREADY_EXISTS"
    assert caught.value.getSqlState() == "42P07"
    assert _rows(spark.sql("SELECT * FROM v ORDER BY id")) == CELL_ROWS


def test_temp_view_if_not_exists_refuses(spark: ReparkSession) -> None:
    """Spark refuses IF NOT EXISTS on a temporary view, existing or not."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT * FROM sc.ns.t")
    for statement in (
        "CREATE TEMPORARY VIEW IF NOT EXISTS v AS SELECT 1 AS id",
        "CREATE TEMPORARY VIEW IF NOT EXISTS vn AS SELECT 7 AS id",
    ):
        refusal = _parse_refusal(spark, statement)
        assert str(refusal) == (
            'SQL error: ParserError("It is not allowed to define a TEMPORARY view with '
            'IF NOT EXISTS.")'
        )
        assert refusal.getCondition() == UNSTRUCTURED_CONDITION
        assert refusal.getSqlState() == UNSTRUCTURED_SQLSTATE
    assert _rows(spark.sql("SELECT * FROM v ORDER BY id")) == CELL_ROWS


def test_or_replace_temp_view_if_not_exists_refuses(spark: ReparkSession) -> None:
    """OR REPLACE with IF NOT EXISTS is Spark's legacy parse refusal."""
    refusal = _parse_refusal(
        spark, "CREATE OR REPLACE TEMPORARY VIEW IF NOT EXISTS v AS SELECT 1 AS id"
    )
    assert str(refusal) == (
        'SQL error: ParserError("CREATE VIEW with both IF NOT EXISTS and REPLACE is not '
        'allowed.")'
    )
    assert refusal.getCondition() == UNSTRUCTURED_CONDITION
    assert refusal.getSqlState() == UNSTRUCTURED_SQLSTATE


def test_temp_view_tblproperties_refuses(spark: ReparkSession) -> None:
    """TBLPROPERTIES cannot coexist with CREATE TEMPORARY VIEW."""
    refusal = _parse_refusal(
        spark, "CREATE TEMPORARY VIEW vp TBLPROPERTIES ('k'='v') AS SELECT 1 AS id"
    )
    assert str(refusal) == (
        "SQL error: ParserError(\"Operation not allowed: TBLPROPERTIES can't coexist with "
        'CREATE TEMPORARY VIEW.")'
    )
    assert refusal.getCondition() == UNSTRUCTURED_CONDITION
    assert refusal.getSqlState() == UNSTRUCTURED_SQLSTATE


def test_temp_view_comment_is_accepted(spark: ReparkSession) -> None:
    """A view COMMENT parses and the view serves its body."""
    spark.sql("CREATE TEMPORARY VIEW vc COMMENT 'doc' AS SELECT id FROM sc.ns.t WHERE id = 1")
    frame = spark.sql("SELECT * FROM vc")
    assert _schema(frame) == [("id", pa.int64(), True)]
    assert _rows(frame) == [[1]]


def test_temp_view_column_aliases_rename(spark: ReparkSession) -> None:
    """Column aliases, with or without a column COMMENT, rename the output."""
    spark.sql("CREATE TEMPORARY VIEW va (i, d) AS SELECT id, data FROM sc.ns.t WHERE id < 2")
    frame = spark.sql("SELECT * FROM va ORDER BY i")
    assert _schema(frame) == [("i", pa.int64(), True), ("d", pa.string(), True)]
    assert _rows(frame) == [[0, "d0"], [1, "d1"]]
    spark.sql("CREATE TEMPORARY VIEW vac (i COMMENT 'the id') AS SELECT id FROM sc.ns.t WHERE id < 1")
    frame = spark.sql("SELECT * FROM vac")
    assert _schema(frame) == [("i", pa.int64(), True)]
    assert _rows(frame) == [[0]]


def test_temp_view_alias_arity_mismatch_refuses(spark: ReparkSession) -> None:
    """Fewer aliases than body columns is Spark's arity refusal."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("CREATE TEMPORARY VIEW vbad (i) AS SELECT id, data FROM sc.ns.t")
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: [CREATE_VIEW_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] "
        "Cannot create view `vbad`, the reason is too many data columns:\n"
        "View columns: `i`.\nData columns: `id`, `data`. SQLSTATE: 21S01"
    )
    assert caught.value.getCondition() == "CREATE_VIEW_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS"
    assert caught.value.getSqlState() == "21S01"


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TEMPORARY VIEW a.b AS SELECT 1 AS id",
        "CREATE OR REPLACE TEMPORARY VIEW a.b AS SELECT 1 AS id",
    ],
)
def test_two_part_temp_view_name_refuses(spark: ReparkSession, statement: str) -> None:
    """TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS for a qualified temp view name."""
    refusal = _parse_refusal(spark, statement)
    assert str(refusal) == (
        'SQL error: ParserError("[TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS] CREATE TEMPORARY VIEW '
        "or the corresponding Dataset APIs only accept single-part view names, but got: "
        '`a`.`b`. SQLSTATE: 428EK")'
    )
    assert refusal.getCondition() == "TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS"
    assert refusal.getSqlState() == "428EK"


def test_three_part_temp_view_name_refuses(spark: ReparkSession) -> None:
    """IDENTIFIER_TOO_MANY_NAME_PARTS for a three-part temp view name."""
    refusal = _parse_refusal(spark, "CREATE TEMPORARY VIEW sc.ns.q AS SELECT 1 AS id")
    assert str(refusal) == (
        'SQL error: ParserError("[IDENTIFIER_TOO_MANY_NAME_PARTS] `sc`.`ns`.`q` is not a '
        'valid identifier as it has more than 2 name parts. SQLSTATE: 42601")'
    )
    assert refusal.getCondition() == "IDENTIFIER_TOO_MANY_NAME_PARTS"
    assert refusal.getSqlState() == "42601"


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE GLOBAL TEMPORARY VIEW g AS SELECT 1 AS id",
        "CREATE OR REPLACE GLOBAL TEMPORARY VIEW g2 AS SELECT 1 AS id",
    ],
)
def test_global_temp_view_refuses(spark: ReparkSession, statement: str) -> None:
    """GLOBAL TEMPORARY VIEW refuses loud by ruling (Spark accepts it)."""
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(statement)
    assert type(caught.value) is UnsupportedOperationException
    assert str(caught.value) == f"This feature is not implemented: {GLOBAL_TEMP_VIEW_REFUSAL}"
    assert caught.value.getCondition() == UNSTRUCTURED_CONDITION
    assert caught.value.getSqlState() == UNSTRUCTURED_SQLSTATE


def test_temp_view_non_query_body_refuses(spark: ReparkSession) -> None:
    """A body that is not a query is Spark's syntax error, and nothing is written."""
    refusal = _parse_refusal(
        spark, "CREATE TEMPORARY VIEW vx AS INSERT INTO sc.ns.t VALUES (9, 'x', 'y')"
    )
    assert str(refusal) == (
        "SQL error: ParserError(\"[PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. "
        'SQLSTATE: 42601")'
    )
    assert refusal.getCondition() == "PARSE_SYNTAX_ERROR"
    assert refusal.getSqlState() == "42601"
    assert _rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id")) == CELL_ROWS


def test_temp_view_write_body_refuses(spark: ReparkSession) -> None:
    """A WITH ... INSERT body refuses loud and writes nothing (no measured Spark text)."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql(
            "CREATE TEMPORARY VIEW vw AS WITH s AS (SELECT 9 AS id) "
            "INSERT INTO sc.ns.t SELECT id, 'x', 'y' FROM s"
        )
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: a temporary view body must be a query: "
        "`WITH ... INSERT/UPDATE/DELETE/MERGE` is a write statement, not a view definition"
    )
    assert caught.value.getCondition() == UNSTRUCTURED_CONDITION
    assert caught.value.getSqlState() == UNSTRUCTURED_SQLSTATE
    assert _rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id")) == CELL_ROWS


def test_temp_view_shadows_catalog_view(spark: ReparkSession) -> None:
    """M-11 — after USE sc.ns a bare name reads the temp view, qualified names the catalog."""
    spark.sql("CREATE VIEW sc.ns.sv AS SELECT id FROM sc.ns.t WHERE id < 1")
    spark.sql("USE sc.ns")
    assert _rows(spark.sql("SELECT * FROM sv")) == [[0]]
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW sv AS SELECT 99 AS id")
    assert _rows(spark.sql("SELECT * FROM sv")) == [[99]]
    assert _rows(spark.sql("SELECT * FROM ns.sv")) == [[0]]
    assert _rows(spark.sql("SELECT * FROM sc.ns.sv")) == [[0]]


def test_temp_view_body_resolves_through_use(spark: ReparkSession) -> None:
    """Bare and two-part body names resolve under USE sc.ns."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW vu AS SELECT * FROM t")
    frame = spark.sql("SELECT * FROM vu ORDER BY id")
    assert _schema(frame) == CELL_SCHEMA
    assert _rows(frame) == CELL_ROWS
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW vu2 AS SELECT * FROM ns.t")
    frame = spark.sql("SELECT * FROM vu2 ORDER BY id")
    assert _schema(frame) == CELL_SCHEMA
    assert _rows(frame) == CELL_ROWS


def test_two_part_temp_view_name_after_use_refuses(spark: ReparkSession) -> None:
    """USE does not make a two-part temp view name legal."""
    spark.sql("USE sc.ns")
    refusal = _parse_refusal(spark, "CREATE TEMPORARY VIEW ns.vq AS SELECT 1 AS id")
    assert str(refusal) == (
        'SQL error: ParserError("[TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS] CREATE TEMPORARY VIEW '
        "or the corresponding Dataset APIs only accept single-part view names, but got: "
        '`ns`.`vq`. SQLSTATE: 428EK")'
    )
    assert refusal.getCondition() == "TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS"
    assert refusal.getSqlState() == "428EK"


def test_temp_view_named_like_a_table_shadows_bare_reads(spark: ReparkSession) -> None:
    """A temp view ``t`` wins bare reads and bare body references; ``ns.t`` stays the table."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 5 AS id")
    assert _rows(spark.sql("SELECT * FROM t")) == [[5]]
    assert _rows(spark.sql("SELECT * FROM ns.t ORDER BY id")) == CELL_ROWS
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW vt AS SELECT * FROM t")
    assert _rows(spark.sql("SELECT * FROM vt")) == [[5]]


def test_self_referencing_temp_view_refuses(spark: ReparkSession) -> None:
    """OR REPLACE onto itself is RECURSIVE_VIEW and keeps the old body."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT id FROM sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT * FROM v")
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: [RECURSIVE_VIEW] Recursive view `v` detected "
        "(cycle: `v` -> `v`). SQLSTATE: 42K0H"
    )
    assert caught.value.getCondition() == "RECURSIVE_VIEW"
    assert caught.value.getSqlState() == "42K0H"
    frame = spark.sql("SELECT * FROM v ORDER BY id")
    assert _schema(frame) == [("id", pa.int64(), True)]
    assert _rows(frame) == [[0], [1], [2]]


def test_temp_view_cycle_refuses(spark: ReparkSession) -> None:
    """A two-view cycle is RECURSIVE_VIEW naming the whole path."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW a AS SELECT id FROM sc.ns.t")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW b AS SELECT * FROM a")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW a AS SELECT * FROM b")
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: [RECURSIVE_VIEW] Recursive view `a` detected "
        "(cycle: `a` -> `b` -> `a`). SQLSTATE: 42K0H"
    )
    assert caught.value.getCondition() == "RECURSIVE_VIEW"
    assert caught.value.getSqlState() == "42K0H"
    assert _rows(spark.sql("SELECT * FROM b ORDER BY id")) == [[0], [1], [2]]


def test_dependent_temp_view_reresolves_a_replaced_temp_view(spark: ReparkSession) -> None:
    """A view over a temp view reads its current body, projected to the original columns."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW x AS SELECT 1 AS id")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW y AS SELECT * FROM x")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW x AS SELECT 2 AS id")
    assert _rows(spark.sql("SELECT * FROM y")) == [[2]]
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW x AS SELECT 'z' AS s, 3 AS id")
    frame = spark.sql("SELECT * FROM y")
    assert [field.name for field in frame.to_arrow().schema] == ["id"]
    assert _rows(frame) == [[3]]


def test_catalog_reference_is_not_captured_by_a_later_temp_view(spark: ReparkSession) -> None:
    """A body name that was the catalog table at CREATE stays the table."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW z AS SELECT * FROM t")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 5 AS id")
    frame = spark.sql("SELECT * FROM z ORDER BY id")
    assert _schema(frame) == CELL_SCHEMA
    assert _rows(frame) == CELL_ROWS
