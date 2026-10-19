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

DESCRIBE_SCHEMA = [
    ("col_name", pa.string(), False),
    ("data_type", pa.string(), False),
    ("comment", pa.string(), True),
]

SHOW_VIEWS_SCHEMA = [
    ("namespace", pa.string(), False),
    ("viewName", pa.string(), False),
    ("isTemporary", pa.bool_(), False),
]

VIEW_NOT_FOUND_TAIL = (
    " cannot be found. Verify the spelling and correctness of the schema and catalog.\n"
    "If you did not qualify the name with a schema, verify the current_schema() output, or "
    "qualify the name with the correct schema and catalog.\n"
    "To tolerate the error on drop use DROP VIEW IF EXISTS. SQLSTATE: 42P01"
)

TABLE_OR_VIEW_NOT_FOUND_TAIL = (
    " cannot be found. Verify the spelling and correctness of the schema and catalog.\n"
    "If you did not qualify the name with a schema, verify the current_schema() output, or "
    "qualify the name with the correct schema and catalog.\n"
    "To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. "
    "SQLSTATE: 42P01"
)

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
        'SQL error: ParserError("CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.")'
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
    spark.sql(
        "CREATE TEMPORARY VIEW vac (i COMMENT 'the id') AS SELECT id FROM sc.ns.t WHERE id < 1"
    )
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
        "[TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS] CREATE TEMPORARY VIEW "
        "or the corresponding Dataset APIs only accept single-part view names, but got: "
        "`a`.`b`. SQLSTATE: 428EK"
    )
    assert refusal.getCondition() == "TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS"
    assert refusal.getSqlState() == "428EK"


def test_three_part_temp_view_name_refuses(spark: ReparkSession) -> None:
    """IDENTIFIER_TOO_MANY_NAME_PARTS for a three-part temp view name."""
    refusal = _parse_refusal(spark, "CREATE TEMPORARY VIEW sc.ns.q AS SELECT 1 AS id")
    assert str(refusal) == (
        "[IDENTIFIER_TOO_MANY_NAME_PARTS] `sc`.`ns`.`q` is not a "
        "valid identifier as it has more than 2 name parts. SQLSTATE: 42601"
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
        "[PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601"
    )
    assert refusal.getCondition() == "PARSE_SYNTAX_ERROR"
    assert refusal.getSqlState() == "42601"
    assert _rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id")) == CELL_ROWS


def test_temp_view_write_body_refuses(spark: ReparkSession) -> None:
    """A WITH ... INSERT body is Spark's syntax error at INSERT, and nothing is written."""
    refusal = _parse_refusal(
        spark,
        "CREATE TEMPORARY VIEW vw AS WITH s AS (SELECT 9 AS id) "
        "INSERT INTO sc.ns.t SELECT id, 'x', 'y' FROM s",
    )
    assert str(refusal) == (
        "[PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601"
    )
    assert refusal.getCondition() == "PARSE_SYNTAX_ERROR"
    assert refusal.getSqlState() == "42601"
    assert _rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id")) == CELL_ROWS


def test_temp_view_shadows_catalog_view(spark: ReparkSession) -> None:
    """M-11 — after USE sc.ns a bare name reads the temp view, qualified names the catalog."""
    spark.sql("CREATE VIEW sc.ns.sv AS SELECT id FROM sc.ns.t WHERE id < 1")
    spark.sql("USE sc.ns")
    assert _rows(spark.sql("SELECT * FROM sv")) == [[0]]
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW sv AS SELECT CAST(99 AS INT) AS id")
    assert _rows(spark.sql("SELECT * FROM sv")) == [[99]]
    assert _rows(spark.sql("SELECT * FROM ns.sv")) == [[0]]
    assert _rows(spark.sql("SELECT * FROM sc.ns.sv")) == [[0]]
    temp_describe = spark.sql("DESCRIBE sv")
    assert _schema(temp_describe) == DESCRIBE_SCHEMA
    assert _rows(temp_describe) == [["id", "int", None]]
    assert _rows(spark.sql("DESCRIBE ns.sv")) == [["id", "bigint", ""]]
    assert _rows(spark.sql("DESCRIBE sc.ns.sv")) == [["id", "bigint", ""]]


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
        "[TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS] CREATE TEMPORARY VIEW "
        "or the corresponding Dataset APIs only accept single-part view names, but got: "
        "`ns`.`vq`. SQLSTATE: 428EK"
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


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        (
            "CREATE TEMPORARY TABLE tt (id INT)",
            "This feature is not implemented: CREATE TEMPORARY TABLE is not supported for "
            "Iceberg tables yet — omit TEMPORARY for a durable catalog table, or use a temp "
            "view (CREATE TEMP VIEW)",
        ),
        (
            "CREATE TEMPORARY TABLE tt AS SELECT 1 AS id",
            "This feature is not implemented: CREATE TEMPORARY TABLE … AS SELECT is not "
            "supported for Iceberg tables yet — omit TEMPORARY for a durable catalog table, or "
            "use a temp view (CREATE TEMP VIEW)",
        ),
    ],
)
def test_temporary_table_near_misses_keep_their_refusals(
    spark: ReparkSession, statement: str, message: str
) -> None:
    """CREATE TEMPORARY TABLE forms still reach the table and CTAS refusals."""
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(statement)
    assert type(caught.value) is UnsupportedOperationException
    assert str(caught.value) == message
    assert caught.value.getCondition() == UNSTRUCTURED_CONDITION
    assert caught.value.getSqlState() == UNSTRUCTURED_SQLSTATE


def test_drop_temporary_function_near_miss_still_runs(spark: ReparkSession) -> None:
    """DROP TEMPORARY FUNCTION IF EXISTS is untouched by the temp view door."""
    assert _rows(spark.sql("DROP TEMPORARY FUNCTION IF EXISTS nofn")) == []


def test_durable_create_view_near_miss_stays_a_catalog_view(spark: ReparkSession) -> None:
    """CREATE VIEW keeps the durable path: SHOW VIEWS lists it as non-temporary."""
    spark.sql("CREATE VIEW sc.ns.dv AS SELECT id FROM sc.ns.t WHERE id > 0")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == [["ns", "dv", False]]
    assert _rows(spark.sql("SELECT * FROM sc.ns.dv ORDER BY id")) == [[1], [2]]


def _analysis_refusal(spark: ReparkSession, statement: str, *, collect: bool = False) -> Any:
    """Run a statement that must refuse as a plain AnalysisException and return it."""
    with pytest.raises(AnalysisException) as caught:
        frame = spark.sql(statement)
        if collect:
            frame.collect()
    assert type(caught.value) is AnalysisException
    return caught.value


def test_drop_temp_view_drops_it_and_a_second_drop_refuses(spark: ReparkSession) -> None:
    """DROP VIEW drops the temp view; again is VIEW_NOT_FOUND; IF EXISTS is a no-op."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW dv AS SELECT 1 AS id")
    spark.sql("DROP VIEW dv")
    assert _rows(spark.sql("SHOW VIEWS")) == []
    refusal = _analysis_refusal(spark, "DROP VIEW dv")
    assert (
        str(refusal)
        == f"Error during planning: [VIEW_NOT_FOUND] The view ns.dv{VIEW_NOT_FOUND_TAIL}"
    )
    assert refusal.getCondition() == "VIEW_NOT_FOUND"
    assert refusal.getSqlState() == "42P01"
    spark.sql("DROP VIEW IF EXISTS dv")
    assert _rows(spark.sql("SHOW VIEWS")) == []


def test_bare_drop_takes_the_temp_view_before_the_catalog_view(spark: ReparkSession) -> None:
    """A bare DROP VIEW removes the shadowing temp view first, then the catalog view."""
    spark.sql("CREATE VIEW sc.ns.sv AS SELECT id FROM sc.ns.t WHERE id < 1")
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW sv AS SELECT CAST(99 AS INT) AS id")
    spark.sql("DROP VIEW sv")
    assert _rows(spark.sql("SELECT * FROM sv")) == [[0]]
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == [["ns", "sv", False]]
    spark.sql("DROP VIEW sv")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == []


def test_qualified_drop_hits_the_catalog_view_and_keeps_the_temp_view(
    spark: ReparkSession,
) -> None:
    """DROP VIEW sc.ns.sv drops the catalog view even when temp sv exists."""
    spark.sql("CREATE VIEW sc.ns.sv AS SELECT id FROM sc.ns.t WHERE id < 1")
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW sv AS SELECT CAST(99 AS INT) AS id")
    spark.sql("DROP VIEW sc.ns.sv")
    assert _rows(spark.sql("SELECT * FROM sv")) == [[99]]
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == [["", "sv", True]]


def test_two_part_drop_ignores_the_temp_view(spark: ReparkSession) -> None:
    """DROP VIEW ns.sv with only a temp sv is the catalog's VIEW_NOT_FOUND."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW sv AS SELECT CAST(99 AS INT) AS id")
    refusal = _analysis_refusal(spark, "DROP VIEW ns.sv")
    assert (
        str(refusal)
        == f"Error during planning: [VIEW_NOT_FOUND] The view ns.sv{VIEW_NOT_FOUND_TAIL}"
    )
    assert refusal.getCondition() == "VIEW_NOT_FOUND"
    assert refusal.getSqlState() == "42P01"
    assert _rows(spark.sql("SELECT * FROM sv")) == [[99]]


def test_qualified_drop_if_exists_near_miss_reaches_the_catalog(spark: ReparkSession) -> None:
    """DROP VIEW IF EXISTS sc.ns.v stays a catalog drop with a same-named temp view."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t WHERE id < 1")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT CAST(99 AS INT) AS id")
    spark.sql("DROP VIEW IF EXISTS sc.ns.v")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == [["", "v", True]]
    assert _rows(spark.sql("SELECT * FROM v")) == [[99]]


@pytest.mark.parametrize("statement", ["DESCRIBE tv", "DESCRIBE TABLE tv", "DESCRIBE EXTENDED tv"])
def test_describe_temp_view_answers_null_comments(spark: ReparkSession, statement: str) -> None:
    """DESCRIBE of a temp view: Spark types and an Arrow NULL comment, EXTENDED adds nothing."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT * FROM sc.ns.t")
    frame = spark.sql(statement)
    assert _schema(frame) == DESCRIBE_SCHEMA
    assert _rows(frame) == [
        ["id", "bigint", None],
        ["data", "string", None],
        ["cat", "string", None],
    ]


def test_describe_temp_view_keeps_alias_comments_only(spark: ReparkSession) -> None:
    """An alias COMMENT shows in DESCRIBE; the view COMMENT does not."""
    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vac (i COMMENT 'the id', d) "
        "AS SELECT id, data FROM sc.ns.t"
    )
    assert _rows(spark.sql("DESCRIBE vac")) == [["i", "bigint", "the id"], ["d", "string", None]]
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW vcm COMMENT 'doc' AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("DESCRIBE vcm")) == [["id", "bigint", None]]


def test_describe_table_near_miss_reaches_the_catalog_view(spark: ReparkSession) -> None:
    """DESCRIBE TABLE sc.ns.v answers the catalog view even with a temp v registered."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t WHERE id < 1")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT CAST(99 AS INT) AS id")
    frame = spark.sql("DESCRIBE TABLE sc.ns.v")
    assert _schema(frame) == DESCRIBE_SCHEMA
    assert _rows(frame) == [["id", "bigint", ""]]


def test_show_views_lists_temp_views_at_session_scope(spark: ReparkSession) -> None:
    """M-1 — SHOW VIEWS lists temp rows; IN sc.ns lists catalog rows then temp rows."""
    assert _rows(spark.sql("SHOW VIEWS")) == []
    spark.sql("CREATE VIEW sc.ns.cv AS SELECT id FROM sc.ns.t WHERE id < 1")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == [["ns", "cv", False]]
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT * FROM sc.ns.t")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW atv AS SELECT id FROM sc.ns.t")
    frame = spark.sql("SHOW VIEWS")
    assert _schema(frame) == SHOW_VIEWS_SCHEMA
    assert _rows(frame) == [["", "atv", True], ["", "tv", True]]
    assert _rows(spark.sql("SHOW VIEWS LIKE 't*'")) == [["", "tv", True]]
    listed = [["ns", "cv", False], ["", "atv", True], ["", "tv", True]]
    frame = spark.sql("SHOW VIEWS IN sc.ns")
    assert _schema(frame) == SHOW_VIEWS_SCHEMA
    assert _rows(frame) == listed
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns LIKE '*v'")) == listed


def test_show_views_after_use_lists_catalog_then_temp_rows(spark: ReparkSession) -> None:
    """After USE sc.ns, SHOW VIEWS and SHOW VIEWS IN ns list the same rows."""
    spark.sql("CREATE VIEW sc.ns.cv AS SELECT id FROM sc.ns.t WHERE id < 1")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT * FROM sc.ns.t")
    spark.sql("USE sc.ns")
    listed = [["ns", "cv", False], ["", "tv", True]]
    assert _rows(spark.sql("SHOW VIEWS")) == listed
    assert _rows(spark.sql("SHOW VIEWS IN ns")) == listed


def test_read_after_a_dropped_temp_dependency_is_table_or_view_not_found(
    spark: ReparkSession,
) -> None:
    """A temp view over a dropped temp view names the missing view; DESCRIBE still answers."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW x AS SELECT CAST(1 AS INT) AS id")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW y AS SELECT * FROM x")
    spark.sql("DROP VIEW x")
    refusal = _analysis_refusal(spark, "SELECT * FROM y", collect=True)
    assert str(refusal) == (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `x`"
        f"{TABLE_OR_VIEW_NOT_FOUND_TAIL}"
    )
    assert refusal.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert refusal.getSqlState() == "42P01"
    assert _rows(spark.sql("DESCRIBE y")) == [["id", "int", None]]


def test_dropped_catalog_column_is_an_incompatible_view_schema_change(
    spark: ReparkSession,
) -> None:
    """A creation column the base table dropped is INCOMPATIBLE_VIEW_SCHEMA_CHANGE."""
    spark.sql("CREATE TABLE sc.ns.t2 (id BIGINT, data STRING) USING iceberg")
    spark.sql("INSERT INTO sc.ns.t2 VALUES (1, 'a')")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW w AS SELECT * FROM sc.ns.t2")
    spark.sql("ALTER TABLE sc.ns.t2 DROP COLUMN data")
    refusal = _analysis_refusal(spark, "SELECT * FROM w", collect=True)
    assert str(refusal) == (
        "Error during planning: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `w` "
        "has an incompatible schema change and column data cannot be resolved. Expected 1 "
        "columns named data but got [].\nPlease try to re-create the view by running: "
        "CREATE OR REPLACE TEMPORARY VIEW. SQLSTATE: 51024"
    )
    assert refusal.getCondition() == "INCOMPATIBLE_VIEW_SCHEMA_CHANGE"
    assert refusal.getSqlState() == "51024"


def test_dropped_dependency_column_is_an_incompatible_view_schema_change(
    spark: ReparkSession,
) -> None:
    """A creation column a replaced temp dependency lost is INCOMPATIBLE_VIEW_SCHEMA_CHANGE."""
    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW x2 AS SELECT CAST(1 AS INT) AS id, CAST(2 AS INT) AS k"
    )
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW y2 AS SELECT * FROM x2")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW x2 AS SELECT CAST(1 AS INT) AS id")
    refusal = _analysis_refusal(spark, "SELECT * FROM y2", collect=True)
    assert str(refusal) == (
        "Error during planning: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `y2` "
        "has an incompatible schema change and column k cannot be resolved. Expected 1 "
        "columns named k but got [].\nPlease try to re-create the view by running: "
        "CREATE OR REPLACE TEMPORARY VIEW. SQLSTATE: 51024"
    )
    assert refusal.getCondition() == "INCOMPATIBLE_VIEW_SCHEMA_CHANGE"
    assert refusal.getSqlState() == "51024"


@pytest.mark.parametrize(
    ("replacement", "source"),
    [("'a'", "STRING"), ("CAST(7 AS BIGINT)", "BIGINT")],
)
def test_retyped_dependency_column_cannot_up_cast(
    spark: ReparkSession, replacement: str, source: str
) -> None:
    """A creation column retyped to a non-up-castable type is CANNOT_UP_CAST_DATATYPE."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW x3 AS SELECT CAST(1 AS INT) AS id")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW y3 AS SELECT * FROM x3")
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW x3 AS SELECT {replacement} AS id")
    refusal = _analysis_refusal(spark, "SELECT * FROM y3", collect=True)
    assert str(refusal) == (
        f'Error during planning: [CANNOT_UP_CAST_DATATYPE] Cannot up cast x3.id from "{source}" '
        'to "INT".\nThe type path of the target object is:\n\nYou can either add an explicit '
        "cast to the input data or choose a higher precision type of the field in the target "
        "object SQLSTATE: 42846"
    )
    assert refusal.getCondition() == "CANNOT_UP_CAST_DATATYPE"
    assert refusal.getSqlState() == "42846"


def test_narrower_dependency_column_up_casts(spark: ReparkSession) -> None:
    """A creation column narrowed to SMALLINT up-casts back to INT."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW x3 AS SELECT CAST(1 AS INT) AS id")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW y3 AS SELECT * FROM x3")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW x3 AS SELECT CAST(7 AS SMALLINT) AS id")
    frame = spark.sql("SELECT * FROM y3")
    assert [(field.name, field.type) for field in frame.to_arrow().schema] == [("id", pa.int32())]
    assert _rows(frame) == [[7]]


def _assert_catalog_t_missing(spark: ReparkSession) -> None:
    """``sc.ns.t`` was dropped from the catalog."""
    assert spark.catalog.tableExists("sc.ns.t") is False


def test_bare_drop_table_drops_the_temp_view(spark: ReparkSession) -> None:
    """DROP TABLE on a bare temp view name drops the temp view (p9, droptable.run)."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW dt AS SELECT 1 AS id")
    spark.sql("DROP TABLE dt")
    assert _rows(spark.sql("SHOW VIEWS")) == []
    assert "dt" not in spark.list_temp_view_names()


@pytest.mark.parametrize(
    "statement", ["DROP TABLE t", "DROP TABLE t PURGE", "DROP TABLE IF EXISTS t"]
)
def test_bare_drop_table_keeps_a_same_named_catalog_table(
    spark: ReparkSession, statement: str
) -> None:
    """The data-loss pin: a bare DROP TABLE drops the shadowing temp view, never the table."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 5 AS id")
    spark.sql(statement)
    assert _rows(spark.sql("SHOW VIEWS")) == []
    assert _rows(spark.sql("SELECT * FROM t ORDER BY id")) == CELL_ROWS
    assert _rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id")) == CELL_ROWS


def test_bare_drop_table_drops_a_dataframe_temp_view_and_keeps_the_table(
    spark: ReparkSession,
) -> None:
    """A createOrReplaceTempView view shadowing the table is dropped, the table is kept."""
    spark.sql("USE sc.ns")
    spark.sql("SELECT 7 AS id").createOrReplaceTempView("t")
    assert _rows(spark.sql("SELECT * FROM t")) == [[7]]
    spark.sql("DROP TABLE t")
    assert _rows(spark.sql("SHOW VIEWS")) == []
    assert _rows(spark.sql("SELECT * FROM t ORDER BY id")) == CELL_ROWS
    assert _rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id")) == CELL_ROWS


def test_drop_table_if_exists_drops_a_temp_only_name(spark: ReparkSession) -> None:
    """DROP TABLE IF EXISTS on a temp-only name drops it; a missing name is a no-op."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tonly AS SELECT 5 AS id")
    spark.sql("DROP TABLE IF EXISTS tonly")
    assert _rows(spark.sql("SHOW VIEWS")) == []
    spark.sql("DROP TABLE IF EXISTS nothing_here")
    assert _rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id")) == CELL_ROWS


@pytest.mark.parametrize("statement", ["DROP TABLE sc.ns.t", "DROP TABLE ns.t"])
def test_qualified_drop_table_near_miss_drops_the_catalog_table(
    spark: ReparkSession, statement: str
) -> None:
    """Two- and three-part DROP TABLE reach the catalog and leave the temp view (p9)."""
    spark.sql("USE sc")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 5 AS id")
    spark.sql(statement)
    assert _rows(spark.sql("SELECT * FROM t")) == [[5]]
    assert _rows(spark.sql("SHOW VIEWS")) == [["", "t", True]]
    _assert_catalog_t_missing(spark)


def test_bare_drop_table_without_a_temp_view_drops_the_catalog_table(
    spark: ReparkSession,
) -> None:
    """With no temp view the bare DROP TABLE is the catalog drop, unchanged."""
    spark.sql("USE sc.ns")
    spark.sql("DROP TABLE t")
    _assert_catalog_t_missing(spark)
