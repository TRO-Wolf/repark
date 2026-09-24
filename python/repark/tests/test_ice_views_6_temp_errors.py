"""IPI-40 views PR6 error-path battery — every temp-view refusal with its complete text.

The class sweep (UNPINNED ERROR PATH) of the SQL temporary-view door: malformed heads and
clauses, SHOW VIEWS parse errors, statement write options, a catalog registered over the
temp-view home, CREATE- and read-time planning refusals, the relocated MERGE OUTPUT refusal,
the qualified DROP near misses, and one pin per branch of the up-cast rule. Expected values
come from Spark 4.1.2 + Iceberg 1.11.0 where the probe answered (p8 to p10); the rest are RePark's
own texts, pinned as is.

pins: ice-views-1/C-018
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession, Row
from repark.errors import AnalysisException, ParseException, UnsupportedOperationException

UNSTRUCTURED_CONDITION: None = None
UNSTRUCTURED_SQLSTATE: None = None


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog ``sc`` with namespace ``ns`` and the cell's table ``t``."""
    session = ReparkSession.builder.appName("pytest-ice-views-6-errors").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.t (id BIGINT, data STRING, cat STRING) USING iceberg")
    session.sql("INSERT INTO sc.ns.t VALUES (0, 'd0', 'a'), (1, 'd1', 'b'), (2, 'd2', 'a')")
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame to plain nested lists for golden comparison."""
    return [list(row) for row in frame.collect()]


def _analysis_refusal(spark: ReparkSession, statement: str, *, collect: bool = False) -> Any:
    """Run a statement that must refuse as a plain AnalysisException and return it."""
    with pytest.raises(AnalysisException) as caught:
        frame = spark.sql(statement)
        if collect:
            frame.collect()
    assert type(caught.value) is AnalysisException
    return caught.value


PLAN = "Error during planning: "


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        (
            "(CREATE TEMPORARY VIEW v AS SELECT 1 AS id",
            "could not parse `CREATE TEMPORARY VIEW`: expected CREATE",
        ),
        (
            'CREATE OR REPLACE "x" TEMPORARY VIEW v AS SELECT 1 AS id',
            "could not parse `CREATE TEMPORARY VIEW`: expected TEMPORARY",
        ),
        (
            "CREATE TEMPORARY ( VIEW v AS SELECT 1 AS id",
            "could not parse `CREATE TEMPORARY VIEW`: expected VIEW",
        ),
        (
            "CREATE TEMPORARY VIEW v SELECT 1",
            "could not parse `CREATE TEMPORARY VIEW`: expected AS with the view query, got "
            "`CREATE TEMPORARY VIEW v SELECT 1`",
        ),
        (
            "CREATE TEMPORARY VIEW v AS",
            "could not parse `CREATE TEMPORARY VIEW`: the view query after AS is empty",
        ),
        (
            "CREATE TEMPORARY VIEW AS SELECT 1 AS id",
            "could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: EOF",
        ),
        (
            "CREATE TEMPORARY VIEW v (, i) AS SELECT 1 AS id",
            "could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: ,",
        ),
        (
            "CREATE TEMPORARY VIEW v (i AS SELECT 1 AS id",
            "could not parse CREATE NAMESPACE: sql parser error: Expected: ), found: EOF",
        ),
        (
            "CREATE TEMPORARY VIEW v COMMENT 5 AS SELECT 1 AS id",
            "could not parse CREATE NAMESPACE: sql parser error: "
            "Expected: literal string, found: 5",
        ),
        (
            "CREATE TEMPORARY VIEW v TBLPROPERTIES ('k') AS SELECT 1 AS id",
            "could not parse CREATE NAMESPACE: sql parser error: Expected: =, found: )",
        ),
        (
            "CREATE TEMPORARY VIEW v extra AS SELECT 1 AS id",
            "could not parse `CREATE TEMPORARY VIEW` at `extra`",
        ),
        (
            "SHOW VIEWS LIKE",
            "SHOW VIEWS … LIKE needs a quoted pattern (e.g. SHOW VIEWS IN cat.ns LIKE 'v*')",
        ),
        (
            "SHOW VIEWS IN a.b.c",
            "expected a two-part `IN <catalog.namespace>` name, got `a.b.c`",
        ),
        (
            "SHOW VIEWS IN sc.ns extra",
            "could not parse `SHOW VIEWS` at `extra` — the supported form is SHOW VIEWS IN "
            "<catalog.namespace> [LIKE] ['pattern']",
        ),
    ],
)
def test_malformed_temp_view_statements_refuse_with_the_full_text(
    spark: ReparkSession, statement: str, message: str
) -> None:
    """Every reachable temp-view parse refusal answers its complete Plan text."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT 1 AS id")
    refusal = _analysis_refusal(spark, statement)
    assert str(refusal) == PLAN + message
    assert refusal.getCondition() == UNSTRUCTURED_CONDITION
    assert refusal.getSqlState() == UNSTRUCTURED_SQLSTATE
    assert _rows(spark.sql("SHOW VIEWS")) == [["", "tv", True]]


@pytest.mark.parametrize(
    ("statement", "context"),
    [
        ("DROP VIEW tv", "DROP VIEW"),
        ("DROP TABLE tv", "DROP TABLE"),
        ("DESCRIBE tv", "DESCRIBE TABLE"),
        ("CREATE OR REPLACE TEMPORARY VIEW tv2 AS SELECT 1 AS id", "CREATE TEMPORARY VIEW"),
    ],
)
def test_temp_view_statements_refuse_statement_write_options(
    spark: ReparkSession, statement: str, context: str
) -> None:
    """Statement write options on a temp-view statement refuse with the full text."""
    from repark import _native

    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT 1 AS id")
    with pytest.raises(AnalysisException) as caught:
        _native.session_sql_with_write_options(
            spark._ensure_alive(), statement, {"write-format": "parquet"}, False, False
        )
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        f"{PLAN}{context} does not support write options (write-format); they are only "
        "honoured on Iceberg table writes (ICE-WRITE-OPTIONS-1)"
    )
    assert _rows(spark.sql("SHOW VIEWS")) == [["", "tv", True]]


@pytest.mark.parametrize(
    "statement",
    ["DROP VIEW tv", "DESCRIBE tv", "SHOW VIEWS", "CREATE TEMPORARY VIEW t2 AS SELECT 1 AS id"],
)
def test_a_catalog_over_the_temp_view_home_refuses_temp_statements(
    tmp_path: Path, statement: str
) -> None:
    """A catalog registered over the temp-view home refuses with the session's full text."""
    session = ReparkSession.builder.appName("pytest-ice-views-6-home").getOrCreate()
    session.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT 1 AS id")
    session.register_memory_catalog("datafusion", tmp_path)
    refusal = _analysis_refusal(session, statement, collect=True)
    assert str(refusal) == (
        f"{PLAN}this session has no session-local temp-view home: 'datafusion.public' (the "
        "build-time `datafusion.catalog.default_catalog` / `default_schema`) is not the "
        "session-local schema the session was built with — a catalog was registered over it. "
        "A temporary view is SESSION-LOCAL and is never created in a catalog or database, so "
        "the temp-view API refuses rather than write that catalog. Build the session with a "
        "`default_catalog` that no registered catalog shares a name with."
    )
    assert refusal.getCondition() == UNSTRUCTURED_CONDITION
    assert refusal.getSqlState() == UNSTRUCTURED_SQLSTATE


def test_qualified_drop_view_near_miss_without_a_temp_view(spark: ReparkSession) -> None:
    """DROP VIEW sc.ns.v with no temp view is main's catalog drop (nearmiss-rebase, 54e2da6a)."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    spark.sql("DROP VIEW sc.ns.v")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == []


def test_qualified_drop_table_near_miss_without_a_temp_view(spark: ReparkSession) -> None:
    """DROP TABLE sc.ns.t with no temp view is main's catalog drop (nearmiss-rebase, 54e2da6a)."""
    spark.sql("DROP TABLE sc.ns.t")
    assert spark.catalog.tableExists("sc.ns.t") is False


def test_more_aliases_than_columns_is_not_enough_data_columns(spark: ReparkSession) -> None:
    """More aliases than body columns is Spark's NOT_ENOUGH_DATA_COLUMNS (p10)."""
    refusal = _analysis_refusal(
        spark, "CREATE TEMPORARY VIEW vbad (i, d, x) AS SELECT id, data FROM sc.ns.t"
    )
    assert str(refusal) == (
        f"{PLAN}[CREATE_VIEW_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot create view "
        "`vbad`, the reason is not enough data columns:\nView columns: `i`, `d`, `x`.\n"
        "Data columns: `id`, `data`. SQLSTATE: 21S01"
    )
    assert refusal.getCondition() == "CREATE_VIEW_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS"
    assert refusal.getSqlState() == "21S01"


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        (
            "CREATE TEMPORARY VIEW vm AS SELECT 1; SELECT 2",
            "a stored view body must hold exactly one statement",
        ),
        (
            "CREATE TEMPORARY VIEW vb AS SELECT 1 +",
            "could not parse a stored view body: sql parser error: Expected: an expression, "
            "found: EOF",
        ),
        (
            "CREATE TEMPORARY VIEW vu AS SELECT * FROM sc.ns.nope",
            "table 'sc.ns.nope' not found",
        ),
    ],
)
def test_temp_view_body_planning_refusals(
    spark: ReparkSession, statement: str, message: str
) -> None:
    """Body planning refusals at CREATE keep the shared view-body texts (Spark differs, p10)."""
    refusal = _analysis_refusal(spark, statement)
    assert str(refusal) == PLAN + message
    assert refusal.getCondition() == UNSTRUCTURED_CONDITION
    assert refusal.getSqlState() == UNSTRUCTURED_SQLSTATE
    assert _rows(spark.sql("SHOW VIEWS")) == []


def test_temp_view_over_a_dropped_catalog_table_refuses_at_read(spark: ReparkSession) -> None:
    """A temp view whose catalog table was dropped answers the planner's missing-table text."""
    spark.sql("CREATE TABLE sc.ns.t2 (id BIGINT) USING iceberg")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW w AS SELECT * FROM sc.ns.t2")
    spark.sql("DROP TABLE sc.ns.t2")
    refusal = _analysis_refusal(spark, "SELECT * FROM w", collect=True)
    assert str(refusal) == f"{PLAN}table 'sc.ns.t2' not found"
    assert refusal.getCondition() == UNSTRUCTURED_CONDITION
    assert refusal.getSqlState() == UNSTRUCTURED_SQLSTATE


def test_held_temp_view_frame_refuses_after_a_catalog_replaces_the_home(tmp_path: Path) -> None:
    """A frame held across a catalog registered over the temp home answers the resolver text."""
    session = ReparkSession.builder.appName("pytest-ice-views-6-held").getOrCreate()
    session.sql("CREATE OR REPLACE TEMPORARY VIEW x AS SELECT 1 AS id")
    session.sql("CREATE OR REPLACE TEMPORARY VIEW y AS SELECT * FROM x")
    held = session.table("y")
    session.register_memory_catalog("datafusion", tmp_path)
    with pytest.raises(AnalysisException) as caught:
        held.collect()
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == f"{PLAN}failed to resolve schema: public"


def test_merge_output_clause_refuses_with_the_full_text(spark: ReparkSession) -> None:
    """The MERGE OUTPUT refusal that moved to merge.rs keeps its complete text."""
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(
            "MERGE INTO sc.ns.t USING sc.ns.t s ON t.id = s.id WHEN MATCHED THEN DELETE OUTPUT d.*"
        )
    assert type(caught.value) is UnsupportedOperationException
    assert str(caught.value) == (
        "This feature is not implemented: MERGE OUTPUT/RETURNING clauses are not supported"
    )
    assert caught.value.getCondition() == UNSTRUCTURED_CONDITION
    assert caught.value.getSqlState() == UNSTRUCTURED_SQLSTATE


def _replaced_dependency(spark: ReparkSession, created: str, replaced: str) -> Any:
    """Create ux AS created, uy over ux, then replace ux's column with replaced."""
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW ux AS SELECT {created} AS c")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW uy AS SELECT * FROM ux")
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW ux AS SELECT {replaced} AS c")
    return spark.sql("SELECT * FROM uy")


@pytest.mark.parametrize(
    ("created", "replaced", "arrow_type", "value"),
    [
        ("CAST(1 AS INT)", "NULL", pa.int32(), None),
        (
            "TIMESTAMP '2020-01-01 00:00:00'",
            "DATE '2020-01-02'",
            pa.timestamp("us", tz="UTC"),
            datetime(2020, 1, 2, 0, 0),
        ),
        ("CAST(1.5 AS DOUBLE)", "CAST(7 AS INT)", pa.float64(), 7.0),
        ("CAST(1.5 AS DOUBLE)", "CAST(2.5 AS FLOAT)", pa.float64(), 2.5),
        ("CAST(1 AS BIGINT)", "CAST(3 AS TINYINT)", pa.int64(), 3),
    ],
)
def test_up_cast_rule_accepts_each_spark_up_cast_class(
    spark: ReparkSession, created: str, replaced: str, arrow_type: pa.DataType, value: Any
) -> None:
    """NULL to any, DATE to TIMESTAMP and numeric widening up-cast to the creation type (p10)."""
    frame = _replaced_dependency(spark, created, replaced)
    assert [(field.name, field.type) for field in frame.to_arrow().schema] == [("c", arrow_type)]
    assert _rows(frame) == [[value]]


@pytest.mark.parametrize(
    ("created", "replaced", "source", "target"),
    [
        ("CAST(1 AS INT)", "CAST(2.5 AS DOUBLE)", "DOUBLE", "INT"),
        ("DATE '2020-01-01'", "TIMESTAMP '2020-01-02 00:00:00'", "TIMESTAMP", "DATE"),
        ("'a'", "CAST(7 AS INT)", "INT", "STRING"),
    ],
)
def test_up_cast_rule_refuses_each_other_class(
    spark: ReparkSession, created: str, replaced: str, source: str, target: str
) -> None:
    """Narrowing, TIMESTAMP to DATE and (RePark only) INT to STRING refuse CANNOT_UP_CAST."""
    with pytest.raises(AnalysisException) as caught:
        _replaced_dependency(spark, created, replaced).collect()
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        f'{PLAN}[CANNOT_UP_CAST_DATATYPE] Cannot up cast ux.c from "{source}" to "{target}".\n'
        "The type path of the target object is:\n\nYou can either add an explicit cast to the "
        "input data or choose a higher precision type of the field in the target object "
        "SQLSTATE: 42846"
    )
    assert caught.value.getCondition() == "CANNOT_UP_CAST_DATATYPE"
    assert caught.value.getSqlState() == "42846"


def test_four_part_temp_view_name_refuses(spark: ReparkSession) -> None:
    """IDENTIFIER_TOO_MANY_NAME_PARTS covers every name of three or more parts."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("CREATE TEMPORARY VIEW a.b.c.d AS SELECT 1 AS id")
    assert str(caught.value) == (
        "[IDENTIFIER_TOO_MANY_NAME_PARTS] `a`.`b`.`c`.`d` is not a valid identifier as it has "
        "more than 2 name parts. SQLSTATE: 42601"
    )
    assert caught.value.getCondition() == "IDENTIFIER_TOO_MANY_NAME_PARTS"
    assert caught.value.getSqlState() == "42601"


@pytest.mark.parametrize(
    ("body", "verb"),
    [
        ("INSERT INTO sc.ns.t SELECT id, 'x', 'y' FROM s", "INSERT"),
        ("UPDATE sc.ns.t SET id = 1", "UPDATE"),
        ("DELETE FROM sc.ns.t", "DELETE"),
        ("MERGE INTO sc.ns.t USING s ON sc.ns.t.id = s.id WHEN MATCHED THEN DELETE", "MERGE"),
    ],
)
def test_with_write_bodies_refuse_at_their_verb(spark: ReparkSession, body: str, verb: str) -> None:
    """Each WITH ... INSERT/UPDATE/DELETE/MERGE body is PARSE_SYNTAX_ERROR at its verb."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"CREATE TEMPORARY VIEW vw AS WITH s AS (SELECT 9 AS id) {body}")
    assert str(caught.value) == (
        f"[PARSE_SYNTAX_ERROR] Syntax error at or near '{verb}'. SQLSTATE: 42601"
    )
    assert caught.value.getCondition() == "PARSE_SYNTAX_ERROR"
    assert caught.value.getSqlState() == "42601"
    assert _rows(spark.sql("SELECT count(*) FROM sc.ns.t")) == [[3]]


@pytest.mark.parametrize("statement", ["DROP TABLE IF EXISTS t PURGE", "DROP VIEW IF EXISTS t"])
def test_if_exists_drops_take_the_temp_view_first(spark: ReparkSession, statement: str) -> None:
    """IF EXISTS drop forms of a bare temp name drop the temp view and keep the table."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 5 AS id")
    spark.sql(statement)
    assert _rows(spark.sql("SHOW VIEWS")) == []
    assert _rows(spark.sql("SELECT count(*) FROM sc.ns.t")) == [[3]]


def test_show_views_like_filters_catalog_and_temp_rows(spark: ReparkSession) -> None:
    """LIKE applies to catalog rows and temp rows alike (each side kept and dropped)."""
    spark.sql("CREATE VIEW sc.ns.cv AS SELECT id FROM sc.ns.t")
    spark.sql("CREATE VIEW sc.ns.tcv AS SELECT id FROM sc.ns.t")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT 1 AS id")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW atv AS SELECT 1 AS id")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns LIKE 't*'")) == [
        ["ns", "tcv", False],
        ["", "tv", True],
    ]


def test_temp_views_are_session_scoped(spark: ReparkSession) -> None:
    """Another session neither lists nor reads this session's temp view."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW scoped AS SELECT 1 AS id")
    other = spark.newSession()
    assert _rows(other.sql("SHOW VIEWS")) == []
    with pytest.raises(AnalysisException) as caught:
        other.sql("SELECT * FROM scoped").collect()
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: table 'spark_catalog.default.scoped' not found"
    )
    assert _rows(spark.sql("SHOW VIEWS")) == [["", "scoped", True]]


def _sql_chain(spark: ReparkSession, levels: int) -> None:
    """Build d0 and ``levels`` SQL temp views, each reading the one below it."""
    spark.sql("CREATE TEMPORARY VIEW d0 AS SELECT 1 AS id")
    for level in range(1, levels + 1):
        spark.sql(f"CREATE TEMPORARY VIEW d{level} AS SELECT * FROM d{level - 1}")


def test_a_hundred_view_sql_chain_reads_and_the_101st_refuses(spark: ReparkSession) -> None:
    """d0..d99 read on the default stack; the 101st view is Spark's nested-depth refusal (p10)."""
    _sql_chain(spark, 100)
    assert spark.sql("SELECT * FROM d99").collect() == [Row(id=1)]
    refusal = _analysis_refusal(spark, "SELECT * FROM d100", collect=True)
    assert str(refusal) == (
        f"{PLAN}[VIEW_EXCEED_MAX_NESTED_DEPTH] The depth of view `d0` exceeds the maximum view "
        "resolution depth (100).\nAnalysis is aborted to avoid errors. If you want to work "
        'around this, please try to increase the value of "spark.sql.view.maxNestedViewDepth". '
        "SQLSTATE: 54K00"
    )
    assert refusal.getCondition() == "VIEW_EXCEED_MAX_NESTED_DEPTH"
    assert refusal.getSqlState() == "54K00"


def test_a_two_view_sql_chain_reads(spark: ReparkSession) -> None:
    """A shallow SQL chain keeps reading through the grown-stack scan."""
    _sql_chain(spark, 2)
    assert spark.sql("SELECT * FROM d2").collect() == [Row(id=1)]


def test_a_ninety_nine_view_dataframe_chain_reads(spark: ReparkSession) -> None:
    """A DataFrame temp-view chain of 99 levels reads as it does on main."""
    spark.sql("SELECT 1 AS id").createOrReplaceTempView("f0")
    for level in range(1, 100):
        spark.table(f"f{level - 1}").createOrReplaceTempView(f"f{level}")
    assert spark.table("f99").collect() == [Row(id=1)]


def _trailing_statement_refusal(spark: ReparkSession, statement: str) -> ParseException:
    """Run a statement followed by a second one; it must refuse as a parse error."""
    with pytest.raises(ParseException) as caught:
        spark.sql(statement).collect()
    assert type(caught.value) is ParseException
    assert caught.value.getCondition() == "PARSE_SYNTAX_ERROR"
    return caught.value


@pytest.mark.parametrize(
    "statement",
    [
        "DESCRIBE sc.ns.t; SELECT 1",
        "DESCRIBE tv; SELECT 1",
        "DESC TABLE tv; SELECT 1",
        "DESCRIBE EXTENDED tv; SELECT 1",
        "SHOW VIEWS; SELECT 1",
        "SHOW VIEWS IN sc.ns; SELECT 1",
        "CREATE VIEW sc.ns.d1; DROP TABLE sc.ns.t AS SELECT 1 AS id",
        "CREATE TEMPORARY VIEW h1; DROP TABLE sc.ns.t AS SELECT 1 AS id",
        "CREATE TEMPORARY VIEW h2 (a); SELECT 1 AS id",
    ],
)
def test_temp_view_statements_with_a_trailing_statement_refuse(
    spark: ReparkSession, statement: str
) -> None:
    """A second statement after a temp-view statement refuses as the catalog path does."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT id FROM sc.ns.t")
    _trailing_statement_refusal(spark, statement)
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == [["", "tv", True]]
    assert spark.catalog.tableExists("sc.ns.t") is True


@pytest.mark.parametrize(
    "statement",
    ["DESCRIBE tv", "DESCRIBE tv;", "DESCRIBE tv ;", "DESC TABLE tv;", "DESCRIBE EXTENDED tv;"],
)
def test_describe_temp_view_with_trailing_semicolons_answers(
    spark: ReparkSession, statement: str
) -> None:
    """Trailing semicolons alone keep the temp-view DESCRIBE rows."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql(statement)) == [["id", "bigint", None]]


def test_show_views_with_a_trailing_semicolon_answers(spark: ReparkSession) -> None:
    """SHOW VIEWS; keeps its catalog and temp rows."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("SHOW VIEWS;")) == [["", "tv", True]]
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns;")) == [["", "tv", True]]


@pytest.mark.parametrize("statement", ["DROP VIEW tv; SELECT 1", "DROP TABLE tv; SELECT 1"])
def test_drop_with_a_trailing_statement_keeps_the_temp_view(
    spark: ReparkSession, statement: str
) -> None:
    """A trailing statement after DROP refuses as the catalog DROP does and drops nothing."""
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT id FROM sc.ns.t")
    temp = _analysis_refusal(spark, statement)
    catalog = _analysis_refusal(spark, statement.replace("tv", "sc.ns.t"))
    assert type(temp) is type(catalog)
    assert temp.getCondition() == catalog.getCondition()
    assert _rows(spark.sql("SHOW VIEWS")) == [["", "tv", True]]
    spark.sql("DROP VIEW tv;")
    assert _rows(spark.sql("SHOW VIEWS")) == []


def test_create_temp_view_with_a_trailing_statement_registers_nothing(
    spark: ReparkSession,
) -> None:
    """A two-statement body refuses at CREATE and leaves no view; a trailing semicolon registers."""
    _analysis_refusal(spark, "CREATE TEMPORARY VIEW vm AS SELECT 1 AS id; SELECT 2")
    assert _rows(spark.sql("SHOW VIEWS")) == []
    spark.sql("CREATE TEMPORARY VIEW vs AS SELECT 1 AS id;")
    assert _rows(spark.sql("SELECT * FROM vs")) == [[1]]
