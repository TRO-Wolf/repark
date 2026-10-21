"""WRITE-BODY-CARRIERS-1 build battery — a write body inside CTAS refuses at parse.

Every CTAS cell replays step 0 against the head module: the 40 recorded cells
(2 creators x 5 bodies x z new and existing x format versions 2 and 3) refuse
with Spark 4.1.2's measured class, condition, SQLSTATE and leading keyword,
and ``t`` and ``z`` read back unchanged. ``REPLACE TABLE`` and parenthesised
bodies refuse the same way.

Expected refusal values come from
``task/wo/write-body-carriers-1/evidence/spark-cells.json`` (class
``ParseException``, condition ``PARSE_SYNTAX_ERROR``, SQLSTATE ``42601``,
parameters ``{'error': '<KEYWORD>', 'hint': ''}``) and the card's rule; the
``(line, pos)`` suffix Spark appends is omitted because the parser does not
report the keyword position.

pins: write-body-carriers-1/C-004, C-005
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark._native import PyReparkSession
from repark.errors import AnalysisException, ParseException, UnsupportedOperationException
from repark.spark.dataframe import DataFrame

CATALOG = "wbc1"
NAMESPACE = "ns"

BODIES: dict[str, str] = {
    "insert": "INSERT INTO {t} VALUES (3, 'c')",
    "overwrite": "INSERT OVERWRITE {t} VALUES (10, 'w')",
    "update": "UPDATE {t} SET v = 'x' WHERE id = 1",
    "delete": "DELETE FROM {t} WHERE id = 2",
    "merge": (
        "MERGE INTO {t} USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS id, 'n' AS v) s "
        "ON {t}.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    ),
}

KEYWORDS: dict[str, str] = {
    "insert": "INSERT",
    "overwrite": "INSERT",
    "update": "UPDATE",
    "delete": "DELETE",
    "merge": "MERGE",
}

CREATORS: dict[str, str] = {
    "create": "CREATE TABLE {z} AS {body}",
    "or_replace": "CREATE OR REPLACE TABLE {z} AS {body}",
}

VERSIONS: tuple[int, ...] = (2, 3)

SEED_ROWS: list[list[Any]] = [[1, "a"], [2, "b"]]

EXPLAIN_ANALYZE_REFUSAL = (
    "EXPLAIN ANALYZE is refused: its statement writes to a table, and EXPLAIN ANALYZE runs the "
    "statement it explains. A plain EXPLAIN of the same statement still shows its plan without "
    "running it, and EXPLAIN ANALYZE of a query still works."
)

PREPARE_REFUSAL = (
    "PREPARE `{name}` is refused: its statement writes to a table, and a prepared write would run "
    "later, at EXECUTE. PREPARE of a query still works."
)


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog session with the format-version opt-in step 0 used."""
    session = (
        ReparkSession.builder.appName("pytest-write-body-carriers-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, str(tmp_path / "spark"))
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    return session


@pytest.fixture()
def ansi(tmp_path: Path) -> dict[str, Any]:
    """Native ANSI-door session with a memory catalog and a frame helper."""
    native = PyReparkSession.native()
    native.register_memory_catalog(CATALOG, str(tmp_path / "ansi"))
    alive: dict[str, bool] = {"alive": True}

    def run(text: str) -> DataFrame:
        """Run one ANSI statement and wrap its frame for collection."""
        return DataFrame(native.sql(text), native, alive)

    namespace_location = tmp_path / "ansi-ns"
    namespace_location.mkdir(parents=True, exist_ok=True)
    run(f"CREATE SCHEMA {CATALOG}.{NAMESPACE} WITH (location = '{namespace_location}')")
    return {"run": run}


def _first_line(error: BaseException) -> str:
    """Return the first non-blank line of one refusal."""
    text = str(error)
    return next(line.strip() for line in text.splitlines() if line.strip())


def _table_exists(session: ReparkSession, table: str) -> bool:
    """Return whether one table answers a read."""
    try:
        session.sql(f"SELECT 1 AS one FROM {table} LIMIT 1").collect()
    except Exception as error:
        if "not found" in str(error).lower():
            return False
        raise
    return True


def _snapshots(session: ReparkSession, table: str) -> int:
    """Return the snapshot count of one table."""
    return int(session.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").collect()[0]["n"])


def _rows(session: ReparkSession, table: str) -> list[list[Any]]:
    """Return the ordered rows of one table as plain lists."""
    return [list(row) for row in session.sql(f"SELECT * FROM {table} ORDER BY id").collect()]


def _state(session: ReparkSession, table: str) -> dict[str, Any]:
    """Return the existence, snapshots and rows of one table."""
    if not _table_exists(session, table):
        return {"exists": False}
    return {"exists": True, "snapshots": _snapshots(session, table), "rows": _rows(session, table)}


def _seed(
    session: ReparkSession, table: str, version: int, values: str = "(1, 'a'), (2, 'b')"
) -> None:
    """Create one versioned table and seed it with the given values."""
    session.sql(f"DROP TABLE IF EXISTS {table}")
    session.sql(
        f"CREATE TABLE {table} (id INT, v STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    session.sql(f"INSERT INTO {table} VALUES {values}")


def _assert_parse_refusal(error: BaseException, keyword: str) -> None:
    """Assert Spark's measured refusal shape for one CTAS write body."""
    assert type(error) is ParseException
    assert error.getCondition() == "PARSE_SYNTAX_ERROR"
    assert error.getErrorClass() == "PARSE_SYNTAX_ERROR"
    assert error.getSqlState() == "42601"
    assert _first_line(error) == (
        f"[PARSE_SYNTAX_ERROR] Syntax error at or near '{keyword}'. SQLSTATE: 42601"
    )


def test_ctas_write_body_refuses_before_any_write(spark: ReparkSession) -> None:
    """All 40 step-0 CTAS cells refuse with Spark's text and move no table."""
    for version in VERSIONS:
        for creator in CREATORS:
            for body in BODIES:
                for zstate in ("z_new", "z_exists"):
                    cell = f"{creator}_{body}_{zstate}_v{version}"
                    t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
                    z = f"{CATALOG}.{NAMESPACE}.z_{cell}"
                    _seed(spark, t, version)
                    if zstate == "z_exists":
                        _seed(spark, z, version, "(100, 'z')")
                    else:
                        spark.sql(f"DROP TABLE IF EXISTS {z}")
                    before_t = _state(spark, t)
                    before_z = _state(spark, z)
                    statement = CREATORS[creator].format(z=z, body=BODIES[body].format(t=t))
                    with pytest.raises(ParseException) as caught:
                        spark.sql(statement).collect()
                    _assert_parse_refusal(caught.value, KEYWORDS[body])
                    assert _state(spark, t) == before_t, cell
                    assert _state(spark, z) == before_z, cell


def test_replace_write_body_refuses_before_any_write(spark: ReparkSession) -> None:
    """REPLACE TABLE with each write body refuses with Spark's text."""
    for body in BODIES:
        t = f"{CATALOG}.{NAMESPACE}.t_replace_{body}"
        z = f"{CATALOG}.{NAMESPACE}.z_replace_{body}"
        _seed(spark, t, 2)
        _seed(spark, z, 2, "(100, 'z')")
        before_t = _state(spark, t)
        before_z = _state(spark, z)
        with pytest.raises(ParseException) as caught:
            spark.sql(f"REPLACE TABLE {z} AS {BODIES[body].format(t=t)}").collect()
        _assert_parse_refusal(caught.value, KEYWORDS[body])
        assert _state(spark, t) == before_t, body
        assert _state(spark, z) == before_z, body


def test_parenthesised_write_body_refuses_before_any_write(spark: ReparkSession) -> None:
    """A parenthesised write body reaches the same refusal and moves no table."""
    for body in BODIES:
        t = f"{CATALOG}.{NAMESPACE}.t_paren_{body}"
        z = f"{CATALOG}.{NAMESPACE}.z_paren_{body}"
        _seed(spark, t, 2)
        spark.sql(f"DROP TABLE IF EXISTS {z}")
        before_t = _state(spark, t)
        with pytest.raises(ParseException) as caught:
            spark.sql(f"CREATE TABLE {z} AS ({BODIES[body].format(t=t)})").collect()
        _assert_parse_refusal(caught.value, KEYWORDS[body])
        assert _state(spark, t) == before_t, body
        assert _state(spark, z) == {"exists": False}, body


def test_ctas_if_not_exists_write_body_refuses_before_any_write(spark: ReparkSession) -> None:
    """All 20 IF NOT EXISTS CTAS cells refuse with Spark text and move no table."""
    for version in VERSIONS:
        for body in BODIES:
            for zstate in ("z_new", "z_exists"):
                cell = f"if_not_exists_{body}_{zstate}_v{version}"
                t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
                z = f"{CATALOG}.{NAMESPACE}.z_{cell}"
                _seed(spark, t, version)
                if zstate == "z_exists":
                    _seed(spark, z, version, "(100, 'z')")
                else:
                    spark.sql(f"DROP TABLE IF EXISTS {z}")
                before_t = _state(spark, t)
                before_z = _state(spark, z)
                statement = f"CREATE TABLE IF NOT EXISTS {z} AS {BODIES[body].format(t=t)}"
                with pytest.raises(ParseException) as caught:
                    spark.sql(statement).collect()
                _assert_parse_refusal(caught.value, KEYWORDS[body])
                assert _state(spark, t) == before_t, cell
                assert _state(spark, z) == before_z, cell


def test_double_parenthesised_write_body_refuses_before_any_write(spark: ReparkSession) -> None:
    """A doubly parenthesised write body reaches the same refusal and moves no table."""
    for version in VERSIONS:
        for body in BODIES:
            cell = f"paren2_{body}_v{version}"
            t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
            z = f"{CATALOG}.{NAMESPACE}.z_{cell}"
            _seed(spark, t, version)
            spark.sql(f"DROP TABLE IF EXISTS {z}")
            before_t = _state(spark, t)
            with pytest.raises(ParseException) as caught:
                spark.sql(f"CREATE TABLE {z} AS (({BODIES[body].format(t=t)}))").collect()
            _assert_parse_refusal(caught.value, KEYWORDS[body])
            assert _state(spark, t) == before_t, cell
            assert _state(spark, z) == {"exists": False}, cell


def test_with_prefixed_write_body_refuses_before_any_write(spark: ReparkSession) -> None:
    """A WITH-prefixed write body refuses with Spark text and moves no table."""
    templates: tuple[str, str] = (
        "CREATE TABLE {z} AS WITH c AS (SELECT 3 AS id, 'c' AS v) INSERT INTO {t} SELECT * FROM c",
        "CREATE TABLE {z} AS (WITH c AS (SELECT 3 AS id, 'c' AS v) "
        "INSERT INTO {t} SELECT * FROM c)",
    )
    for version in VERSIONS:
        for index, template in enumerate(templates):
            cell = f"cte_prefixed_{index}_v{version}"
            t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
            z = f"{CATALOG}.{NAMESPACE}.z_{cell}"
            _seed(spark, t, version)
            spark.sql(f"DROP TABLE IF EXISTS {z}")
            before_t = _state(spark, t)
            with pytest.raises(ParseException) as caught:
                spark.sql(template.format(z=z, t=t)).collect()
            _assert_parse_refusal(caught.value, "INSERT")
            assert _state(spark, t) == before_t, cell
            assert _state(spark, z) == {"exists": False}, cell


def test_replace_missing_write_body_refuses_before_any_write(spark: ReparkSession) -> None:
    """REPLACE TABLE onto a missing target refuses with Spark text and moves no table."""
    for version in VERSIONS:
        for body in BODIES:
            cell = f"replace_missing_{body}_v{version}"
            t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
            z = f"{CATALOG}.{NAMESPACE}.z_{cell}"
            _seed(spark, t, version)
            spark.sql(f"DROP TABLE IF EXISTS {z}")
            before_t = _state(spark, t)
            with pytest.raises(ParseException) as caught:
                spark.sql(f"REPLACE TABLE {z} AS {BODIES[body].format(t=t)}").collect()
            _assert_parse_refusal(caught.value, KEYWORDS[body])
            assert _state(spark, t) == before_t, cell
            assert _state(spark, z) == {"exists": False}, cell


def test_wrappers_refuse_their_write(spark: ReparkSession) -> None:
    for version in VERSIONS:
        for body in BODIES:
            cell = f"explain_{body}_v{version}"
            t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
            _seed(spark, t, version)
            before = _snapshots(spark, t)
            with pytest.raises(AnalysisException) as caught:
                spark.sql(f"EXPLAIN ANALYZE {BODIES[body].format(t=t)}").collect()
            assert EXPLAIN_ANALYZE_REFUSAL in str(caught.value), cell
            assert _snapshots(spark, t) == before, cell
            assert _rows(spark, t) == SEED_ROWS, cell
        for body in ("insert", "overwrite"):
            cell = f"prepare_{body}_v{version}"
            t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
            name = f"wbc_{cell}"
            _seed(spark, t, version)
            before = _snapshots(spark, t)
            with pytest.raises(AnalysisException) as caught:
                spark.sql(f"PREPARE {name} AS {BODIES[body].format(t=t)}").collect()
            assert PREPARE_REFUSAL.format(name=name) in str(caught.value), cell
            with pytest.raises(Exception, match=f"Prepared statement '{name}' does not exist"):
                spark.sql(f"EXECUTE {name}").collect()
            assert _snapshots(spark, t) == before, cell
            assert _rows(spark, t) == SEED_ROWS, cell


def test_ansi_door_refuses_before_any_write(ansi: dict[str, Any]) -> None:
    """The ANSI door plans the write body lazily and refuses before committing."""
    run = ansi["run"]
    for body in BODIES:
        t = f"{CATALOG}.{NAMESPACE}.ta_{body}"
        z = f"{CATALOG}.{NAMESPACE}.za_{body}"
        run(f"DROP TABLE IF EXISTS {t}").collect()
        run(f"CREATE TABLE {t} (id INT, v VARCHAR)").collect()
        run(f"INSERT INTO {t} VALUES (1, 'a'), (2, 'b')").collect()
        before = [list(row) for row in run(f"SELECT * FROM {t} ORDER BY id").collect()]
        assert before == SEED_ROWS, body
        with pytest.raises(UnsupportedOperationException) as caught:
            run(f"CREATE TABLE {z} AS {BODIES[body].format(t=t)}").collect()
        assert type(caught.value) is UnsupportedOperationException
        assert "not implemented yet" in str(caught.value), body
        assert KEYWORDS[body] in str(caught.value), body
        after = [list(row) for row in run(f"SELECT * FROM {t} ORDER BY id").collect()]
        assert after == before, body
    run("CREATE TABLE wbc1.ns.za_sel AS SELECT * FROM wbc1.ns.ta_insert ORDER BY id")
    za_sel = [list(row) for row in run("SELECT * FROM wbc1.ns.za_sel ORDER BY id").collect()]
    assert za_sel == SEED_ROWS


def test_plain_ctas_shapes_keep_answering(spark: ReparkSession) -> None:
    """Non-write CTAS bodies and the already-exists check answer as on main."""
    _seed(spark, f"{CATALOG}.{NAMESPACE}.t", 2)
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.z_sel")
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.z_sel AS SELECT * FROM {CATALOG}.{NAMESPACE}.t")
    assert _rows(spark, f"{CATALOG}.{NAMESPACE}.z_sel") == SEED_ROWS
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.z_paren")
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.z_paren AS (SELECT * FROM {CATALOG}.{NAMESPACE}.t)"
    )
    assert _rows(spark, f"{CATALOG}.{NAMESPACE}.z_paren") == SEED_ROWS
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.z_values")
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.z_values AS VALUES (3, 'c')")
    assert _table_exists(spark, f"{CATALOG}.{NAMESPACE}.z_values")
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.t AS SELECT * FROM {CATALOG}.{NAMESPACE}.t")
    assert type(caught.value) is AnalysisException
    assert caught.value.getCondition() == "TABLE_OR_VIEW_ALREADY_EXISTS"
    assert caught.value.getSqlState() == "42P07"
    assert _rows(spark, f"{CATALOG}.{NAMESPACE}.t") == SEED_ROWS
    spark.sql(
        f"CREATE OR REPLACE TABLE {CATALOG}.{NAMESPACE}.t AS SELECT * FROM {CATALOG}.{NAMESPACE}.t"
    )
    assert _rows(spark, f"{CATALOG}.{NAMESPACE}.t") == SEED_ROWS
