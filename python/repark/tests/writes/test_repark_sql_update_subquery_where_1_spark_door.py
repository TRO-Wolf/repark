"""REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 on the Spark SQL door: a wrapped subquery DML refuses.

Owner ruling 139, first bullet: the Spark SQL door's strict pregate,
``refuse_dml_subquery_predicate_in_statement``, refuses each ``DELETE`` and ``UPDATE`` the
statement executes, through the walk ``repark.sql()`` uses. ``PREPARE`` of a subquery-predicate
``DELETE`` or ``UPDATE``, alone or around ``EXPLAIN ANALYZE``, refuses with the G3-E8 text, so
``EXECUTE`` finds nothing to run; on main it emptied the table or rewrote every row. The bare
``id IN (SELECT ...)`` forms still change exactly the matching rows on RePark's own executor, a
``PREPARE`` with a plain predicate still runs, and a plain ``EXPLAIN`` answers its plan. Every
table is an Iceberg table created through the catalog, and a guard fails on any other provider;
each pin reads the rows and the snapshot count before and after.

pins: repark-sql-update-subquery-where-1/C-025, C-026
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

SEED = (
    "CREATE TABLE ice.ns.t (id INT, v INT)",
    "INSERT INTO ice.ns.t VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    "CREATE TABLE ice.ns.s (id INT, w INT)",
    "INSERT INTO ice.ns.s VALUES (2, 20), (3, 99)",
)
SEEDED_T = [(1, 10), (2, 20), (3, 30), (4, 40)]
SEEDED_SNAPSHOTS = 1
SUB = "id IN (SELECT id FROM ice.ns.s)"
UNPREPARED = "Prepared statement 'p' does not exist"
G3E8_PARTS = (
    "subquery predicates are silently mis-executed",
    "(defect G3-E8, silent data loss)",
    "MERGE INTO",
    "Support returns when the underlying fix lands",
)
PREPARED_CELLS = [
    pytest.param(
        "DELETE", f"PREPARE p AS DELETE FROM ice.ns.t WHERE {SUB}", id="P-S1-prepare_delete"
    ),
    pytest.param(
        "UPDATE", f"PREPARE p AS UPDATE ice.ns.t SET v = 5 WHERE {SUB}", id="P-S2-prepare_update"
    ),
    pytest.param(
        "DELETE",
        f"PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE {SUB}",
        id="P-S3-prepare_explain_analyze_delete",
    ),
]
BARE_CELLS = [
    pytest.param(f"DELETE FROM ice.ns.t WHERE {SUB}", [(1, 10), (4, 40)], id="bare_delete_in"),
    pytest.param(
        f"UPDATE ice.ns.t SET v = 5 WHERE {SUB}",
        [(1, 10), (2, 5), (3, 5), (4, 40)],
        id="bare_update_in",
    ),
]
PLAIN_PREPARED_CELLS = [
    pytest.param(
        "PREPARE p AS DELETE FROM ice.ns.t WHERE id = 2",
        [(1, 10), (3, 30), (4, 40)],
        id="prepare_delete_plain",
    ),
    pytest.param(
        "PREPARE p AS UPDATE ice.ns.t SET v = 5 WHERE id = 2",
        [(1, 10), (2, 5), (3, 30), (4, 40)],
        id="prepare_update_plain",
    ),
]


def assert_iceberg(spark: ReparkSession, table: str) -> None:
    """Fail unless the Spark SQL door scans ``table`` through Iceberg."""
    plan = " ".join(str(row[1]) for row in spark.sql(f"EXPLAIN SELECT * FROM {table}").collect())
    assert "IcebergTableScan" in plan, f"{table} is not an Iceberg table: {plan}"


def target_state(spark: ReparkSession) -> tuple[list[tuple[int, int]], int]:
    """Return ``ice.ns.t``'s rows and its snapshot count."""
    rows = [tuple(row) for row in spark.sql("SELECT id, v FROM ice.ns.t ORDER BY id").collect()]
    snapshots = spark.sql("SELECT count(*) FROM ice.ns.t.snapshots").collect()[0][0]
    return rows, int(snapshots)


def assert_g3e8(text: str, verb: str) -> None:
    """Assert the G3-E8 refusal for ``verb``, whatever spelling of the target it carries."""
    assert f"{verb} with a subquery in its WHERE clause is refused on `" in text, text
    for part in G3E8_PARTS:
        assert part in text, text


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[ReparkSession]:
    """A Spark SQL session over a memory catalog holding the two seeded Iceberg tables."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    session = ReparkSession.builder.appName("repark-sql-update-subquery-where-1").getOrCreate()
    session.register_memory_catalog("ice", str(warehouse))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    for statement in SEED:
        session.sql(statement).collect()
    assert_iceberg(session, "ice.ns.t")
    assert_iceberg(session, "ice.ns.s")
    assert target_state(session) == (SEEDED_T, SEEDED_SNAPSHOTS)
    yield session
    session.stop()


@pytest.mark.parametrize(("verb", "prepare"), PREPARED_CELLS)
def test_a_prepared_subquery_predicate_refuses(
    spark: ReparkSession, verb: str, prepare: str
) -> None:
    """PREPARE refuses with the G3-E8 text, so EXECUTE finds no statement and nothing is written."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql(prepare).collect()
    assert_g3e8(str(caught.value), verb)
    with pytest.raises(PySparkException, match=UNPREPARED):
        spark.sql("EXECUTE p").collect()
    assert target_state(spark) == (SEEDED_T, SEEDED_SNAPSHOTS), prepare


@pytest.mark.parametrize(("statement", "expected"), BARE_CELLS)
def test_the_bare_in_subquery_dml_changes_exactly_the_matching_rows(
    spark: ReparkSession, statement: str, expected: list[tuple[int, int]]
) -> None:
    """The bare IN form runs on RePark's own executor and changes only ids 2 and 3."""
    spark.sql(statement).collect()
    assert target_state(spark) == (expected, SEEDED_SNAPSHOTS + 1), statement


@pytest.mark.parametrize(("prepare", "expected"), PLAIN_PREPARED_CELLS)
def test_a_plain_predicate_prepare_still_runs(
    spark: ReparkSession, prepare: str, expected: list[tuple[int, int]]
) -> None:
    """A PREPARE with no subquery still prepares, and EXECUTE writes the matching row."""
    spark.sql(prepare).collect()
    spark.sql("EXECUTE p").collect()
    assert target_state(spark) == (expected, SEEDED_SNAPSHOTS + 1), prepare


def test_a_plain_explain_of_the_subquery_delete_answers_its_plan(spark: ReparkSession) -> None:
    """A plain EXPLAIN executes nothing: it answers its plan and the Iceberg table holds."""
    plan = spark.sql(f"EXPLAIN DELETE FROM ice.ns.t WHERE {SUB}").collect()
    assert {"logical_plan", "physical_plan"} <= {str(row[0]) for row in plan}, plan
    assert target_state(spark) == (SEEDED_T, SEEDED_SNAPSHOTS)
