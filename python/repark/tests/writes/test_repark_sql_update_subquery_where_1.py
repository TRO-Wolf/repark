"""REPARK-SQL-UPDATE-SUBQUERY-WHERE-1: ``repark.sql()`` refuses a subquery in a DML WHERE.

Owner ruling 106 (a) and its stopgap. The native door has no identity path, so every
subquery-predicate ``UPDATE`` and ``DELETE`` raises the G3-E8 text verbatim, the rows read back
as seeded, and no file appears under the warehouse. On main the same statements wrote every row,
emptied the table, or refused with DataFusion's planner text. Spark 4.1.2 answers each form with
the matching rows only; the sketch records its answers.

Owner ruling 135 (a): every pin seeds its tables through the Iceberg catalog and asserts the scan
is an ``IcebergTableScan``, so no pin can fall back to an in-memory table unnoticed; each checks the
rows, the snapshot count and the warehouse files. The refusal reads every write the statement
executes, so ``PREPARE`` of a subquery-predicate ``DELETE`` or ``UPDATE`` refuses, alone or around
``EXPLAIN ANALYZE``, while a plain ``EXPLAIN`` executes nothing and answers its plan. The refusal
parses with the session's SQL dialect.

pins: repark-sql-update-subquery-where-1/C-001, C-002, C-003, C-004, C-012
pins: repark-sql-update-subquery-where-1/C-017, C-018, C-019, C-020
"""

from __future__ import annotations

from pathlib import Path

import pytest

import repark
from repark._native import PyReparkSession
from repark.errors import AnalysisException, PySparkException

SEED = (
    "CREATE TABLE ice.ns.t (id INT, v INT)",
    "INSERT INTO ice.ns.t VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    "CREATE TABLE ice.ns.s (id INT, w INT)",
    "INSERT INTO ice.ns.s VALUES (2, 20), (3, 99)",
)
MEM_SEED = "CREATE TABLE mem3 AS SELECT * FROM (VALUES (1), (2), (3)) AS x(a)"
SEEDED_T = [(1, 10), (2, 20), (3, 30), (4, 40)]
SEEDED_SNAPSHOTS = 1
UNPREPARED = "Prepared statement 'p' does not exist"
EXPLAIN_ANALYZE_REFUSAL = (
    "EXPLAIN ANALYZE is refused: its statement writes to a table, and EXPLAIN ANALYZE runs the "
    "statement it explains. A plain EXPLAIN of the same statement still shows its plan without "
    "running it, and EXPLAIN ANALYZE of a query still works."
)
PREPARE_REFUSAL = (
    "PREPARE `p` is refused: its statement writes to a table, and a prepared write would run "
    "later, at EXECUTE. PREPARE of a query still works."
)
SEEDED_MEM = [1, 2, 3]
PREDICATES = (
    ("in", "id IN (SELECT id FROM ice.ns.s)"),
    ("eq_scalar", "id = (SELECT max(id) FROM ice.ns.s)"),
    ("exists_correlated", "EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)"),
    ("correlated_scalar", "v = (SELECT s.w FROM ice.ns.s WHERE s.id = t.id)"),
    ("not_in", "id NOT IN (SELECT id FROM ice.ns.s)"),
    ("exists_no_match", "EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = 100)"),
    ("in_and_literal", "id IN (SELECT id FROM ice.ns.s) AND v > 25"),
    ("eq_any", "id = ANY (SELECT id FROM ice.ns.s)"),
)
VERB_STATEMENTS = {
    "UPDATE": "UPDATE {table} SET {column} = 5 WHERE {predicate}",
    "DELETE": "DELETE FROM {table} WHERE {predicate}",
}
CONSEQUENCES = {
    "UPDATE": "update EVERY row of the table",
    "DELETE": "delete EVERY row of the table",
}
MERGE_ACTIONS = {"UPDATE": "UPDATE SET <assignments>", "DELETE": "DELETE"}
ICEBERG_CELLS = [
    pytest.param(verb, predicate, id=f"P-{verb[0]}{index}-{name}")
    for verb in ("UPDATE", "DELETE")
    for index, (name, predicate) in enumerate(PREDICATES, start=1)
]
COINCIDENCE_CELLS = [
    pytest.param(
        "DELETE",
        "DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.t)",
        id="P-C1-delete_in_self",
    ),
    pytest.param(
        "UPDATE",
        "UPDATE ice.ns.t SET v = 5 WHERE EXISTS (SELECT 1 FROM ice.ns.s)",
        id="P-C2-update_exists_any_row",
    ),
]
PLAIN_EXPLAINED_CELLS = [
    pytest.param(
        "EXPLAIN DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        id="P-E1-explain_delete",
    ),
    pytest.param(
        "EXPLAIN UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
        id="P-E2-explain_update",
    ),
]
ANALYZED_CELLS = [
    pytest.param(
        "DELETE",
        "EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        id="P-E3-explain_analyze_delete",
    ),
    pytest.param(
        "UPDATE",
        "EXPLAIN ANALYZE UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
        id="P-E4-explain_analyze_update",
    ),
]
PREPARED_CELLS = [
    pytest.param(
        "DELETE",
        "PREPARE p AS DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        "EXECUTE p",
        id="P-P1-prepare_delete",
    ),
    pytest.param(
        "UPDATE",
        "PREPARE p AS UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
        "EXECUTE p",
        id="P-P2-prepare_update",
    ),
    pytest.param(
        "DELETE",
        "PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        "EXECUTE p",
        id="P-P3-prepare_explain_analyze_delete",
    ),
    pytest.param(
        "DELETE",
        "PREPARE p(INT) AS DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s WHERE w > $1)",
        "EXECUTE p(0)",
        id="P-P4-prepare_parameterised_delete",
    ),
]
CONTROL_CELLS = [
    pytest.param("UPDATE ice.ns.t SET v = 5 WHERE id = 2", [(1, 10), (2, 5), (3, 30), (4, 40)]),
    pytest.param("DELETE FROM ice.ns.t WHERE id = 2", [(1, 10), (3, 30), (4, 40)]),
    pytest.param(
        "UPDATE ice.ns.t SET v = 5 WHERE id IN (1, 3)", [(1, 5), (2, 20), (3, 5), (4, 40)]
    ),
    pytest.param("DELETE FROM ice.ns.t WHERE id IN (1, 3)", [(2, 20), (4, 40)]),
    pytest.param(
        "INSERT INTO ice.ns.t SELECT id + 10, w FROM ice.ns.s "
        "WHERE id IN (SELECT id FROM ice.ns.t)",
        [(1, 10), (2, 20), (3, 30), (4, 40), (12, 20), (13, 99)],
    ),
]


def g3e8_text(verb: str, table: str) -> str:
    """Return the G3-E8 refusal text the ANSI door and the Spark SQL door raise, verbatim."""
    return (
        f"{verb} with a subquery in its WHERE clause is refused on `{table}`: subquery "
        "predicates are silently mis-executed today — DataFusion's DML planner decorrelates "
        "IN / NOT IN / EXISTS / ANY / ALL / correlated predicates into a semi-join and then "
        "recovers NO filter for the Iceberg writer, so this statement would "
        f"{CONSEQUENCES[verb]} instead of only the matching ones (defect G3-E8, silent data "
        f"loss). Rewrite it as `MERGE INTO {table} AS target USING (<the subquery>) AS source "
        f"ON <join keys> WHEN MATCHED THEN {MERGE_ACTIONS[verb]}` — the RePark-owned MERGE "
        "executor never crosses that seam, and it is the dbt adapter's proven vehicle. Support "
        f"returns when the underlying fix lands; non-subquery {verb} predicates are unaffected."
    )


def files_under(root: Path) -> list[str]:
    """Return every file below ``root`` as a sorted relative path."""
    return sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())


def scan_plan(table: str) -> str:
    """Return the plans ``repark.sql()`` builds to scan ``table``."""
    rows = repark.sql(f"EXPLAIN SELECT * FROM {table}").to_arrow().to_pylist()
    return "\n".join(str(row["plan"]) for row in rows)


def assert_iceberg(table: str) -> None:
    """Fail unless ``repark.sql()`` scans ``table`` through Iceberg, never an in-memory table."""
    plan = scan_plan(table)
    assert "IcebergTableScan" in plan, f"{table} is not an Iceberg table:\n{plan}"


def target_state(
    native: PyReparkSession, warehouse: Path
) -> tuple[list[tuple[int, int]], int, list[str]]:
    """Return ``ice.ns.t``'s rows, its snapshot count and every file under the warehouse."""
    table = repark.sql("SELECT id, v FROM ice.ns.t ORDER BY id").to_arrow()
    rows = [(int(row["id"]), int(row["v"])) for row in table.to_pylist()]
    return rows, len(native.testing_list_snapshots("ice.ns.t")), files_under(warehouse)


def seeded_warehouse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, PyReparkSession]:
    """Seed ``ice.ns.t`` and ``ice.ns.s`` through the Iceberg catalog behind ``repark.sql()``."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    native = PyReparkSession.native()
    native.register_memory_catalog("ice", str(warehouse))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", native)
    native.create_namespace("ice", "ns", str(warehouse / "ns"))
    for statement in SEED:
        repark.sql(statement).collect()
    assert_iceberg("ice.ns.t")
    assert_iceberg("ice.ns.s")
    rows, snapshots, _ = target_state(native, warehouse)
    assert (rows, snapshots) == (SEEDED_T, SEEDED_SNAPSHOTS)
    return warehouse, native


def refuse_and_hold(
    statement: str, verb: str, table: str, warehouse: Path, native: PyReparkSession
) -> None:
    """Run one statement, expect the G3-E8 text, and hold the rows, snapshots and files."""
    before = target_state(native, warehouse)
    with pytest.raises(AnalysisException) as caught:
        repark.sql(statement).collect()
    assert g3e8_text(verb, table) in str(caught.value), str(caught.value)
    assert target_state(native, warehouse) == before, statement


def refuse_wrapper_and_hold(
    statement: str, text: str, warehouse: Path, native: PyReparkSession
) -> None:
    before = target_state(native, warehouse)
    with pytest.raises(AnalysisException) as caught:
        repark.sql(statement).collect()
    assert text in str(caught.value), str(caught.value)
    assert target_state(native, warehouse) == before, statement


@pytest.mark.parametrize(("verb", "predicate"), ICEBERG_CELLS)
def test_a_subquery_predicate_refuses_on_an_iceberg_table(
    verb: str, predicate: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each of the eight predicate forms refuses for both verbs; rows, snapshots and files hold."""
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    statement = VERB_STATEMENTS[verb].format(table="ice.ns.t", column="v", predicate=predicate)
    refuse_and_hold(statement, verb, "ice.ns.t", warehouse, native)


@pytest.mark.parametrize(
    ("verb", "statement"),
    [
        pytest.param(
            "UPDATE", "UPDATE t SET v = 5 WHERE id IN (SELECT id FROM s)", id="P-U9-card_spelling"
        ),
        pytest.param(
            "DELETE", "DELETE FROM t WHERE id IN (SELECT id FROM s)", id="P-D9-card_spelling"
        ),
    ],
)
def test_the_cards_unqualified_spelling_refuses(
    verb: str, statement: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The card's spelling, resolved through the session defaults, refuses and writes nothing."""
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    repark.sql("SET datafusion.catalog.default_catalog = 'ice'").collect()
    repark.sql("SET datafusion.catalog.default_schema = 'ns'").collect()
    refuse_and_hold(statement, verb, "t", warehouse, native)


@pytest.mark.parametrize(
    ("verb", "statement"),
    [
        pytest.param("UPDATE", "UPDATE mem3 SET a = 5 WHERE a IN (SELECT 1)", id="P-U10-in_memory"),
        pytest.param("DELETE", "DELETE FROM mem3 WHERE a IN (SELECT 1)", id="P-D10-in_memory"),
    ],
)
def test_a_subquery_predicate_refuses_on_an_in_memory_table(
    verb: str, statement: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The defect is not Iceberg-only: a table labelled in-memory refuses the same way."""
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    repark.sql(MEM_SEED).collect()
    assert "IcebergTableScan" not in scan_plan("mem3")
    refuse_and_hold(statement, verb, "mem3", warehouse, native)
    table = repark.sql("SELECT a FROM mem3 ORDER BY a").to_arrow()
    assert [int(value) for value in table.column("a").to_pylist()] == SEEDED_MEM, statement


@pytest.mark.parametrize(("verb", "statement"), COINCIDENCE_CELLS)
def test_a_shape_main_answered_by_coincidence_refuses(
    verb: str, statement: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Main wrote Spark's rows here only because the predicate holds for every row."""
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    refuse_and_hold(statement, verb, "ice.ns.t", warehouse, native)


@pytest.mark.parametrize(("statement", "expected"), CONTROL_CELLS)
def test_a_statement_outside_the_refused_class_still_answers(
    statement: str, expected: list[tuple[int, int]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plain predicate, a literal IN list and an INSERT that reads a subquery still write."""
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    repark.sql(statement).collect()
    rows, snapshots, _ = target_state(native, warehouse)
    assert (rows, snapshots) == (expected, SEEDED_SNAPSHOTS + 1), statement


@pytest.mark.parametrize("statement", PLAIN_EXPLAINED_CELLS)
def test_a_plain_explain_of_a_subquery_predicate_answers_its_plan(
    statement: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plain EXPLAIN executes nothing: it answers its plan and the Iceberg table holds."""
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    before = target_state(native, warehouse)
    plan = repark.sql(statement).to_arrow().to_pylist()
    assert {"logical_plan", "physical_plan"} <= {row["plan_type"] for row in plan}, plan
    assert target_state(native, warehouse) == before, statement


@pytest.mark.parametrize(("verb", "statement"), ANALYZED_CELLS)
def test_an_explain_analyze_of_a_subquery_predicate_refuses(
    verb: str, statement: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EXPLAIN ANALYZE executes the statement it wraps, so either verb refuses, writing nothing."""
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    assert verb in statement
    refuse_wrapper_and_hold(statement, EXPLAIN_ANALYZE_REFUSAL, warehouse, native)


@pytest.mark.parametrize(("verb", "prepare", "execute"), PREPARED_CELLS)
def test_a_prepared_subquery_predicate_refuses(
    verb: str, prepare: str, execute: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    before = target_state(native, warehouse)
    assert verb in prepare
    refuse_wrapper_and_hold(prepare, PREPARE_REFUSAL, warehouse, native)
    with pytest.raises(PySparkException, match=UNPREPARED):
        repark.sql(execute).collect()
    assert target_state(native, warehouse) == before, execute


def test_a_non_default_parser_dialect_still_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The refusal parses with the session's SQL dialect: MsSQL's bracketed names still refuse."""
    warehouse, native = seeded_warehouse(tmp_path, monkeypatch)
    repark.sql("SET datafusion.sql_parser.dialect = 'MsSQL'").collect()
    refuse_and_hold(
        "DELETE FROM [ice].[ns].[t] WHERE [id] IN (SELECT [id] FROM [ice].[ns].[s])",
        "DELETE",
        "[ice].[ns].[t]",
        warehouse,
        native,
    )
