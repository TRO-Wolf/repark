"""REPARK-SQL-UPDATE-SUBQUERY-WHERE-1: ``repark.sql()`` refuses a subquery in a DML WHERE.

Owner ruling 106 (a) and its stopgap. The native door has no identity path, so every
subquery-predicate ``UPDATE`` and ``DELETE`` raises the G3-E8 text verbatim, the rows read back
as seeded, and no file appears under the warehouse. On main the same statements wrote every row,
emptied the table, or refused with DataFusion's planner text. Spark 4.1.2 answers each form with
the matching rows only; the sketch records its answers.

Owner ruling 131 (b): the refusal also fires inside ``EXPLAIN`` and ``EXPLAIN ANALYZE``, where
reading the result wrote the rows on ``repark.sql()``.

pins: repark-sql-update-subquery-where-1/C-001, C-002, C-003, C-004, C-012
"""

from __future__ import annotations

from pathlib import Path

import pytest

import repark
from repark._native import PyReparkSession
from repark.errors import AnalysisException

T_SEED = (
    "CREATE TABLE ice.ns.t AS SELECT CAST(id AS INT) AS id, CAST(v AS INT) AS v "
    "FROM (VALUES (1, 10), (2, 20), (3, 30), (4, 40)) AS x(id, v)"
)
S_SEED = (
    "CREATE TABLE ice.ns.s AS SELECT CAST(id AS INT) AS id, CAST(w AS INT) AS w "
    "FROM (VALUES (2, 20), (3, 99)) AS x(id, w)"
)
MEM_SEED = "CREATE TABLE mem3 AS SELECT * FROM (VALUES (1), (2), (3)) AS x(a)"
SEEDED_T = [(1, 10), (2, 20), (3, 30), (4, 40)]
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
EXPLAINED_CELLS = [
    pytest.param(
        "DELETE",
        "EXPLAIN DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        id="P-E1-explain_delete",
    ),
    pytest.param(
        "UPDATE",
        "EXPLAIN UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
        id="P-E2-explain_update",
    ),
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


def seeded_warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``repark.sql()`` at a fresh native session over a seeded memory catalog."""
    warehouse = tmp_path / "warehouse"
    namespace = warehouse / "ns"
    namespace.mkdir(parents=True)
    native = PyReparkSession.native()
    native.register_memory_catalog("ice", str(warehouse))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", native)
    repark.sql(f"CREATE SCHEMA ice.ns WITH (location = '{namespace}')").collect()
    repark.sql(T_SEED).collect()
    repark.sql(S_SEED).collect()
    return warehouse


def files_under(root: Path) -> list[str]:
    """Return every file below ``root`` as a sorted relative path."""
    return sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())


def target_rows() -> list[tuple[int, int]]:
    """Read ``ice.ns.t`` back through ``repark.sql()``."""
    table = repark.sql("SELECT id, v FROM ice.ns.t ORDER BY id").to_arrow()
    return [(int(row["id"]), int(row["v"])) for row in table.to_pylist()]


def refuse_and_hold(statement: str, verb: str, table: str, warehouse: Path) -> None:
    """Run one statement, expect the G3-E8 text, and hold every file under the warehouse."""
    before = files_under(warehouse)
    with pytest.raises(AnalysisException) as caught:
        repark.sql(statement).collect()
    assert g3e8_text(verb, table) in str(caught.value), str(caught.value)
    assert files_under(warehouse) == before, statement


@pytest.mark.parametrize(("verb", "predicate"), ICEBERG_CELLS)
def test_a_subquery_predicate_refuses_on_an_iceberg_table(
    verb: str, predicate: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each of the eight predicate forms refuses for both verbs; rows and files hold."""
    warehouse = seeded_warehouse(tmp_path, monkeypatch)
    statement = VERB_STATEMENTS[verb].format(table="ice.ns.t", column="v", predicate=predicate)
    refuse_and_hold(statement, verb, "ice.ns.t", warehouse)
    assert target_rows() == SEEDED_T, statement


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
    warehouse = seeded_warehouse(tmp_path, monkeypatch)
    repark.sql("SET datafusion.catalog.default_catalog = 'ice'").collect()
    repark.sql("SET datafusion.catalog.default_schema = 'ns'").collect()
    refuse_and_hold(statement, verb, "t", warehouse)
    assert target_rows() == SEEDED_T, statement


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
    """The defect is not Iceberg-only: an in-memory table refuses the same way."""
    warehouse = seeded_warehouse(tmp_path, monkeypatch)
    repark.sql(MEM_SEED).collect()
    refuse_and_hold(statement, verb, "mem3", warehouse)
    table = repark.sql("SELECT a FROM mem3 ORDER BY a").to_arrow()
    assert [int(value) for value in table.column("a").to_pylist()] == SEEDED_MEM, statement


@pytest.mark.parametrize(("verb", "statement"), COINCIDENCE_CELLS)
def test_a_shape_main_answered_by_coincidence_refuses(
    verb: str, statement: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Main wrote Spark's rows here only because the predicate holds for every row."""
    warehouse = seeded_warehouse(tmp_path, monkeypatch)
    refuse_and_hold(statement, verb, "ice.ns.t", warehouse)
    assert target_rows() == SEEDED_T, statement


@pytest.mark.parametrize(("statement", "expected"), CONTROL_CELLS)
def test_a_statement_outside_the_refused_class_still_answers(
    statement: str, expected: list[tuple[int, int]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plain predicate, a literal IN list and an INSERT that reads a subquery still write."""
    seeded_warehouse(tmp_path, monkeypatch)
    repark.sql(statement).collect()
    assert target_rows() == expected, statement


@pytest.mark.parametrize(("verb", "statement"), EXPLAINED_CELLS)
def test_an_explained_subquery_predicate_refuses(
    verb: str, statement: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EXPLAIN and EXPLAIN ANALYZE of either verb refuse; reading no result writes nothing."""
    warehouse = seeded_warehouse(tmp_path, monkeypatch)
    refuse_and_hold(statement, verb, "ice.ns.t", warehouse)
    assert target_rows() == SEEDED_T, statement
