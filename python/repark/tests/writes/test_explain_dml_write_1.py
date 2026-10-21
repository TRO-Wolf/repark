from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark._native import PyReparkSession
from repark.errors import AnalysisException, PySparkException

SEED = (
    "CREATE TABLE ice.ns.t (id INT, v INT)",
    "INSERT INTO ice.ns.t VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    "CREATE TABLE ice.ns.s (id INT, w INT)",
    "INSERT INTO ice.ns.s VALUES (2, 20), (3, 99)",
)
SEEDED_T = [(1, 10), (2, 20), (3, 30), (4, 40)]
SEEDED_SNAPSHOTS = 1
IN_SUBQUERY = "id IN (SELECT id FROM ice.ns.s)"
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
DOORS = ("repark_sql", "spark_session")
PREDICATES = {"plain": "id = 2", "in": IN_SUBQUERY}
VERBS = {
    "delete": "DELETE FROM ice.ns.t WHERE {predicate}",
    "update": "UPDATE ice.ns.t SET v = 5 WHERE {predicate}",
    "insert": "INSERT INTO ice.ns.t SELECT id + 10, w FROM ice.ns.s WHERE {predicate}",
    "merge": (
        "MERGE INTO ice.ns.t AS target USING (SELECT id, w FROM ice.ns.s WHERE {predicate}) "
        "AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET v = source.w"
    ),
}
WRITES = [
    pytest.param(VERBS[verb].format(predicate=predicate), id=f"{verb}_{name}")
    for verb in VERBS
    for name, predicate in PREDICATES.items()
]
PLANNED_WRITES = [
    pytest.param(VERBS[verb].format(predicate=predicate), id=f"{verb}_{name}")
    for verb in ("delete", "update", "insert")
    for name, predicate in PREDICATES.items()
]
WRAPPED_FORMS = [
    pytest.param(
        "EXPLAIN ANALYZE VERBOSE DELETE FROM ice.ns.t WHERE id = 2",
        EXPLAIN_ANALYZE_REFUSAL,
        id="explain_analyze_verbose_delete",
    ),
    pytest.param(
        f"PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE {IN_SUBQUERY}",
        PREPARE_REFUSAL,
        id="prepare_explain_analyze_delete",
    ),
    pytest.param(
        "PREPARE p(INT) AS DELETE FROM ice.ns.t WHERE id = $1",
        PREPARE_REFUSAL,
        id="prepare_parameterised_delete",
    ),
]


class Door:
    def __init__(self, warehouse: Path, spark: ReparkSession | None) -> None:
        self.warehouse = warehouse
        self.spark = spark

    def run(self, statement: str) -> list[Any]:
        raise NotImplementedError(statement)

    def snapshots(self) -> int:
        raise NotImplementedError

    def state(self) -> tuple[list[tuple[int, int]], int, list[str]]:
        read = self.run("SELECT id, v FROM ice.ns.t ORDER BY id")
        rows = [(int(row[0]), int(row[1])) for row in read]
        files = sorted(
            str(path.relative_to(self.warehouse))
            for path in self.warehouse.rglob("*")
            if path.is_file()
        )
        return rows, self.snapshots(), files


class NativeDoor(Door):
    def __init__(self, warehouse: Path, native: PyReparkSession) -> None:
        super().__init__(warehouse, None)
        self.native = native

    def run(self, statement: str) -> list[Any]:
        return [tuple(row.values()) for row in repark.sql(statement).to_arrow().to_pylist()]

    def snapshots(self) -> int:
        return len(self.native.testing_list_snapshots("ice.ns.t"))


class SparkDoor(Door):
    def __init__(self, warehouse: Path, spark: ReparkSession) -> None:
        super().__init__(warehouse, spark)
        self.session = spark

    def run(self, statement: str) -> list[Any]:
        return [tuple(row) for row in self.session.sql(statement).collect()]

    def snapshots(self) -> int:
        return int(self.session.sql("SELECT count(*) FROM ice.ns.t.snapshots").collect()[0][0])


def assert_iceberg(door: Door, table: str) -> None:
    plan = " ".join(str(row[1]) for row in door.run(f"EXPLAIN SELECT * FROM {table}"))
    assert "IcebergTableScan" in plan, f"{table} is not an Iceberg table: {plan}"


def repark_sql_door(warehouse: Path, monkeypatch: pytest.MonkeyPatch) -> Door:
    native = PyReparkSession.native()
    native.register_memory_catalog("ice", str(warehouse))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", native)
    native.create_namespace("ice", "ns", str(warehouse / "ns"))
    return NativeDoor(warehouse, native)


def spark_session_door(warehouse: Path) -> SparkDoor:
    session = ReparkSession.builder.appName("explain-dml-write-1").getOrCreate()
    session.register_memory_catalog("ice", str(warehouse))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    return SparkDoor(warehouse, session)


@pytest.fixture(params=DOORS)
def door(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Door]:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    if request.param == "repark_sql":
        opened = repark_sql_door(warehouse, monkeypatch)
    else:
        opened = spark_session_door(warehouse)
    for statement in SEED:
        opened.run(statement)
    assert_iceberg(opened, "ice.ns.t")
    assert_iceberg(opened, "ice.ns.s")
    assert opened.state()[:2] == (SEEDED_T, SEEDED_SNAPSHOTS)
    yield opened
    if opened.spark is not None:
        opened.spark.stop()


def refuses_and_holds(door: Door, statement: str, text: str) -> None:
    before = door.state()
    with pytest.raises(AnalysisException) as caught:
        door.run(statement)
    assert text in str(caught.value), str(caught.value)
    with pytest.raises(PySparkException, match=UNPREPARED):
        door.run("EXECUTE p")
    assert door.state() == before, statement
    assert before[:2] == (SEEDED_T, SEEDED_SNAPSHOTS), statement


@pytest.mark.parametrize("write", WRITES)
def test_an_explain_analyze_of_a_write_refuses_and_writes_nothing(door: Door, write: str) -> None:
    refuses_and_holds(door, f"EXPLAIN ANALYZE {write}", EXPLAIN_ANALYZE_REFUSAL)


@pytest.mark.parametrize("write", WRITES)
def test_a_prepared_write_refuses_and_leaves_nothing_to_execute(door: Door, write: str) -> None:
    refuses_and_holds(door, f"PREPARE p AS {write}", PREPARE_REFUSAL)


@pytest.mark.parametrize(("statement", "text"), WRAPPED_FORMS)
def test_every_spelling_of_a_wrapped_write_refuses(door: Door, statement: str, text: str) -> None:
    refuses_and_holds(door, statement, text)


def test_a_prepared_query_still_executes(door: Door) -> None:
    door.run("PREPARE q AS SELECT v FROM ice.ns.t WHERE id = 2")
    assert [int(row[0]) for row in door.run("EXECUTE q")] == [20]
    assert door.state()[:2] == (SEEDED_T, SEEDED_SNAPSHOTS)


@pytest.mark.parametrize("write", PLANNED_WRITES)
def test_a_plain_explain_of_a_write_answers_its_plan_and_writes_nothing(
    door: Door, write: str
) -> None:
    before = door.state()
    plan = door.run(f"EXPLAIN {write}")
    assert {"logical_plan", "physical_plan"} <= {str(row[0]) for row in plan}, plan
    assert door.state() == before, write


def test_explain_and_explain_analyze_of_a_select_answer(door: Door) -> None:
    before = door.state()
    for statement in (
        "EXPLAIN SELECT * FROM ice.ns.t WHERE id = 2",
        f"EXPLAIN ANALYZE SELECT * FROM ice.ns.t WHERE {IN_SUBQUERY}",
    ):
        assert door.run(statement), statement
    assert door.state() == before


def test_df_explain_cost_still_answers(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    door = spark_session_door(warehouse)
    try:
        for statement in SEED:
            door.run(statement)
        door.session.table("ice.ns.t").explain("cost")
        assert "== Physical Plan ==" in capsys.readouterr().out
        assert door.state()[:2] == (SEEDED_T, SEEDED_SNAPSHOTS)
    finally:
        door.session.stop()


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        pytest.param(
            f"DELETE FROM ice.ns.t WHERE {IN_SUBQUERY}", [(1, 10), (4, 40)], id="delete_in"
        ),
        pytest.param(
            f"UPDATE ice.ns.t SET v = 5 WHERE {IN_SUBQUERY}",
            [(1, 10), (2, 5), (3, 5), (4, 40)],
            id="update_in",
        ),
    ],
)
def test_the_bare_in_forms_keep_the_matching_rows_on_the_spark_door(
    tmp_path: Path, statement: str, expected: list[tuple[int, int]]
) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    door = spark_session_door(warehouse)
    try:
        for seed in SEED:
            door.run(seed)
        door.run(statement)
        assert door.state()[:2] == (expected, SEEDED_SNAPSHOTS + 1), statement
    finally:
        door.session.stop()
