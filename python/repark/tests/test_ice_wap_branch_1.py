"""ICE-WAP-BRANCH-1 — ``spark.wap.branch`` redirects writes and reads to an audit branch.

Oracle: ``ice_wap_branch_1_spark_oracle.json`` (live PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0, re-derived by
``_record_ice_wap_branch_1_oracle.py``; the recorded cells reproduce the orchestrator's
run-25c measurement observation for observation). The catalog is ``sc`` so the table names
in the fixture's statements resolve unchanged.

Every cell seeds ``(1,'a','x'),(2,'b','y')``, optionally creates the ``audit`` branch, sets
the session confs, runs the cell's steps, and compares five observations against the
fixture: ``session-read`` (a plain read while the conf is set), ``refs``, ``main``,
``branch`` and ``plain-read-after-unset``. Reads go through the Arrow path
(``to_arrow``), never ``show``.

pins: ice-wap-branch-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark.errors import IllegalArgumentException
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_wap_branch_1_spark_oracle.json").read_text(encoding="utf-8")
)
CELLS: dict[str, Any] = FIXTURE["cells"]
OK_CELL_IDS: list[str] = [cell_id for cell_id, cell in CELLS.items() if cell["status"] == "ok"]
ERROR_CELL_IDS: list[str] = [
    cell_id for cell_id, cell in CELLS.items() if cell["status"] == "error"
]
CATALOG = "sc"
NAMESPACE = "ns"
SEED_DDL = "(id BIGINT, data STRING, cat STRING)"
SEED_ROWS = "(1, 'a', 'x'), (2, 'b', 'y')"
APPEND_SCHEMA = "id BIGINT, data STRING, cat STRING"
APPEND_ROW = (9, "z", "q")
WAP_BRANCH_KEY = "spark.wap.branch"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``sc``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-wap-branch-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _table_names(cell_id: str) -> tuple[str, str, str]:
    """Return the ``(qualified, second, short)`` table names one cell writes."""
    suffix = cell_id.lower().replace("-", "_")
    short = f"{NAMESPACE}.t_{suffix}"
    return f"{CATALOG}.{short}", f"{CATALOG}.{NAMESPACE}.u_{suffix}", short


def _render(text: str, cell_id: str) -> str:
    """Substitute the per-cell table names into one fixture statement template."""
    qualified, second, short = _table_names(cell_id)
    return text.replace("{T2}", second).replace("{T}", qualified).replace("{SHORT}", short)


def _rows(session: Any, query: str) -> list[list[Any]]:
    """Collect one query on the Arrow path as sorted plain lists."""
    table = session.sql(query).to_arrow()
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    rows = ([column[index] for column in columns] for index in range(table.num_rows))
    return sorted(rows, key=repr)


def _snapshot_positions(session: Any, table: str) -> dict[int, int]:
    """Map snapshot id to its position in ``(committed_at, snapshot_id)`` order."""
    arrow = session.sql(f"SELECT snapshot_id, committed_at FROM {table}.snapshots").to_arrow()
    ordered = sorted(
        zip(
            arrow.column("snapshot_id").to_pylist(),
            arrow.column("committed_at").to_pylist(),
            strict=True,
        ),
        key=lambda pair: (pair[1], pair[0]),
    )
    return {snapshot_id: index for index, (snapshot_id, _) in enumerate(ordered)}


def _refs(session: Any, table: str) -> list[list[Any]]:
    """Return sorted ``[ref, type, snapshot position]`` triples, the fixture's shape."""
    positions = _snapshot_positions(session, table)
    arrow = session.sql(f"SELECT name, type, snapshot_id FROM {table}.refs").to_arrow()
    names = arrow.column("name").to_pylist()
    kinds = arrow.column("type").to_pylist()
    ids = arrow.column("snapshot_id").to_pylist()
    return sorted(
        [name, kind.lower(), positions.get(snapshot_id)]
        for name, kind, snapshot_id in zip(names, kinds, ids, strict=True)
    )


def _ref_names(session: Any, table: str) -> list[str]:
    """Return the sorted ref names of one table."""
    arrow = session.sql(f"SELECT name FROM {table}.refs").to_arrow()
    return sorted(arrow.column("name").to_pylist())


def _run_frame_step(session: Any, kind: str, qualified: str) -> None:
    """Run one DataFrame write step (``writeTo().append()`` or ``saveAsTable``)."""
    frame = session.createDataFrame([APPEND_ROW], APPEND_SCHEMA)
    if kind == "write_to_append":
        frame.writeTo(qualified).append()
        return
    if kind == "save_as_table_append":
        frame.write.format("iceberg").mode("append").saveAsTable(qualified)
        return
    raise AssertionError(f"unknown frame step {kind!r}")


def _run_steps(session: Any, cell_id: str, steps: list[dict[str, Any]]) -> dict[str, Any]:
    """Run one cell's step list, returning the observations the steps record."""
    qualified, second, _ = _table_names(cell_id)
    observations: dict[str, Any] = {}
    for step in steps:
        if "frame" in step:
            _run_frame_step(session, step["frame"], qualified)
            continue
        if "observe_ref_names" in step:
            observations[step["observe_ref_names"]] = _ref_names(session, second)
            continue
        statement = _render(step["sql"], cell_id)
        if "observe" in step:
            observations[step["observe"]] = _rows(session, statement)
            continue
        session.sql(statement).collect()
    return observations


def _seed(session: Any, cell_id: str) -> str:
    """Create and seed one cell's table (and its branch); return the qualified name."""
    cell = CELLS[cell_id]
    qualified, _, _ = _table_names(cell_id)
    session.sql(
        f"CREATE TABLE {qualified} {SEED_DDL} USING iceberg"
        f" TBLPROPERTIES ('format-version'='2'{cell['properties']})"
    )
    session.sql(f"INSERT INTO {qualified} VALUES {SEED_ROWS}")
    if cell["branch"]:
        session.sql(f"ALTER TABLE {qualified} CREATE BRANCH {cell['branch']}")
    return qualified


def _run_cell(session: Any, cell_id: str) -> dict[str, Any]:
    """Run one fixture cell end to end and return its observations."""
    cell = CELLS[cell_id]
    qualified = _seed(session, cell_id)
    observations: dict[str, Any] = {}
    for key, value in cell["conf"].items():
        session.conf.set(key, value)
    try:
        observations.update(_run_steps(session, cell_id, cell["steps"]))
        observations["session-read"] = _rows(session, f"SELECT * FROM {qualified}")
    finally:
        for key in cell["conf"]:
            session.conf.unset(key)
    observations["refs"] = _refs(session, qualified)
    observations["main"] = _rows(session, f"SELECT * FROM {qualified} VERSION AS OF 'main'")
    if cell["branch"]:
        observations["branch"] = _rows(
            session, f"SELECT * FROM {qualified} VERSION AS OF '{cell['branch']}'"
        )
    observations["plain-read-after-unset"] = _rows(session, f"SELECT * FROM {qualified}")
    observations.update(_run_steps(session, cell_id, cell["after"]))
    return observations


@pytest.mark.parametrize("cell_id", OK_CELL_IDS)
def test_wap_branch_cell_matches_spark(spark: Any, cell_id: str) -> None:
    """Every recorded WAP cell answers Spark's rows and refs. pins: ice-wap-branch-1/C-001"""
    expected = CELLS[cell_id]["obs"]
    observed = _run_cell(spark, cell_id)
    assert sorted(observed) == sorted(expected), cell_id
    for key, value in expected.items():
        assert observed[key] == value, f"{cell_id}/{key}"


@pytest.mark.parametrize("cell_id", ERROR_CELL_IDS)
def test_wap_branch_cell_refuses_like_spark(spark: Any, cell_id: str) -> None:
    """wap.branch with wap.id refuses with Java's text. pins: ice-wap-branch-1/C-006"""
    cell = CELLS[cell_id]
    _seed(spark, cell_id)
    for key, value in cell["conf"].items():
        spark.conf.set(key, value)
    try:
        with pytest.raises(IllegalArgumentException) as caught:
            _run_steps(spark, cell_id, cell["steps"])
    finally:
        for key in cell["conf"]:
            spark.conf.unset(key)
    assert str(caught.value) == cell["error"]["message"]
    assert type(caught.value).__name__ == cell["error"]["type"]


def test_wap_branch_write_leaves_main_and_creates_the_branch(spark: Any) -> None:
    """A wap write stacks on the branch and never moves main. pins: ice-wap-branch-1/C-002"""
    observed = _run_cell(spark, "QW-INSERT-NO-BRANCH")
    assert observed["refs"] == [["audit", "branch", 1], ["main", "branch", 0]]
    assert observed["main"] == [[1, "a", "x"], [2, "b", "y"]]
    assert observed["session-read"] == [[1, "a", "x"], [2, "b", "y"], [9, "z", "q"]]


def test_wap_branch_read_keeps_the_arrow_types(spark: Any) -> None:
    """The redirected read keeps the table's Arrow types. pins: ice-wap-branch-1/C-003"""
    qualified = _seed(spark, "QW-READ-ONLY")
    spark.sql(f"INSERT INTO {qualified} VALUES (9, 'z', 'q')").collect()
    main_schema = spark.sql(f"SELECT * FROM {qualified}").to_arrow().schema
    spark.conf.set(WAP_BRANCH_KEY, "audit")
    try:
        branch_table = spark.sql(f"SELECT * FROM {qualified}").to_arrow()
    finally:
        spark.conf.unset(WAP_BRANCH_KEY)
    assert branch_table.schema == main_schema
    assert branch_table.num_rows == 2


def test_sql_set_wap_branch_answers_the_pair_row(spark: Any) -> None:
    """SET spark.wap.branch answers the (key, value) row. pins: ice-wap-branch-1/C-007"""
    answer = spark.sql(f"SET {WAP_BRANCH_KEY} = audit").to_arrow()
    assert answer.to_pylist() == [{"key": WAP_BRANCH_KEY, "value": "audit"}]
    assert spark.conf.get(WAP_BRANCH_KEY) == "audit"
    spark.sql(f"RESET {WAP_BRANCH_KEY}").collect()
    assert spark.conf.get(WAP_BRANCH_KEY, None) is None


def test_native_door_refuses_the_spark_wap_conf(spark: Any) -> None:
    """The ANSI door carries no WAP conf. pins: ice-wap-branch-1/C-008"""
    qualified = _seed(spark, "QW-READ-ONLY")
    with pytest.raises(IllegalArgumentException) as caught:
        repark.sql(f"SET {WAP_BRANCH_KEY} = audit").collect()
    assert "spark" in str(caught.value)
    spark.conf.set(WAP_BRANCH_KEY, "audit")
    try:
        assert _rows(spark, f"SELECT * FROM {qualified}") == [[1, "a", "x"], [2, "b", "y"]]
    finally:
        spark.conf.unset(WAP_BRANCH_KEY)


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The generator re-derives the fixture on live Spark. pins: ice-wap-branch-1/C-009"""
    sparkenv = Path("/tmp/sparkenv/bin/python")
    generator = Path(__file__).with_name("_record_ice_wap_branch_1_oracle.py")
    environ = dict(os.environ)
    environ.setdefault("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64")
    environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    completed = subprocess.run(
        [str(sparkenv), str(generator), "--warehouse", str(tmp_path / "live-wh"), "check"],
        capture_output=True,
        text=True,
        env=environ,
        timeout=1800,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
