"""ICE-REPLACE-COLUMNS-1 — ``ALTER TABLE … REPLACE COLUMNS`` drops and re-adds every column.

Oracle: ``ice_replace_columns_1_spark_oracle.json`` (live PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0, re-derived by
``_record_ice_replace_columns_1_oracle.py``). Spark lowers Hive-style REPLACE
COLUMNS to ``DeleteColumn`` for every current top-level column plus one
``AddColumn`` per listed column, so every field id is fresh and every existing
row reads NULL through the new ids. The catalog is ``sc`` and the table names
are the oracle's, so refusal texts line up with the fixture.

One test per measured cell (v2 and v3) asserts the field ids, the required
flags, ``last-column-id``, the schema count, the schema and the data — and for
a refusal the exception class plus Spark's message substring. The native
``repark.sql`` door has no Hive-style REPLACE COLUMNS and is pinned at its
registered parse refusal.

pins: ice-replace-columns-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
from _record_ice_replace_columns_1_oracle import (
    normalize_rows,
    schema_observations,
)

import repark
from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_replace_columns_1_spark_oracle.json").read_text(encoding="utf-8")
)
CELLS: dict[str, Any] = FIXTURE["cells"]

REFUSALS: dict[str, tuple[type[Exception], str]] = {
    "NOT-NULL": (ParseException, "NOT NULL is not supported in Hive-style REPLACE COLUMNS."),
    "DUP": (AnalysisException, "[COLUMN_ALREADY_EXISTS] The column `id` already exists."),
    "PART-DROP-SOURCE": (
        PySparkException,
        "Cannot find source column for partition field: 1000: cat: identity(3)",
    ),
    "PART-KEEP-NAME": (
        PySparkException,
        "Cannot find source column for partition field: 1000: cat: identity(3)",
    ),
    "SORTED": (
        PySparkException,
        "Cannot find source column for sort field: identity(1) ASC NULLS FIRST",
    ),
    "IDENTIFIER": (UnsupportedOperationException, "SET NOT NULL"),
}


def _case(cell_id: str) -> str:
    return cell_id.removeprefix("RC-").rsplit("-V", 1)[0]


REFUSED_REPLACE_CELLS = sorted(
    cell_id
    for cell_id, cell in CELLS.items()
    if cell["status"] == "error" and _case(cell_id) != "IDENTIFIER"
)


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """A RePark facade session with the oracle's catalog name and namespace."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-replace-columns-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", tmp_path / "wh")
    session.sql("CREATE NAMESPACE sc.ns")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _metadata(warehouse: Path, table: str) -> dict[str, Any]:
    name = table.split(".")[-1]
    files = [
        path
        for path in warehouse.rglob("metadata/*.metadata.json")
        if path.parent.parent.name == name
    ]
    latest = max(files, key=lambda path: (path.stat().st_mtime_ns, path.name))
    return json.loads(latest.read_text(encoding="utf-8"))


def _first_snapshot(metadata: dict[str, Any]) -> int:
    snapshots = sorted(
        metadata["snapshots"],
        key=lambda snapshot: (snapshot["timestamp-ms"], snapshot["snapshot-id"]),
    )
    return int(snapshots[0]["snapshot-id"])


def _observations(session: Any, warehouse: Path, cell: dict[str, Any]) -> dict[str, Any]:
    table = cell["table"]
    metadata = _metadata(warehouse, table)
    observations = schema_observations(metadata)
    observations["data"] = normalize_rows(
        [list(row) for row in session.sql(f"SELECT * FROM {table}").collect()]
    )
    if cell["time_travel_first_snapshot"]:
        snapshot = _first_snapshot(metadata)
        observations["time-travel-rows"] = normalize_rows(
            [
                list(row)
                for row in session.sql(f"SELECT * FROM {table} VERSION AS OF {snapshot}").collect()
            ]
        )
    return observations


def _run(session: Any, cell: dict[str, Any]) -> None:
    for statement in (cell["create"], *cell["seed"], *cell["statements"]):
        session.sql(statement).collect()


@pytest.mark.parametrize("cell_id", sorted(CELLS))
def test_replace_columns_cell_matches_spark(cell_id: str, spark: Any, tmp_path: Path) -> None:
    """Every measured RC-* cell: RePark's schema, ids and rows are Spark's."""
    cell = CELLS[cell_id]
    warehouse = tmp_path / "wh"
    if cell["status"] == "ok":
        _run(spark, cell)
        assert _observations(spark, warehouse, cell) == cell["obs"]
        return
    exception, substring = REFUSALS[_case(cell_id)]
    with pytest.raises(exception) as caught:
        _run(spark, cell)
    assert substring in str(caught.value)


@pytest.mark.parametrize("cell_id", REFUSED_REPLACE_CELLS)
def test_refused_replace_columns_leaves_the_table_untouched(
    cell_id: str, spark: Any, tmp_path: Path
) -> None:
    """A refused REPLACE COLUMNS commits nothing: the seeded schema and rows survive."""
    cell = CELLS[cell_id]
    statements = [cell["create"], *cell["seed"], *cell["statements"]]
    failing = statements.index(cell["error"]["failing_statement"])
    for statement in statements[:failing]:
        spark.sql(statement).collect()
    with pytest.raises(REFUSALS[_case(cell_id)][0]):
        spark.sql(statements[failing]).collect()
    observations = schema_observations(_metadata(tmp_path / "wh", cell["table"]))
    assert [name for name, _, _ in observations["field-ids"]] == ["id", "data", "cat"]
    assert [field[1] for field in observations["field-ids"]] == [1, 2, 3]
    rows = normalize_rows(
        [list(row) for row in spark.sql(f"SELECT * FROM {cell['table']}").collect()]
    )
    assert rows == [[1, "a", "x"], [2, "b", "y"]]


def test_native_door_refuses_hive_style_replace_columns(spark: Any) -> None:
    """The native ANSI door has no Hive-style REPLACE COLUMNS — it refuses at the parser."""
    spark.sql("CREATE TABLE sc.ns.native_rc (id BIGINT, data STRING) USING iceberg").collect()
    with pytest.raises(ParseException) as caught:
        repark.sql("ALTER TABLE sc.ns.native_rc REPLACE COLUMNS (id BIGINT)").collect()
    assert "REPLACE" in str(caught.value)
    names = [field.name for field in spark.sql("SELECT * FROM sc.ns.native_rc").to_arrow().schema]
    assert names == ["id", "data"]


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The recorder re-derives the fixture on live Spark. pins: ice-replace-columns-1/C-001"""
    sparkenv = Path("/tmp/sparkenv/bin/python")
    generator = Path(__file__).with_name("_record_ice_replace_columns_1_oracle.py")
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
