"""ICE-META-DELETE-1 — a DELETE that covers whole data files deletes the files, on both modes.

Replays the 72 recorded Spark 4.1.2 cells of ``ice_meta_delete_1_spark_oracle.json`` against
RePark's Spark SQL door and asserts, per cell, the surviving rows, the operation of every
snapshot the cell produced, the recorded summary counters, and the live data files.

pins: ice-meta-delete-1/C-001, C-002, C-003, C-004, C-005, C-008
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _record_ice_meta_delete_1 import (
    MODES,
    SHAPES,
    VERSIONS,
    cell_id,
    create_sql,
    keep_summary,
    load_cells,
    table_name,
)

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = load_cells()
DECLARED_DIVERGENCES: dict[str, str] = {
    "prior_deletes_then_rest_v2_mor": (
        "registry ICE-META-DELETE-1-D1 — a v2 merge-on-read DELETE against a data file that "
        "already carries a position-delete file adds a SECOND delete file where Spark rewrites "
        "the superseded one (rows equal; delete-file bookkeeping differs). The row-level "
        "delete-write path, which this unit's decision sits above."
    ),
}
CELLS = [
    pytest.param(
        shape,
        version,
        mode,
        id=cell_id(shape, version, mode),
        marks=(
            [
                pytest.mark.xfail(
                    strict=True,
                    reason=DECLARED_DIVERGENCES[cell_id(shape, version, mode)],
                )
            ]
            if cell_id(shape, version, mode) in DECLARED_DIVERGENCES
            else []
        ),
    )
    for shape in SHAPES
    for version in VERSIONS
    for mode in MODES
]


def _ordered_snapshots(session: ReparkSession, table: str) -> list[dict[str, Any]]:
    """Return the table's snapshots oldest first, walking the parent chain."""
    rows = session.sql(
        f"SELECT snapshot_id, parent_id, operation, summary FROM {table}.snapshots"
    ).collect()
    by_parent: dict[Any, tuple[Any, str, dict[str, str]]] = {}
    for row in rows:
        summary = dict(row[3]) if row[3] else {}
        by_parent[row[1]] = (row[0], row[2], keep_summary(summary))
    ordered: list[dict[str, Any]] = []
    parent: Any = None
    while parent in by_parent:
        snapshot_id, operation, summary = by_parent.pop(parent)
        ordered.append({"op": operation, **summary})
        parent = snapshot_id
    assert not by_parent, f"{table} snapshots do not form one chain: {by_parent}"
    return ordered


def _run_cell(session: ReparkSession, shape: str, version: int, mode: str) -> dict[str, Any]:
    """Seed one cell's table, run its DELETE, and read back the recorded shape."""
    table = table_name(shape, version, mode)
    _, _, seeds, dml = SHAPES[shape]
    cell: dict[str, Any] = {}
    try:
        session.sql(create_sql(shape, version, mode))
        for statement in (*seeds, *dml):
            session.sql(statement.format(t=table))
        cell["rows"] = [
            list(row) for row in session.sql(f"SELECT * FROM {table} ORDER BY id").collect()
        ]
        cell["snapshots"] = _ordered_snapshots(session, table)
        cell["files"] = sorted(
            [row[0], row[1]]
            for row in session.sql(f"SELECT content, record_count FROM {table}.files").collect()
        )
    except Exception as error:
        cell["error"] = f"{type(error).__name__}: {str(error)[:300]}"
    return cell


@pytest.fixture(scope="module")
def replayed(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    """Run all 72 cells once on one RePark session and return their answers."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-meta-delete-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    warehouse = tmp_path_factory.mktemp("ice-meta-delete-1") / "wh"
    session.register_memory_catalog("sc", str(warehouse))
    session.sql("CREATE NAMESPACE sc.ns")
    answers = {
        cell_id(shape, version, mode): _run_cell(session, shape, version, mode)
        for shape in SHAPES
        for version in VERSIONS
        for mode in MODES
    }
    yield answers
    session.stop()
    _reset_active_session_for_tests()


@pytest.mark.parametrize(("shape", "version", "mode"), CELLS)
def test_meta_delete_cell_matches_spark(
    replayed: dict[str, Any], shape: str, version: int, mode: str
) -> None:
    """Rows, every snapshot operation and summary, and the live files equal the Spark cell."""
    key = cell_id(shape, version, mode)
    want = FIXTURE[key]
    got = replayed[key]
    assert "error" not in got, f"{key} refused: {got.get('error')}"
    assert got["rows"] == want["rows"], key
    assert [snapshot["op"] for snapshot in got["snapshots"]] == [
        snapshot["op"] for snapshot in want["snapshots"]
    ], f"{key} snapshot operations: {json.dumps(got['snapshots'])}"
    assert got["snapshots"] == want["snapshots"], key
    assert got["files"] == sorted(want["files"]), key


def test_fixture_provenance_covers_every_cell() -> None:
    """The committed fixture carries all 72 cells and its recorder provenance."""
    document = json.loads(
        Path(__file__).with_name("ice_meta_delete_1_spark_oracle.json").read_text(encoding="utf-8")
    )
    assert document["provenance"]["spark"] == "4.1.2"
    assert document["provenance"]["iceberg"] == "1.11.0"
    assert len(document["cells"]) == 72
    assert sorted(document["cells"]) == sorted(
        cell_id(shape, version, mode) for shape in SHAPES for version in VERSIONS for mode in MODES
    )


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The recorder re-derives every committed cell from live Spark."""
    from _record_ice_meta_delete_1 import main

    assert main(["--check", str(tmp_path / "wh")]) == 0
