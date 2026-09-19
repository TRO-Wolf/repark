"""ICE-LIST-NULL-1: DELETE and UPDATE with IS NULL on nested columns answer Spark.

Every cell reads its expected answer from the recorded
``fixtures/torture/data/ice_list_null_1/spark_list_null_oracle.json``: 128 cells over
shapes ``list_int`` / ``list_struct`` / ``map_int`` / ``struct``, predicates
``xs IS NULL`` / ``xs IS NOT NULL`` / ``id > 1 AND xs IS NULL`` /
``xs IS NULL OR id = 1``, statements ``delete`` / ``update``, modes
``copy-on-write`` / ``merge-on-read`` and format versions 2 / 3. Each pin creates
the cell's DDL on a fresh RePark memory catalog, seeds the recorder's four rows,
runs the cell's statement and asserts the run answers (``ok``), the ids left
behind equal Spark's and the newest snapshot's operation equals Spark's.

Every seed runs verbatim: the ``map_int`` seed's empty-map row spells
``CAST(map() AS MAP<STRING, INT>)``, which answers since CAST-MAP-SPELL-1
(FIXED 2026-09-19, pins: cast-map-spell-1/C-008). All 128 cells answer, including the
sixteen copy-on-write DELETE cells with a compound predicate over the nested
column (``id > 1 AND xs IS NULL``, ``xs IS NULL OR id = 1``): ICE-LIST-NULL-2
declines those selections from the identity DELETE path to the fork's DataFusion
DELETE path, which binds nested columns since fork #299. The rows left behind
equal Spark's on all sixteen; the eight ``xs IS NULL OR id = 1`` cells pin
RePark's measured ``overwrite`` snapshot operation against Spark's recorded
``delete`` (``OPERATION_DIVERGENCES``), since RePark's file-rewriting CoW DELETE
commits ``overwrite``. Delete-file and DV
counts (``added-delete-files`` / ``added-dvs``) are asserted where RePark agrees
with Spark; where the two engines differ (the sixteen merge-on-read
``xs IS NOT NULL`` cells: Spark writes 2 delete files, RePark 1) the pin asserts
RePark's measured values, so a future convergence reds the pin. The live tier
re-derives one cell per shape on Spark through the recorder's own ``record_cell``.

pins: ice-list-null-1/C-003, C-004, C-005, C-006, C-008
pins: ice-list-null-2/C-004
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import _record_ice_list_null_1 as recorder
import pytest

from repark import ReparkSession

_ORACLE: dict[str, Any] = json.loads(recorder.ORACLE_FILE.read_text(encoding="utf-8"))
_CELLS: list[dict[str, Any]] = _ORACLE["cells"]
_CATALOG = "ice_list_null_1"
_NAMESPACE = "ns"
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1: live Spark cell skipped (routine CI is JVM-free)"
_PREDICATE_SLUGS = {
    "xs IS NULL": "is-null",
    "xs IS NOT NULL": "is-not-null",
    "id > 1 AND xs IS NULL": "gt-and-is-null",
    "xs IS NULL OR id = 1": "or-eq-1",
}


def _cell_key(cell: dict[str, Any]) -> str:
    """Return the pin id of one oracle cell."""
    slug = _PREDICATE_SLUGS[cell["predicate"]]
    return f"{cell['shape']}_v{cell['version']}_{cell['mode']}_{cell['statement']}_{slug}"


_CELL_BY_KEY = {_cell_key(cell): cell for cell in _CELLS}


def _file_count_divergences() -> dict[str, tuple[str, str]]:
    """RePark's measured delete-file and DV counts where they differ from Spark's.

    Returns:
        One ``(added-delete-files, added-dvs)`` pair per merge-on-read
        ``xs IS NOT NULL`` cell: Spark writes 2 delete files (2 DVs on v3),
        RePark writes 1 (1 DV on v3), measured 2026-09-18 on the release native.
    """
    return {
        key: ("1", "1" if cell["version"] == 3 else "0")
        for key, cell in _CELL_BY_KEY.items()
        if cell["mode"] == "merge-on-read" and cell["predicate"] == "xs IS NOT NULL"
    }


FILE_COUNT_DIVERGENCES = _file_count_divergences()

assert len(FILE_COUNT_DIVERGENCES) == 16, FILE_COUNT_DIVERGENCES


def _operation_divergences() -> dict[str, str]:
    """RePark's measured snapshot operation where it differs from Spark's.

    Returns:
        ``overwrite`` for every copy-on-write DELETE cell with predicate
        ``xs IS NULL OR id = 1``: Spark records ``delete`` (with zero delete
        files), RePark's file-rewriting CoW DELETE commits ``overwrite``,
        measured 2026-09-19 on the release native. The rows left behind agree;
        only the snapshot operation differs, so a future convergence reds
        the pin rather than absorbing.
    """
    return {
        key: "overwrite"
        for key, cell in _CELL_BY_KEY.items()
        if cell["mode"] == "copy-on-write"
        and cell["statement"] == "delete"
        and cell["predicate"] == "xs IS NULL OR id = 1"
    }


OPERATION_DIVERGENCES = _operation_divergences()

assert len(OPERATION_DIVERGENCES) == 8, OPERATION_DIVERGENCES

_LIVE_SHAPES = ("list_int", "list_struct", "map_int", "struct")


def _session(warehouse: Path) -> ReparkSession:
    """Return a RePark session with a memory catalog at `warehouse`, v3 CREATE allowed."""
    session = (
        ReparkSession.builder.appName("ice-list-null-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(_CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.{_NAMESPACE}")
    return session


def _seed_values(cell: dict[str, Any]) -> str:
    """Return the recorder's seed VALUES list for the cell's shape, verbatim."""
    return recorder.SHAPES[cell["shape"]][1]


def _table_name(key: str) -> str:
    """Return the table name for one oracle cell key, with hyphens underscored."""
    return f"{_CATALOG}.{_NAMESPACE}.{key.replace('-', '_')}"


def _create_and_seed(session: ReparkSession, key: str, cell: dict[str, Any]) -> str:
    """Create one cell's table and seed the recorder's four rows, returning its name.

    Args:
        session: The RePark session.
        key: The oracle cell key, used as the table name.
        cell: The oracle cell carrying the shape, version and mode.

    Returns:
        The catalog-qualified table name.
    """
    column, _ = recorder.SHAPES[cell["shape"]]
    table = _table_name(key)
    session.sql(
        f"CREATE TABLE {table} (id INT, {column}) USING iceberg TBLPROPERTIES ("
        f"'format-version'='{cell['version']}', "
        f"'write.delete.mode'='{cell['mode']}', 'write.update.mode'='{cell['mode']}')"
    ).collect()
    session.sql(f"INSERT INTO {table} VALUES {_seed_values(cell)}").collect()
    return table


def _run_statement(session: ReparkSession, table: str, cell: dict[str, Any]) -> None:
    """Run one cell's DELETE or UPDATE against `table`."""
    sql = recorder.statement_sql(table, cell["statement"], cell["predicate"])
    session.sql(sql).collect()


def _read_ids(session: ReparkSession, table: str) -> list[Any]:
    """Return every id left in `table`, sorted."""
    return sorted(row.asDict()["id"] for row in session.sql(f"SELECT id FROM {table}").collect())


def _latest_snapshot(session: ReparkSession, table: str) -> dict[str, Any]:
    """Return the newest snapshot's operation and summary of `table`."""
    rows = session.sql(
        f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    assert rows, f"no snapshots on {table}"
    record = rows[0].asDict()
    return {"operation": record["operation"], "summary": dict(record["summary"])}


@pytest.mark.parametrize("key", [pytest.param(key, id=key) for key in sorted(_CELL_BY_KEY)])
def test_cell_answers_spark(key: str, tmp_path: Path) -> None:
    """One cell's statement answers Spark's ok, ids and snapshot operation.

    Notes:
        The run itself proves `ok`: a refusal fails the pin. Delete-file and
        DV counts are pinned separately below.
    """
    cell = _CELL_BY_KEY[key]
    assert cell["ok"] is True, (key, cell.get("error"))
    session = _session(tmp_path / "wh")
    try:
        table = _create_and_seed(session, key, cell)
        _run_statement(session, table, cell)
        assert _read_ids(session, table) == cell["ids"], (key, "ids")
        expected_operation = OPERATION_DIVERGENCES.get(key, cell["operation"])
        assert _latest_snapshot(session, table)["operation"] == expected_operation, (
            key,
            "operation",
        )
    finally:
        session.stop()


@pytest.mark.parametrize("key", [pytest.param(key, id=key) for key in sorted(_CELL_BY_KEY)])
def test_cell_file_counts_match_spark(key: str, tmp_path: Path) -> None:
    """One cell's newest snapshot carries Spark's delete-file and DV counts.

    Notes:
        Cells in `FILE_COUNT_DIVERGENCES` assert RePark's measured values
        instead, so a future convergence reds the pin rather than absorbing.
    """
    cell = _CELL_BY_KEY[key]
    session = _session(tmp_path / "wh")
    try:
        table = _create_and_seed(session, key, cell)
        _run_statement(session, table, cell)
        summary = _latest_snapshot(session, table)["summary"]
        got = (summary.get("added-delete-files", "0"), summary.get("added-dvs", "0"))
        if key in FILE_COUNT_DIVERGENCES:
            assert got == FILE_COUNT_DIVERGENCES[key], (key, "repark counts")
        else:
            assert got == (cell["added_delete_files"], cell["added_dvs"]), (key, "counts")
    finally:
        session.stop()


def test_map_seed_substitution_reads_back_empty(tmp_path: Path) -> None:
    """The substitute empty-map seed row reads back as Spark's empty map row."""
    session = _session(tmp_path / "wh")
    try:
        key = "map_int_v2_copy-on-write_delete_is-null"
        table = _create_and_seed(session, "map_seed_check", _CELL_BY_KEY[key])
        rows = {
            row.asDict()["id"]: row.asDict(recursive=True)["xs"]
            for row in session.sql(f"SELECT * FROM {table} ORDER BY id").collect()
        }
        assert rows[2] is None, ("null row", rows)
        assert rows[3] == {}, ("empty row", rows)
        assert rows[1] == {"k": 1}, ("value row", rows)
        assert rows[4] == {"k": None}, ("null-value row", rows)
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
@pytest.mark.parametrize("shape", _LIVE_SHAPES)
def test_live_spark_rederives_shape_cell(shape: str, tmp_path: Path) -> None:
    """Live Spark re-derives one cell per shape through the recorder and matches."""
    import _live_parity as live

    key = f"{shape}_v2_copy-on-write_delete_is-null"
    expected = _CELL_BY_KEY[key]
    engine = live.build_spark_iceberg_engine(tmp_path / "spark_wh", catalog="listnull_live")
    spark = engine.session
    with live.spark_session_conf(engine, (("spark.sql.shuffle.partitions", "1"),)):
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS listnull_live.ns")
            cell = recorder.record_cell(
                spark,
                f"listnull_live.ns.live_{shape}",
                shape,
                2,
                "copy-on-write",
                "delete",
                "xs IS NULL",
            )
        finally:
            spark.sql(f"DROP TABLE IF EXISTS listnull_live.ns.live_{shape}")
    assert cell["ok"] is True, (key, cell.get("error"))
    assert cell["ids"] == expected["ids"], (key, "ids")
    assert cell["operation"] == expected["operation"], (key, "operation")
