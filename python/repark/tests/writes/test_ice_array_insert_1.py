"""ICE-ARRAY-INSERT-1 — inserts into array columns answer Spark 4.1.2 on every door.

Every cell reads its expected answer from the recorded
`fixtures/torture/data/ice_array_insert_1/spark_array_insert_oracle.json`: 30 cells keyed
`{shape}_{door}_v{version}` over shapes `list_int` / `list_struct` / `map_list`, doors
`sql_values` / `sql_select` / `writeto_append` / `insert_into` / `save_as_table_append`
and format versions 2 / 3. Each pin creates the cell's DDL on a fresh RePark memory
catalog, writes through the cell's door, and asserts the rows read back (`ORDER BY id`,
`asDict(recursive=True)`) equal the cell's rows and the first data file's parquet footer
field ids — walked with the recorder's own `field_ids` — equal the cell's ids. Spark's
file count is recorded but never pinned: it follows Spark's task count.

Every cell runs its recorded statement verbatim, including the eight non-VALUES
`map_list` cells whose NULL-map row spells `CAST(NULL AS MAP<STRING, ARRAY<INT>>)`
(CAST-MAP-SPELL-1, FIXED 2026-09-19). The live tier re-derives the `sql_values` shape on
Spark and cross-reads RePark's table.

pins: ice-array-insert-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
pins: cast-map-spell-1/C-008
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import _record_ice_array_insert_1 as recorder
import pytest

from repark import ReparkSession

_ORACLE: dict[str, Any] = json.loads(recorder.ORACLE_FILE.read_text(encoding="utf-8"))
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]
_CELL_KEYS = tuple(sorted(_CELLS))
_CATALOG = "ice_array_insert_1"
_NAMESPACE = "ns"
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_LIVE_SHAPES = ("list_int", "list_struct", "map_list")
_LIVE_VERSIONS = ("2", "3")


def _session(warehouse: Path) -> ReparkSession:
    """Return a RePark session with a memory catalog at `warehouse`, v3 CREATE allowed."""
    session = (
        ReparkSession.builder.appName("ice-array-insert-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(_CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.{_NAMESPACE}")
    return session


def _write_cell(session: ReparkSession, table: str, cell: dict[str, Any], statement: str) -> None:
    """Write one cell's statement into `table` through the cell's door.

    Args:
        session: The RePark session.
        table: The catalog-qualified table name, already created and empty.
        cell: The oracle cell carrying the `door`.
        statement: The write to run (`<t>` already pointed at `table`).
    """
    door = cell["door"]
    if door in ("sql_values", "sql_select"):
        session.sql(statement).collect()
    elif door == "writeto_append":
        session.sql(statement).writeTo(table).append()
    elif door == "insert_into":
        session.sql(statement).write.insertInto(table)
    else:
        session.sql(statement).write.format("iceberg").mode("append").saveAsTable(table)


def _read_rows(session: ReparkSession, table: str) -> list[dict[str, Any]]:
    """Return every row of `table` ordered by id as recursive dicts."""
    return [
        row.asDict(recursive=True)
        for row in session.sql(f"SELECT * FROM {table} ORDER BY id").collect()
    ]


def _footer_ids(session: ReparkSession, table: str) -> list[list[str]]:
    """Return the first data file's parquet footer field ids in schema order."""
    paths = sorted(
        row.asDict()["file_path"]
        for row in session.sql(f"SELECT file_path FROM {table}.files").collect()
    )
    return recorder.field_ids(paths[0])


def _create_and_write(
    session: ReparkSession, key: str, cell: dict[str, Any], statement: str
) -> str:
    """Create one cell's table and write its statement, returning the table name.

    Args:
        session: The RePark session.
        key: The oracle cell key, used as the table name.
        cell: The oracle cell carrying `table_ddl` and `door`.
        statement: The write to run with `<t>` pointed at the new table.

    Returns:
        The catalog-qualified table name.
    """
    table = f"{_CATALOG}.{_NAMESPACE}.{key}"
    session.sql(cell["table_ddl"].replace("<t>", table)).collect()
    _write_cell(session, table, cell, statement.replace("<t>", table))
    return table


def _assert_cell(session: ReparkSession, table: str, cell: dict[str, Any], key: str) -> None:
    """Assert one written table answers the cell's rows and footer field ids.

    Args:
        session: The RePark session.
        table: The catalog-qualified table the cell wrote.
        cell: The oracle cell carrying `rows` and `parquet_field_ids`.
        key: The oracle cell key, naming the failure.
    """
    assert cell.get("error") is None, (key, cell.get("error"))
    assert _read_rows(session, table) == cell["rows"], (key, "rows")
    assert _footer_ids(session, table) == cell["parquet_field_ids"], (key, "field ids")


@pytest.mark.parametrize("key", _CELL_KEYS)
def test_cell_matches_spark(key: str, tmp_path: Path) -> None:
    """One cell's DDL plus its recorded write answers Spark's rows and footer ids."""
    cell = _CELLS[key]
    session = _session(tmp_path / "wh")
    try:
        table = _create_and_write(session, key, cell, cell["statement"])
        _assert_cell(session, table, cell, key)
    finally:
        session.stop()


def _latest_metadata(warehouse: Path) -> Path:
    """Return the newest metadata JSON under a RePark memory-catalog warehouse."""
    return sorted(warehouse.rglob("*.metadata.json"))[-1]


def _live_spark_session(warehouse: Path, catalog: str) -> Any:
    """Return the shared live PySpark session with a private Hadoop catalog at `warehouse`."""
    import _live_parity as live

    return live.build_spark_iceberg_engine(warehouse, catalog=catalog).session


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
@pytest.mark.parametrize("version", _LIVE_VERSIONS)
@pytest.mark.parametrize("shape", _LIVE_SHAPES)
def test_live_spark_reads_repark_table(shape: str, version: str, tmp_path: Path) -> None:
    """Live Spark adopts a RePark-written table and reads the recorded cell's rows."""
    key = f"{shape}_sql_values_v{version}"
    cell = _CELLS[key]
    session = _session(tmp_path / "repark_wh")
    try:
        table = _create_and_write(session, key, cell, cell["statement"])
        assert _read_rows(session, table) == cell["rows"], (key, "repark rows")
        metadata = _latest_metadata(tmp_path / "repark_wh")
        catalog = f"arrlive_{shape}_{version}"
        spark = _live_spark_session(tmp_path / "spark_wh", catalog)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{_NAMESPACE}")
        spark.sql(
            f"CALL {catalog}.system.register_table("
            f"table => '{_NAMESPACE}.{key}', metadata_file => '{metadata}')"
        ).collect()
        spark_rows = [
            row.asDict(recursive=True)
            for row in spark.sql(
                f"SELECT * FROM {catalog}.{_NAMESPACE}.{key} ORDER BY id"
            ).collect()
        ]
    finally:
        session.stop()
    assert spark_rows == cell["rows"], (key, "spark rows")
