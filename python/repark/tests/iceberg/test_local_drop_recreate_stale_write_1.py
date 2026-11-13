"""LOCAL-DROP-RECREATE-STALE-WRITE-1 — a Hadoop drop and recreate reads and writes the new table.

Every cell runs one session over a fresh Hadoop warehouse with the metadata cache on, the
default. Each cell asserts three things: the session read equals the truth; a path read,
which loads through ``StaticTable::from_metadata_file`` and never consults the session cache,
equals the truth; and the current ``vN.metadata.json`` named by ``version-hint.text`` carries
the recreated table's uuid. The live cell has Spark 4.1.2 read the six wrong-write rows by path.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

CATALOG = "sw1"
TABLE = f"{CATALOG}.ns.t"
LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
LIVE_CATALOG = "sw1live"
REPLACE_AS = (
    "CREATE OR REPLACE TABLE {t} AS SELECT CAST(id AS BIGINT) AS id, 1 AS n FROM range(2, 5)"
)
MERGE_INSERT = (
    "MERGE INTO {t} USING (SELECT CAST(9 AS BIGINT) AS id, 1 AS n) s ON {t}.id = s.id "
    "WHEN NOT MATCHED THEN INSERT *"
)
MERGE_UPDATE = (
    "MERGE INTO {t} USING (SELECT CAST(3 AS BIGINT) AS id, 5 AS n) s ON {t}.id = s.id "
    "WHEN MATCHED THEN UPDATE SET n = s.n WHEN NOT MATCHED THEN INSERT *"
)

Rows = list[tuple[int, int]]


def _merge_update(rows: Rows) -> Rows:
    """Apply the id-3 MERGE: update n to 5 when matched, else insert (3, 5)."""
    if any(row_id == 3 for row_id, _ in rows):
        return [(row_id, 5 if row_id == 3 else n) for row_id, n in rows]
    return [*rows, (3, 5)]


def _merge_insert(rows: Rows) -> Rows:
    """Apply the id-9 MERGE: insert (9, 1) unless id 9 is present."""
    return rows if any(row_id == 9 for row_id, _ in rows) else [*rows, (9, 1)]


def _replace_with(key: int) -> tuple[str, Callable[[Rows | None], Rows | None]]:
    """A replace-only step that builds the table as the single row (key, 1)."""
    sql = f"CREATE OR REPLACE TABLE {{t}} AS SELECT CAST({key} AS BIGINT) AS id, 1 AS n"
    return sql, lambda _rows: [(key, 1)]


STEPS: dict[str, tuple[str, Callable[[Any], Rows | None]]] = {
    "C": ("CREATE TABLE {t} (id BIGINT, n INT) USING iceberg", lambda _rows: []),
    "CT": ("CREATE TABLE {t} AS SELECT CAST(9 AS BIGINT) AS id, 1 AS n", lambda _rows: [(9, 1)]),
    "I7": ("INSERT INTO {t} VALUES (7, 1)", lambda rows: [*rows, (7, 1)]),
    "I8": ("INSERT INTO {t} VALUES (8, 1)", lambda rows: [*rows, (8, 1)]),
    "I8Z": ("INSERT INTO {t} VALUES (8, 1, 0)", lambda rows: [*rows, (8, 1)]),
    "R": (REPLACE_AS, lambda _rows: [(2, 1), (3, 1), (4, 1)]),
    "RC": ("CREATE OR REPLACE TABLE {t} (id BIGINT, n INT) USING iceberg", lambda _rows: []),
    "S": ("SELECT * FROM {t}", lambda rows: rows),
    "M": (MERGE_INSERT, _merge_insert),
    "M3": (MERGE_UPDATE, _merge_update),
    "U": ("UPDATE {t} SET n = 5 WHERE id >= 0", lambda rows: [(row_id, 5) for row_id, _ in rows]),
    "DEL": ("DELETE FROM {t} WHERE id = 7", lambda rows: [r for r in rows if r[0] != 7]),
    "A": ("ALTER TABLE {t} ADD COLUMN z INT", lambda rows: rows),
    "D": ("DROP TABLE {t}", lambda _rows: None),
    "DP": ("DROP TABLE {t} PURGE", lambda _rows: None),
    **{f"R{key}": _replace_with(key) for key in range(1, 6)},
}

STALE = "C,I7,R,D,C,R"
WRONG_READ = (STALE, f"{STALE},S", "C,I7,R,D,C,RC", "C,I7,R,D,CT,R")
WRONG_WRITE = tuple(f"{STALE},{tail}" for tail in ("I8", "M", "M3", "U", "DEL", "A,I8Z"))
REPLACE_ONLY = ("R1,R2,R3,D,R4,R5",)
PURGE = ("C,I7,R,DP,C,R",)
CONTROLS = (
    "C,I7,D,C,R",
    "C,I7,I7,D,C,R",
    "C,I7,S,D,C,R",
    "C,R,D,C,R",
    "C,I7,R,D,C,I8",
    "C,I7,R,D,C,M",
    "R1,R2,D,R3,R4",
)

CELLS = [
    pytest.param(sequence, id=f"{kind}-{sequence}")
    for kind, group in (
        ("wrong-read", WRONG_READ),
        ("wrong-write", WRONG_WRITE),
        ("replace-only", REPLACE_ONLY),
        ("purge", PURGE),
        ("control", CONTROLS),
    )
    for sequence in group
]


def truth(sequence: str) -> Rows:
    """Fold the step model over the sequence and return the sorted surviving rows."""
    rows: Rows | None = None
    for step in sequence.split(","):
        rows = STEPS[step][1](rows)
    assert rows is not None, f"{sequence}: the sequence ends dropped"
    return sorted(rows)


def new_session(warehouse: Path, cache: bool = True) -> Any:
    """Build a session with one private Hadoop catalog over the warehouse."""
    _reset_active_session_for_tests()
    builder = (
        ReparkSession.builder.appName("test-local-drop-recreate-stale-write-1")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .config("repark.iceberg.metadataCache", "true" if cache else "false")
    )
    session = builder.getOrCreate()
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.ns").collect()
    return session


def table_dir(warehouse: Path) -> Path:
    """The table directory the Hadoop catalog writes under the warehouse."""
    return warehouse / "ns" / "t"


def disk_uuid(warehouse: Path, name: str) -> str:
    """The table uuid recorded in one metadata file of the table."""
    document = json.loads((table_dir(warehouse) / "metadata" / name).read_text(encoding="utf-8"))
    return str(document["table-uuid"])


def current_file(warehouse: Path) -> str:
    """The metadata file name that ``version-hint.text`` names as current."""
    hint = (table_dir(warehouse) / "metadata" / "version-hint.text").read_text(encoding="utf-8")
    return f"v{hint.strip()}.metadata.json"


def run_sequence(session: Any, warehouse: Path, sequence: str) -> list[str]:
    """Run each step; return the uuid of every incarnation, oldest first."""
    incarnations: list[str] = []
    present = False
    for step in sequence.split(","):
        session.sql(STEPS[step][0].format(t=TABLE)).collect()
        dropped = step in ("D", "DP")
        if not present and not dropped:
            incarnations.append(disk_uuid(warehouse, "v1.metadata.json"))
        present = not dropped
    return incarnations


def session_rows(session: Any) -> Rows:
    """The rows the session reads through its catalog."""
    return sorted(
        (int(row["id"]), int(row["n"]))
        for row in session.sql(f"SELECT id, n FROM {TABLE}").to_arrow().to_pylist()
    )


def path_rows(session: Any, warehouse: Path) -> Rows:
    """The rows a path read of the table directory returns, bypassing the catalog."""
    frame = session.read.format("iceberg").load(str(table_dir(warehouse))).select("id", "n")
    return sorted((int(row["id"]), int(row["n"])) for row in frame.to_arrow().to_pylist())


def assert_cell(session: Any, warehouse: Path, sequence: str, incarnations: list[str]) -> None:
    """Assert the session read, the path read and the current file's uuid."""
    expected = truth(sequence)
    assert session_rows(session) == expected, f"{sequence}: session read"
    assert path_rows(session, warehouse) == expected, f"{sequence}: path read of disk"
    current = current_file(warehouse)
    assert disk_uuid(warehouse, current) == incarnations[-1], f"{sequence}: {current} uuid"
    assert incarnations[-1] not in incarnations[:-1], f"{sequence}: uuid reused"


@pytest.mark.parametrize("sequence", CELLS)
def test_drop_and_recreate_reads_and_writes_the_new_table(tmp_path: Path, sequence: str) -> None:
    """Each matrix row reads the truth in session and on disk, under the new uuid."""
    warehouse = tmp_path / "hadoop"
    session = new_session(warehouse)
    try:
        incarnations = run_sequence(session, warehouse, sequence)
        assert_cell(session, warehouse, sequence, incarnations)
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_cache_off_workaround_reads_and_writes_the_new_table(tmp_path: Path) -> None:
    """The 1.5.6 workaround, ``repark.iceberg.metadataCache=false``, answers the truth."""
    warehouse = tmp_path / "hadoop"
    sequence = WRONG_WRITE[0]
    session = new_session(warehouse, cache=False)
    try:
        incarnations = run_sequence(session, warehouse, sequence)
        assert_cell(session, warehouse, sequence, incarnations)
    finally:
        session.stop()
        _reset_active_session_for_tests()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
@pytest.mark.parametrize("sequence", WRONG_WRITE)
def test_live_spark_reads_the_wrong_write_rows_from_disk(tmp_path: Path, sequence: str) -> None:
    """Spark 4.1.2 reads each wrong-write row from disk as the truth, cache on."""
    import _live_parity as lp

    warehouse = tmp_path / "hadoop"
    session = new_session(warehouse)
    try:
        run_sequence(session, warehouse, sequence)
    finally:
        session.stop()
        _reset_active_session_for_tests()
    spark = lp.build_spark_iceberg_engine(tmp_path / "live", catalog=LIVE_CATALOG).session
    frame = spark.read.format("iceberg").load(str(table_dir(warehouse))).select("id", "n")
    rows = sorted((int(row["id"]), int(row["n"])) for row in frame.collect())
    assert rows == truth(sequence), f"{sequence}: Spark read of disk"
