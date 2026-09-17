"""ICE-SORTED-INSERT-1 — plain INSERT sorts and stamps per the declared order.

pins: ice-sorted-insert-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import datetime
import json
import math
import os
import shutil
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from repark import ReparkSession

_HERE = Path(__file__).resolve().parent
_ORACLE = json.loads((_HERE / "ice_sorted_insert_1_spark_oracle.json").read_text())
LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live sort oracle is skipped (CI is JVM-free)"

CATALOG = "icesorted1"
CANONICAL_ROOT = Path("/tmp/repark-ice-sorted-insert-1")
FIXTURE_DAYS = _HERE / "fixtures" / "ice_sorted_insert_1" / "days"
TRANSFORM_REFUSAL = "only identity sort fields are supported"


def _session(name: str, warehouse: Path) -> ReparkSession:
    """One memory-catalog engine."""
    engine = ReparkSession.builder.appName(name).getOrCreate()
    engine.register_memory_catalog(CATALOG, warehouse)
    engine.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    return engine


def _files(engine: ReparkSession, table: str) -> list[dict[str, Any]]:
    """file_path / record_count / sort_order_id per live data file."""
    arrow = engine.sql(
        f"SELECT file_path, record_count, sort_order_id FROM {table}.files"
    ).to_arrow()
    return [
        {
            "file_path": path.as_py(),
            "record_count": int(count.as_py()),
            "sort_order_id": stamp.as_py(),
        }
        for path, count, stamp in zip(
            arrow.column("file_path"),
            arrow.column("record_count"),
            arrow.column("sort_order_id"),
            strict=True,
        )
    ]


def _is_sorted_int(keys: list[tuple[Any, ...]], spec: list[tuple[bool, bool]]) -> bool:
    """Sortedness of int-or-None key tuples under (desc, nulls_first) per key."""

    def key_of(item: tuple[Any, ...]) -> tuple[tuple[int, int], ...]:
        return tuple(
            (
                0 if (value is None) == nulls_first else 1,
                0 if value is None else (-value if desc else value),
            )
            for value, (desc, nulls_first) in zip(item, spec, strict=True)
        )

    return [key_of(k) for k in keys] == sorted(key_of(k) for k in keys)


def _is_sorted_float(keys: list[float | None]) -> bool:
    """Sortedness under ASC NULLS FIRST with NaN largest, the Spark order."""

    def key_of(value: float | None) -> tuple[int, float]:
        if value is None:
            return (0, 0.0)
        if isinstance(value, float) and math.isnan(value):
            return (2, math.inf)
        return (1, float(value))

    return [key_of(k) for k in keys] == sorted(key_of(k) for k in keys)


def _column(path: str, name: str) -> list[Any]:
    """One parquet column as plain Python values."""
    return pq.read_table(path, columns=[name]).column(name).to_pylist()


def _assert_identity_cell(
    engine: ReparkSession,
    table: str,
    columns: list[str],
    spec: list[tuple[bool, bool]] | str,
    expected_ids: list[int],
    order_id: int = 1,
) -> None:
    """Every file sorted on the order keys, stamped, holding the full row set."""
    files = _files(engine, table)
    assert files, table
    seen: list[int] = []
    for entry in files:
        assert entry["record_count"] > 0, entry
        assert entry["sort_order_id"] == order_id, entry
        if spec == "float":
            assert _is_sorted_float(_column(entry["file_path"], "f")), entry
            seen.extend(_column(entry["file_path"], "id"))
        elif spec == "day":
            stamps = _column(entry["file_path"], "ts")
            days = [
                None if ts is None else (ts.date() - datetime.date(1970, 1, 1)).days
                for ts in stamps
            ]
            keys = list(zip(days, _column(entry["file_path"], "id"), strict=True))
            assert _is_sorted_int(keys, [(False, True), (False, True)]), entry
            seen.extend(_column(entry["file_path"], "id"))
        else:
            assert isinstance(spec, list)
            keys = list(
                zip(
                    *[_column(entry["file_path"], name) for name in columns],
                    strict=True,
                )
            )
            assert _is_sorted_int(keys, spec), entry
            seen.extend(_column(entry["file_path"], "id"))
    assert sorted(seen) == expected_ids


def _shuffled_insert(engine: ReparkSession, table: str, select: str) -> None:
    """INSERT INTO over range(2000) with the oracle's shuffled expressions."""
    engine.range(2000).createOrReplaceTempView("r")
    engine.sql(f"INSERT INTO {table} {select}").collect()


SQL_CELLS = {
    "so1": {
        "ddl": "(id BIGINT, p INT) USING iceberg PARTITIONED BY (p)",
        "order": "WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY (id)",
        "select": "SELECT (id * 7919) % 2000 AS id, CAST(id % 2 AS INT) AS p FROM r",
        "columns": ["id"],
        "spec": [(False, True)],
    },
    "so2": {
        "ddl": "(id BIGINT, s STRING) USING iceberg",
        "order": "WRITE ORDERED BY (id DESC)",
        "select": "SELECT (id * 7919) % 2000 AS id, CAST(id AS STRING) FROM r",
        "columns": ["id"],
        "spec": [(True, False)],
    },
    "so3": {
        "ddl": "(a INT, b BIGINT, id BIGINT) USING iceberg",
        "order": "WRITE ORDERED BY (a ASC NULLS LAST, b DESC NULLS FIRST)",
        "select": "SELECT CASE WHEN id % 7 = 0 THEN NULL ELSE CAST(id % 5 AS INT)"
        " END AS a, CASE WHEN id % 11 = 0 THEN NULL ELSE (id * 31) % 97 END AS b,"
        " id FROM r",
        "columns": ["a", "b"],
        "spec": [(False, False), (True, True)],
    },
    "so5": {
        "ddl": "(id BIGINT, p INT) USING iceberg PARTITIONED BY (p)",
        "order": "WRITE LOCALLY ORDERED BY (id)",
        "select": "SELECT (id * 7919) % 2000 AS id, CAST(id % 2 AS INT) AS p FROM r",
        "columns": ["id"],
        "spec": [(False, True)],
    },
    "so8": {
        "ddl": "(id BIGINT, f FLOAT) USING iceberg",
        "order": "WRITE ORDERED BY (f)",
        "select": "SELECT id, CASE WHEN id % 13 = 0 THEN CAST('NaN' AS FLOAT)"
        " WHEN id % 17 = 0 THEN CAST(NULL AS FLOAT)"
        " ELSE CAST((id * 7919) % 2000 AS FLOAT) END AS f FROM r",
        "columns": ["id", "f"],
        "spec": "float",
    },
}


@pytest.mark.parametrize("cell", sorted(SQL_CELLS))
def test_sql_insert_writes_sorted_stamped_files(tmp_path: Path, cell: str) -> None:
    """C-001: SQL INSERT INTO commits sorted files stamped with the order id."""
    shape = SQL_CELLS[cell]
    engine = _session(f"sorted-sql-{cell}", tmp_path / "wh")
    try:
        table = f"{CATALOG}.w.{cell}"
        engine.sql(f"CREATE TABLE {table} {shape['ddl']}").collect()
        engine.sql(f"ALTER TABLE {table} {shape['order']}").collect()
        _shuffled_insert(engine, table, shape["select"])
        _assert_identity_cell(engine, table, shape["columns"], shape["spec"], list(range(2000)))
    finally:
        engine.stop()


@pytest.mark.parametrize("door", ["writeTo", "saveAsTable", "insertInto"])
def test_dataframe_doors_write_sorted_stamped_files(tmp_path: Path, door: str) -> None:
    """C-002: writeTo/append, saveAsTable/append and insertInto sort and stamp."""
    engine = _session(f"sorted-df-{door}", tmp_path / "wh")
    try:
        table = f"{CATALOG}.w.df_{door}"
        engine.sql(
            f"CREATE TABLE {table} (id BIGINT, p INT) USING iceberg PARTITIONED BY (p)"
        ).collect()
        engine.sql(f"ALTER TABLE {table} WRITE ORDERED BY (id)").collect()
        frame = engine.range(2000).selectExpr(
            "(id * 7919) % 2000 AS id", "CAST(id % 2 AS INT) AS p"
        )
        if door == "writeTo":
            frame.writeTo(f"{CATALOG}.w.df_{door}").append()
        elif door == "saveAsTable":
            frame.write.format("iceberg").mode("append").saveAsTable(f"{CATALOG}.w.df_{door}")
        else:
            frame.write.format("iceberg").insertInto(f"{CATALOG}.w.df_{door}")
        _assert_identity_cell(engine, table, ["id"], [(False, True)], list(range(2000)))
    finally:
        engine.stop()


def test_repark_owned_paths_write_sorted_stamped_files(tmp_path: Path) -> None:
    """C-003: INSERT OVERWRITE, MERGE and CTAS stamp the files they sort."""
    engine = _session("sorted-owned", tmp_path / "wh")
    try:
        engine.range(2000).createOrReplaceTempView("r")
        table = f"{CATALOG}.w.owned"
        engine.sql(
            f"CREATE TABLE {table} (id BIGINT, p INT) USING iceberg PARTITIONED BY (p)"
        ).collect()
        engine.sql(f"ALTER TABLE {table} WRITE ORDERED BY (id)").collect()
        engine.sql(
            f"INSERT OVERWRITE {table} SELECT (id * 7919) % 2000 AS id,"
            " CAST(id % 2 AS INT) AS p FROM r"
        ).collect()
        _assert_identity_cell(engine, table, ["id"], [(False, True)], list(range(2000)))
        engine.sql(
            f"MERGE INTO {table} t USING (SELECT CAST(1 AS BIGINT) AS id,"
            " CAST(1 AS INT) AS p) s ON t.id = s.id"
            " WHEN NOT MATCHED THEN INSERT *"
        ).collect()
        _assert_identity_cell(engine, table, ["id"], [(False, True)], list(range(2000)))
        engine.sql(
            f"CREATE OR REPLACE TABLE {table} USING iceberg PARTITIONED BY (p)"
            " AS SELECT (id * 7919) % 2000 AS id,"
            " CAST(id % 2 AS INT) AS p FROM r"
        ).collect()
        files = _files(engine, table)
        assert files, table
        for entry in files:
            assert entry["sort_order_id"] == 0, entry
        seen = [row for entry in files for row in _column(entry["file_path"], "id")]
        assert sorted(seen) == list(range(2000))
    finally:
        engine.stop()


def test_days_transform_insert_sorts_and_stamps(tmp_path: Path) -> None:
    """C-003: plain INSERT into an adopted days-ordered table sorts and stamps."""
    if CANONICAL_ROOT.exists():
        shutil.rmtree(CANONICAL_ROOT)
    shutil.copytree(FIXTURE_DAYS, CANONICAL_ROOT / "wh" / "ns" / "so7")
    warehouse = tmp_path / "wh"
    engine = _session("sorted-days", warehouse)
    try:
        metadata = CANONICAL_ROOT / "wh" / "ns" / "so7" / "metadata"
        version = (metadata / "version-hint.text").read_text().strip()
        newest = sorted(metadata.glob(f"v{version}*.metadata.json"))[-1]
        engine.sql(
            f"CALL {CATALOG}.system.register_table(table => 'w.so7', metadata_file => '{newest}')"
        ).collect()
        table = f"{CATALOG}.w.so7"
        engine.range(500).createOrReplaceTempView("r")
        engine.sql(
            f"INSERT INTO {table} SELECT (id * 7919) % 2000 AS id,"
            " CAST(DATE_ADD(DATE '2026-02-01', CAST(id % 40 AS INT)) AS TIMESTAMP)"
            " AS ts FROM r"
        ).collect()
        _assert_identity_cell(
            engine,
            table,
            ["id", "ts"],
            "day",
            sorted(list(range(2000)) + [(i * 7919) % 2000 for i in range(500)]),
        )
        with pytest.raises(Exception, match=TRANSFORM_REFUSAL):
            engine.sql(f"INSERT OVERWRITE {table} SELECT id, ts FROM {table}").collect()
    finally:
        engine.stop()
        shutil.rmtree(CANONICAL_ROOT, ignore_errors=True)


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
@pytest.mark.parametrize("cell", sorted(_ORACLE["cells"]))
def test_live_cells_match_fixture(tmp_path: Path, cell: str) -> None:
    """C-004: live Spark re-derives every oracle cell from the recorded DDL."""
    import _live_parity as live_parity

    recorded = _ORACLE["cells"][cell]
    oracle = live_parity.build_spark_iceberg_engine(
        tmp_path / "spark-wh", catalog="ice_sorted_insert_1_live"
    )
    session = oracle.session
    try:
        session.conf.set("spark.sql.shuffle.partitions", "200")
        session.sql("CREATE NAMESPACE IF NOT EXISTS ice_sorted_insert_1_live.ns")
        session.range(2000).createOrReplaceTempView("r")
        table = f"ice_sorted_insert_1_live.ns.{cell}"
        session.sql(f"CREATE TABLE {table} {recorded['ddl']}")
        if recorded["order"]:
            session.sql(f"ALTER TABLE {table} {recorded['order']}")
        if recorded["door"] == "sql":
            session.sql(recorded["insert"].format(t=table))
        else:
            session.sql(recorded["insert"]).writeTo(table).append()
        want = sorted((f["records"], f["sort_order_id"], f["sorted"]) for f in recorded["files"])
        got = sorted(
            (f["records"], f["sort_order_id"], f["sorted"])
            for f in _live_files(session, table, recorded)
        )
        assert got == want, (cell, got, want)
    finally:
        session.conf.set("spark.sql.shuffle.partitions", "2")


def _live_files(session: Any, table: str, recorded: dict[str, Any]) -> list[dict]:
    """Per-file (records, stamp, sorted) recomputed on the live session."""
    select = _live_select(recorded)
    by_file: dict[str, list[dict[str, Any]]] = {}
    for row in session.sql(f"SELECT input_file_name() AS fp, {select} FROM {table}").collect():
        by_file.setdefault(row.fp, []).append(row.asDict())
    stamps = {
        row.file_path.rsplit("/", 1)[-1]: (row.record_count, row.sort_order_id)
        for row in session.sql(f"SELECT * FROM {table}.files").collect()
    }
    out = []
    for path, values in by_file.items():
        records, stamp = stamps[path.rsplit("/", 1)[-1]]
        out.append(
            {
                "records": records,
                "sort_order_id": stamp,
                "sorted": _live_sorted(recorded, values),
            }
        )
    return out


def _live_select(recorded: dict[str, Any]) -> str:
    """The value columns one oracle cell reads back."""
    ddl = recorded["ddl"]
    if "TIMESTAMP" in ddl:
        return "id, ts"
    if "FLOAT" in ddl:
        return "id, f"
    if "(a INT, b BIGINT" in ddl:
        return "a, b"
    return "id"


def _live_sorted(recorded: dict[str, Any], values: list[dict[str, Any]]) -> bool:
    """Sortedness under the cell's recorded order."""
    ddl = recorded["ddl"]
    order = recorded["order"]
    if "TIMESTAMP" in ddl:
        keys = [
            (
                (v["ts"].date() - datetime.date(1970, 1, 1)).days,
                v["id"],
            )
            for v in values
        ]
        return _is_sorted_int(keys, [(False, True), (False, True)])
    if "FLOAT" in ddl:
        return _is_sorted_float([v["f"] for v in values])
    if "(a INT, b BIGINT" in ddl:
        keys = [(v["a"], v["b"]) for v in values]
        return _is_sorted_int(keys, [(False, False), (True, True)])
    if "DESC" in order:
        return _is_sorted_int([(v["id"],) for v in values], [(True, False)])
    return _is_sorted_int([(v["id"],) for v in values], [(False, True)])
