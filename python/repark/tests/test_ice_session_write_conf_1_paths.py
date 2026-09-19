"""ICE-SESSION-WRITE-CONF-1 paths — the session write confs on every remaining door.

Round 1 (2026-09-19) adds the QS / QZ / QR oracle cells of
``ice_session_write_conf_1_spark_oracle.json`` (recorded on live PySpark 4.1.2 +
Iceberg 1.11.0): branch writes (SQL INSERT, DataFrame append, DELETE / UPDATE /
MERGE in CoW and MoR), TRUNCATE and metadata-only DELETE (Spark stamps
neither), MoR UPDATE / MERGE, position-delete file codecs (session conf wins
over ``write.delete.parquet.compression-codec`` and over
``write.parquet.compression-codec``), and the summary-key family — a session
snapshot property naming a key the engine also produced for that commit fails
the write with Spark's ``Multiple entries with same key`` text, while a key the
engine did not produce is stamped and feeds the totals.

Each test drives the cell's own statements through the RePark facade and
compares every observation with the Spark cell. The native `repark.sql` door
carries its own session (its own carrier), so its pins live in Rust
(`crates/repark-sql/src/session_write_conf.rs`). The statement tuples come from
:mod:`_record_ice_session_write_conf_1_paths`, so the pin and the live
re-recording can never drift apart.

pins: ice-session-write-conf-1/C-036, C-037, C-038, C-039, C-040, C-041
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from _record_ice_session_write_conf_1_paths import (
    CODEC_CELLS,
    PROPERTY_CELLS,
    RESERVED_CELLS,
    SEED_INSERT,
    SUMMARY_KEYS,
    cell_conf,
)

from repark import ReparkSession
from repark.errors import IllegalArgumentException
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__)
    .with_name("ice_session_write_conf_1_spark_oracle.json")
    .read_text(encoding="utf-8")
)

IPI_08 = (
    "IPI-08 (2026-09-19): RePark's whole-partition DELETE rewrites data files where Spark "
    "commits a metadata-only delete, so the session snapshot property is stamped where Spark "
    "stamps nothing. Routing is owned by IPI-08, not by this unit."
)


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """A session with the two memory catalogs the cells use."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-session-write-conf-1-paths")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", tmp_path / "wh")
    session.register_memory_catalog("hc", tmp_path / "hc")
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE NAMESPACE hc.ns")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _cell(cell_id: str) -> dict[str, Any]:
    """Return one committed Spark oracle cell by id."""
    return FIXTURE["cells"][cell_id]


def _table(cell_id: str, catalog: str) -> str:
    """The table name one cell writes, matching the recorder."""
    safe = "".join(char if char.isalnum() else "_" for char in cell_id.lower())
    return f"{catalog}.ns.t_{safe}"


def _snapshot_rows(session: Any, table: str) -> list[list[Any]]:
    """Read operation plus the observed summary keys per snapshot, oldest-first."""
    rows = (
        session.sql(
            f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
        )
        .to_arrow()
        .to_pylist()
    )
    return [
        [row["operation"]] + [dict(row["summary"]).get(key) for key in SUMMARY_KEYS] for row in rows
    ]


def _ref_rows(session: Any, table: str) -> list[list[Any]]:
    """Read each ref with the ordinal of the snapshot it points at."""
    ordered = (
        session.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at, snapshot_id")
        .to_arrow()
        .to_pylist()
    )
    ordinal = {row["snapshot_id"]: index for index, row in enumerate(ordered)}
    refs = session.sql(f"SELECT name, snapshot_id FROM {table}.refs").to_arrow().to_pylist()
    return sorted([row["name"], ordinal.get(row["snapshot_id"])] for row in refs)


def _data_rows(session: Any, query: str) -> list[list[Any]]:
    """Read one query's rows in repr order."""
    return sorted((list(row) for row in session.sql(query).collect()), key=repr)


def _codec_rows(session: Any, query: str) -> list[list[str]]:
    """Read the footer codec set of every file the query names, sorted."""
    out: list[list[str]] = []
    for row in session.sql(query).collect():
        path = str(row[0])
        if not path.endswith(".parquet"):
            out.append(["non-parquet"])
            continue
        handle = pq.ParquetFile(path.replace("file:", ""))
        out.append(
            sorted(
                {
                    handle.metadata.row_group(index).column(column).compression
                    for index in range(handle.metadata.num_row_groups)
                    for column in range(handle.metadata.num_columns)
                }
            )
        )
    return sorted(out)


def _apply(session: Any, conf: dict[str, str]) -> None:
    """Apply one cell's session confs."""
    for key, value in conf.items():
        session.conf.set(key, value)


def _release(session: Any, conf: dict[str, str], table: str) -> None:
    """Unset one cell's session confs and drop its table."""
    for key in conf:
        session.conf.unset(key)
    session.sql(f"DROP TABLE IF EXISTS {table}").collect()


def _seed_property_table(session: Any, table: str, extra: str, version: str, part: str) -> None:
    """Create and seed the three-row snapshot-property table."""
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg {part} "
        f"TBLPROPERTIES ('format-version'='{version}'{extra})"
    ).collect()
    session.sql(SEED_INSERT.format(table=table)).collect()


_PROPERTY_IDS = [cell[0] for cell in PROPERTY_CELLS]
_CODEC_IDS = [cell[0] for cell in CODEC_CELLS]
_RESERVED_IDS = [cell[0] for cell in RESERVED_CELLS]


@pytest.mark.parametrize("case", PROPERTY_CELLS, ids=_PROPERTY_IDS)
def test_session_property_path_matches_spark(spark: Any, case: tuple[Any, ...]) -> None:
    """Every snapshot-property path answers Spark. pins: ice-session-write-conf-1/C-038"""
    cell_id, statements, extra, version, part, branch = case
    if cell_id == "QS-DELETE-PART-META":
        pytest.xfail(IPI_08)
    cell = _cell(cell_id)
    table = _table(cell_id, "sc")
    conf = cell_conf(cell_id)
    try:
        _seed_property_table(spark, table, extra, version, part)
        if branch:
            spark.sql(f"ALTER TABLE {table} CREATE BRANCH b").collect()
        _apply(spark, conf)
        for statement in statements:
            spark.sql(statement.format(table=table)).collect()
        assert _snapshot_rows(spark, table) == cell["obs"]["summaries"]
        assert _ref_rows(spark, table) == cell["obs"]["refs"]
        assert _data_rows(spark, f"SELECT * FROM {table}") == cell["obs"]["data"]
        if branch:
            branch_rows = _data_rows(spark, f"SELECT * FROM {table} VERSION AS OF 'b'")
            assert branch_rows == cell["obs"]["branch-data"]
    finally:
        _release(spark, conf, table)


def test_branch_dataframe_append_stamps_session_team(spark: Any) -> None:
    """A DataFrame append on a branch stamps the conf. pins: ice-session-write-conf-1/C-039"""
    cell = _cell("QS-BRANCH-DF-APPEND")
    table = _table("QS-BRANCH-DF-APPEND", "sc")
    conf = cell_conf("QS-BRANCH-DF-APPEND")
    try:
        _seed_property_table(spark, table, "", "2", "")
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH b").collect()
        _apply(spark, conf)
        spark.createDataFrame([(4, "d", "x")], "id BIGINT, data STRING, cat STRING").writeTo(
            f"{table}.branch_b"
        ).append()
        assert _snapshot_rows(spark, table) == cell["obs"]["summaries"]
        assert _ref_rows(spark, table) == cell["obs"]["refs"]
        assert _data_rows(spark, f"SELECT * FROM {table}") == cell["obs"]["data"]
        branch_rows = _data_rows(spark, f"SELECT * FROM {table} VERSION AS OF 'b'")
        assert branch_rows == cell["obs"]["branch-data"]
    finally:
        _release(spark, conf, table)


@pytest.mark.parametrize("case", CODEC_CELLS, ids=_CODEC_IDS)
def test_session_codec_path_matches_spark(spark: Any, case: tuple[Any, ...]) -> None:
    """Every codec path answers Spark. pins: ice-session-write-conf-1/C-040"""
    cell_id, statements, extra, version, branch, delete_query = case
    cell = _cell(cell_id)
    table = _table(cell_id, "hc")
    conf = cell_conf(cell_id)
    try:
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, s STRING) USING iceberg "
            f"TBLPROPERTIES ('format-version'='{version}'{extra})"
        ).collect()
        spark.sql(f"INSERT INTO {table} SELECT id, concat('v', id) FROM range(50)").collect()
        if branch:
            spark.sql(f"ALTER TABLE {table} CREATE BRANCH b").collect()
        _apply(spark, conf)
        for statement in statements:
            spark.sql(statement.format(table=table)).collect()
        assert _codec_rows(spark, delete_query.format(table=table)) == cell["obs"]["delete-codecs"]
        all_files = f"SELECT file_path FROM {table}.all_files"
        assert _codec_rows(spark, all_files) == cell["obs"]["all-codecs"]
        assert _snapshot_rows(spark, table) == cell["obs"]["summaries"]
    finally:
        _release(spark, conf, table)


@pytest.mark.parametrize("case", RESERVED_CELLS, ids=_RESERVED_IDS)
def test_summary_key_collision_matches_spark(spark: Any, case: tuple[Any, ...]) -> None:
    """A colliding key refuses, a free key stamps. pins: ice-session-write-conf-1/C-041"""
    cell_id, _key, _value, statements, extra = case
    cell = _cell(cell_id)
    table = _table(cell_id, "sc")
    conf = cell_conf(cell_id)
    try:
        _seed_property_table(spark, table, extra, "2", "")
        _apply(spark, conf)
        before = _snapshot_rows(spark, table)
        if cell["status"] == "error":
            with pytest.raises(IllegalArgumentException) as caught:
                for statement in statements:
                    spark.sql(statement.format(table=table)).collect()
            assert cell["error"]["msg"] in str(caught.value)
            assert _snapshot_rows(spark, table) == before
            return
        for statement in statements:
            spark.sql(statement.format(table=table)).collect()
        assert _snapshot_rows(spark, table) == cell["obs"]["summaries"]
        assert _data_rows(spark, f"SELECT * FROM {table}") == cell["obs"]["data"]
    finally:
        _release(spark, conf, table)
