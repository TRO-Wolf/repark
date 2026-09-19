"""ICE-SESSION-WRITE-CONF-1 — session ``spark.sql.iceberg.*`` write confs reach every write.

Oracle: ``ice_session_write_conf_1_spark_oracle.json`` (live PySpark 4.1.2 +
Iceberg 1.11.0, re-derived by ``_record_ice_session_write_conf_1_oracle.py``).
``spark.sql.iceberg.snapshot-property.*`` stamps every snapshot the session
commits; ``spark.sql.iceberg.compression-codec`` sets every data file codec
(writer option wins over conf wins over table property; bogus codec refuses
naming the codec, Spark text ``Unsupported compression codec: bogus``).
Format-version 3 twins pin the v2 Spark answers on v3 tables. ``SP-SET-SQL``
pins the RePark door (bare-dotted SET applies; Spark refuses it
``INVALID_SET_SYNTAX``). Offline pins RePark against the fixture; the live
tier replays the recorder under ``REPARK_PARITY_LIVE=1``.

pins: ice-session-write-conf-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
pins: ice-session-write-conf-1/C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016
pins: ice-session-write-conf-1/C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-024
pins: ice-session-write-conf-1/C-025, C-026, C-027, C-028, C-029, C-030, C-031, C-032
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__)
    .with_name("ice_session_write_conf_1_spark_oracle.json")
    .read_text(encoding="utf-8")
)


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """A session with memory catalogs plus namespaces for both doors."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-session-write-conf-1")
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


def _snapshots(session: Any, table: str) -> list[tuple[Any, dict[str, Any]]]:
    """Read per-snapshot operation plus summary oldest-first for one table."""
    rows = (
        session.sql(
            f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
        )
        .to_arrow()
        .to_pylist()
    )
    return [(row["operation"], dict(row["summary"])) for row in rows]


def _summaries(session: Any, table: str) -> list[dict[str, Any]]:
    """Read snapshot summaries oldest-first for one table."""
    return [summary for _, summary in _snapshots(session, table)]


def _ops_keys(session: Any, table: str, keys: tuple[str, ...]) -> list[list[Any]]:
    """Read per-snapshot operation plus observed summary keys oldest-first."""
    return [
        [operation] + [(operation if key == "operation" else summary.get(key)) for key in keys]
        for operation, summary in _snapshots(session, table)
    ]


def _rows(session: Any, table: str) -> list[list[Any]]:
    """Read all rows of one table in repr order."""
    return sorted((list(row) for row in session.sql(f"SELECT * FROM {table}").collect()), key=repr)


def _codecs(session: Any, table: str) -> list[list[str]]:
    """Read the footer codec set of every live file, sorted."""
    paths = [row[0] for row in session.sql(f"SELECT file_path FROM {table}.all_files").collect()]
    out = []
    for path in paths:
        handle = pq.ParquetFile(str(path).replace("file:", ""))
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


def _snapshot_count(session: Any, table: str) -> int:
    """Count snapshots on one table."""
    rows = session.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").to_arrow().to_pylist()
    return int(rows[0]["n"])


def _begin(session: Any, conf: dict[str, str]) -> None:
    """Apply one cell's session confs."""
    for key, value in conf.items():
        session.conf.set(key, value)


def _end(session: Any, conf: dict[str, str], tables: list[str]) -> None:
    """Unset one cell's session confs and drop its tables."""
    for key in conf:
        session.conf.unset(key)
    for table in tables:
        session.sql(f"DROP TABLE IF EXISTS {table}").collect()


def _sp_ddl(table: str, version: str = "2", extra: str = "") -> str:
    """Build the shared snapshot-property CREATE TABLE statement."""
    return (
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}'{extra})"
    )


def _sp_insert(table: str) -> str:
    """Build the shared two-row INSERT statement."""
    return f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')"


def test_sp_insert_stamps_session_team(spark: Any) -> None:
    """INSERT carries team=a on its append snapshot. pins: ice-session-write-conf-1/C-003"""
    cell = _cell("SP-INSERT")
    table = "sc.ns.t_sp_insert"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_two_keys_stamps_both(spark: Any) -> None:
    """Two conf keys both land on the snapshot. pins: ice-session-write-conf-1/C-004"""
    cell = _cell("SP-TWO-KEYS")
    table = "sc.ns.t_sp_two_keys"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        assert _ops_keys(spark, table, ("team", "run")) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_df_append_stamps_session_team(spark: Any) -> None:
    """writeTo append carries team=a. pins: ice-session-write-conf-1/C-005"""
    cell = _cell("SP-DF-APPEND")
    table = "sc.ns.t_sp_df_append"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.createDataFrame([(1, "a", "x")], "id BIGINT, data STRING, cat STRING").writeTo(
            table
        ).append()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_df_saveastable_stamps_session_team(spark: Any) -> None:
    """saveAsTable append carries team=a. pins: ice-session-write-conf-1/C-006"""
    cell = _cell("SP-DF-SAVEASTABLE")
    table = "sc.ns.t_sp_df_saveastable"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.createDataFrame([(1, "a", "x")], "id BIGINT, data STRING, cat STRING").write.format(
            "iceberg"
        ).mode("append").saveAsTable(table)
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_df_option_wins_over_conf(spark: Any) -> None:
    """Writer option team=opt wins over conf team=a. pins: ice-session-write-conf-1/C-007"""
    cell = _cell("SP-DF-OPTION-WINS")
    table = "sc.ns.t_sp_df_option_wins"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.createDataFrame([(1, "a", "x")], "id BIGINT, data STRING, cat STRING").writeTo(
            table
        ).option("snapshot-property.team", "opt").append()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_delete_cow_stamps_overwrite(spark: Any) -> None:
    """COW DELETE stamps team=a on append and overwrite. pins: ice-session-write-conf-1/C-008"""
    cell = _cell("SP-DELETE-COW")
    table = "sc.ns.t_sp_delete_cow"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql(f"DELETE FROM {table} WHERE id = 1").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_delete_mor_stamps_delete(spark: Any) -> None:
    """MOR DELETE stamps team=a on append and delete. pins: ice-session-write-conf-1/C-009"""
    cell = _cell("SP-DELETE-MOR")
    table = "sc.ns.t_sp_delete_mor"
    extra = (
        ", 'write.delete.mode'='merge-on-read'"
        ", 'write.update.mode'='merge-on-read'"
        ", 'write.merge.mode'='merge-on-read'"
    )
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table, extra=extra)).collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql(f"DELETE FROM {table} WHERE id = 1").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


@pytest.mark.xfail(
    strict=True,
    reason="F-RDF-SESSION-CONF-1 2026-09-19: UPDATE commits through the fork's DataFusion DML, "
    "which takes no session snapshot properties",
)
def test_sp_update_stamps_overwrite(spark: Any) -> None:
    """UPDATE stamps team=a on append and overwrite. pins: ice-session-write-conf-1/C-010"""
    cell = _cell("SP-UPDATE")
    table = "sc.ns.t_sp_update"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql(f"UPDATE {table} SET data = 'z' WHERE id = 1").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_merge_stamps_overwrite(spark: Any) -> None:
    """MERGE stamps team=a on append and overwrite. pins: ice-session-write-conf-1/C-011"""
    cell = _cell("SP-MERGE")
    table = "sc.ns.t_sp_merge"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT 1 AS id, 'm' AS data, 'x' AS cat) s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        ).collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_overwrite_stamps_overwrite(spark: Any) -> None:
    """INSERT OVERWRITE stamps team=a on both snapshots. pins: ice-session-write-conf-1/C-012"""
    cell = _cell("SP-OVERWRITE")
    table = "sc.ns.t_sp_overwrite"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql(f"INSERT OVERWRITE {table} VALUES (9, 'z', 'x')").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_ctas_stamps_session_team(spark: Any) -> None:
    """CTAS carries team=a on the new table snapshot. pins: ice-session-write-conf-1/C-013"""
    cell = _cell("SP-CTAS")
    table = "sc.ns.t_sp_ctas"
    table_two = "sc.ns.u_sp_ctas"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(f"CREATE TABLE {table_two} USING iceberg AS SELECT 1 AS id").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert [summary.get("team") for summary in _summaries(spark, table_two)] == cell["obs"][
            "ctas"
        ]
    finally:
        _end(spark, cell["spark_conf"], [table, table_two])


@pytest.mark.xfail(
    strict=True,
    reason=(
        "F-RDF-SESSION-CONF-1 2026-09-19: the fork's rewrite_data_files takes "
        "no session writer/snapshot properties"
    ),
)
def test_sp_call_rdf_keeps_stamps(spark: Any) -> None:
    """rewrite_data_files leaves stamped appends intact. pins: ice-session-write-conf-1/C-014"""
    cell = _cell("SP-CALL-RDF")
    table = "sc.ns.t_sp_call_rdf"
    short = "ns.t_sp_call_rdf"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')").collect()
        spark.sql(f"CALL sc.system.rewrite_data_files(table => '{short}')").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_rollback_makes_no_snapshot(spark: Any) -> None:
    """rollback_to_snapshot adds no snapshot. pins: ice-session-write-conf-1/C-015"""
    cell = _cell("SP-CALL-EXPIRE-NOSNAP")
    table = "sc.ns.t_sp_rollback"
    short = "ns.t_sp_rollback"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        first = (
            spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at LIMIT 1")
            .to_arrow()
            .to_pylist()[0]["snapshot_id"]
        )
        spark.sql(f"CALL sc.system.rollback_to_snapshot('{short}', {first})").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_reserved_operation_untouched(spark: Any) -> None:
    """operation conf never overrides the engine operation. pins: ice-session-write-conf-1/C-016"""
    cell = _cell("SP-RESERVED-OPERATION")
    table = "sc.ns.t_sp_reserved"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        assert _ops_keys(spark, table, ("operation",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_set_sql_applies_team(spark: Any) -> None:
    """SQL SET applies team=s to the later INSERT. pins: ice-session-write-conf-1/C-017"""
    cell = _cell("SP-SET-SQL")
    table = "sc.ns.t_sp_set_sql"
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql("SET spark.sql.iceberg.snapshot-property.team = s").collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql("RESET spark.sql.iceberg.snapshot-property.team").collect()
        assert _ops_keys(spark, table, ("team",)) == [["append", "s"]]
        assert _rows(spark, table) == [[1, "a", "x"], [2, "b", "y"]]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_empty_value_stamps_empty(spark: Any) -> None:
    """Empty conf value stamps an empty string. pins: ice-session-write-conf-1/C-018"""
    cell = _cell("SP-EMPTY-VALUE")
    table = "sc.ns.t_sp_empty"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table)).collect()
        spark.sql(_sp_insert(table)).collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_insert_v3_stamps_session_team(spark: Any) -> None:
    """INSERT on v3 carries team=a like v2. pins: ice-session-write-conf-1/C-019"""
    cell = _cell("SP-INSERT")
    table = "sc.ns.t_sp_insert_v3"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table, version="3")).collect()
        spark.sql(_sp_insert(table)).collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_df_append_v3_stamps_session_team(spark: Any) -> None:
    """writeTo append on v3 carries team=a like v2. pins: ice-session-write-conf-1/C-020"""
    cell = _cell("SP-DF-APPEND")
    table = "sc.ns.t_sp_df_append_v3"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table, version="3")).collect()
        spark.createDataFrame([(1, "a", "x")], "id BIGINT, data STRING, cat STRING").writeTo(
            table
        ).append()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_delete_cow_v3_stamps_overwrite(spark: Any) -> None:
    """Copy-on-write DELETE on v3 stamps team=a like v2. pins: ice-session-write-conf-1/C-021"""
    cell = _cell("SP-DELETE-COW")
    table = "sc.ns.t_sp_delete_cow_v3"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table, version="3")).collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql(f"DELETE FROM {table} WHERE id = 1").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_sp_delete_mor_v3_stamps_delete(spark: Any) -> None:
    """Merge-on-read DELETE on v3 stamps team=a like v2. pins: ice-session-write-conf-1/C-022"""
    cell = _cell("SP-DELETE-MOR")
    table = "sc.ns.t_sp_delete_mor_v3"
    extra = (
        ", 'write.delete.mode'='merge-on-read'"
        ", 'write.update.mode'='merge-on-read'"
        ", 'write.merge.mode'='merge-on-read'"
    )
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_sp_ddl(table, version="3", extra=extra)).collect()
        spark.sql(_sp_insert(table)).collect()
        spark.sql(f"DELETE FROM {table} WHERE id = 1").collect()
        assert _ops_keys(spark, table, ("team",)) == cell["obs"]["ops+keys"]
        assert _rows(spark, table) == cell["obs"]["data"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def _cz_ddl(table: str, props: str = "") -> str:
    """Build the shared compression-codec CREATE TABLE statement."""
    return (
        f"CREATE TABLE {table} (id BIGINT, s STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2'{props})"
    )


def _cz_frame(spark: Any) -> Any:
    """Build the shared fifty-row codec DataFrame."""
    return spark.createDataFrame(
        [(index, f"v{index}") for index in range(50)], "id BIGINT, s STRING"
    )


def _cz_insert_sql(table: str) -> str:
    """Build the shared fifty-row codec INSERT statement."""
    return f"INSERT INTO {table} SELECT id, concat('v', id) FROM range(50)"


def test_cz_conf_gzip_df_writes_gzip(spark: Any) -> None:
    """Conf gzip sets the DataFrame append codec. pins: ice-session-write-conf-1/C-023"""
    cell = _cell("CZ-CONF-GZIP-DF")
    table = "hc.ns.t_cz_gzip_df"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table)).collect()
        _cz_frame(spark).writeTo(table).append()
        assert _codecs(spark, table) == cell["obs"]["codec"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_cz_conf_gzip_sql_writes_gzip(spark: Any) -> None:
    """Conf gzip sets the SQL INSERT codec. pins: ice-session-write-conf-1/C-024"""
    cell = _cell("CZ-CONF-GZIP-SQL")
    table = "hc.ns.t_cz_gzip_sql"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table)).collect()
        spark.sql(_cz_insert_sql(table)).collect()
        assert _codecs(spark, table) == cell["obs"]["codec"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_cz_conf_snappy_sql_writes_snappy(spark: Any) -> None:
    """Conf snappy sets the SQL INSERT codec. pins: ice-session-write-conf-1/C-025"""
    cell = _cell("CZ-CONF-SNAPPY-SQL")
    table = "hc.ns.t_cz_snappy_sql"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table)).collect()
        spark.sql(_cz_insert_sql(table)).collect()
        assert _codecs(spark, table) == cell["obs"]["codec"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_cz_conf_upper_writes_gzip(spark: Any) -> None:
    """Upper-case GZIP conf sets the codec. pins: ice-session-write-conf-1/C-026"""
    cell = _cell("CZ-CONF-UPPER")
    table = "hc.ns.t_cz_upper"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table)).collect()
        spark.sql(_cz_insert_sql(table)).collect()
        assert _codecs(spark, table) == cell["obs"]["codec"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_cz_conf_beats_table_property(spark: Any) -> None:
    """Conf snappy wins over table property gzip. pins: ice-session-write-conf-1/C-027"""
    cell = _cell("CZ-CONF-OVER-PROP")
    table = "hc.ns.t_cz_over_prop"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table, props=", 'write.parquet.compression-codec'='gzip'")).collect()
        spark.sql(_cz_insert_sql(table)).collect()
        assert _codecs(spark, table) == cell["obs"]["codec"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_cz_option_beats_conf(spark: Any) -> None:
    """Writer option snappy wins over conf gzip. pins: ice-session-write-conf-1/C-028"""
    cell = _cell("CZ-OPT-OVER-CONF")
    table = "hc.ns.t_cz_opt_over_conf"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table)).collect()
        _cz_frame(spark).writeTo(table).option("compression-codec", "snappy").append()
        assert _codecs(spark, table) == cell["obs"]["codec"]
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_cz_conf_bogus_refuses_naming_codec(spark: Any) -> None:
    """Bogus codec refuses naming bogus with no snapshot. pins: ice-session-write-conf-1/C-029"""
    cell = _cell("CZ-CONF-BOGUS")
    table = "hc.ns.t_cz_bogus"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table)).collect()
        with pytest.raises(Exception) as excinfo:
            spark.sql(_cz_insert_sql(table)).collect()
        assert "bogus" in str(excinfo.value)
        assert _snapshot_count(spark, table) == 0
    finally:
        _end(spark, cell["spark_conf"], [table])


def test_cz_conf_delete_cow_rewrites_gzip(spark: Any) -> None:
    """Conf gzip sets the rewritten file codec on DELETE. pins: ice-session-write-conf-1/C-030"""
    cell = _cell("CZ-CONF-DELETE-COW")
    table = "hc.ns.t_cz_delete_cow"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table)).collect()
        spark.sql(_cz_insert_sql(table)).collect()
        spark.sql(f"DELETE FROM {table} WHERE id < 10").collect()
        assert _codecs(spark, table) == cell["obs"]["codec"]
    finally:
        _end(spark, cell["spark_conf"], [table])


@pytest.mark.xfail(
    strict=True,
    reason=(
        "F-RDF-SESSION-CONF-1 2026-09-19: the fork's rewrite_data_files takes "
        "no session writer/snapshot properties"
    ),
)
def test_cz_conf_rdf_writes_gzip(spark: Any) -> None:
    """Conf gzip sets the rewrite_data_files output codec. pins: ice-session-write-conf-1/C-031"""
    cell = _cell("CZ-CONF-RDF")
    table = "hc.ns.t_cz_rdf"
    short = "ns.t_cz_rdf"
    _begin(spark, cell["spark_conf"])
    try:
        spark.sql(_cz_ddl(table)).collect()
        spark.sql(_cz_insert_sql(table)).collect()
        spark.sql(f"INSERT INTO {table} SELECT id, 'w' FROM range(5)").collect()
        spark.sql(
            f"CALL hc.system.rewrite_data_files(table => '{short}', "
            "options => map('rewrite-all', 'true'))"
        ).collect()
        assert _codecs(spark, table) == cell["obs"]["codec"]
    finally:
        _end(spark, cell["spark_conf"], [table])


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The recorder re-derives the fixture on live Spark. pins: ice-session-write-conf-1/C-032"""
    sparkenv = Path("/tmp/sparkenv/bin/python")
    generator = Path(__file__).with_name("_record_ice_session_write_conf_1_oracle.py")
    environ = dict(os.environ)
    environ.setdefault("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64")
    environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    completed = subprocess.run(
        [
            str(sparkenv),
            str(generator),
            "--warehouse",
            str(tmp_path / "live-wh"),
            "check",
        ],
        capture_output=True,
        text=True,
        env=environ,
        timeout=1500,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
