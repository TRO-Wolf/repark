"""Record the Spark 4.1.2 oracle for the RP-32 rewrite_data_files keep-live-deletes flip."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger("record_rp32_rdf_cow_bytes")

GAV = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

DEFAULT_OUTPUT = Path(__file__).with_name("rp32_rdf_cow_bytes_spark_oracle.json")


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse warehouse, ivy cache, and output paths."""
    parser = argparse.ArgumentParser(description="Record the RP-32 Spark oracle.")
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--ivy", required=True)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    return parser.parse_args(argv)


def start_session(warehouse: str, ivy: str) -> Any:
    """Start the pinned PySpark session against a fresh warehouse."""
    from pyspark.sql import SparkSession

    path = Path(warehouse)
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    session = (
        SparkSession.builder.master("local[4]")
        .appName("rp32-rdf-cow-bytes")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", GAV)
        .config("spark.jars.ivy", ivy)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(path))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    return session


def run_call(session: Any, sql: str) -> dict[str, Any]:
    """Run one CALL and return its result row or its error shape."""
    try:
        row = session.sql(sql).collect()[0].asDict()
        return {"result": json.loads(json.dumps(row, default=str))}
    except Exception as error:
        return {"error_class": type(error).__name__, "error": str(error)[:600]}


def base_name(path: str) -> str:
    """Strip a warehouse prefix down to the file name."""
    return path.rsplit("/", 1)[-1]


def file_state(session: Any, table: str) -> dict[str, Any]:
    """Read data and delete file basenames, counts, and live rows."""
    files = session.sql(
        f"SELECT content, file_path, record_count FROM {table}.files ORDER BY file_path"
    ).collect()
    data = sorted(base_name(row.file_path) for row in files if row.content == 0)
    deletes = sorted(base_name(row.file_path) for row in files if row.content == 1)
    other = sorted(base_name(row.file_path) for row in files if row.content not in (0, 1))
    delete_tables = session.sql(f"SELECT count(*) c FROM {table}.delete_files").collect()[0].c
    live = session.sql(f"SELECT count(*) c FROM {table}").collect()[0].c
    return {
        "data_files": data,
        "delete_files": deletes,
        "other_content_files": other,
        "delete_files_table_rows": int(delete_tables),
        "live_rows": int(live),
    }


def snapshot_details(session: Any, table: str) -> list[dict[str, Any]]:
    """Read every snapshot operation with its summary map."""
    return [
        {
            "operation": str(row.operation),
            "summary": json.loads(json.dumps(row.summary, default=str)),
        }
        for row in session.sql(
            f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


def table_properties(session: Any, table: str) -> dict[str, str]:
    """Read one table properties map."""
    rows = session.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return {str(row.key): str(row.value) for row in rows}


def snapshot_ops(session: Any, table: str) -> list[str]:
    """Read snapshot operations in commit order."""
    return [
        str(row.operation)
        for row in session.sql(
            f"SELECT operation FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


def cell_rwd_legacy_flag(session: Any) -> dict[str, Any]:
    """Record the legacy-flag spelling Spark cannot parse."""
    table = "sc.ns.rwd"
    session.sql(
        f"CREATE TABLE {table} (id INT, name STRING, part INT) USING iceberg "
        f"PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '2', "
        f"'write.delete.mode' = 'merge-on-read')"
    )
    for index in range(1, 7):
        session.sql(f"INSERT INTO {table} VALUES ({index}, 'n{index}', 0)")
    session.sql(f"DELETE FROM {table} WHERE id = 2")
    before = file_state(session, table)
    sql = "CALL sc.system.rewrite_data_files(table => 'ns.rwd', 'remove-dangling-deletes' => true)"
    record: dict[str, Any] = {"sql": sql, "before": before}
    record.update(run_call(session, sql))
    record["after"] = file_state(session, table)
    record["ops"] = snapshot_ops(session, table)
    return record


def cell_rwd_remove_dangling(session: Any) -> dict[str, Any]:
    """Record six single-row files, one DELETE, and the options-map rewrite."""
    table = "sc.ns.rwdd"
    session.sql(
        f"CREATE TABLE {table} (id INT, name STRING, part INT) USING iceberg "
        f"PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '2', "
        f"'write.delete.mode' = 'merge-on-read')"
    )
    for index in range(1, 7):
        session.sql(f"INSERT INTO {table} VALUES ({index}, 'n{index}', 0)")
    after_inserts = file_state(session, table)
    session.sql(f"DELETE FROM {table} WHERE id = 2")
    before = file_state(session, table)
    sql = (
        "CALL sc.system.rewrite_data_files(table => 'ns.rwdd', "
        "options => map('remove-dangling-deletes', 'true'))"
    )
    record: dict[str, Any] = {
        "sql": sql,
        "properties": table_properties(session, table),
        "after_inserts": after_inserts,
        "before": before,
    }
    record.update(run_call(session, sql))
    record["after"] = file_state(session, table)
    record["snapshots"] = snapshot_details(session, table)
    record["id_2_rows"] = int(
        session.sql(f"SELECT count(*) c FROM {table} WHERE id = 2").collect()[0].c
    )
    return record


def cell_rfs_merge_rewrite(session: Any) -> dict[str, Any]:
    """Record six single-row files, one MERGE update, and the plain rewrite."""
    table = "sc.ns.rfs"
    session.sql(
        f"CREATE TABLE {table} (id INT, v STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')"
    )
    for index in range(1, 7):
        session.sql(f"INSERT INTO {table} VALUES ({index}, 'v{index}')")
    session.sql(
        f"MERGE INTO {table} AS t USING (SELECT 2 AS id) AS s ON t.id = s.id "
        f"WHEN MATCHED THEN UPDATE SET t.v = 'merged'"
    )
    before = file_state(session, table)
    sql = "CALL sc.system.rewrite_data_files(table => 'ns.rfs')"
    record: dict[str, Any] = {"sql": sql, "before": before}
    record.update(run_call(session, sql))
    record["after"] = file_state(session, table)
    record["ops"] = snapshot_ops(session, table)
    record["merged_rows"] = int(
        session.sql(f"SELECT count(*) c FROM {table} WHERE v = 'merged'").collect()[0].c
    )
    return record


def build_half_deleted(session: Any, table: str) -> None:
    """Create the 2-partition 8-file merge-on-read shape with every file half deleted."""
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, p INT, v STRING) USING iceberg "
        f"PARTITIONED BY (p) TBLPROPERTIES ('format-version'='2',"
        f"'write.delete.mode'='merge-on-read')"
    )
    key = 0
    for part in range(2):
        for _ in range(8):
            session.sql(
                f"INSERT INTO {table} SELECT id, {part}, repeat('x', 20) "
                f"FROM range({key}, {key + 50})"
            )
            key += 50
    session.sql(f"DELETE FROM {table} WHERE id % 2 = 0")


def cell_null_map_key_legacy(session: Any) -> dict[str, Any]:
    """Record the NULL-map-key plus legacy-flag spelling Spark cannot parse."""
    table = "sc.ns.dnull"
    build_half_deleted(session, table)
    session.sql("CALL sc.system.rewrite_position_delete_files(table => 'ns.dnull')")
    before = file_state(session, table)
    sql = (
        "CALL sc.system.rewrite_data_files(table => 'ns.dnull', "
        "options => map('remove-dangling-deletes', NULL), "
        "'remove-dangling-deletes' => true)"
    )
    record: dict[str, Any] = {"sql": sql, "before": before}
    record.update(run_call(session, sql))
    record["after"] = file_state(session, table)
    record["ops"] = snapshot_ops(session, table)
    return record


def cell_null_map_key(session: Any) -> dict[str, Any]:
    """Record RPD then the NULL-map-key-only rewrite."""
    table = "sc.ns.dnullo"
    build_half_deleted(session, table)
    session.sql("CALL sc.system.rewrite_position_delete_files(table => 'ns.dnullo')")
    before = file_state(session, table)
    sql = (
        "CALL sc.system.rewrite_data_files(table => 'ns.dnullo', "
        "options => map('remove-dangling-deletes', NULL))"
    )
    record: dict[str, Any] = {"sql": sql, "before": before}
    record.update(run_call(session, sql))
    record["after"] = file_state(session, table)
    record["ops"] = snapshot_ops(session, table)
    return record


def cell_dead_file(session: Any) -> dict[str, Any]:
    """Record one 100-percent-dead data file through RPD then the rewrite."""
    table = "sc.ns.tile"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, p INT, v STRING) USING iceberg "
        f"PARTITIONED BY (p) TBLPROPERTIES ('format-version'='2',"
        f"'write.delete.mode'='merge-on-read','write.update.mode'='merge-on-read',"
        f"'write.merge.mode'='merge-on-read')"
    )
    session.sql(
        f"INSERT INTO {table} SELECT /*+ REPARTITION(1) */ id, 0, repeat('x', 20) "
        f"FROM range(0, 2000)"
    )
    seeded = file_state(session, table)
    session.sql(
        f"MERGE INTO {table} AS target USING "
        f"(SELECT id, concat('m', CAST(id AS STRING)) AS v FROM range(0, 2000)) AS source "
        f"ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.v = source.v"
    )
    merged = file_state(session, table)
    rpd_sql = "CALL sc.system.rewrite_position_delete_files(table => 'ns.tile')"
    rpd: dict[str, Any] = {"sql": rpd_sql}
    rpd.update(run_call(session, rpd_sql))
    rpd["state"] = file_state(session, table)
    rdf_sql = "CALL sc.system.rewrite_data_files(table => 'ns.tile')"
    rdf: dict[str, Any] = {"sql": rdf_sql}
    rdf.update(run_call(session, rdf_sql))
    rdf["state"] = file_state(session, table)
    return {
        "seeded": seeded,
        "merged": merged,
        "rewrite_position_delete_files": rpd,
        "rewrite_data_files": rdf,
        "ops": snapshot_ops(session, table),
    }


def record_all(session: Any) -> dict[str, Any]:
    """Record every RP-32 cell with the live Spark banner beside it."""
    out: dict[str, Any] = {
        "spark": session.version,
        "iceberg": GAV,
        "session_zone": session.conf.get("spark.sql.session.timeZone"),
    }
    cells: dict[str, Any] = {}
    cells["rwd_legacy_flag"] = cell_rwd_legacy_flag(session)
    logger.info("rwd_legacy_flag %s", json.dumps(cells["rwd_legacy_flag"], default=str)[:800])
    cells["rwd_remove_dangling"] = cell_rwd_remove_dangling(session)
    logger.info(
        "rwd_remove_dangling %s", json.dumps(cells["rwd_remove_dangling"], default=str)[:3000]
    )
    cells["rfs_merge_rewrite"] = cell_rfs_merge_rewrite(session)
    logger.info("rfs_merge_rewrite %s", json.dumps(cells["rfs_merge_rewrite"], default=str))
    cells["null_map_key_legacy"] = cell_null_map_key_legacy(session)
    logger.info(
        "null_map_key_legacy %s", json.dumps(cells["null_map_key_legacy"], default=str)[:800]
    )
    cells["null_map_key"] = cell_null_map_key(session)
    logger.info("null_map_key %s", json.dumps(cells["null_map_key"], default=str)[:2000])
    cells["dead_file"] = cell_dead_file(session)
    logger.info("dead_file %s", json.dumps(cells["dead_file"], default=str)[:2000])
    out["cells"] = cells
    return out


def main(argv: list[str]) -> int:
    """Record the oracle JSON to the output path."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args(argv)
    session = start_session(args.warehouse, args.ivy)
    try:
        out = record_all(session)
    finally:
        session.stop()
    Path(args.output).write_text(json.dumps(out, indent=1, default=str) + "\n", encoding="utf-8")
    logger.info("wrote %d cells to %s", len(out["cells"]), args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
