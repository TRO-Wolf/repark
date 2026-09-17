"""Record the Spark 4.1.2 oracle for ICE-RDF-OPTIONS-1 rewrite options cells."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger("record_rdf_options_1_oracle")

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = REPO_ROOT / "python" / "repark" / "tests" / "ice_rdf_options_1_spark_oracle.json"


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse warehouse, ivy cache, and output paths."""
    parser = argparse.ArgumentParser(description="Record the ICE-RDF-OPTIONS-1 Spark oracle.")
    parser.add_argument("--warehouse", default="/tmp/ic-rdf-options-1-wh")
    parser.add_argument("--ivy", default="/tmp/ic-build/.ivy2")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    return parser.parse_args(argv)


def start_spark(warehouse: str, ivy: str) -> Any:
    """Start the pinned PySpark session against a fresh warehouse."""
    from pyspark.sql import SparkSession

    path = Path(warehouse)
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    session = (
        SparkSession.builder.master("local[4]")
        .appName("ic-rdf-options-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        )
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


def build_table(session: Any, counter: list[int], parts: int, files_per: int, mor: bool) -> str:
    """Create one partitioned table with single-INSERT files and return its name."""
    counter[0] += 1
    table = f"sc.ns.t{counter[0]}"
    props = "'format-version'='2'" + (",'write.delete.mode'='merge-on-read'" if mor else "")
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, p INT, v STRING) USING iceberg "
        f"PARTITIONED BY (p) TBLPROPERTIES ({props})"
    )
    key = 0
    for part in range(parts):
        for _ in range(files_per):
            session.sql(
                f"INSERT INTO {table} SELECT id, {part}, repeat('x', 20) "
                f"FROM range({key}, {key + 50})"
            )
            key += 50
    return table


def table_state(session: Any, table: str) -> dict[str, Any]:
    """Read snapshot, file, and spec counts for one table."""
    snaps = [
        row.asDict()
        for row in session.sql(
            f"SELECT snapshot_id, operation, summary FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]
    files = session.sql(f"SELECT count(*) c FROM {table}.files").collect()[0].c
    dfiles = session.sql(f"SELECT count(*) c FROM {table}.delete_files").collect()[0].c
    spec_rows = session.sql(f"SELECT spec_id FROM {table}.files").collect()
    specs = sorted({row.spec_id for row in spec_rows})
    return {
        "snapshots": len(snaps),
        "ops": [snap["operation"] for snap in snaps],
        "data_files": files,
        "delete_files": dfiles,
        "spec_ids": specs,
    }


def short_name(table: str) -> str:
    """Strip the catalog prefix from a table name."""
    return table.split(".", 1)[1]


def run_call(session: Any, sql: str) -> dict[str, Any]:
    """Run one CALL and return its result row or its error shape."""
    try:
        row = session.sql(sql).collect()[0].asDict()
        return {"result": row}
    except Exception as error:
        record: dict[str, Any] = {
            "error_class": type(error).__name__,
            "error": str(error)[:600],
        }
        with contextlib.suppress(Exception):
            record["spark_error_class"] = error.getCondition()  # type: ignore[attr-defined]
        return record


def rdf_cell(
    session: Any,
    out: dict[str, Any],
    counter: list[int],
    name: str,
    opts: list[tuple[str, str]] | None,
    where: str | None = None,
    parts: int = 2,
    files_per: int = 4,
    mor: bool = False,
    pre: list[str] | None = None,
) -> None:
    """Record one rewrite_data_files cell on a fresh table."""
    table = build_table(session, counter, parts, files_per, mor)
    for stmt in pre or []:
        session.sql(stmt.format(t=table))
    before = table_state(session, table)
    args = f"table => '{short_name(table)}'"
    if opts is not None:
        args += ", options => map(" + ", ".join(f"'{key}','{value}'" for key, value in opts) + ")"
    if where:
        args += f", where => '{where}'"
    sql = f"CALL sc.system.rewrite_data_files({args})"
    record: dict[str, Any] = {"sql": sql.replace(short_name(table), "ns.t"), "before": before}
    record.update(run_call(session, sql))
    if "result" in record:
        record["after"] = table_state(session, table)
    out[name] = record
    logger.info("%s %s", name, json.dumps(record, default=str)[:400])


def rpd_cell(
    session: Any,
    out: dict[str, Any],
    counter: list[int],
    name: str,
    opts: list[tuple[str, str]] | None,
) -> None:
    """Record one rewrite_position_delete_files cell on a fresh MoR table with deletes."""
    table = build_table(session, counter, 2, 4, True)
    session.sql(f"DELETE FROM {table} WHERE id % 2 = 0")
    before = table_state(session, table)
    args = f"table => '{short_name(table)}'"
    if opts is not None:
        args += ", options => map(" + ", ".join(f"'{key}','{value}'" for key, value in opts) + ")"
    sql = f"CALL sc.system.rewrite_position_delete_files({args})"
    record: dict[str, Any] = {"sql": sql.replace(short_name(table), "ns.t"), "before": before}
    record.update(run_call(session, sql))
    if "result" in record:
        record["after"] = table_state(session, table)
    out[name] = record
    logger.info("%s %s", name, json.dumps(record, default=str)[:400])


def residue_sequence(session: Any, counter: list[int], rdf_only: bool) -> dict[str, Any]:
    """Run Spark's maintenance sequence step by step and record every step."""
    table = build_table(session, counter, 2, 8, True)
    session.sql(f"DELETE FROM {table} WHERE id % 2 = 0")
    steps: dict[str, Any] = {"start": table_state(session, table)}
    if not rdf_only:
        sql = f"CALL sc.system.rewrite_position_delete_files(table => '{short_name(table)}')"
        step: dict[str, Any] = {"sql": sql.replace(short_name(table), "ns.t")}
        step.update(run_call(session, sql))
        step["state"] = table_state(session, table)
        steps["rewrite_position_delete_files"] = step
    sql = f"CALL sc.system.rewrite_data_files(table => '{short_name(table)}')"
    final: dict[str, Any] = {"sql": sql.replace(short_name(table), "ns.t")}
    final.update(run_call(session, sql))
    final["state"] = table_state(session, table)
    steps["rewrite_data_files"] = final
    return steps


def record_all(session: Any) -> dict[str, Any]:
    """Record every oracle cell in table-number order."""
    out: dict[str, Any] = {}
    counter = [0]
    rdf_cell(session, out, counter, "baseline", None)
    rdf_cell(session, out, counter, "min_input_files_1", [("min-input-files", "1")])
    rdf_cell(session, out, counter, "min_input_files_9", [("min-input-files", "9")])
    rdf_cell(session, out, counter, "rewrite_all", [("rewrite-all", "true")])
    rdf_cell(
        session,
        out,
        counter,
        "target_small",
        [("rewrite-all", "true"), ("target-file-size-bytes", "2000")],
    )
    rdf_cell(
        session,
        out,
        counter,
        "min_max",
        [
            ("min-file-size-bytes", "0"),
            ("max-file-size-bytes", "100000000"),
            ("min-input-files", "2"),
        ],
    )
    rdf_cell(
        session,
        out,
        counter,
        "max_group_size",
        [("rewrite-all", "true"), ("max-file-group-size-bytes", "2500")],
    )
    rdf_cell(
        session,
        out,
        counter,
        "partial_progress",
        [("rewrite-all", "true"), ("partial-progress.enabled", "true")],
    )
    rdf_cell(
        session,
        out,
        counter,
        "partial_progress_max1",
        [
            ("rewrite-all", "true"),
            ("partial-progress.enabled", "true"),
            ("partial-progress.max-commits", "1"),
        ],
    )
    rdf_cell(
        session,
        out,
        counter,
        "partial_progress_groups",
        [
            ("rewrite-all", "true"),
            ("max-file-group-size-bytes", "2500"),
            ("partial-progress.enabled", "true"),
            ("partial-progress.max-commits", "3"),
        ],
    )
    rdf_cell(
        session,
        out,
        counter,
        "job_order_bytes_desc",
        [("rewrite-all", "true"), ("rewrite-job-order", "bytes-desc")],
    )
    rdf_cell(
        session,
        out,
        counter,
        "job_order_files_asc",
        [("rewrite-all", "true"), ("rewrite-job-order", "files-asc")],
    )
    rdf_cell(
        session,
        out,
        counter,
        "use_start_seq_false",
        [("rewrite-all", "true"), ("use-starting-sequence-number", "false")],
    )
    rdf_cell(
        session,
        out,
        counter,
        "concurrent",
        [("rewrite-all", "true"), ("max-concurrent-file-group-rewrites", "4")],
    )
    rdf_cell(session, out, counter, "where_plus_options", [("min-input-files", "1")], where="p = 1")
    rdf_cell(
        session,
        out,
        counter,
        "delete_file_threshold",
        [("delete-file-threshold", "1")],
        mor=True,
        pre=["DELETE FROM {t} WHERE id = 3"],
    )
    rdf_cell(
        session,
        out,
        counter,
        "output_spec_id",
        [("rewrite-all", "true"), ("output-spec-id", "0")],
        pre=["ALTER TABLE {t} DROP PARTITION FIELD p"],
    )
    rdf_cell(
        session,
        out,
        counter,
        "output_spec_id_current",
        [("rewrite-all", "true")],
        pre=["ALTER TABLE {t} DROP PARTITION FIELD p"],
    )
    rdf_cell(
        session,
        out,
        counter,
        "remove_dangling",
        [("rewrite-all", "true"), ("remove-dangling-deletes", "true")],
        mor=True,
        pre=["DELETE FROM {t} WHERE id < 30"],
    )
    for name, opts in [
        ("err_unknown_key", [("foo", "1")]),
        ("err_bad_int", [("min-input-files", "abc")]),
        ("err_zero_min_input", [("min-input-files", "0")]),
        ("err_bad_bool", [("rewrite-all", "maybe")]),
        ("err_bad_job_order", [("rewrite-job-order", "bogus")]),
        ("err_bad_spec", [("output-spec-id", "99")]),
        (
            "err_max_commits_0",
            [("partial-progress.enabled", "true"), ("partial-progress.max-commits", "0")],
        ),
        ("err_target_le_min", [("target-file-size-bytes", "100"), ("min-file-size-bytes", "200")]),
        ("err_target_ge_max", [("target-file-size-bytes", "300"), ("max-file-size-bytes", "200")]),
        ("err_neg_target", [("target-file-size-bytes", "-1")]),
        ("err_concurrent_0", [("max-concurrent-file-group-rewrites", "0")]),
        ("err_delete_ratio", [("delete-ratio-threshold", "2")]),
        ("err_empty_key", [("", "1")]),
        ("err_upper_key", [("MIN-INPUT-FILES", "1")]),
        ("err_group_size_0", [("max-file-group-size-bytes", "0")]),
        ("err_delete_threshold_neg", [("delete-file-threshold", "-1")]),
        (
            "err_dup_key",
            [("min-input-files", "1"), ("min-input-files", "2")],
        ),
    ]:
        rdf_cell(session, out, counter, name, opts, parts=1, files_per=2)
    rpd_cell(session, out, counter, "rpd_baseline", None)
    rpd_cell(session, out, counter, "rpd_rewrite_all", [("rewrite-all", "true")])
    rpd_cell(session, out, counter, "rpd_min_input_files_1", [("min-input-files", "1")])
    rpd_cell(session, out, counter, "rpd_target_small", [("target-file-size-bytes", "2000")])
    rpd_cell(
        session,
        out,
        counter,
        "rpd_max_group_size",
        [("max-file-group-size-bytes", "2500")],
    )
    rpd_cell(session, out, counter, "rpd_unknown_key", [("foo", "1")])
    rpd_cell(session, out, counter, "rpd_bad_int", [("min-input-files", "abc")])
    out["residue_rpd_then_rdf"] = residue_sequence(session, counter, rdf_only=False)
    out["residue_rdf_only"] = residue_sequence(session, counter, rdf_only=True)
    return out


def main(argv: list[str]) -> int:
    """Record the oracle JSON to the output path."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args(argv)
    session = start_spark(args.warehouse, args.ivy)
    try:
        out = record_all(session)
    finally:
        session.stop()
    Path(args.output).write_text(json.dumps(out, indent=1, default=str) + "\n", encoding="utf-8")
    logger.info("wrote %d cells to %s", len(out), args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
