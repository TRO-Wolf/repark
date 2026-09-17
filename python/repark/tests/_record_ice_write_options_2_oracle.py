"""Follow-up Spark probes for ICE-WRITE-OPTIONS-1 (appends cells P2-* to the fixture).

Run::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        /tmp/sparkenv/bin/python python/repark/tests/_record_ice_write_options_2_oracle.py

Not collected by pytest.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Any

JAR = "/tmp/ic-build/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar"
CATALOG = "local"
NAMESPACE = "ns"
FIXTURE = Path(__file__).resolve().parent / "ice_write_options_1_spark_oracle.json"

CELLS: list[dict[str, Any]] = []


def _spark(warehouse: str) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-ice-write-options-2-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .getOrCreate()
    )


def _summary(spark: Any, table: str) -> dict[str, str]:
    rows = spark.sql(
        f"SELECT summary FROM {CATALOG}.{NAMESPACE}.{table}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return dict(rows[0]["summary"]) if rows else {}


def _files(spark: Any, table: str) -> list[dict[str, Any]]:
    rows = spark.sql(
        f"SELECT file_path, file_format, file_size_in_bytes, record_count "
        f"FROM {CATALOG}.{NAMESPACE}.{table}.files"
    ).collect()
    return [
        {
            "suffix": str(row["file_path"]).rsplit(".", 1)[-1],
            "file_format": row["file_format"],
            "size": int(row["file_size_in_bytes"]),
            "records": int(row["record_count"]),
        }
        for row in rows
    ]


def _snapshot_count(spark: Any, table: str) -> int:
    return spark.sql(
        f"SELECT COUNT(*) AS n FROM {CATALOG}.{NAMESPACE}.{table}.snapshots"
    ).collect()[0]["n"]


def _record(cell_id: str, spark: Any, table: str, write: Any) -> None:
    entry: dict[str, Any] = {"id": cell_id, "error": None}
    try:
        write()
        entry["summary"] = _summary(spark, table)
        entry["files"] = _files(spark, table)
        entry["snapshot_count"] = _snapshot_count(spark, table)
    except Exception as exc:  # noqa: BLE001 - oracle records the refusal verbatim
        entry["error"] = {
            "class": type(exc).__name__,
            "message": str(exc)[:2000],
            "trace_tail": traceback.format_exc(limit=8)[-1500:],
        }
        try:
            entry["summary"] = _summary(spark, table)
            entry["files"] = _files(spark, table)
            entry["snapshot_count"] = _snapshot_count(spark, table)
        except Exception:  # noqa: BLE001 - table may not exist after a refused create
            entry["summary"] = {}
            entry["files"] = []
            entry["snapshot_count"] = 0
    CELLS.append(entry)
    print(f"cell {cell_id}: " + ("ERROR " + str(entry["error"])[:150] if entry["error"] else f"summary_extra={sorted(k for k in entry['summary'] if k in ('run_id',) or k.startswith('UP'))} files={len(entry['files'])}"), flush=True)


def _frame(spark: Any, n: int = 4) -> Any:
    return spark.createDataFrame([(i, f"name-{i}") for i in range(n)], ["id", "name"])


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=None)
    out_path = Path(parser.parse_args().out) if parser.parse_args().out else None
    warehouse = tempfile.mkdtemp(prefix="ice-write-opts2-oracle-")
    spark = _spark(warehouse)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

        def fresh(table: str, ddl_suffix: str = "(id BIGINT, name STRING) USING iceberg",
                  partitioned: bool = False) -> None:
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.{table}")
            part = " PARTITIONED BY (name)" if partitioned else ""
            spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.{table} {ddl_suffix}{part}")

        t = f"{CATALOG}.{NAMESPACE}"
        F = __import__("pyspark.sql.functions", fromlist=["lit", "col"])

        fresh("p2_level")
        _record("P2-01-level-alone", spark, "p2_level", lambda: (
            _frame(spark).writeTo(f"{t}.p2_level").option("compression-level", "5").append()
        ))

        fresh("p2_zstd")
        _record("P2-02-zstd-level", spark, "p2_zstd", lambda: (
            _frame(spark).writeTo(f"{t}.p2_zstd")
            .option("compression-codec", "zstd")
            .option("compression-level", "1")
            .append()
        ))

        spark.sql(f"DROP TABLE IF EXISTS {t}.p2_req")
        spark.sql(f"CREATE TABLE {t}.p2_req (id BIGINT NOT NULL, name STRING) USING iceberg")
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        null_schema = StructType(
            [StructField("id", LongType(), True), StructField("name", StringType(), True)]
        )
        nulls = spark.createDataFrame([(None, "n")], schema=null_schema)
        _record("P2-03-required-default", spark, "p2_req", lambda: (
            nulls.writeTo(f"{t}.p2_req").append()
        ))
        _record("P2-04-required-false", spark, "p2_req", lambda: (
            nulls.writeTo(f"{t}.p2_req").option("check-nullability", "false").append()
        ))
        _record("P2-05-nullability-bogus", spark, "p2_req", lambda: (
            nulls.writeTo(f"{t}.p2_req").option("check-nullability", "bogus").append()
        ))

        fresh("p2_ord", partitioned=True)
        unsorted = spark.createDataFrame(
            [(3, "c"), (1, "a"), (4, "b"), (0, "d")], ["id", "name"]
        )
        _record("P2-06-ordering-true", spark, "p2_ord", lambda: (
            unsorted.writeTo(f"{t}.p2_ord").option("check-ordering", "true").append()
        ))

        fresh("p2_orddef", partitioned=True)
        _record("P2-07-ordering-default", spark, "p2_orddef", lambda: (
            unsorted.writeTo(f"{t}.p2_orddef").append()
        ))

        fresh("p2_distbad")
        _record("P2-08-dist-bogus", spark, "p2_distbad", lambda: (
            _frame(spark).writeTo(f"{t}.p2_distbad")
            .option("distribution-mode", "bogus").append()
        ))

        fresh("p2_distnone")
        _record("P2-09-dist-none", spark, "p2_distnone", lambda: (
            _frame(spark).writeTo(f"{t}.p2_distnone")
            .option("distribution-mode", "none").append()
        ))

        fresh("p2_fanfalse", partitioned=True)
        _record("P2-10-fanout-false", spark, "p2_fanfalse", lambda: (
            _frame(spark).writeTo(f"{t}.p2_fanfalse")
            .option("fanout-enabled", "false").append()
        ))

        fresh("p2_fanbad")
        _record("P2-11-fanout-bogus", spark, "p2_fanbad", lambda: (
            _frame(spark).writeTo(f"{t}.p2_fanbad")
            .option("fanout-enabled", "bogus").append()
        ))

        fresh("p2_isoapp")
        _record("P2-12-isolation-append", spark, "p2_isoapp", lambda: (
            _frame(spark).writeTo(f"{t}.p2_isoapp")
            .option("isolation-level", "serializable").append()
        ))

        fresh("p2_isosnap", partitioned=True)
        _frame(spark).writeTo(f"{t}.p2_isosnap").append()
        _record("P2-13-isolation-snapshot", spark, "p2_isosnap", lambda: (
            _frame(spark, 2).writeTo(f"{t}.p2_isosnap")
            .option("isolation-level", "snapshot").overwritePartitions()
        ))

        fresh("p2_cond")
        _frame(spark).writeTo(f"{t}.p2_cond").append()
        _record("P2-14-overwrite-cond", spark, "p2_cond", lambda: (
            _frame(spark, 2).writeTo(f"{t}.p2_cond")
            .option("snapshot-property.run_id", "cond-1")
            .overwrite(F.col("id") < 100)
        ))

        fresh("p2_orcup")
        _record("P2-15-format-upper", spark, "p2_orcup", lambda: (
            _frame(spark).writeTo(f"{t}.p2_orcup")
            .option("write-format", "ORC").append()
        ))

        fresh("p2_case")
        _record("P2-16-prop-case", spark, "p2_case", lambda: (
            _frame(spark).writeTo(f"{t}.p2_case")
            .option("SNAPSHOT-PROPERTY.UPPER_KEY", "v").append()
        ))

        fresh("p2_sizebad")
        _record("P2-17-size-bogus", spark, "p2_sizebad", lambda: (
            _frame(spark).writeTo(f"{t}.p2_sizebad")
            .option("target-file-size-bytes", "abc").append()
        ))

        fresh("p2_emptykey")
        _record("P2-18-empty-suffix", spark, "p2_emptykey", lambda: (
            _frame(spark).writeTo(f"{t}.p2_emptykey")
            .option("snapshot-property.", "v").append()
        ))

        fresh("p2_v1app")
        _frame(spark).writeTo(f"{t}.p2_v1app").append()
        _record("P2-19-v1-append-mode", spark, "p2_v1app", lambda: (
            _frame(spark, 2).write.format("iceberg")
            .option("snapshot-property.run_id", "v1-app-1")
            .mode("append").saveAsTable(f"{t}.p2_v1app")
        ))

        base = out_path if out_path is not None and out_path.exists() else FIXTURE
        dest = out_path or FIXTURE
        out = json.loads(base.read_text(encoding="utf-8"))
        out["cells"].extend(CELLS)
        out["meta"]["probes"] = "P2 follow-up 2026-09-17"
        dest.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
        print(f"appended {len(CELLS)} cells to {dest}", flush=True)
    finally:
        spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
