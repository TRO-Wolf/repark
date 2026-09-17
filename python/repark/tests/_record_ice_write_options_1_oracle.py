"""Record the Spark oracle for ICE-WRITE-OPTIONS-1 (DataFrame write options).

Produces ``ice_write_options_1_spark_oracle.json`` beside this file. One JVM, stopped
at the end. Each cell uses a fresh table so summaries never cross-talk.

Run::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        /tmp/sparkenv/bin/python python/repark/tests/_record_ice_write_options_1_oracle.py

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

CELLS: list[dict[str, Any]] = []


def _spark(warehouse: str) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("repark-ice-write-options-1-record")
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
    if not rows:
        return {}
    return dict(rows[0]["summary"])


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
        spark.sql("SELECT 1").collect()
        entry["summary"] = _summary(spark, table)
        entry["files"] = _files(spark, table)
        entry["snapshot_count"] = _snapshot_count(spark, table)
    except Exception as exc:
        entry["error"] = {
            "class": type(exc).__name__,
            "message": str(exc)[:2000],
            "trace_tail": traceback.format_exc(limit=8)[-1500:],
        }
        try:
            entry["summary"] = _summary(spark, table)
            entry["files"] = _files(spark, table)
            entry["snapshot_count"] = _snapshot_count(spark, table)
        except Exception:
            entry["summary"] = {}
            entry["files"] = []
            entry["snapshot_count"] = 0
    CELLS.append(entry)
    outcome = (
        "ERROR " + str(entry["error"])[:160]
        if entry["error"]
        else f"summary_keys={sorted(entry['summary'])} files={len(entry['files'])}"
    )
    print(f"cell {cell_id}: " + outcome, flush=True)


def _frame(spark: Any, n: int = 4) -> Any:
    return spark.createDataFrame([(i, f"name-{i}") for i in range(n)], ["id", "name"])


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=None)
    default_out = Path(__file__).resolve().parent / "ice_write_options_1_spark_oracle.json"
    out_path = Path(parser.parse_args().out or default_out)
    warehouse = tempfile.mkdtemp(prefix="ice-write-opts-oracle-")
    spark = _spark(warehouse)
    try:
        print(f"spark={spark.version}", flush=True)

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

        def fresh(table: str, partitioned: bool = False) -> None:
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.{table}")
            part = " PARTITIONED BY (name)" if partitioned else ""
            spark.sql(
                f"CREATE TABLE {CATALOG}.{NAMESPACE}.{table} "
                f"(id BIGINT, name STRING) USING iceberg{part}"
            )

        t = f"{CATALOG}.{NAMESPACE}"

        fresh("snap_base")
        _frame(spark).writeTo(f"{t}.snap_base").append()
        _record(
            "SNAP-00-baseline-append",
            spark,
            "snap_base",
            lambda: _frame(spark, 2).writeTo(f"{t}.snap_base").append(),
        )

        fresh("snap_prop")
        _frame(spark).writeTo(f"{t}.snap_prop").append()
        _record(
            "SNAP-01-run-id-append",
            spark,
            "snap_prop",
            lambda: (
                _frame(spark, 2)
                .writeTo(f"{t}.snap_prop")
                .option("snapshot-property.run_id", "abc-123")
                .append()
            ),
        )

        fresh("snap_two")
        _frame(spark).writeTo(f"{t}.snap_two").append()
        _record(
            "SNAP-02-two-props",
            spark,
            "snap_two",
            lambda: (
                _frame(spark, 2)
                .writeTo(f"{t}.snap_two")
                .option("snapshot-property.run_id", "abc-123")
                .option("snapshot-property.pipeline.batch", "7")
                .append()
            ),
        )

        fresh("snap_part", partitioned=True)
        _frame(spark).writeTo(f"{t}.snap_part").append()
        _record(
            "SNAP-03-dyn-overwrite",
            spark,
            "snap_part",
            lambda: (
                _frame(spark, 2)
                .writeTo(f"{t}.snap_part")
                .option("snapshot-property.run_id", "dyn-1")
                .overwritePartitions()
            ),
        )

        _record(
            "SNAP-04-create-replace",
            spark,
            "snap_create",
            lambda: (
                _frame(spark)
                .writeTo(f"{t}.snap_create")
                .option("snapshot-property.run_id", "ctas-1")
                .createOrReplace()
            ),
        )

        _record(
            "SNAP-05-v1-saveastable",
            spark,
            "snap_v1ctas",
            lambda: (
                _frame(spark)
                .write.format("iceberg")
                .option("snapshot-property.run_id", "v1-ctas-1")
                .saveAsTable(f"{t}.snap_v1ctas")
            ),
        )

        fresh("snap_v1ins")
        _frame(spark).writeTo(f"{t}.snap_v1ins").append()
        _record(
            "SNAP-06-v1-insertinto",
            spark,
            "snap_v1ins",
            lambda: (
                _frame(spark, 2)
                .write.format("iceberg")
                .option("snapshot-property.run_id", "v1-ins-1")
                .insertInto(f"{t}.snap_v1ins")
            ),
        )

        fresh("snap_stale")
        _frame(spark).writeTo(f"{t}.snap_stale").option(
            "snapshot-property.run_id", "first"
        ).append()
        _record(
            "SNAP-07-no-prop-after",
            spark,
            "snap_stale",
            lambda: _frame(spark, 2).writeTo(f"{t}.snap_stale").append(),
        )

        fresh("fmt_parquet")
        _record(
            "FORMAT-01-parquet",
            spark,
            "fmt_parquet",
            lambda: (
                _frame(spark).writeTo(f"{t}.fmt_parquet").option("write-format", "parquet").append()
            ),
        )

        fresh("fmt_orc")
        _record(
            "FORMAT-02-orc",
            spark,
            "fmt_orc",
            lambda: _frame(spark).writeTo(f"{t}.fmt_orc").option("write-format", "orc").append(),
        )

        fresh("fmt_avro")
        _record(
            "FORMAT-03-avro",
            spark,
            "fmt_avro",
            lambda: _frame(spark).writeTo(f"{t}.fmt_avro").option("write-format", "avro").append(),
        )

        fresh("fmt_bogus")
        _record(
            "FORMAT-04-bogus",
            spark,
            "fmt_bogus",
            lambda: (
                _frame(spark).writeTo(f"{t}.fmt_bogus").option("write-format", "bogus").append()
            ),
        )

        fresh("opt_size")
        big = (
            spark.range(20000)
            .withColumnRenamed("id", "id")
            .withColumn("name", __import__("pyspark.sql.functions", fromlist=["lit"]).lit("pad"))
        )
        big.writeTo(f"{t}.opt_size").append()
        _record(
            "OPT-01-target-size",
            spark,
            "opt_size",
            lambda: (
                big.limit(20000)
                .writeTo(f"{t}.opt_size")
                .option("target-file-size-bytes", "65536")
                .append()
            ),
        )

        fresh("opt_codec")
        _record(
            "OPT-02-codec",
            spark,
            "opt_codec",
            lambda: (
                _frame(spark).writeTo(f"{t}.opt_codec").option("compression-codec", "gzip").append()
            ),
        )

        fresh("opt_level")
        _record(
            "OPT-03-level",
            spark,
            "opt_level",
            lambda: (
                _frame(spark)
                .writeTo(f"{t}.opt_level")
                .option("compression-codec", "gzip")
                .option("compression-level", "1")
                .append()
            ),
        )

        fresh("opt_dist")
        _record(
            "OPT-04-dist",
            spark,
            "opt_dist",
            lambda: (
                _frame(spark).writeTo(f"{t}.opt_dist").option("distribution-mode", "hash").append()
            ),
        )

        fresh("opt_fanout")
        _record(
            "OPT-05-fanout",
            spark,
            "opt_fanout",
            lambda: (
                _frame(spark).writeTo(f"{t}.opt_fanout").option("fanout-enabled", "true").append()
            ),
        )

        fresh("opt_iso", partitioned=True)
        _frame(spark).writeTo(f"{t}.opt_iso").append()
        _record(
            "OPT-06-isolation",
            spark,
            "opt_iso",
            lambda: (
                _frame(spark, 2)
                .writeTo(f"{t}.opt_iso")
                .option("isolation-level", "serializable")
                .overwritePartitions()
            ),
        )

        fresh("opt_iso_bad", partitioned=True)
        _frame(spark).writeTo(f"{t}.opt_iso_bad").append()
        _record(
            "OPT-07-isolation-bad",
            spark,
            "opt_iso_bad",
            lambda: (
                _frame(spark, 2)
                .writeTo(f"{t}.opt_iso_bad")
                .option("isolation-level", "bogus")
                .overwritePartitions()
            ),
        )

        fresh("opt_null")
        _record(
            "OPT-08-nullability",
            spark,
            "opt_null",
            lambda: (
                _frame(spark).writeTo(f"{t}.opt_null").option("check-nullability", "false").append()
            ),
        )

        fresh("opt_order", partitioned=True)
        _record(
            "OPT-09-ordering",
            spark,
            "opt_order",
            lambda: (
                _frame(spark).writeTo(f"{t}.opt_order").option("check-ordering", "false").append()
            ),
        )

        fresh("opt_unknown")
        _record(
            "UNKNOWN-01",
            spark,
            "opt_unknown",
            lambda: (
                _frame(spark)
                .writeTo(f"{t}.opt_unknown")
                .option("repark-totally-unknown-key", "zzz")
                .append()
            ),
        )

        fresh("sql_base")
        spark.sql(f"INSERT INTO {t}.sql_base SELECT 1 AS id, 'a' AS name")
        _record(
            "SQL-00-baseline-insert",
            spark,
            "sql_base",
            lambda: spark.sql(f"INSERT INTO {t}.sql_base SELECT 2 AS id, 'b' AS name"),
        )

        fresh("sql_conf")
        spark.sql(f"INSERT INTO {t}.sql_conf SELECT 1 AS id, 'a' AS name")
        spark.conf.set("spark.sql.iceberg.write.snapshot-property.run_id", "conf-1")
        _record(
            "SQL-01-set-conf",
            spark,
            "sql_conf",
            lambda: spark.sql(f"INSERT INTO {t}.sql_conf SELECT 2 AS id, 'b' AS name"),
        )
        spark.conf.unset("spark.sql.iceberg.write.snapshot-property.run_id")

        out = {
            "meta": {
                "spark": spark.version,
                "iceberg_jar": Path(JAR).name,
                "catalog": "hadoop",
            },
            "cells": CELLS,
        }
        out_path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
        print(f"wrote {out_path} ({len(CELLS)} cells)", flush=True)
    finally:
        spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
