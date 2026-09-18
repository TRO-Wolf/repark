"""Record the V3-MULTIARG-1 Spark oracle — the multi-argument bucket DDL refusal.

Runs the ``bucket(4, id, name)`` CREATE TABLE against live PySpark 4.1.2 +
Iceberg 1.11.0 on a Hadoop catalog and writes the refusal cell into
``v3_multiarg_1_spark_oracle.json`` beside this file. Re-running it re-derives
the cell from live Spark and exits non-zero on drift.

Run with a PySpark 4.1.2 interpreter; the warehouse is ``--warehouse`` (a fresh
temp dir when omitted) and ``REPARK_ORACLE_IVY`` optionally points
``spark.jars.ivy`` at a warm cache::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        /tmp/sparkenv/bin/python python/repark/tests/_record_v3_multiarg_1.py \\
        --warehouse <dir>

Not collected by pytest.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

CATALOG = "local"
NAMESPACE = "ns"
CELL_ID = "MULTIARG-DDL-01"
DDL_TEMPLATE = (
    "CREATE TABLE {catalog}.{namespace}.t (id INT, name STRING) "
    "USING iceberg PARTITIONED BY (bucket(4, id, name))"
)
FIXTURE = Path(__file__).resolve().parent / "v3_multiarg_1_spark_oracle.json"
EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def spark_session(warehouse: str) -> Any:
    """A local Spark session on a Hadoop Iceberg catalog rooted at warehouse."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("repark-v3-multiarg-1-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", EXTENSIONS)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    return builder.getOrCreate()


def record_cell(spark: Any) -> dict[str, Any]:
    """Run the multi-argument DDL and record Spark's refusal cell."""
    from pyspark.sql import SparkSession

    assert isinstance(spark, SparkSession)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    statement = DDL_TEMPLATE.format(catalog=CATALOG, namespace=NAMESPACE)
    try:
        spark.sql(statement)
    except Exception as error:
        return {
            "id": CELL_ID,
            "class": type(error).__name__,
            "module": type(error).__module__,
            "message": str(error),
        }
    raise AssertionError(f"live Spark created the multi-argument table: {statement}")


def build_oracle(cell: dict[str, Any]) -> dict[str, Any]:
    """The fixture document carrying one recorded refusal cell."""
    return {
        "provenance": (
            "live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 on a "
            "Hadoop catalog, recorded 2026-09-18 by _record_v3_multiarg_1.py"
        ),
        "spark_version": "4.1.2",
        "iceberg_runtime": ICEBERG_SPARK_RUNTIME_GAV,
        "ddl": DDL_TEMPLATE.format(catalog=CATALOG, namespace=NAMESPACE),
        "cell": cell,
    }


def main() -> None:
    """Record the refusal cell and write the fixture beside this driver."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", default=None)
    args = parser.parse_args()
    warehouse = args.warehouse or tempfile.mkdtemp(prefix="v3-multiarg-1-oracle-")
    spark = spark_session(warehouse)
    try:
        cell = record_cell(spark)
        oracle = build_oracle(cell)
        if FIXTURE.exists():
            recorded = json.loads(FIXTURE.read_text(encoding="utf-8"))
            if recorded.get("cell") != cell:
                raise AssertionError(
                    f"live Spark drifted from {FIXTURE}: {cell!r} != {recorded.get('cell')!r}"
                )
            print(f"fixture {FIXTURE} matches live Spark", flush=True)
            return
        FIXTURE.write_text(json.dumps(oracle, indent=2, sort_keys=True), encoding="utf-8")
        print(f"wrote {FIXTURE}", flush=True)
    finally:
        spark.stop()
        if args.warehouse is None:
            shutil.rmtree(warehouse, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
