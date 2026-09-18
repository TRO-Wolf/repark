"""Snapshot-property collision cells for ICE-WRITE-OPTIONS-1 (appends COLL-* to the fixture).

Run with a Spark 4.1.2 interpreter; the warehouse is ``--warehouse`` (a fresh temp dir when
omitted) and ``REPARK_ORACLE_IVY`` optionally points ``spark.jars.ivy`` at a warm cache::

    python python/repark/tests/_record_ice_write_options_3_oracle.py --warehouse <dir>

Not collected by pytest.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

CATALOG = "sc"
NAMESPACE = "ns"
FIXTURE = Path(__file__).resolve().parent / "ice_write_options_1_spark_oracle.json"
EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
STABLE_MESSAGE = re.compile(r"Multiple entries with same key: \S+=\S* and \S+=\S*")
CASES: tuple[tuple[str, str], ...] = (
    ("added-records", "999"),
    ("engine-name", "custom-engine"),
    ("engine-version", "9.9.9"),
    ("operation", "stolen-op"),
    ("engine.operation-id", "stolen-id"),
    ("changed-partition-count", "42"),
    ("total-records", "7"),
    ("deleted-data-files", "3"),
    ("run_id", "control"),
)


def spark_session(warehouse: str) -> Any:
    """A local Spark session on a Hadoop Iceberg catalog rooted at ``warehouse``."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("repark-ice-write-options-3-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", EXTENSIONS)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    return builder.getOrCreate()


def stable_message(raw: str) -> str:
    """The run-stable ``Multiple entries with same key`` text, without wrapper or job ids."""
    found = STABLE_MESSAGE.search(raw)
    return found.group(0) if found else raw[:600]


def collision_cell(
    index: int,
    key: str,
    value: str,
    snapshots_before: int,
    snapshot_count: int,
    error: tuple[str, str] | None,
    summary: dict[str, str] | None,
    rows: int | None,
) -> dict[str, Any]:
    """One COLL fixture cell; commit cells carry the summary facts, error cells the message."""
    cell: dict[str, Any] = {
        "id": f"COLL-{index:02d}-{key}",
        "key": key,
        "value": value,
        "snapshots_before": snapshots_before,
        "snapshot_count": snapshot_count,
        "error": None,
    }
    if error is not None:
        cell["error"] = {"class": error[0], "message": stable_message(error[1])}
        return cell
    cell["summary_value_for_key"] = (summary or {}).get(key)
    cell["summary_keys"] = sorted(summary or {})
    cell["rows"] = rows
    return cell


def count_snapshots(spark: Any, table: str) -> int:
    """Snapshot count of ``table``."""
    return int(spark.sql(f"SELECT count(*) AS c FROM {table}.snapshots").collect()[0]["c"])


def latest_summary(spark: Any, table: str) -> dict[str, str]:
    """Summary map of the newest snapshot of ``table``."""
    rows = spark.sql(
        f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return dict(rows[0]["summary"]) if rows else {}


def record_case(spark: Any, index: int, key: str, value: str) -> dict[str, Any]:
    """Seed one table, append with one colliding extra, and record Spark's answer."""
    table = f"{CATALOG}.{NAMESPACE}.c{index}"
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1)")
    before = count_snapshots(spark, table)
    writer = spark.range(2, 4).toDF("id").writeTo(table)
    try:
        writer.option(f"snapshot-property.{key}", value).append()
    except Exception as exc:
        error = (type(exc).__name__, str(exc))
        return collision_cell(
            index, key, value, before, count_snapshots(spark, table), error, None, None
        )
    rows = int(spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0]["c"])
    return collision_cell(
        index,
        key,
        value,
        before,
        count_snapshots(spark, table),
        None,
        latest_summary(spark, table),
        rows,
    )


def main() -> None:
    """Record the COLL cells and append them to ``--out`` (or the committed fixture)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=None)
    parser.add_argument("--warehouse", default=None)
    args = parser.parse_args()
    out_path = Path(args.out) if args.out else None
    warehouse = args.warehouse or tempfile.mkdtemp(prefix="ice-write-opts3-oracle-")
    spark = spark_session(warehouse)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        cells = [record_case(spark, index, key, value) for index, (key, value) in enumerate(CASES)]
        base = out_path if out_path is not None and out_path.exists() else FIXTURE
        dest = out_path or FIXTURE
        out = json.loads(base.read_text(encoding="utf-8"))
        out["cells"].extend(cells)
        dest.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
        print(f"appended {len(cells)} cells to {dest}", flush=True)
    finally:
        spark.stop()
        if args.warehouse is None:
            shutil.rmtree(warehouse, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
