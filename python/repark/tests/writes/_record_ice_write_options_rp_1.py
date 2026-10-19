"""Record Spark 4.1.2's answers for ICE-WRITE-OPTIONS-RP-1 — caller summary on replace commits.

NOT a ``test_`` module: pytest never collects it. It creates one fresh table per cell
on live PySpark from the DDL below, seeds ``(1, 'a'), (2, 'b')``, writes the source
``(3, 'a')`` through the cell's door with the cell's ``snapshot-property`` option, and
records whether the write committed plus the newest snapshot summary's
``replace-partitions`` and ``k`` values into
``python/repark-parity/fixtures/torture/data/ice_write_options_rp_1/spark_rp_oracle.json``.

Cells: ``insertInto_dynamic_false`` / ``insertInto_dynamic_true`` (``insertInto`` overwrite
under ``partitionOverwriteMode=dynamic`` with the option ``false`` / ``true``),
``overwritePartitions_true`` / ``overwritePartitions_false``
(``writeTo(t).overwritePartitions()`` with the option ``true`` / ``false``),
``overwritePartitions_control_other`` (``overwritePartitions()`` with
``snapshot-property.k=v`` and no ``replace-partitions`` option). The pins in
``test_ice_write_options_rp_1.py`` read every expectation from the oracle file.

Run with a PySpark 4.1.2 interpreter that can resolve the Iceberg runtime GAV from
``_oracle_pins``; ``REPARK_ORACLE_IVY`` optionally points ``spark.jars.ivy`` at a warm
cache::

    _record_ice_write_options_rp_1.py [warehouse]

``warehouse`` is created fresh (removed first) and defaults to env-driven scratch.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE_DIR = (
    Path(__file__).resolve().parents[2]
    / "repark-parity/fixtures/torture/data/ice_write_options_rp_1"
)
ORACLE_FILE = FIXTURE_DIR / "spark_rp_oracle.json"
SPARK_CATALOG = "rp"
SPARK_NAMESPACE = "ns"
TABLE_DDL = "CREATE TABLE <t> (id INT, p STRING) USING iceberg PARTITIONED BY (p)"
SEED_SQL = "INSERT INTO <t> VALUES (1, 'a'), (2, 'b')"
SOURCE_SCHEMA = "id INT, p STRING"
SOURCE_ROWS: tuple[tuple[int, str], ...] = ((3, "a"),)
DYNAMIC_CONF = "spark.sql.sources.partitionOverwriteMode"
REPLACE_KEY = "snapshot-property.replace-partitions"
CONTROL_KEY = "snapshot-property.k"
CELLS: tuple[tuple[str, str, str, str], ...] = (
    ("insertInto_dynamic_false", "insertInto", REPLACE_KEY, "false"),
    ("insertInto_dynamic_true", "insertInto", REPLACE_KEY, "true"),
    ("overwritePartitions_true", "overwritePartitions", REPLACE_KEY, "true"),
    ("overwritePartitions_false", "overwritePartitions", REPLACE_KEY, "false"),
    ("overwritePartitions_control_other", "overwritePartitions", CONTROL_KEY, "v"),
)


def start_spark(warehouse: Path) -> Any:
    """Start the one short-lived local Spark session with a Hadoop catalog at the root.

    Args:
        warehouse: The warehouse directory the Hadoop catalog serves.

    Returns:
        The PySpark session.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ice-write-options-rp-1-oracle")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{SPARK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.warehouse", str(warehouse))
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    return builder.getOrCreate()


def snapshot_count(spark: Any, table: str) -> int:
    """Return the snapshot count of ``table``.

    Args:
        spark: The live PySpark session.
        table: The catalog-qualified table name.

    Returns:
        The number of rows in the table's snapshots view.
    """
    return int(spark.sql(f"SELECT count(*) AS c FROM {table}.snapshots").collect()[0]["c"])


def latest_summary(spark: Any, table: str) -> dict[str, str]:
    """Return the summary map of the newest snapshot of ``table``.

    Args:
        spark: The live PySpark session.
        table: The catalog-qualified table name.

    Returns:
        The newest snapshot's summary as a plain string map.
    """
    rows = spark.sql(
        f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return dict(rows[0]["summary"]) if rows else {}


def statement_text(door: str, key: str, value: str) -> str:
    """Return the recorded write statement for one cell, with ``<t>`` as the table.

    Args:
        door: The write door, ``insertInto`` or ``overwritePartitions``.
        key: The ``snapshot-property`` option key the cell sets.
        value: The option value the cell sets.

    Returns:
        The statement text the cell replays.
    """
    if door == "insertInto":
        return (
            f'src.write.format("iceberg").option("{key}", "{value}")'
            f'.mode("overwrite").insertInto("<t>") under {DYNAMIC_CONF}=dynamic'
        )
    return f'src.writeTo("<t>").option("{key}", "{value}").overwritePartitions()'


def record_cell(spark: Any, name: str, door: str, key: str, value: str) -> dict[str, Any]:
    """Seed one table, write the source through the cell's door, record Spark's answer.

    Args:
        spark: The live PySpark session.
        name: The oracle cell key, used as the table name.
        door: The write door, ``insertInto`` or ``overwritePartitions``.
        key: The ``snapshot-property`` option key the cell sets.
        value: The option value the cell sets.

    Returns:
        The cell record: ``door``, ``statement``, ``committed`` and the newest
        summary's ``replace-partitions`` and ``k`` values, or ``error`` on refusal.
    """
    table = f"{SPARK_CATALOG}.{SPARK_NAMESPACE}.{name}"
    spark.sql(TABLE_DDL.replace("<t>", table))
    spark.sql(SEED_SQL.replace("<t>", table))
    source = spark.createDataFrame(list(SOURCE_ROWS), SOURCE_SCHEMA)
    before = snapshot_count(spark, table)
    cell: dict[str, Any] = {
        "door": door,
        "statement": statement_text(door, key, value),
        "option_key": key,
        "option_value": value,
    }
    try:
        if door == "insertInto":
            spark.conf.set(DYNAMIC_CONF, "dynamic")
            try:
                (
                    source.write.format("iceberg")
                    .option(key, value)
                    .mode("overwrite")
                    .insertInto(table)
                )
            finally:
                spark.conf.unset(DYNAMIC_CONF)
        else:
            source.writeTo(table).option(key, value).overwritePartitions()
        cell["outcome"] = "ok"
    except Exception as exc:
        cell["outcome"] = "error"
        cell["error"] = f"{type(exc).__name__}: {str(exc).splitlines()[0]}"
    summary = latest_summary(spark, table)
    after = snapshot_count(spark, table)
    cell["committed"] = after > before
    cell["last_summary_replace_partitions"] = summary.get("replace-partitions")
    cell["last_summary_k"] = summary.get("k")
    return cell


def record_all(spark: Any) -> dict[str, Any]:
    """Record every cell on the live session.

    Args:
        spark: The live PySpark session.

    Returns:
        The oracle document with ``oracle`` and ``cells``.
    """
    short_gav = ICEBERG_SPARK_RUNTIME_GAV.split(":", 1)[1]
    truth: dict[str, Any] = {
        "oracle": f"PySpark {spark.version} + {short_gav}",
        "table_ddl": TABLE_DDL,
        "seed": SEED_SQL,
        "source": [list(row) for row in SOURCE_ROWS],
        "cells": {},
    }
    for name, door, key, value in CELLS:
        truth["cells"][name] = record_cell(spark, name, door, key, value)
        print(name, truth["cells"][name].get("outcome"), flush=True)
    return truth


def main(warehouse: Path | None = None) -> None:
    """Record every cell and write the fixture oracle JSON.

    Args:
        warehouse: The scratch warehouse root, created fresh. Defaults to
            env-driven scratch when omitted.
    """
    root = warehouse or Path(tempfile.mkdtemp(prefix="repark-ice-write-options-rp-1-"))
    if root.exists():
        shutil.rmtree(root)
    spark = start_spark(root)
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{SPARK_NAMESPACE}")
    truth = record_all(spark)
    spark.stop()
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    ORACLE_FILE.write_text(json.dumps(truth, indent=1, sort_keys=True), encoding="utf-8")
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else None)
