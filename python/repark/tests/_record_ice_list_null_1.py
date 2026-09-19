"""Record Spark 4.1.2's answers for ICE-LIST-NULL-1: DELETE and UPDATE with IS NULL.

NOT a ``test_`` module: pytest never collects it. It creates each cell's table on live
PySpark from the shape DDL below, seeds four rows, runs the cell's DELETE or UPDATE
with a nested-column IS NULL predicate, and records whether the statement answered,
the ids left behind, the newest snapshot's operation and its ``added-delete-files`` /
``added-dvs`` summary values into
``python/repark-parity/fixtures/torture/data/ice_list_null_1/spark_list_null_oracle.json``.

Cells: shapes ``list_int`` / ``list_struct`` / ``map_int`` / ``struct`` x predicates
``xs IS NULL`` / ``xs IS NOT NULL`` / ``id > 1 AND xs IS NULL`` /
``xs IS NULL OR id = 1`` x statements ``delete`` / ``update`` x modes
``copy-on-write`` / ``merge-on-read`` x format versions 2 / 3: 128 cells. The pins
in ``test_ice_list_null_1.py`` import :func:`record_cell` from this module for the
live tier, so the one-cell recording lives here once.

Run with a PySpark 4.1.2 interpreter that can resolve the Iceberg runtime GAV from
``_oracle_pins``; ``REPARK_ORACLE_IVY`` optionally points ``spark.jars.ivy`` at a warm
cache::

    _record_ice_list_null_1.py [warehouse]

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
    Path(__file__).resolve().parents[2] / "repark-parity/fixtures/torture/data/ice_list_null_1"
)
ORACLE_FILE = FIXTURE_DIR / "spark_list_null_oracle.json"
SPARK_CATALOG = "ora"
SPARK_NAMESPACE = "ns"
FORMAT_VERSIONS = (2, 3)
MODES = ("copy-on-write", "merge-on-read")
STATEMENTS = ("delete", "update")
PREDICATES = ("xs IS NULL", "xs IS NOT NULL", "id > 1 AND xs IS NULL", "xs IS NULL OR id = 1")
SHAPES: dict[str, tuple[str, str]] = {
    "list_int": (
        "xs ARRAY<INT>",
        "(1, array(1, 2)), (2, NULL), (3, CAST(array() AS ARRAY<INT>)), "
        "(4, array(CAST(NULL AS INT)))",
    ),
    "list_struct": (
        "xs ARRAY<STRUCT<a: INT>>",
        "(1, array(named_struct('a', 1))), (2, NULL), "
        "(3, CAST(array() AS ARRAY<STRUCT<a: INT>>)), "
        "(4, array(CAST(NULL AS STRUCT<a: INT>)))",
    ),
    "map_int": (
        "xs MAP<STRING, INT>",
        "(1, map('k', 1)), (2, NULL), (3, CAST(map() AS MAP<STRING, INT>)), "
        "(4, map('k', CAST(NULL AS INT)))",
    ),
    "struct": (
        "xs STRUCT<a: INT>",
        "(1, named_struct('a', 1)), (2, NULL), "
        "(3, named_struct('a', CAST(NULL AS INT))), (4, named_struct('a', 4))",
    ),
}
UPDATE_SET = "id = id + 100"


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
        .appName("ice-list-null-1-oracle")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
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


def statement_sql(table: str, statement: str, predicate: str) -> str:
    """Return the DELETE or UPDATE statement one cell runs.

    Args:
        table: The catalog-qualified table name.
        statement: ``delete`` or ``update``.
        predicate: The WHERE predicate over the nested column.

    Returns:
        The Spark SQL text with the table name in place.
    """
    if statement == "delete":
        return f"DELETE FROM {table} WHERE {predicate}"
    return f"UPDATE {table} SET {UPDATE_SET} WHERE {predicate}"


def record_cell(
    spark: Any, table: str, shape: str, version: int, mode: str, statement: str, predicate: str
) -> dict[str, Any]:
    """Create, seed and run one cell on the live session and record Spark's answer.

    Args:
        spark: The live PySpark session.
        table: The catalog-qualified table name, created fresh by this function.
        shape: One of ``SHAPES``.
        version: The Iceberg format version, 2 or 3.
        mode: ``copy-on-write`` or ``merge-on-read``.
        statement: ``delete`` or ``update``.
        predicate: The WHERE predicate over the nested column.

    Returns:
        The cell record: ``shape``, ``version``, ``mode``, ``statement``,
        ``predicate``, ``sql`` (with the table as ``<t>``), ``ok``, ``ids``,
        ``operation``, ``added_delete_files`` and ``added_dvs``: or ``ok``
        False with ``error`` when the statement refused.
    """
    column, values = SHAPES[shape]
    spark.sql(
        f"CREATE TABLE {table} (id INT, {column}) USING iceberg TBLPROPERTIES ("
        f"'format-version'='{version}', 'write.delete.mode'='{mode}', "
        f"'write.update.mode'='{mode}')"
    )
    spark.sql(f"INSERT INTO {table} VALUES {values}")
    cell: dict[str, Any] = {
        "shape": shape,
        "version": version,
        "mode": mode,
        "statement": statement,
        "predicate": predicate,
    }
    sql = statement_sql(table, statement, predicate)
    cell["sql"] = sql.replace(table, "<t>")
    try:
        spark.sql(sql)
        cell["ok"] = True
        cell["ids"] = sorted(row.id for row in spark.sql(f"SELECT id FROM {table}").collect())
        snap = spark.sql(
            f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()[0]
        cell["operation"] = snap.operation
        cell["added_delete_files"] = snap.summary.get("added-delete-files", "0")
        cell["added_dvs"] = snap.summary.get("added-dvs", "0")
    except Exception as exc:
        cell["ok"] = False
        cell["error"] = str(exc).splitlines()[0][:300]
    return cell


def record_all(spark: Any) -> dict[str, Any]:
    """Record every shape x version x mode x statement x predicate cell on the live session.

    Args:
        spark: The live PySpark session.

    Returns:
        The oracle document with ``spark``, ``iceberg`` and ``cells``.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{SPARK_NAMESPACE}")
    cells: list[dict[str, Any]] = []
    number = 0
    for shape in SHAPES:
        for version in FORMAT_VERSIONS:
            for mode in MODES:
                for statement in STATEMENTS:
                    for predicate in PREDICATES:
                        number += 1
                        table = f"{SPARK_CATALOG}.{SPARK_NAMESPACE}.t{number}"
                        cell = record_cell(spark, table, shape, version, mode, statement, predicate)
                        cells.append(cell)
                        print(json.dumps(cell), flush=True)
    return {"spark": spark.version, "iceberg": ICEBERG_SPARK_RUNTIME_GAV, "cells": cells}


def main(warehouse: Path | None = None) -> None:
    """Record every cell and write the fixture oracle JSON.

    Args:
        warehouse: The scratch warehouse root, created fresh. Defaults to
            env-driven scratch when omitted.
    """
    root = warehouse or Path(tempfile.mkdtemp(prefix="repark-ice-list-null-1-"))
    if root.exists():
        shutil.rmtree(root)
    spark = start_spark(root)
    spark.sparkContext.setLogLevel("ERROR")
    truth = record_all(spark)
    spark.stop()
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    ORACLE_FILE.write_text(json.dumps(truth, indent=1), encoding="utf-8")
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else None)
