"""Record Spark 4.1.2's answers for ICE-NESTED-EVO-1 and the Spark-written nested-evolution tables.

Run with a PySpark 4.1.2 interpreter that can resolve iceberg-spark-runtime 1.11.0. The driver
writes `oracle.json` and copies four Spark-written tables into the fixture directory. The table
metadata keeps absolute paths under `ADOPTED_WAREHOUSE`, so the pins copy each table back there.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

FIXTURE_DIR = (
    Path(__file__).resolve().parents[2] / "repark-parity/fixtures/torture/data/ice_nested_evo_1"
)
ORACLE_FILE = FIXTURE_DIR / "oracle.json"
ADOPTED_WAREHOUSE = Path("/tmp/repark-ice-nested-evo-1")
ADOPTED_NAMESPACE = "ns"
ADOPTED_TABLES = ("st_add_v2", "st_add_v3", "list_add_v3", "map_add_v3")
SPARK_CATALOG = "sc"
RUNTIME_GAV = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
FORMAT_VERSIONS = ("2", "3")


def build_cells(format_version: str) -> list[tuple[str, list[str], str | None]]:
    """Return the ordered `(label, statements, query)` cells for one format version.

    Args:
        format_version: The Iceberg format version, `"2"` or `"3"`.

    Returns:
        The cells. A cell with no statements reads a table an earlier cell built.
    """
    properties = f"TBLPROPERTIES ('format-version'='{format_version}')"
    prefix = f"{SPARK_CATALOG}.{ADOPTED_NAMESPACE}"
    struct_table = f"{prefix}.st_add_v{format_version}"
    list_table = f"{prefix}.list_add_v{format_version}"
    map_table = f"{prefix}.map_add_v{format_version}"
    create_table = f"{prefix}.nested_create_v{format_version}"
    rename_table = f"{prefix}.nested_rename_v{format_version}"
    drop_table = f"{prefix}.nested_drop_v{format_version}"
    two_child = "(id INT, s STRUCT<a: INT, b: STRING>) USING iceberg"
    version = f"v{format_version}"
    return [
        (
            f"{version}_struct_child_add_read",
            [
                f"CREATE TABLE {struct_table} (id INT, s STRUCT<a: INT>) USING iceberg {properties}",
                f"INSERT INTO {struct_table} SELECT 1, named_struct('a', 1)",
                f"ALTER TABLE {struct_table} ADD COLUMN s.b STRING",
                f"INSERT INTO {struct_table} SELECT 2, named_struct('a', 2, 'b', 'y')",
            ],
            f"SELECT id, s FROM {struct_table} ORDER BY id",
        ),
        (
            f"{version}_struct_child_add_leaf_read",
            [],
            f"SELECT id, s.a, s.b FROM {struct_table} ORDER BY id",
        ),
        (
            f"{version}_struct_child_add_filter_null",
            [],
            f"SELECT id FROM {struct_table} WHERE s.b IS NULL ORDER BY id",
        ),
        (
            f"{version}_list_element_child_add_read",
            [
                f"CREATE TABLE {list_table} (id INT, arrs ARRAY<STRUCT<x: INT>>) USING iceberg "
                f"{properties}",
                f"INSERT INTO {list_table} SELECT 1, array(named_struct('x', 1))",
                f"ALTER TABLE {list_table} ADD COLUMN arrs.element.y INT",
                f"INSERT INTO {list_table} SELECT 2, array(named_struct('x', 2, 'y', 20))",
            ],
            f"SELECT id, arrs FROM {list_table} ORDER BY id",
        ),
        (f"{version}_list_element_child_add_describe", [], f"DESCRIBE TABLE {list_table}"),
        (
            f"{version}_map_value_child_add_read",
            [
                f"CREATE TABLE {map_table} (id INT, m MAP<STRING, STRUCT<p: INT>>) USING iceberg "
                f"{properties}",
                f"INSERT INTO {map_table} SELECT 1, map('k', named_struct('p', 1))",
                f"ALTER TABLE {map_table} ADD COLUMN m.value.q STRING",
                f"INSERT INTO {map_table} SELECT 2, map('k', named_struct('p', 2, 'q', 'z'))",
            ],
            f"SELECT id, m FROM {map_table} ORDER BY id",
        ),
        (f"{version}_map_value_child_add_describe", [], f"DESCRIBE TABLE {map_table}"),
        (
            f"{version}_nested_create_then_add",
            [
                f"CREATE TABLE {create_table} {two_child} {properties}",
                f"INSERT INTO {create_table} SELECT 1, named_struct('a', 1, 'b', 'p')",
                f"ALTER TABLE {create_table} ADD COLUMN s.c BIGINT",
            ],
            f"SELECT id, s FROM {create_table} ORDER BY id",
        ),
        (f"{version}_nested_create_describe", [], f"DESCRIBE TABLE {create_table}"),
        (
            f"{version}_nested_rename_read",
            [
                f"CREATE TABLE {rename_table} {two_child} {properties}",
                f"INSERT INTO {rename_table} SELECT 1, named_struct('a', 1, 'b', 'p')",
                f"ALTER TABLE {rename_table} RENAME COLUMN s.a TO a2",
            ],
            f"SELECT id, s FROM {rename_table} ORDER BY id",
        ),
        (
            f"{version}_nested_drop_read",
            [
                f"CREATE TABLE {drop_table} {two_child} {properties}",
                f"INSERT INTO {drop_table} SELECT 1, named_struct('a', 1, 'b', 'p')",
                f"ALTER TABLE {drop_table} DROP COLUMN s.b",
            ],
            f"SELECT id, s FROM {drop_table} ORDER BY id",
        ),
        (
            f"{version}_add_required_nested_child",
            [f"ALTER TABLE {struct_table} ADD COLUMN s.r INT NOT NULL"],
            f"SELECT id, s FROM {struct_table} ORDER BY id",
        ),
    ]


def describe_error(exc: Exception) -> dict[str, Any]:
    """Return the exception class, error condition and first message line Spark raised.

    Args:
        exc: The exception PySpark raised for a statement.

    Returns:
        `python_class`, `java_class` (the JVM exception class when py4j did not convert it),
        `condition` (Spark's error class when it has one), `java_cause_class` and `message`
        (the first line).
    """
    java_exception = getattr(exc, "java_exception", None)
    condition = None
    cause_class = None
    if java_exception is not None:
        java_class = java_exception.getClass().getName()
        message = str(java_exception.getMessage() or "")
        condition = java_exception.getCondition()
        cause = java_exception.getCause()
        if cause is not None:
            cause_class = cause.getClass().getName()
    else:
        java_class = None
        message = str(exc)
        method = getattr(exc, "getCondition", None)
        if callable(method):
            condition = method()
    return {
        "python_class": type(exc).__name__,
        "java_class": java_class,
        "condition": condition,
        "java_cause_class": cause_class,
        "message": message.splitlines()[0][:400] if message else "",
    }


def record_cell(spark: Any, statements: list[str], query: str | None) -> dict[str, Any]:
    """Run one cell's statements, then its query when every statement succeeded.

    Args:
        spark: The live PySpark session.
        statements: The DDL and DML to run in order.
        query: The read to record, or `None`.

    Returns:
        The cell record with `error` set when a statement failed.
    """
    cell: dict[str, Any] = {"statements": statements, "error": None}
    for statement in statements:
        try:
            spark.sql(statement)
        except Exception as exc:  # noqa: BLE001
            cell["error"] = describe_error(exc)
            break
    if query is not None and cell["error"] is None:
        frame = spark.sql(query)
        cell["query"] = query
        cell["schema"] = frame.schema.jsonValue()
        cell["rows"] = [row.asDict(recursive=True) for row in frame.collect()]
    return cell


def start_spark() -> Any:
    """Start the one short-lived local Spark session with a Hadoop catalog at the baked root.

    Returns:
        The PySpark session.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ice-nested-evo-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{SPARK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.warehouse", str(ADOPTED_WAREHOUSE))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
    )
    ivy = os.environ.get("REPARK_SPARK_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    return builder.getOrCreate()


def copy_adopted_tables() -> None:
    """Copy the four adoption tables into the fixture directory without Hadoop `.crc` files."""
    for table in ADOPTED_TABLES:
        target = FIXTURE_DIR / table
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(
            ADOPTED_WAREHOUSE / ADOPTED_NAMESPACE / table,
            target,
            ignore=shutil.ignore_patterns("*.crc"),
        )


def main() -> None:
    """Record every cell for format versions 2 and 3 and write the fixture."""
    shutil.rmtree(ADOPTED_WAREHOUSE, ignore_errors=True)
    ADOPTED_WAREHOUSE.mkdir(parents=True)
    spark = start_spark()
    spark.sparkContext.setLogLevel("ERROR")
    oracle: dict[str, Any] = {
        "spark_version": spark.version,
        "iceberg_runtime": RUNTIME_GAV,
        "warehouse": str(ADOPTED_WAREHOUSE),
        "cells": {},
    }
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{ADOPTED_NAMESPACE}")
    for format_version in FORMAT_VERSIONS:
        for label, statements, query in build_cells(format_version):
            cell = record_cell(spark, statements, query)
            oracle["cells"][label] = cell
            print(label, cell["error"], cell.get("rows"), flush=True)
    spark.stop()
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    copy_adopted_tables()
    ORACLE_FILE.write_text(json.dumps(oracle, indent=2, default=str) + "\n", encoding="utf-8")
    shutil.rmtree(ADOPTED_WAREHOUSE, ignore_errors=True)


if __name__ == "__main__":
    main()
