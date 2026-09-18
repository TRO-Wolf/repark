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
                f"CREATE TABLE {struct_table} (id INT, s STRUCT<a: INT>) USING iceberg "
                f"{properties}",
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


def build_schema_cells(format_version: str) -> list[tuple[str, list[str], str]]:
    """Return the ordered `(label, statements, table)` cells whose answer is the table metadata.

    Args:
        format_version: The Iceberg format version, `"2"` or `"3"`.

    Returns:
        The cells. Each records the current schema of `table` from its metadata file, with
        field ids, `required` and `doc`, plus the statement error when one refused.
    """
    properties = f"TBLPROPERTIES ('format-version'='{format_version}')"
    prefix = f"{SPARK_CATALOG}.{ADOPTED_NAMESPACE}"
    version = f"v{format_version}"
    two_child = "(id INT, s STRUCT<a: INT, b: STRING>) USING iceberg"
    deep = (
        "(id INT, s STRUCT<a: INT, b: STRUCT<c: INT>>, arr ARRAY<STRUCT<x: INT>>, "
        "m MAP<STRING, STRUCT<q: INT>>) USING iceberg"
    )
    required = "(id INT, s STRUCT<a: INT NOT NULL, b: STRING>) USING iceberg"
    evolutions = (
        ("rename_dotted", "RENAME COLUMN s.a TO `x.y`"),
        ("add_dotted_leaf", "ADD COLUMN s.`x.y` INT"),
        ("add_dotted_top", "ADD COLUMN `p.q` INT"),
        ("rename_double_quoted", 'RENAME COLUMN s.a TO "x.y"'),
        ("add_double_quoted_leaf", 'ADD COLUMN s."x.y" INT'),
        ("add_first", "ADD COLUMN s.z INT FIRST"),
        ("add_after", "ADD COLUMN s.w INT AFTER a"),
        ("add_after_dotted", "ADD COLUMN s.w INT AFTER s.a"),
        ("add_comment", "ADD COLUMN s.d INT COMMENT 'c'"),
        ("add_comment_double_quoted_dotted", 'ADD COLUMN s.d INT COMMENT "x.y"'),
        ("add_comment_double_quoted", 'ADD COLUMN s.e INT COMMENT "c"'),
        ("add_comment_double_quoted_first", 'ADD COLUMN s.f INT COMMENT "x.y" FIRST'),
        ("add_map_child", "ADD COLUMN s.mm MAP<STRING, INT>"),
        ("add_struct_child", "ADD COLUMN s.st STRUCT<u: INT, v: STRUCT<w: INT>>"),
        ("add_list_child", "ADD COLUMN s.al ARRAY<STRUCT<k: INT>>"),
        ("add_duplicate_child", "ADD COLUMN s.a INT"),
        ("add_unknown_parent", "ADD COLUMN nope.z INT"),
    )
    cells = [
        (
            f"{version}_create_field_ids",
            [f"CREATE TABLE {prefix}.ids_{version} {deep} {properties}"],
            f"ids_{version}",
        ),
        (
            f"{version}_create_required_child",
            [f"CREATE TABLE {prefix}.req_{version} {required} {properties}"],
            f"req_{version}",
        ),
    ]
    for label, clause in evolutions:
        table = f"{label}_{version}"
        cells.append(
            (
                f"{version}_{label}",
                [
                    f"CREATE TABLE {prefix}.{table} {two_child} {properties}",
                    f"ALTER TABLE {prefix}.{table} {clause}",
                ],
                table,
            )
        )
    return cells


def current_metadata_schema(table: str) -> dict[str, Any] | None:
    """Return the current schema JSON of a Hadoop-catalog table, or `None` when it has none.

    Args:
        table: The table name under `ADOPTED_WAREHOUSE / ADOPTED_NAMESPACE`.

    Returns:
        The `schemas` entry whose id is `current-schema-id`, read from the metadata file
        `version-hint.text` names.
    """
    metadata_dir = ADOPTED_WAREHOUSE / ADOPTED_NAMESPACE / table / "metadata"
    hint = metadata_dir / "version-hint.text"
    if not hint.exists():
        return None
    version = hint.read_text(encoding="utf-8").strip()
    metadata = json.loads((metadata_dir / f"v{version}.metadata.json").read_text("utf-8"))
    for schema in metadata["schemas"]:
        if schema["schema-id"] == metadata["current-schema-id"]:
            return schema
    return None


def record_schema_cell(spark: Any, statements: list[str], table: str) -> dict[str, Any]:
    """Run one schema cell's statements and record the table's metadata schema and read shape.

    Args:
        spark: The live PySpark session.
        statements: The DDL to run in order; the first failure stops the cell.
        table: The table whose metadata the cell records.

    Returns:
        `statements`, `table`, `error`, `metadata_schema` (after the statements, including a
        refused one), and Spark's `SELECT *` schema JSON when the table exists.
    """
    cell = record_cell(spark, statements, None)
    cell["table"] = table
    cell["metadata_schema"] = current_metadata_schema(table)
    if cell["metadata_schema"] is not None:
        frame = spark.sql(f"SELECT * FROM {SPARK_CATALOG}.{ADOPTED_NAMESPACE}.{table}")
        cell["read_schema"] = frame.schema.jsonValue()
    return cell


def describe_error(exc: Exception) -> dict[str, Any]:
    """Return the exception class, error condition and first message line Spark raised.

    Args:
        exc: The exception PySpark raised for a statement.

    Returns:
        `python_class`, `java_class` (the JVM exception class when py4j did not convert it),
        `condition` (Spark's error class when it has one), `java_cause_class` and `message`
        (the first non-empty line).
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
        "message": next((line for line in message.splitlines() if line.strip()), "")[:400],
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
        except Exception as exc:
            cell["error"] = describe_error(exc)
            break
    if query is not None and cell["error"] is None:
        frame = spark.sql(query)
        cell["query"] = query
        cell["schema"] = frame.schema.jsonValue()
        cell["rows"] = [row.asDict(recursive=True) for row in frame.collect()]
    return cell


DATAFRAME_CREATE_LABELS = ("create_field_ids", "create_required_child")


def record_dataframe_create(
    spark: Any, format_version: str, source: dict[str, Any]
) -> dict[str, Any]:
    """Create a table through `writeTo(...).create()` with a SQL-created table's read schema.

    Args:
        spark: The live PySpark session.
        format_version: The Iceberg format version, `"2"` or `"3"`.
        source: The recorded schema cell whose `read_schema` the DataFrame carries.

    Returns:
        `table`, the `read_schema` used and the new table's `metadata_schema`.
    """
    from pyspark.sql.types import StructType

    table = f"df_{source['table']}"
    schema = StructType.fromJson(source["read_schema"])
    frame = spark.createDataFrame([], schema)
    frame.writeTo(f"{SPARK_CATALOG}.{ADOPTED_NAMESPACE}.{table}").using("iceberg").tableProperty(
        "format-version", format_version
    ).create()
    return {
        "table": table,
        "read_schema": source["read_schema"],
        "metadata_schema": current_metadata_schema(table),
    }


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
        "schema_cells": {},
        "dataframe_create_cells": {},
    }
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{ADOPTED_NAMESPACE}")
    for format_version in FORMAT_VERSIONS:
        for label, statements, query in build_cells(format_version):
            cell = record_cell(spark, statements, query)
            oracle["cells"][label] = cell
            print(label, cell["error"], cell.get("rows"), flush=True)
        for label, statements, table in build_schema_cells(format_version):
            cell = record_schema_cell(spark, statements, table)
            oracle["schema_cells"][label] = cell
            print(label, cell["error"], json.dumps(cell["metadata_schema"]), flush=True)
        for label in DATAFRAME_CREATE_LABELS:
            source = oracle["schema_cells"][f"v{format_version}_{label}"]
            cell = record_dataframe_create(spark, format_version, source)
            oracle["dataframe_create_cells"][f"v{format_version}_{label}"] = cell
            print(label, "dataframe", json.dumps(cell["metadata_schema"]), flush=True)
    spark.stop()
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    copy_adopted_tables()
    ORACLE_FILE.write_text(json.dumps(oracle, indent=2, default=str) + "\n", encoding="utf-8")
    shutil.rmtree(ADOPTED_WAREHOUSE, ignore_errors=True)


if __name__ == "__main__":
    main()
