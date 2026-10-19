"""Record Spark 4.1.2's answers for ICE-ARRAY-INSERT-1 — inserts into array columns.

NOT a ``test_`` module: pytest never collects it. It creates each cell's table on live
PySpark from the shape DDL below, writes it through the cell's door, and records the
read-back rows, Spark's schema string, the file count and the first data file's parquet
footer field ids into
``python/repark-parity/fixtures/torture/data/ice_array_insert_1/spark_array_insert_oracle.json``.

Cells: shapes ``list_int`` / ``list_struct`` / ``map_list`` x doors ``sql_values`` /
``sql_select`` / ``writeto_append`` / ``insert_into`` / ``save_as_table_append`` x format
versions 2 / 3 — 30 cells. The pins in ``test_ice_array_insert_1.py`` import
:func:`field_ids` from this module, so the footer walk lives here once.

Run with a PySpark 4.1.2 interpreter that can resolve the Iceberg runtime GAV from
``_oracle_pins``; ``REPARK_ORACLE_IVY`` optionally points ``spark.jars.ivy`` at a warm
cache::

    _record_ice_array_insert_1.py [warehouse]

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

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE_DIR = (
    Path(__file__).resolve().parents[2] / "repark-parity/fixtures/torture/data/ice_array_insert_1"
)
ORACLE_FILE = FIXTURE_DIR / "spark_array_insert_oracle.json"
SPARK_CATALOG = "ora"
SPARK_NAMESPACE = "ns"
FORMAT_VERSIONS = ("2", "3")
DOORS = ("sql_values", "sql_select", "writeto_append", "insert_into", "save_as_table_append")
SHAPES: dict[str, tuple[str, str, str]] = {
    "list_int": (
        "xs ARRAY<INT>",
        "SELECT 1 AS id, array(1, 2, 3) AS xs "
        "UNION ALL SELECT 2, CAST(NULL AS ARRAY<INT>) "
        "UNION ALL SELECT 3, CAST(array() AS ARRAY<INT>) "
        "UNION ALL SELECT 4, array(5, NULL, 7)",
        "(1, array(1, 2, 3)), (2, NULL), (3, CAST(array() AS ARRAY<INT>)), (4, array(5, NULL, 7))",
    ),
    "list_struct": (
        "xs ARRAY<STRUCT<a: INT, b: STRING>>",
        "SELECT 1 AS id, array(named_struct('a', 1, 'b', 'x'), "
        "named_struct('a', 2, 'b', NULL)) AS xs "
        "UNION ALL SELECT 2, CAST(NULL AS ARRAY<STRUCT<a: INT, b: STRING>>) "
        "UNION ALL SELECT 3, CAST(array() AS ARRAY<STRUCT<a: INT, b: STRING>>)",
        "(1, array(named_struct('a', 1, 'b', 'x'), "
        "named_struct('a', 2, 'b', CAST(NULL AS STRING)))), "
        "(2, NULL), (3, CAST(array() AS ARRAY<STRUCT<a: INT, b: STRING>>))",
    ),
    "map_list": (
        "m MAP<STRING, ARRAY<INT>>",
        "SELECT 1 AS id, map('k1', array(1, 2), 'k2', CAST(array() AS ARRAY<INT>)) AS m "
        "UNION ALL SELECT 2, CAST(NULL AS MAP<STRING, ARRAY<INT>>) "
        "UNION ALL SELECT 3, map('k3', CAST(NULL AS ARRAY<INT>))",
        "(1, map('k1', array(1, 2), 'k2', CAST(array() AS ARRAY<INT>))), (2, NULL), "
        "(3, map('k3', CAST(NULL AS ARRAY<INT>)))",
    ),
}


def to_python(value: Any) -> Any:
    """Return nested Spark rows, maps and arrays as plain Python values.

    Args:
        value: A Spark row, dict, list or scalar from a collected row.

    Returns:
        The value with every nested row converted to a dict with sorted map keys.
    """
    if hasattr(value, "asDict"):
        return {key: to_python(item) for key, item in value.asDict().items()}
    if isinstance(value, dict):
        return {key: to_python(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [to_python(item) for item in value]
    return value


def walk_field(field: Any, prefix: str) -> list[list[str]]:
    """Return the `[path, field id]` rows of one parquet schema field and its children.

    Args:
        field: One field of a parquet schema read with pyarrow.
        prefix: The dotted path of the field's parent, empty at the top level.

    Returns:
        The field's own row plus one row per descendant. A map's repeated group
        reports as `<name>.<name>` with an empty id, exactly as the footer stamps it.
    """
    rows = [
        [
            prefix + field.name,
            (field.metadata or {}).get(b"PARQUET:field_id", b"").decode(),
        ]
    ]
    child_type = field.type
    for index in range(getattr(child_type, "num_fields", 0)):
        rows += walk_field(child_type.field(index), prefix + field.name + ".")
    return rows


def field_ids(path: str) -> list[list[str]]:
    """Return the `[path, field id]` rows of a parquet data file's footer schema.

    Args:
        path: The data file path, with or without Spark's `file:` prefix.

    Returns:
        One `[dotted path, id]` pair per footer field in schema order.
    """
    schema = pq.read_schema(path.replace("file:", ""))
    rows: list[list[str]] = []
    for top in schema:
        rows += walk_field(top, "")
    return rows


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
        .appName("ice-array-insert-1-oracle")
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


def record_door(
    spark: Any, table: str, door: str, select_sql: str, values_sql: str
) -> dict[str, Any]:
    """Write one door's source into `table` and record Spark's answer.

    Args:
        spark: The live PySpark session.
        table: The catalog-qualified table name, already created and empty.
        door: One of `DOORS`.
        select_sql: The SELECT source the non-VALUES doors write.
        values_sql: The VALUES row list the `sql_values` door writes.

    Returns:
        The cell record: `statement`, `rows`, `schema`, `data_files` and
        `parquet_field_ids`, or `error` when the write refused.
    """
    cell: dict[str, Any] = {}
    try:
        if door == "sql_values":
            statement = f"INSERT INTO <t> VALUES {values_sql}"
            spark.sql(statement.replace("<t>", table))
        elif door == "sql_select":
            statement = f"INSERT INTO <t> {select_sql}"
            spark.sql(statement.replace("<t>", table))
        else:
            statement = select_sql
            frame = spark.sql(select_sql)
            if door == "writeto_append":
                frame.writeTo(table).append()
            elif door == "insert_into":
                frame.write.insertInto(table)
            else:
                frame.write.format("iceberg").mode("append").saveAsTable(table)
        cell["statement"] = statement
        rows = spark.sql(f"SELECT * FROM {table} ORDER BY id").collect()
        cell["rows"] = [to_python(row.asDict()) for row in rows]
        cell["schema"] = spark.table(table).schema.simpleString()
        files = [row[0] for row in spark.sql(f"SELECT file_path FROM {table}.files").collect()]
        cell["data_files"] = len(files)
        cell["parquet_field_ids"] = field_ids(files[0]) if files else []
    except Exception as exc:
        cell["error"] = f"{type(exc).__name__}: {str(exc).splitlines()[0]}"
    return cell


def record_all(spark: Any) -> dict[str, Any]:
    """Record every shape x door x version cell on the live session.

    Args:
        spark: The live PySpark session.

    Returns:
        The oracle document with `oracle` and `cells`.
    """
    short_gav = ICEBERG_SPARK_RUNTIME_GAV.split(":", 1)[1]
    truth: dict[str, Any] = {
        "oracle": f"PySpark {spark.version} + {short_gav}",
        "cells": {},
    }
    for version in FORMAT_VERSIONS:
        for shape, (column, select_sql, values_sql) in SHAPES.items():
            for door in DOORS:
                name = f"{SPARK_CATALOG}.{SPARK_NAMESPACE}.{shape}_{door}_v{version}"
                spark.sql(
                    f"CREATE TABLE {name} (id INT, {column}) USING iceberg "
                    f"TBLPROPERTIES ('format-version'='{version}')"
                )
                cell = record_door(spark, name, door, select_sql, values_sql)
                cell["table_ddl"] = (
                    f"CREATE TABLE <t> (id INT, {column}) USING iceberg "
                    f"TBLPROPERTIES ('format-version'='{version}')"
                )
                cell["door"] = door
                ordered = {
                    key: cell[key]
                    for key in (
                        "table_ddl",
                        "door",
                        "statement",
                        "rows",
                        "schema",
                        "data_files",
                        "parquet_field_ids",
                    )
                    if key in cell
                }
                if "error" in cell:
                    ordered["error"] = cell["error"]
                truth["cells"][f"{shape}_{door}_v{version}"] = ordered
                print(shape, door, version, "error" in cell, flush=True)
    return truth


def main(warehouse: Path | None = None) -> None:
    """Record every cell and write the fixture oracle JSON.

    Args:
        warehouse: The scratch warehouse root, created fresh. Defaults to
            env-driven scratch when omitted.
    """
    root = warehouse or Path(tempfile.mkdtemp(prefix="repark-ice-array-insert-1-"))
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
