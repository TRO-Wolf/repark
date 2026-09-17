"""Record the ICE-V3-WRITE-DEFAULT-1 Spark oracle fixture and truth JSON.

Builds seven small format-v3 tables with a Hadoop catalog at the canonical
warehouse ``/tmp/repark-ice-v3-write-default-1`` (the metadata embeds that
path, so the suite materializes the fixture at the same path), copies the
clean tree into this directory, then runs every oracle write shape against the
canonical working copy and writes ``truth.json`` beside it.

Run with PySpark 4.1.2 + Iceberg 1.11.0 on Java 17, Spark local mode, from the
repository root (the script locates the fixture through its own path, so run
this file, never a copy)::

    source /tmp/ib-scratch/live-env.sh && unset PYSPARK_SUBMIT_ARGS && \\
    /tmp/oc-worker/jb-jvm.sh /tmp/ib-build/.venv/bin/python \\
        python/repark-parity/fixtures/torture/data/ice_v3_write_default_1/record.py

Spark-only: imports neither repark nor the suite. Re-recording is
deterministic in rows, schema and errors; file names carry fresh UUIDs.

pins: ice-v3-write-default-1/C-002
"""

from __future__ import annotations

import json
import shutil
import time
from functools import partial
from pathlib import Path
from typing import Any, Callable

CANONICAL = Path("/tmp/repark-ice-v3-write-default-1")
FIXTURE_DIR = Path(__file__).resolve().parent
CATALOG = "sc"
GAV = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
GAV_WRITER = "PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, hadoop catalog"
TABLES = ("defaults", "strdef", "decdef", "temporal", "differ", "nodefault", "required")


class Log:
    """Timestamped stdout log of the recording session."""

    def __init__(self) -> None:
        """Open the session log."""
        self.lines: list[str] = []

    def __call__(self, message: str) -> None:
        """Print and retain one log line."""
        line = f"[{time.strftime('%H:%M:%S')}] {message}"
        print(line, flush=True)
        self.lines.append(line)


def _spark_session(warehouse: Path) -> Any:
    """Build the recording Spark session with the Iceberg catalog armed."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[4]")
        .appName("ice-v3-write-default-1-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", GAV)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def _newest_metadata(table_root: Path) -> Path:
    """The newest metadata JSON of a Hadoop table root."""
    candidates = sorted(
        table_root.glob("metadata/v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    if not candidates:
        raise ValueError(f"no metadata under {table_root}/metadata")
    return candidates[-1]


def _schema_fields(table_root: Path) -> list[Any]:
    """The current-schema field list of a table root."""
    doc = json.loads(_newest_metadata(table_root).read_text(encoding="utf-8"))
    current = [s for s in doc["schemas"] if s["schema-id"] == doc["current-schema-id"]][0]
    return list(current["fields"])


def _rows(spark: Any, query: str) -> list[list[Any]]:
    """Collect a query as sorted plain rows."""
    arrow_table = spark.sql(query).toArrow()
    cols = arrow_table.column_names
    return sorted(
        ([row[col] for col in cols] for row in arrow_table.to_pylist()),
        key=repr,
    )


def _seed_record(spark: Any, log: Log, table: str) -> dict[str, Any]:
    """Record one table's seed rows, or the read error when Spark cannot read them."""
    try:
        return {"outcome": "ok", "rows": _rows(spark, f"SELECT * FROM sc.ns.{table} ORDER BY id")}
    except Exception as exc:
        log(f"ERR-SEED {table}: {type(exc).__name__}")
        return {
            "outcome": "error",
            "class": type(exc).__name__,
            "message": str(exc).strip().splitlines()[:3],
        }


def _add_default(spark: Any, table: str, column: str, itype: Any, doc: str, lit: Any) -> None:
    """Add an optional column carrying initial- and write-defaults through the Java API."""
    jvm = spark._jvm
    catalog_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"{CATALOG}.ns.{table}"
    )
    catalog_table.updateSchema().addColumn(column, itype, doc, lit).commit()


def _build(spark: Any) -> None:
    """Create and seed the seven fixture tables at the canonical warehouse."""
    jvm = spark._jvm
    integer = jvm.org.apache.iceberg.types.Types.IntegerType.get()
    string = jvm.org.apache.iceberg.types.Types.StringType.get()
    literal = jvm.org.apache.iceberg.expressions.Literal.of
    spark.sql(
        "CREATE TABLE sc.ns.defaults (id INT, name STRING)"
        " USING iceberg TBLPROPERTIES ('format-version'='3')"
    )
    spark.sql("INSERT INTO sc.ns.defaults VALUES (1, 'a'), (2, 'b')")
    _add_default(spark, "defaults", "c", integer, "doc", literal(5))
    spark.sql("INSERT INTO sc.ns.defaults (id, name) VALUES (3, 'c')")
    spark.sql("CREATE TABLE sc.ns.strdef (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')")
    spark.sql("INSERT INTO sc.ns.strdef VALUES (1)")
    _add_default(spark, "strdef", "s", string, "doc", literal("hi"))
    spark.sql("INSERT INTO sc.ns.strdef (id) VALUES (2)")
    spark.sql("CREATE TABLE sc.ns.decdef (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')")
    _add_default(
        spark,
        "decdef",
        "d",
        jvm.org.apache.iceberg.types.Types.DecimalType.of(10, 2),
        "doc",
        literal(jvm.java.math.BigDecimal("3.14")),
    )
    spark.sql("INSERT INTO sc.ns.decdef VALUES (1, 3.14)")
    spark.sql("CREATE TABLE sc.ns.temporal (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')")
    spark.sql("INSERT INTO sc.ns.temporal VALUES (1)")
    _add_default(
        spark, "temporal", "dt", jvm.org.apache.iceberg.types.Types.DateType.get(), "doc", literal(20000)
    )
    _add_default(
        spark,
        "temporal",
        "ts",
        jvm.org.apache.iceberg.types.Types.TimestampType.withZone(),
        "doc",
        literal(20000 * 86400 * 1_000_000),
    )
    spark.sql("CREATE TABLE sc.ns.differ (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')")
    spark.sql("INSERT INTO sc.ns.differ VALUES (1)")
    _add_default(spark, "differ", "e", integer, "doc", literal(5))
    differ = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"{CATALOG}.ns.differ"
    )
    differ.updateSchema().updateColumnDefault("e", literal(7)).commit()
    spark.sql("INSERT INTO sc.ns.temporal (id) VALUES (2)")
    spark.sql("INSERT INTO sc.ns.differ (id) VALUES (2)")
    spark.sql(
        "CREATE TABLE sc.ns.nodefault (id INT, name STRING, note STRING)"
        " USING iceberg TBLPROPERTIES ('format-version'='3')"
    )
    spark.sql("INSERT INTO sc.ns.nodefault VALUES (1, 'a', 'x')")
    spark.sql(
        "CREATE TABLE sc.ns.required (id INT, req INT NOT NULL)"
        " USING iceberg TBLPROPERTIES ('format-version'='3')"
    )
    spark.sql("INSERT INTO sc.ns.required VALUES (1, 100)")
    for table in TABLES:
        spark.sql(f"REFRESH TABLE sc.ns.{table}")


def _record_cell(
    spark: Any, log: Log, cells: dict[str, Any], table: str, shape: str, run: Callable[[], Any]
) -> None:
    """Run one oracle shape, recording full-table rows or the error."""
    try:
        run()
    except Exception as exc:
        cells[shape] = {
            "table": table,
            "outcome": "error",
            "class": type(exc).__name__,
            "message": str(exc).strip().splitlines()[:3],
        }
        log(f"ERR {shape}: {type(exc).__name__}")
        return
    try:
        rows = _rows(spark, f"SELECT * FROM sc.ns.{table} ORDER BY id")
    except Exception as exc:
        cells[shape] = {
            "table": table,
            "outcome": "ok_write_read_failed",
            "class": type(exc).__name__,
            "message": str(exc).strip().splitlines()[:3],
        }
        log(f"ERR-READ {shape}: {type(exc).__name__}")
        return
    cells[shape] = {"table": table, "outcome": "ok", "rows": rows}
    log(f"OK {shape}: {len(rows)} rows")


def _collect(spark: Any, statement: str) -> Any:
    """Collect one SQL statement on the recording session."""
    return spark.sql(statement).collect()


def _append_frame(spark: Any, table: str, rows: list[Any], schema: str) -> Any:
    """Append a literal frame through ``writeTo``."""
    return spark.createDataFrame(rows, schema).writeTo(table).append()


def _saveas_append_frame(spark: Any, table: str, rows: list[Any], schema: str) -> Any:
    """Append a literal frame through ``saveAsTable`` in append mode."""
    return spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(table)


def _insert_into_frame(spark: Any, table: str, rows: list[Any], schema: str) -> Any:
    """Append a literal frame through ``insertInto``."""
    return spark.createDataFrame(rows, schema).write.insertInto(table)


def _record_shapes(spark: Any, log: Log) -> dict[str, Any]:
    """Run every oracle shape against the canonical working copy."""
    cells: dict[str, Any] = {}
    go = partial(_record_cell, spark, log, cells)
    go("defaults", "insert_column_list",
       partial(_collect, spark, "INSERT INTO sc.ns.defaults (id, name) VALUES (11, 'k')"))
    go("defaults", "merge_not_matched",
       partial(_collect, spark,
               "MERGE INTO sc.ns.defaults t USING (SELECT 12 AS id, 'l' AS name) s"
               " ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"))
    go("defaults", "writeto_append",
       partial(_append_frame, spark, "sc.ns.defaults", [(13, "m")], "id int, name string"))
    go("defaults", "saveas_append",
       partial(_saveas_append_frame, spark, "sc.ns.defaults", [(14, "n")], "id int, name string"))
    go("defaults", "insert_values_default_kw",
       partial(_collect, spark, "INSERT INTO sc.ns.defaults VALUES (15, 'o', DEFAULT)"))
    go("defaults", "explicit_null_values",
       partial(_collect, spark, "INSERT INTO sc.ns.defaults (id, name, c) VALUES (16, 'p', NULL)"))
    go("defaults", "explicit_null_select",
       partial(_collect, spark, "INSERT INTO sc.ns.defaults SELECT 17, 'q', NULL"))
    go("defaults", "select_position_default",
       partial(_collect, spark, "INSERT INTO sc.ns.defaults (id, name, c) SELECT 18, 'r', DEFAULT"))
    go("defaults", "insert_positional_short",
       partial(_collect, spark, "INSERT INTO sc.ns.defaults VALUES (8, 'h')"))
    go("defaults", "insert_select_short",
       partial(_collect, spark, "INSERT INTO sc.ns.defaults SELECT 9, 'i'"))
    go("defaults", "insertInto_missing",
       partial(_insert_into_frame, spark, "sc.ns.defaults", [(19, "s")], "id int, name string"))
    go("defaults", "writeto_extra_col",
       partial(_append_frame, spark, "sc.ns.defaults", [(21, "u", "zzz")], "id int, name string, zzz string"))
    go("nodefault", "nodefault_insert_column_list",
       partial(_collect, spark, "INSERT INTO sc.ns.nodefault (id, name) VALUES (2, 'b')"))
    go("nodefault", "nodefault_writeto_append",
       partial(_append_frame, spark, "sc.ns.nodefault", [(3, "c")], "id int, name string"))
    go("nodefault", "nodefault_values_default_kw",
       partial(_collect, spark, "INSERT INTO sc.ns.nodefault VALUES (4, 'd', DEFAULT)"))
    go("strdef", "string_default_insert",
       partial(_collect, spark, "INSERT INTO sc.ns.strdef (id) VALUES (12)"))
    go("decdef", "decimal_default_insert",
       partial(_collect, spark, "INSERT INTO sc.ns.decdef (id) VALUES (2)"))
    go("temporal", "temporal_default_insert",
       partial(_collect, spark, "INSERT INTO sc.ns.temporal (id) VALUES (12)"))
    go("differ", "differ_insert",
       partial(_collect, spark, "INSERT INTO sc.ns.differ (id) VALUES (12)"))
    go("required", "required_missing_insert",
       partial(_collect, spark, "INSERT INTO sc.ns.required (id) VALUES (2)"))
    go("required", "required_missing_writeto",
       partial(_append_frame, spark, "sc.ns.required", [(3,)], "id int"))
    go("defaults", "overwrite_column_list",
       partial(_collect, spark, "INSERT OVERWRITE sc.ns.defaults (id, name) SELECT 30, 'ov'"))
    return cells


def main() -> None:
    """Build at canonical, copy the clean tree, record shapes, write the fixture."""
    log = Log()
    if CANONICAL.exists():
        shutil.rmtree(CANONICAL)
    CANONICAL.mkdir(parents=True)
    spark = _spark_session(CANONICAL)
    log(f"BANNER spark={spark.version} tz={spark.conf.get('spark.sql.session.timeZone')}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.ns")
    _build(spark)
    for entry in FIXTURE_DIR.iterdir():
        if entry.name in ("map.md", "record.py"):
            continue
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink()
    shutil.copytree(CANONICAL / "ns", FIXTURE_DIR / "ns", copy_function=shutil.copy)
    truth: dict[str, Any] = {
        "unit": "ice-v3-write-default-1",
        "writer": GAV_WRITER,
        "banner": {"spark": spark.version, "tz": spark.conf.get("spark.sql.session.timeZone")},
        "tables": {
            table: {
                "schema": _schema_fields(CANONICAL / "ns" / table),
                "seed": _seed_record(spark, log, table),
            }
            for table in TABLES
        },
        "cells": _record_shapes(spark, log),
    }
    (FIXTURE_DIR / "truth.json").write_text(json.dumps(truth, indent=2, default=str), encoding="utf-8")
    spark.stop()
    cells = truth["cells"]
    assert isinstance(cells, dict)
    log(f"fixture recorded: {len(cells)} cells")


if __name__ == "__main__":
    main()
