"""V3-11 live oracle: one commit's data files take Spark's ascending-partition order.

pins: v3-11-row-id-determinism/C-004
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live v3 oracle skipped (routine CI is JVM-free)"
_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_MOR_V3 = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)
_CTAS_VALUES = "(1, 2), (2, 0), (3, 1), (4, 0), (5, 2)"
_CTAS_LINEAGE = [(1, 3, 1), (2, 0, 1), (3, 2, 1), (4, 1, 1), (5, 4, 1)]
_MERGE_LINEAGE = [(1, 0, 1), (2, 1, 2), (7, 3, 2), (8, 2, 2)]
_LINEAGE_SELECT = "SELECT id, _row_id, _last_updated_sequence_number FROM {table} ORDER BY id"


def _triples(table: pa.Table) -> list[tuple[int, int, int]]:
    """``(id, _row_id, _last_updated_sequence_number)`` rows from a lineage read."""
    ids = table.column("id").to_pylist()
    row_ids = table.column("_row_id").to_pylist()
    sequences = table.column("_last_updated_sequence_number").to_pylist()
    return [
        (int(one), int(two), int(three))
        for one, two, three in zip(ids, row_ids, sequences, strict=True)
    ]


def _merge_sql(target: str) -> str:
    """MoR MERGE that updates one partition and inserts into two more."""
    return (
        f"MERGE INTO {target} AS t USING "
        "(SELECT 2 AS id, 'm' AS name, 2 AS part "
        "UNION ALL SELECT 7, 'g', 1 UNION ALL SELECT 8, 'h', 0) AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.name = s.name "
        "WHEN NOT MATCHED THEN INSERT (id, name, part) VALUES (s.id, s.name, s.part)"
    )


def _iceberg_runtime_jar() -> str | None:
    """Local Iceberg Spark runtime JAR when Ivy cannot write the default cache."""
    candidates = (
        os.environ.get("V37_ICEBERG_RUNTIME_JAR"),
        "/tmp/rp6-oracle/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        "/tmp/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
    )
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    return None


def test_v3_same_commit_file_order_live_matches_spark(tmp_path: Path) -> None:
    """A partitioned v3 CTAS and a three-partition MoR MERGE take Spark's exact `_row_id` map."""
    from repark import ReparkSession

    repark = (
        ReparkSession.builder.appName("v3-11-file-order-live")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        repark.register_memory_catalog("ice", tmp_path)
        repark.sql("CREATE NAMESPACE ice.sales")
        repark.sql(
            "CREATE TABLE ice.sales.ctas_order USING iceberg PARTITIONED BY (part) "
            "TBLPROPERTIES ('format-version' = '3') AS "
            f"SELECT * FROM (VALUES {_CTAS_VALUES}) AS t(id, part)"
        ).collect()
        ctas = repark.sql(_LINEAGE_SELECT.format(table="ice.sales.ctas_order")).to_arrow()
        assert _triples(ctas) == _CTAS_LINEAGE
        repark.sql(
            "CREATE TABLE ice.sales.merge_order (id INT, name STRING, part INT) USING iceberg "
            f"PARTITIONED BY (part) TBLPROPERTIES ({_MOR_V3})"
        )
        repark.sql("INSERT INTO ice.sales.merge_order VALUES (1, 'a', 2), (2, 'b', 2)")
        repark.sql(_merge_sql("ice.sales.merge_order")).collect()
        merged = repark.sql(_LINEAGE_SELECT.format(table="ice.sales.merge_order")).to_arrow()
        assert _triples(merged) == _MERGE_LINEAGE
        if not _LIVE:
            pytest.skip(_LIVE_SKIP)
        _assert_live_against_spark()
    finally:
        repark.stop()


def _assert_live_against_spark() -> None:
    """Live Spark reads the same two id-to-`_row_id` maps at the same layout."""
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    catalog = "local"
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3-11-live-"))
    ivy_home = Path(tempfile.mkdtemp(prefix="repark-v3-11-ivy-"))
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("v3-11-file-order-live")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.ivy", str(ivy_home))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    )
    jar = _iceberg_runtime_jar()
    if jar is not None:
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")
        ctas_table = f"{catalog}.sales.ctas_order"
        session.sql(
            f"CREATE TABLE {ctas_table} USING iceberg PARTITIONED BY (part) "
            "TBLPROPERTIES ('format-version' = '3') AS "
            f"SELECT * FROM (VALUES {_CTAS_VALUES}) AS t(id, part)"
        )
        ctas = session.sql(_LINEAGE_SELECT.format(table=ctas_table)).toArrow()
        assert _triples(ctas) == _CTAS_LINEAGE
        merge_table = f"{catalog}.sales.merge_order"
        session.sql(
            f"CREATE TABLE {merge_table} (id INT, name STRING, part INT) USING iceberg "
            f"PARTITIONED BY (part) TBLPROPERTIES ({_MOR_V3})"
        )
        session.sql(f"INSERT INTO {merge_table} VALUES (1, 'a', 2), (2, 'b', 2)")
        session.sql(_merge_sql(merge_table))
        merged = session.sql(_LINEAGE_SELECT.format(table=merge_table)).toArrow()
        assert _triples(merged) == _MERGE_LINEAGE
    finally:
        session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)
        shutil.rmtree(ivy_home, ignore_errors=True)
    assert ICEBERG_SPARK_RUNTIME_GAV == "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
