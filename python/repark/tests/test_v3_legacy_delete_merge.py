"""V3-12 — a legacy parquet position delete merges into the DV on the next MoR write."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pyarrow as pa
import pytest
from test_v3_live_oracle import (
    _ALLOW_CREATE_V3_KEY,
    _LIVE,
    _LIVE_SKIP,
    _v37_iceberg_runtime_jar,
)

_MOR_V2 = (
    "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read'"
)
_SEED = "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
_DELETE_FILES = "SELECT file_format, record_count, referenced_data_file FROM {t}.delete_files"
_LINEAGE = "SELECT id, _row_id, _last_updated_sequence_number FROM {t} ORDER BY id"
_MERGE_DELETE = (
    "MERGE INTO {t} AS target USING (SELECT {id} AS id) AS source ON target.id = source.id "
    "WHEN MATCHED THEN DELETE"
)
_EXPECTED_DELETE_FILES = [("PUFFIN", 2, True)]
_EXPECTED_LINEAGE = [(1, 0, 1), (4, 3, 1)]


def _delete_files(arrow: pa.Table) -> list[tuple[str, int, bool]]:
    """``(format, record_count, references a data file)`` per live delete file, sorted."""
    rows = [
        (
            str(fmt).upper(),
            int(count),
            referenced is not None,
        )
        for fmt, count, referenced in zip(
            arrow.column("file_format").to_pylist(),
            arrow.column("record_count").to_pylist(),
            arrow.column("referenced_data_file").to_pylist(),
            strict=True,
        )
    ]
    rows.sort()
    return rows


def _lineage(arrow: pa.Table) -> list[tuple[int, int, int]]:
    return [
        (int(row["id"]), int(row["_row_id"]), int(row["_last_updated_sequence_number"]))
        for row in arrow.to_pylist()
    ]


def _repark_legacy_merge_shape(warehouse: Path) -> dict:
    """Seed v2 + a parquet position delete, upgrade to v3, then delete again on the facade."""
    from repark import ReparkSession

    repark = (
        ReparkSession.builder.appName("v3-12-legacy-delete-merge")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    target = "ice.sales.legacy"
    try:
        repark.register_memory_catalog("ice", warehouse)
        repark.sql("CREATE NAMESPACE ice.sales")
        repark.sql(
            f"CREATE TABLE {target} (id INT, name STRING) USING iceberg TBLPROPERTIES ({_MOR_V2})"
        )
        repark.sql(f"INSERT INTO {target} VALUES {_SEED}").collect()
        repark.sql(_MERGE_DELETE.format(t=target, id=2)).collect()
        before = _delete_files(repark.sql(_DELETE_FILES.format(t=target)).to_arrow())
        repark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('format-version' = '3')").collect()
        repark.sql(_MERGE_DELETE.format(t=target, id=3)).collect()
        return {
            "before": before,
            "after": _delete_files(repark.sql(_DELETE_FILES.format(t=target)).to_arrow()),
            "lineage": _lineage(repark.sql(_LINEAGE.format(t=target)).to_arrow()),
        }
    finally:
        repark.stop()


def _spark_legacy_merge_shape() -> dict:
    """Live Spark running the same five statements at a matched one-data-file layout."""
    import tempfile

    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    catalog = "local"
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v312-legacy-"))
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("v3-12-legacy-delete-merge-live")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    )
    jar = _v37_iceberg_runtime_jar()
    if jar is not None:
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    target = f"{catalog}.sales.legacy"
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")
        session.sql(
            f"CREATE TABLE {target} (id INT, name STRING) USING iceberg TBLPROPERTIES ({_MOR_V2})"
        )
        session.sql(f"INSERT INTO {target} VALUES {_SEED}")
        session.sql(_MERGE_DELETE.format(t=target, id=2))
        before = _delete_files(session.sql(_DELETE_FILES.format(t=target)).toArrow())
        session.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('format-version' = '3')")
        session.sql(_MERGE_DELETE.format(t=target, id=3))
        return {
            "before": before,
            "after": _delete_files(session.sql(_DELETE_FILES.format(t=target)).toArrow()),
            "lineage": _lineage(session.sql(_LINEAGE.format(t=target)).toArrow()),
        }
    finally:
        session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


def test_v3_legacy_parquet_position_delete_merges_into_the_dv(tmp_path: Path) -> None:
    """The facade door merges the legacy positions and drops the superseded parquet file."""
    shape = _repark_legacy_merge_shape(tmp_path)
    assert shape["before"] == [("PARQUET", 1, False)]
    assert shape["after"] == _EXPECTED_DELETE_FILES
    assert shape["lineage"] == _EXPECTED_LINEAGE
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    assert shape == _spark_legacy_merge_shape()


def test_v2_merge_on_read_delete_still_writes_parquet_position_deletes(tmp_path: Path) -> None:
    """Incidental control: a table that stays v2 keeps the parquet position-delete path."""
    from repark import ReparkSession

    repark = ReparkSession.builder.appName("v3-12-v2-control").getOrCreate()
    target = "ice.sales.stay_v2"
    try:
        repark.register_memory_catalog("ice", tmp_path)
        repark.sql("CREATE NAMESPACE ice.sales")
        repark.sql(
            f"CREATE TABLE {target} (id INT, name STRING) USING iceberg TBLPROPERTIES ({_MOR_V2})"
        )
        repark.sql(f"INSERT INTO {target} VALUES {_SEED}").collect()
        repark.sql(_MERGE_DELETE.format(t=target, id=2)).collect()
        repark.sql(_MERGE_DELETE.format(t=target, id=3)).collect()
        after = _delete_files(repark.sql(_DELETE_FILES.format(t=target)).to_arrow())
        ids = repark.sql(f"SELECT id FROM {target} ORDER BY id").to_arrow()
        rows = ids.column("id").to_pylist()
    finally:
        repark.stop()
    assert after == [("PARQUET", 1, False), ("PARQUET", 1, False)]
    assert rows == [1, 4]
