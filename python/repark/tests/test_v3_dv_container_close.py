"""RP-7 — the shared-Puffin deletion-vector container close, repark against live Spark."""

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

_DV_COLUMNS = (
    "SELECT referenced_data_file, file_path, content_offset, record_count FROM {table}.delete_files"
)
_SHARED_PUFFIN_ROWS = [(3, "c", 0), (4, "d", 1), (6, "f", 1)]
_SHARED_PUFFIN_SEED = [(1, "a", 0), (2, "b", 0), (3, "c", 0), (4, "d", 1), (5, "e", 1), (6, "f", 1)]
_SHARED_PUFFIN_MOR = "'format-version' = '3', 'write.delete.mode' = 'merge-on-read'"


def _dv_entries(arrow: pa.Table) -> list[tuple[str, str, int, int]]:
    """Return ``(referenced, container, offset, records)`` per live DV, ordered by referenced."""
    referenced = arrow.column("referenced_data_file").to_pylist()
    container = arrow.column("file_path").to_pylist()
    offset = arrow.column("content_offset").to_pylist()
    records = arrow.column("record_count").to_pylist()
    rows = [
        (str(ref).rsplit("/", 1)[-1], str(path), int(off), int(count))
        for ref, path, off, count in zip(referenced, container, offset, records, strict=True)
    ]
    rows.sort()
    return rows


def _dv_close_shape(before: list, after: list, rows: list) -> dict:
    """Engine-independent shape of a shared-Puffin close: what moved, what stayed."""
    was = {entry[0]: entry[1:] for entry in before}
    now = {entry[0]: entry[1:] for entry in after}
    touched = [ref for ref, entry in now.items() if entry[2] == 2]
    sibling = [ref for ref, entry in now.items() if entry[2] == 1]
    assert len(touched) == 1 and len(sibling) == 1, (before, after)
    touched, sibling = touched[0], sibling[0]
    return {
        "before_containers": len({entry[1] for entry in before}),
        "after_containers": len({entry[1] for entry in after}),
        "touched_moved": now[touched][0] != was[touched][0],
        "touched_offset": now[touched][1],
        "touched_records": now[touched][2],
        "sibling_unchanged": now[sibling] == was[sibling],
        "sibling_records": now[sibling][2],
        "rows": rows,
    }


def test_v3_shared_puffin_container_close_live(tmp_path: Path) -> None:
    """A second DELETE rewrites only the touched blob and leaves the sibling entry put."""
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("rp-7-shared-puffin-live")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        session.sql(
            "CREATE TABLE ice.sales.partdv (id INT, name STRING, part INT) USING iceberg "
            f"PARTITIONED BY (part) TBLPROPERTIES ({_SHARED_PUFFIN_MOR})"
        )
        seed = ", ".join(f"({row[0]}, '{row[1]}', {row[2]})" for row in _SHARED_PUFFIN_SEED)
        session.sql(f"INSERT INTO ice.sales.partdv VALUES {seed}").collect()
        session.sql("DELETE FROM ice.sales.partdv WHERE id IN (2, 5)").collect()
        before = _dv_entries(session.sql(_DV_COLUMNS.format(table="ice.sales.partdv")).to_arrow())
        session.sql("DELETE FROM ice.sales.partdv WHERE id = 1").collect()
        after = _dv_entries(session.sql(_DV_COLUMNS.format(table="ice.sales.partdv")).to_arrow())
        rows = [
            (row["id"], row["name"], row["part"])
            for row in session.sql("SELECT id, name, part FROM ice.sales.partdv ORDER BY id")
            .to_arrow()
            .to_pylist()
        ]
        shape = _dv_close_shape(before, after, rows)
    finally:
        session.stop()
    assert shape["rows"] == _SHARED_PUFFIN_ROWS
    assert shape["after_containers"] == 2
    assert shape["touched_moved"]
    assert shape["sibling_unchanged"]
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    assert shape == _live_shared_puffin_close_shape()


def _live_shared_puffin_close_shape() -> dict:
    """Live Spark running the same two DELETE statements over the partitioned v3 MoR seed."""
    import tempfile

    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    catalog = "local"
    warehouse = Path(tempfile.mkdtemp(prefix="repark-rp7-shared-puffin-"))
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("rp-7-shared-puffin-live")
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
    target = f"{catalog}.sales.partdv"
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")
        session.sql(
            f"CREATE TABLE {target} (id INT, name STRING, part INT) USING iceberg "
            f"PARTITIONED BY (part) TBLPROPERTIES ({_SHARED_PUFFIN_MOR})"
        )
        values = ", ".join(f"({row[0]}, '{row[1]}', {row[2]})" for row in _SHARED_PUFFIN_SEED)
        session.sql(f"INSERT INTO {target} VALUES {values}")
        session.sql(f"DELETE FROM {target} WHERE id IN (2, 5)")
        before = _dv_entries(session.sql(_DV_COLUMNS.format(table=target)).toArrow())
        session.sql(f"DELETE FROM {target} WHERE id = 1")
        after = _dv_entries(session.sql(_DV_COLUMNS.format(table=target)).toArrow())
        rows = [
            (row["id"], row["name"], row["part"])
            for row in session.sql(f"SELECT id, name, part FROM {target} ORDER BY id")
            .toArrow()
            .to_pylist()
        ]
        return _dv_close_shape(before, after, rows)
    finally:
        session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)
