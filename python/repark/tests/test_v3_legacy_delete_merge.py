"""V3-12 — a legacy parquet position delete merges into the DV on the next MoR write."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest
from test_v3_live_oracle import (
    _ALLOW_CREATE_V3_KEY,
    _LIVE,
    _LIVE_SKIP,
    _v37_iceberg_runtime_jar,
)

_CATALOG = "v312legacy"
_MOR_V2 = (
    "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read'"
)
_SEED_ROWS = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
_SEED_SQL = "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
_SEED_SCHEMA = "id INT, name STRING"
_DELETE_FILES = "SELECT file_format, record_count, referenced_data_file FROM {t}.delete_files"
_LINEAGE = "SELECT id, _row_id, _last_updated_sequence_number FROM {t} ORDER BY id"
_MERGE_DELETE = (
    "MERGE INTO {t} AS target USING (SELECT {id} AS id) AS source ON target.id = source.id "
    "WHEN MATCHED THEN DELETE"
)
_UPGRADE = "ALTER TABLE {t} SET TBLPROPERTIES ('format-version' = '3')"
_MOR_V2_PARTITION_GRANULARITY = _MOR_V2 + ", 'write.delete.granularity' = 'partition'"
_EXPECTED_BEFORE = [("PARQUET", 1, False)]
_EXPECTED_AFTER = [("PUFFIN", 2, True)]
_EXPECTED_LINEAGE = [(1, 0, 1), (4, 3, 1)]


def _delete_files(arrow: pa.Table) -> list[tuple[str, int, bool]]:
    """``(format, record_count, references a data file)`` per live delete file, sorted."""
    rows = [
        (str(fmt).upper(), int(count), referenced is not None)
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
            f"CREATE TABLE {target} ({_SEED_SCHEMA}) USING iceberg TBLPROPERTIES ({_MOR_V2})"
        )
        repark.sql(f"INSERT INTO {target} VALUES {_SEED_SQL}").collect()
        repark.sql(_MERGE_DELETE.format(t=target, id=2)).collect()
        before = _delete_files(repark.sql(_DELETE_FILES.format(t=target)).to_arrow())
        repark.sql(_UPGRADE.format(t=target)).collect()
        repark.sql(_MERGE_DELETE.format(t=target, id=3)).collect()
        return {
            "before": before,
            "after": _delete_files(repark.sql(_DELETE_FILES.format(t=target)).to_arrow()),
            "lineage": _lineage(repark.sql(_LINEAGE.format(t=target)).to_arrow()),
        }
    finally:
        repark.stop()


def _live_session(warehouse: Path):
    """Reuse the collection's live session when one is alive; otherwise build one."""
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    owned = session is None
    if owned:
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("v3-12-legacy-delete-merge-live")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
        )
        jar = _v37_iceberg_runtime_jar()
        builder = (
            builder.config("spark.jars", jar)
            if jar is not None
            else builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        )
        session = builder.getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
    session.conf.set(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    session.conf.set(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
    session.conf.set(f"spark.sql.catalog.{_CATALOG}.warehouse", str(warehouse))
    return session, owned


def _spark_legacy_merge_shape() -> dict:
    """Live Spark running the same statements over a layout-independent one-file seed."""
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v312-legacy-"))
    session, owned = _live_session(warehouse)
    target = f"{_CATALOG}.sales.legacy"
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.sales")
        session.sql(
            f"CREATE TABLE {target} ({_SEED_SCHEMA}) USING iceberg TBLPROPERTIES ({_MOR_V2})"
        )
        session.createDataFrame(_SEED_ROWS, _SEED_SCHEMA).coalesce(1).writeTo(target).append()
        session.sql(_MERGE_DELETE.format(t=target, id=2))
        before = _delete_files(session.sql(_DELETE_FILES.format(t=target)).toArrow())
        session.sql(_UPGRADE.format(t=target))
        session.sql(_MERGE_DELETE.format(t=target, id=3))
        return {
            "before": before,
            "after": _delete_files(session.sql(_DELETE_FILES.format(t=target)).toArrow()),
            "lineage": _lineage(session.sql(_LINEAGE.format(t=target)).toArrow()),
        }
    finally:
        session.sql(f"DROP TABLE IF EXISTS {target}")
        if owned:
            session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


def test_v3_legacy_parquet_position_delete_merges_into_the_dv(tmp_path: Path) -> None:
    """The facade door merges the legacy positions and drops the superseded parquet file."""
    shape = _repark_legacy_merge_shape(tmp_path)
    assert shape["before"] == _EXPECTED_BEFORE
    assert shape["after"] == _EXPECTED_AFTER
    assert shape["lineage"] == _EXPECTED_LINEAGE


def test_v3_legacy_parquet_position_delete_merge_matches_spark(tmp_path: Path) -> None:
    """The same five statements leave repark and live Spark in the same shape."""
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    assert _repark_legacy_merge_shape(tmp_path) == _spark_legacy_merge_shape()


def test_v2_merge_on_read_delete_still_writes_parquet_position_deletes(tmp_path: Path) -> None:
    """Incidental control: a table that stays v2 keeps the parquet position-delete path."""
    from repark import ReparkSession

    repark = ReparkSession.builder.appName("v3-12-v2-control").getOrCreate()
    target = "ice.sales.stay_v2"
    try:
        repark.register_memory_catalog("ice", tmp_path)
        repark.sql("CREATE NAMESPACE ice.sales")
        repark.sql(
            f"CREATE TABLE {target} ({_SEED_SCHEMA}) USING iceberg TBLPROPERTIES ({_MOR_V2})"
        )
        repark.sql(f"INSERT INTO {target} VALUES {_SEED_SQL}").collect()
        repark.sql(_MERGE_DELETE.format(t=target, id=2)).collect()
        repark.sql(_MERGE_DELETE.format(t=target, id=3)).collect()
        after = _delete_files(repark.sql(_DELETE_FILES.format(t=target)).to_arrow())
        ids = repark.sql(f"SELECT id FROM {target} ORDER BY id").to_arrow()
        rows = ids.column("id").to_pylist()
    finally:
        repark.stop()
    assert after == [("PARQUET", 1, False), ("PARQUET", 1, False)]
    assert rows == [1, 4]


def _upgraded_legacy_session(warehouse: Path, target: str, properties: str, seeds: tuple[str, ...]):
    """A v3 table carrying the legacy parquet position delete the seeds produced."""
    from repark import ReparkSession

    repark = (
        ReparkSession.builder.appName("v3-12-legacy-refusal")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    repark.register_memory_catalog("ice", warehouse)
    repark.sql("CREATE NAMESPACE ice.sales")
    repark.sql(f"CREATE TABLE {target} ({_SEED_SCHEMA}) USING iceberg TBLPROPERTIES ({properties})")
    for values in seeds:
        repark.sql(f"INSERT INTO {target} VALUES {values}").collect()
    return repark


def test_plain_where_mor_delete_over_a_legacy_parquet_delete_refuses_loudly(tmp_path: Path) -> None:
    """V3-UPGRADE-DV-PLAIN-1 on the facade: the fork's delete exec refuses before any IO."""
    from repark.errors import PySparkException

    target = "ice.sales.plain"
    repark = _upgraded_legacy_session(tmp_path, target, _MOR_V2, (_SEED_SQL,))
    try:
        repark.sql(_MERGE_DELETE.format(t=target, id=2)).collect()
        repark.sql(_UPGRADE.format(t=target)).collect()
        with pytest.raises(PySparkException) as raised:
            repark.sql(f"DELETE FROM {target} WHERE id = 3").collect()
        after = _delete_files(repark.sql(_DELETE_FILES.format(t=target)).to_arrow())
    finally:
        repark.stop()
    message = str(raised.value)
    assert "is still covered by a Parquet position-delete file" in message
    assert "loadPreviousDeletes" in message
    assert after == _EXPECTED_BEFORE


def test_partition_scoped_legacy_delete_refuses_loudly(tmp_path: Path) -> None:
    """V3-UPGRADE-DV-PART-1 on the facade: a delete covering two data files is not removable."""
    from repark.errors import PySparkException

    target = "ice.sales.partsc"
    repark = _upgraded_legacy_session(
        tmp_path,
        target,
        _MOR_V2_PARTITION_GRANULARITY,
        ("(1, 'a'), (2, 'b')", "(3, 'c'), (4, 'd')"),
    )
    try:
        repark.sql(
            f"MERGE INTO {target} AS target USING (SELECT 1 AS id UNION ALL SELECT 3) AS source "
            "ON target.id = source.id WHEN MATCHED THEN DELETE"
        ).collect()
        repark.sql(_UPGRADE.format(t=target)).collect()
        with pytest.raises(PySparkException) as raised:
            repark.sql(_MERGE_DELETE.format(t=target, id=2)).collect()
        ids = repark.sql(f"SELECT id FROM {target} ORDER BY id").to_arrow()
        rows = ids.column("id").to_pylist()
    finally:
        repark.stop()
    message = str(raised.value)
    assert "Cannot commit deletion vector" in message
    assert "live position delete file" in message
    assert rows == [2, 4]
