"""ICE-WRITE-OPTIONS-RP-1 — a caller ``replace-partitions`` summary value wins.

Every cell reads its expected answer from the recorded Spark 4.1.2 + Iceberg 1.11.0
fixture
(``fixtures/torture/data/ice_write_options_rp_1/spark_rp_oracle.json``): five cells, one
test each. Each pin creates the cell's table (``id INT, p STRING``,
``PARTITIONED BY (p)``, seed ``(1, 'a'), (2, 'b')``) on a fresh RePark memory catalog,
writes the source ``(3, 'a')`` through the cell's door with the cell's
``snapshot-property`` option — ``insertInto`` overwrite under
``partitionOverwriteMode=dynamic``, or ``writeTo(t).overwritePartitions()`` — then
asserts the write committed and the newest snapshot's summary ``replace-partitions``
(and ``k`` for the control cell) equals Spark's. The live tier re-derives one cell on
Spark under ``REPARK_PARITY_LIVE=1``.

pins: ice-write-options-rp-1/C-003, C-004, C-005, C-006
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

_CATALOG = "ice_write_options_rp_1"
_NAMESPACE = "ns"
_ORACLE: dict[str, Any] = json.loads(
    (
        Path(__file__).resolve().parents[2]
        / "repark-parity/fixtures/torture/data/ice_write_options_rp_1/spark_rp_oracle.json"
    ).read_text(encoding="utf-8")
)
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_DYNAMIC_CONF = "spark.sql.sources.partitionOverwriteMode"
_REPLACE_OPTION = "snapshot-property.replace-partitions"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog and one namespace."""
    session = ReparkSession.builder.appName("pytest-ice-write-options-rp-1").getOrCreate()
    session.register_memory_catalog(_CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
    return session


def _table(name: str) -> str:
    """Return the catalog-qualified table name for one cell."""
    return f"{_CATALOG}.{_NAMESPACE}.{name}"


def _seed(spark: ReparkSession, name: str) -> str:
    """Create one cell's table and seed ``(1, 'a'), (2, 'b')``, returning its name."""
    table = _table(name)
    spark.sql(f"CREATE TABLE {table} (id INT, p STRING) USING iceberg PARTITIONED BY (p)")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    return table


def _source(spark: ReparkSession) -> Any:
    """Return the ``(3, 'a')`` source frame every cell writes."""
    return spark.sql("SELECT CAST(3 AS INT) AS id, 'a' AS p")


def _snapshot_count(spark: ReparkSession, table: str) -> int:
    """Return the snapshot count of ``table``."""
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").to_arrow().to_pylist()
    return int(rows[0]["n"])


def _latest_summary(spark: ReparkSession, table: str) -> dict[str, str]:
    """Return the newest snapshot's summary of ``table`` as a string map."""
    rows = (
        spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1")
        .to_arrow()
        .to_pylist()
    )
    assert rows, f"no snapshots on {table}"
    return dict(rows[0]["summary"])


def _assert_cell(spark: ReparkSession, table: str, key: str, before: int) -> None:
    """Assert the write committed and its summary answers the recorded cell."""
    cell = _CELLS[key]
    assert cell["outcome"] == "ok", (key, cell.get("error"))
    assert _snapshot_count(spark, table) == before + 1, (key, "committed")
    summary = _latest_summary(spark, table)
    assert summary.get("replace-partitions") == cell["last_summary_replace_partitions"], (
        key,
        summary,
    )
    assert summary.get("k") == cell["last_summary_k"], (key, summary)


def test_insert_into_dynamic_false(spark: ReparkSession) -> None:
    """Dynamic ``insertInto`` overwrite lands the caller ``false`` in the summary."""
    table = _seed(spark, "insertInto_dynamic_false")
    before = _snapshot_count(spark, table)
    spark.conf.set(_DYNAMIC_CONF, "dynamic")
    try:
        (
            _source(spark)
            .write.format("iceberg")
            .option(_REPLACE_OPTION, "false")
            .mode("overwrite")
            .insertInto(table)
        )
    finally:
        spark.conf.unset(_DYNAMIC_CONF)
    _assert_cell(spark, table, "insertInto_dynamic_false", before)


def test_insert_into_dynamic_true(spark: ReparkSession) -> None:
    """Dynamic ``insertInto`` overwrite lands the caller ``true`` in the summary."""
    table = _seed(spark, "insertInto_dynamic_true")
    before = _snapshot_count(spark, table)
    spark.conf.set(_DYNAMIC_CONF, "dynamic")
    try:
        (
            _source(spark)
            .write.format("iceberg")
            .option(_REPLACE_OPTION, "true")
            .mode("overwrite")
            .insertInto(table)
        )
    finally:
        spark.conf.unset(_DYNAMIC_CONF)
    _assert_cell(spark, table, "insertInto_dynamic_true", before)


def test_overwrite_partitions_true(spark: ReparkSession) -> None:
    """``overwritePartitions`` with the option ``true`` stamps ``true``."""
    table = _seed(spark, "overwritePartitions_true")
    before = _snapshot_count(spark, table)
    _source(spark).writeTo(table).option(_REPLACE_OPTION, "true").overwritePartitions()
    _assert_cell(spark, table, "overwritePartitions_true", before)


def test_overwrite_partitions_false(spark: ReparkSession) -> None:
    """``overwritePartitions`` with the option ``false`` stamps ``false``."""
    table = _seed(spark, "overwritePartitions_false")
    before = _snapshot_count(spark, table)
    _source(spark).writeTo(table).option(_REPLACE_OPTION, "false").overwritePartitions()
    _assert_cell(spark, table, "overwritePartitions_false", before)


def test_overwrite_partitions_control_other(spark: ReparkSession) -> None:
    """``overwritePartitions`` with an unrelated option keeps the engine marker."""
    table = _seed(spark, "overwritePartitions_control_other")
    before = _snapshot_count(spark, table)
    _source(spark).writeTo(table).option("snapshot-property.k", "v").overwritePartitions()
    _assert_cell(spark, table, "overwritePartitions_control_other", before)


def _live_spark_session(warehouse: Path, catalog: str) -> Any:
    """Return the shared live PySpark session with a private Hadoop catalog."""
    import _live_parity as live

    return live.build_spark_iceberg_engine(warehouse, catalog=catalog).session


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_spark_rederives_false_cell(tmp_path: Path) -> None:
    """Live Spark replays the ``false`` cell and answers the recorded summary."""
    key = "overwritePartitions_false"
    cell = _CELLS[key]
    spark = _live_spark_session(tmp_path / "spark_wh", "rp1_live")
    table = f"rp1_live.ns.{key}"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rp1_live.ns")
    spark.sql(f"CREATE TABLE {table} (id INT, p STRING) USING iceberg PARTITIONED BY (p)")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    before = int(spark.sql(f"SELECT count(*) AS c FROM {table}.snapshots").collect()[0]["c"])
    source = spark.createDataFrame([(3, "a")], "id INT, p STRING")
    source.writeTo(table).option(_REPLACE_OPTION, "false").overwritePartitions()
    after = int(spark.sql(f"SELECT count(*) AS c FROM {table}.snapshots").collect()[0]["c"])
    assert after == before + 1, (key, "committed")
    summary = dict(
        spark.sql(
            f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()[0]["summary"]
    )
    assert summary.get("replace-partitions") == cell["last_summary_replace_partitions"], (
        key,
        summary,
    )
    assert summary.get("k") == cell["last_summary_k"], (key, summary)
