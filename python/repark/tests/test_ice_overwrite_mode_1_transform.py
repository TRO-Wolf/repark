"""ICE-OVERWRITE-MODE-1 — ``writeTo(t).overwritePartitions()`` on specs that hold a transform.

Oracle: ``ice_overwrite_mode_1_transform_spark_oracle.json`` (live PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0, re-derived by
``_record_ice_overwrite_mode_1_transform.py``). Spark replaces the partitions the frame's rows
land in, whatever the transform, in both session modes. The facade sends the write with the
dynamic intent and no ``PARTITION`` clause, so Rust decides the partition set; a clause naming
the spec's transform fields refused ``[NON_PARTITION_COLUMN]`` (verification critic,
2026-09-19). The live tier re-derives the fixture.

pins: ice-overwrite-mode-1/C-018
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__)
    .with_name("ice_overwrite_mode_1_transform_spark_oracle.json")
    .read_text(encoding="utf-8")
)
CELLS: dict[str, Any] = FIXTURE["cells"]


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-overwrite-mode-1-transform")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", tmp_path / "wh")
    session.sql("CREATE NAMESPACE sc.ns")
    yield session
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    session.stop()
    _reset_active_session_for_tests()


@pytest.mark.parametrize("key", sorted(CELLS))
def test_overwrite_partitions_matches_spark(spark: Any, key: str) -> None:
    """The rows left and the last operation equal Spark's for every spec, mode and version."""
    cell = CELLS[key]
    table = f"sc.ns.{key}"
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", cell["mode"])
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"{cell['spec']} TBLPROPERTIES ('format-version'='{cell['version']}')"
    ).collect()
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')").collect()
    frame = spark.createDataFrame([(1, "z", "x")], "id BIGINT, data STRING, cat STRING")
    frame.writeTo(table).overwritePartitions()
    rows = spark.sql(f"SELECT * FROM {table} ORDER BY id, data").to_arrow().to_pylist()
    assert [list(row.values()) for row in rows] == cell["rows"]
    last = spark.sql(
        f"SELECT operation FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).to_arrow()
    assert last.column("operation").to_pylist() == [cell["last_operation"]]


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The generator re-derives the fixture on live Spark."""
    environ = dict(os.environ)
    environ.setdefault("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64")
    environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    completed = subprocess.run(
        [
            "/tmp/sparkenv/bin/python",
            str(Path(__file__).with_name("_record_ice_overwrite_mode_1_transform.py")),
            "--warehouse",
            str(tmp_path / "live-wh"),
            "check",
        ],
        capture_output=True,
        text=True,
        env=environ,
        timeout=1200,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
