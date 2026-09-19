"""ICE-AVRO-NAME-1 — partition columns whose names are not valid Avro names write and read back.

Oracle: ``ice_avro_name_1_spark_oracle.json`` (live PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0, re-derived by ``_record_ice_avro_name_1_oracle.py``).
Before fork #308 (RP-35) a RePark table partitioned by ``my col`` wrote a manifest whose
partition record kept the raw name, and every later read failed. Each cell creates the same
table on RePark, inserts the same two rows, and compares the rows, the ``partitions`` metadata
table and the data manifest's Avro partition record (sanitised name, ``iceberg-field-name``,
field id) with Spark's. The live tier re-derives the fixture.

pins: rp-35-fork-pin/C-001
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _record_ice_avro_name_1_oracle import avro_header

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_avro_name_1_spark_oracle.json").read_text(encoding="utf-8")
)
CELLS: dict[str, Any] = FIXTURE["cells"]


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-avro-name-1").getOrCreate()
    session.register_memory_catalog("sc", tmp_path / "wh")
    session.sql("CREATE NAMESPACE sc.ns")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _build(session: Any, key: str) -> str:
    cell = CELLS[key]
    table = f"sc.ns.{key}"
    session.sql(
        f"CREATE TABLE {table} {cell['create'].split(') PARTITIONED BY ')[0]}) USING iceberg "
        f"PARTITIONED BY {cell['create'].split(') PARTITIONED BY ')[1]} "
        f"TBLPROPERTIES ('format-version'='{cell['version']}')"
    ).collect()
    session.sql(f"INSERT INTO {table} VALUES (1, {cell['value']}), (2, {cell['value']})").collect()
    return table


def _manifest_partition_fields(session: Any, table: str) -> list[dict[str, Any]]:
    frame = session.sql(f"SELECT path FROM {table}.manifests WHERE content = 0").to_arrow()
    path = Path(str(frame.column("path").to_pylist()[0]).removeprefix("file:"))
    schema = json.loads(avro_header(path)["avro.schema"])
    data_file = next(field for field in schema["fields"] if field["name"] == "data_file")
    partition = next(field for field in data_file["type"]["fields"] if field["name"] == "partition")
    return [
        {
            "name": field["name"],
            "iceberg-field-name": field.get("iceberg-field-name"),
            "field-id": field.get("field-id"),
        }
        for field in partition["type"]["fields"]
    ]


@pytest.mark.parametrize("key", sorted(CELLS))
def test_rows_read_back(spark: Any, key: str) -> None:
    """The table reads back Spark's rows through a filter on the partition column."""
    cell = CELLS[key]
    table = _build(spark, key)
    frame = spark.sql(
        f"SELECT * FROM {table} WHERE {cell['column']} = {cell['value']} ORDER BY id"
    ).to_arrow()
    rows = [list(row.values()) for row in frame.to_pylist()]
    assert rows == cell["rows"]


@pytest.mark.parametrize("key", sorted(CELLS))
def test_partitions_metadata_table(spark: Any, key: str) -> None:
    """The ``partitions`` metadata table names the partition field as Spark does."""
    table = _build(spark, key)
    frame = spark.sql(f"SELECT partition FROM {table}.partitions").to_arrow()
    assert frame.column("partition").to_pylist() == CELLS[key]["parts"]


@pytest.mark.parametrize("key", sorted(CELLS))
def test_manifest_partition_record_matches_spark(spark: Any, key: str) -> None:
    """The data manifest's Avro partition record carries Java's sanitised names."""
    table = _build(spark, key)
    assert _manifest_partition_fields(spark, table) == CELLS[key]["manifest_partition_fields"]


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The generator re-derives the fixture on live Spark."""
    environ = dict(os.environ)
    environ.setdefault("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64")
    environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    completed = subprocess.run(
        [
            "/tmp/sparkenv/bin/python",
            str(Path(__file__).with_name("_record_ice_avro_name_1_oracle.py")),
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
