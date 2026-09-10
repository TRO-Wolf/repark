from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

TABLE = "mem.ns.policy"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-maintenance-policy-1").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    session.sql(f"CREATE TABLE {TABLE} USING iceberg AS SELECT 1 AS id, 'a' AS name")
    return session


def _schema_names(table: pa.Table) -> list[str]:
    return [field.name for field in table.schema]


def test_run_maintenance_dry_run_defaults_to_true(spark: ReparkSession) -> None:
    frame = spark.run_maintenance(
        TABLE, target_file_size_bytes=67108864, snapshot_retain_last=5
    ).to_arrow()
    assert _schema_names(frame) == ["step", "procedure", "arguments", "status", "result"]
    assert frame.num_rows >= 2
    assert set(frame.column("status").to_pylist()) == {"planned"}


def test_run_maintenance_override_kwargs_reach_the_call(spark: ReparkSession) -> None:
    frame = spark.run_maintenance(
        TABLE, target_file_size_bytes=67108864, snapshot_retain_last=7
    ).to_arrow()
    procedures: list[str] = frame.column("procedure").to_pylist()
    calls: list[str] = frame.column("arguments").to_pylist()
    rows = dict(zip(procedures, calls, strict=True))
    assert "target-file-size-bytes', '67108864'" in rows["rewrite_data_files"]
    assert "retain_last => 7" in rows["expire_snapshots"]


def test_run_maintenance_no_policy_refuses_by_class(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match=r"no \[default\.maintenance\] table"):
        spark.run_maintenance(TABLE)


def test_session_run_maintenance_frame_shape(spark: ReparkSession) -> None:
    frame = spark.run_maintenance(
        TABLE, target_file_size_bytes=67108864, snapshot_retain_last=5
    ).to_arrow()
    assert frame.schema.field("step").type == pa.int32()
    for name in ["procedure", "arguments", "status", "result"]:
        assert frame.schema.field(name).type == pa.string()
    assert frame.column("procedure").to_pylist() == [
        "rewrite_data_files",
        "expire_snapshots",
    ]
    assert frame.column("step").to_pylist() == [2, 4]


def test_run_maintenance_adaptive_partitioning_refuses_reserved(
    spark: ReparkSession,
) -> None:
    with pytest.raises(AnalysisException, match=r"not yet supported"):
        spark.run_maintenance(TABLE, adaptive_partitioning=True)
