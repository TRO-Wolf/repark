"""ICE-OVERWRITE-MODE-1 — Spark's overwrite partition set on every facade overwrite door.

pins: ice-overwrite-mode-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-011,
C-012, C-013, C-014, C-016
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _record_ice_overwrite_mode_1_oracle import (
    SHAPES,
    VERSIONS,
    OverwriteShape,
    apply_shape,
    run_cell,
    seed_table,
    table_name,
)

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_overwrite_mode_1_spark_oracle.json").read_text(encoding="utf-8")
)["cells"]
RTAS_HISTORY = frozenset({"SAVEASTABLE-OW-OPT", "SAVEASTABLE-OW-DYNSESSION"})
ARROW_TYPES: dict[str, tuple[pa.DataType, ...]] = {
    "BIGINT": (pa.int64(),),
    "STRING": (pa.string(), pa.large_string()),
    "DATE": (pa.date32(),),
    "TIMESTAMP": (pa.timestamp("us", tz="UTC"),),
}
CELLS = [
    pytest.param(shape, version, id=shape.cell_id(version))
    for shape in SHAPES
    for version in VERSIONS
]
RTAS_CELLS = [
    pytest.param(shape, version, id=shape.cell_id(version))
    for shape in SHAPES
    for version in VERSIONS
    if shape.key in RTAS_HISTORY
]


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[ReparkSession]:
    """Fresh facade session with catalog ``sc`` and v3 creation allowed."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-overwrite-mode-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE sc.ns")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _assert_arrow_types(session: ReparkSession, shape: OverwriteShape, version: int) -> None:
    """Pin the Arrow column types of the overwritten table."""
    schema = session.sql(f"SELECT * FROM {table_name(shape, version)}").to_arrow().schema
    for column in shape.column_ddl().split(", "):
        name, sql_type = column.split(" ")
        assert schema.field(name).type in ARROW_TYPES[sql_type], column


@pytest.mark.parametrize(("shape", "version"), CELLS)
def test_overwrite_cell_matches_spark(
    spark: ReparkSession, shape: OverwriteShape, version: int
) -> None:
    """Rows, snapshot history, or refusal equal the recorded Spark cell."""
    want = FIXTURE[shape.cell_id(version)]
    got = run_cell(spark, shape, version)
    if want["status"] == "error":
        assert got["status"] == "error", got
        assert got["error_class"] == want["error_class"], got
        assert f"[{want['condition']}]" in got["message"], got
        return
    assert got["status"] == "ok", got
    assert got["data"] == want["data"]
    _assert_arrow_types(spark, shape, version)
    if shape.key not in RTAS_HISTORY:
        assert got["snapshots"] == want["snapshots"]


@pytest.mark.parametrize(("shape", "version"), RTAS_CELLS)
def test_save_as_table_history_matches_spark(
    spark: ReparkSession, shape: OverwriteShape, version: int
) -> None:
    """saveAsTable overwrite snapshot history equals Spark's RTAS history."""
    got = run_cell(spark, shape, version)
    assert got["snapshots"] == FIXTURE[shape.cell_id(version)]["snapshots"]


def test_non_partition_column_refusal_keeps_every_row(spark: ReparkSession) -> None:
    """The NON_PARTITION_COLUMN refusal leaves the unpartitioned table untouched."""
    shape = next(item for item in SHAPES if item.key == "UNPART-PDYN-ERR")
    table = seed_table(spark, shape, 2)
    with pytest.raises(Exception, match=r"\[NON_PARTITION_COLUMN\]") as excinfo:
        apply_shape(spark, shape, table)
    assert type(excinfo.value).__name__ == "AnalysisException"
    rows = sorted(tuple(row) for row in spark.sql(f"SELECT * FROM {table}").collect())
    assert rows == [(1, "a", "x"), (2, "b", "y"), (3, "c", "x")]


def test_invalid_static_date_refuses_like_the_engine_cast(spark: ReparkSession) -> None:
    """A static value the DATE cast rejects refuses with the engine's CAST refusal text.

    Spark answers ``CAST_INVALID_INPUT`` there; the pin holds the engine's own cast text.
    """
    shape = next(item for item in SHAPES if item.key == "CAST-STATIC-DATE")
    table = seed_table(spark, shape, 2)
    with pytest.raises(Exception) as cast_error:
        spark.sql("SELECT CAST('2024-13-45' AS DATE)").collect()
    with pytest.raises(Exception) as insert_error:
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (d = '2024-13-45') SELECT 9, 'z'").collect()
    refusal = "Cast error: Cannot cast string '2024-13-45' to value of Date32 type"
    assert refusal in str(cast_error.value), cast_error.value
    assert refusal in str(insert_error.value), insert_error.value
    rows = sorted(tuple(row) for row in spark.sql(f"SELECT id, data FROM {table}").collect())
    assert rows == [(1, "a"), (2, "b")]


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The recorder re-derives every fixture cell on live Spark."""
    recorder = Path(__file__).with_name("_record_ice_overwrite_mode_1_oracle.py")
    environ = dict(os.environ)
    environ.setdefault("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64")
    environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    completed = subprocess.run(
        [sys.executable, str(recorder), "--warehouse", str(tmp_path / "live-wh"), "check"],
        capture_output=True,
        text=True,
        env=environ,
        timeout=1200,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
