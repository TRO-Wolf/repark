"""Local cross-read leg: live Spark reads RePark-written acceptance tables.

Each offline acceptance scenario runs unchanged through its own runner, then
every table it wrote is adopted into a private live Spark catalog and read on
both engines: Spark rows equal RePark rows. Skips cleanly without the live
flag or without PySpark; the shared Spark session is never stopped.

pins: ice-spark-cross-read-1/C-002, C-003
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _live_parity as lp
import pyarrow.fs as pafs
import pytest
from _acceptance import assert_mor_maintenance_outcome, run_mor_merge_compact_expire
from _acceptance_cross_read import (
    SPARK_CANNOT_READ,
    assert_outcomes_expected,
    cross_read_local_tables,
    format_outcome_table,
    metadata_has_ns_types,
)
from _acceptance_replace import assert_replace_twice_outcome, run_create_or_replace_twice
from _acceptance_v3 import (
    V3_ADOPTED_SUFFIX,
    V3_ALLOW_CREATE_KEY,
    assert_v3_acceptance_outcome,
    current_metadata_location,
    run_v3_acceptance,
)

from repark import ReparkSession

_LIVE = lp.LIVE
_LIVE_SKIP = lp.LIVE_SKIP_REASON


def _repark_session(app_name: str, extra_config: dict[str, str] | None = None) -> ReparkSession:
    """Fresh RePark session pinned to UTC; the caller stops it."""
    builder = ReparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC")
    if extra_config is not None:
        for key, value in extra_config.items():
            builder = builder.config(key, value)
    return builder.getOrCreate()


def _spark_session(warehouse: Path, catalog: str) -> Any:
    """Live Spark Iceberg engine on a private catalog; never stopped here."""
    return lp.build_spark_iceberg_engine(warehouse, catalog=catalog).session


def _second_session(first: ReparkSession, warehouse: Path) -> ReparkSession:
    """Distinct engine handle over the same warehouse for the v3 adopt step."""
    session = first.newSession()
    session.register_memory_catalog("ice", warehouse)
    location = warehouse / "sales"
    session.sql(f"CREATE NAMESPACE ice.sales LOCATION '{location}'")
    return session


def _adopt_same_metadata(
    repark: ReparkSession, catalog: str, namespace: str, name: str, metadata_file: str
) -> None:
    """Register one metadata file under a new name on this session."""
    repark.sql(
        f"CALL {catalog}.system.register_table("
        f"table => '{namespace}.{name}', metadata_file => '{metadata_file}')"
    )


def _cross_read_and_assert(
    repark: ReparkSession,
    repark_catalog: str,
    spark: Any,
    spark_catalog: str,
    namespace: str,
    tables: list[str],
) -> None:
    """Cross-read tables, print the outcome table, assert the expected rule."""
    outcomes = cross_read_local_tables(
        repark, repark_catalog, spark, spark_catalog, namespace, tables
    )
    print(format_outcome_table(outcomes))
    assert_outcomes_expected(outcomes)


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_mor_table_reads_row_equal_on_live_spark(tmp_path: Path) -> None:
    """Live Spark reads the MOR acceptance table row-equal after compact and expire."""
    pytest.importorskip("pyspark")
    warehouse = tmp_path
    repark = _repark_session("xread-mor")
    try:
        repark.register_memory_catalog("mem", warehouse)
        owned = warehouse / "owned"
        repark.sql(f"CREATE NAMESPACE mem.ns LOCATION '{owned}'")
        outcome = run_mor_merge_compact_expire(repark, "mem", "ns", "mw4mor")
        assert_mor_maintenance_outcome(repark, outcome)
        assert outcome.table == "mem.ns.mw4mor"
        spark = _spark_session(warehouse, "xread_mor")
        _cross_read_and_assert(repark, "mem", spark, "xread_mor", "ns", ["mw4mor"])
    finally:
        repark.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_replace_twice_table_reads_row_equal_on_live_spark(tmp_path: Path) -> None:
    """Live Spark reads the create-or-replace-twice table row-equal."""
    pytest.importorskip("pyspark")
    warehouse = tmp_path
    repark = _repark_session("xread-replace")
    try:
        repark.register_memory_catalog("mem", warehouse)
        owned = warehouse / "owned"
        repark.sql(f"CREATE NAMESPACE mem.ns LOCATION '{owned}'")
        outcome = run_create_or_replace_twice(repark, "mem", "ns", "replace2mem")
        assert_replace_twice_outcome(outcome)
        assert outcome.table == "mem.ns.replace2mem"
        spark = _spark_session(warehouse, "xread_replace")
        _cross_read_and_assert(repark, "mem", spark, "xread_replace", "ns", ["replace2mem"])
    finally:
        repark.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_v3_tables_read_row_equal_on_live_spark(tmp_path: Path) -> None:
    """Live Spark reads the v3 DV table and its adopted twin row-equal."""
    pytest.importorskip("pyspark")
    warehouse = tmp_path
    repark = _repark_session("xread-v3", {V3_ALLOW_CREATE_KEY: "true"})
    try:
        repark.register_memory_catalog("ice", warehouse)
        owned = warehouse / "sales"
        repark.sql(f"CREATE NAMESPACE ice.sales LOCATION '{owned}'")
        outcome = run_v3_acceptance(
            repark, "ice", "sales", "v3acc", adopt_with=lambda: _second_session(repark, warehouse)
        )
        assert_v3_acceptance_outcome(outcome)
        assert outcome.table == "ice.sales.v3acc"
        assert outcome.adopted_table == f"ice.sales.v3acc{V3_ADOPTED_SUFFIX}"
        adopted_name = f"v3acc{V3_ADOPTED_SUFFIX}"
        metadata_file = current_metadata_location(repark, "ice.sales.v3acc")
        _adopt_same_metadata(repark, "ice", "sales", adopted_name, metadata_file)
        spark = _spark_session(warehouse, "xread_v3")
        _cross_read_and_assert(repark, "ice", spark, "xread_v3", "sales", ["v3acc", adopted_name])
    finally:
        repark.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_ns_table_is_expected_spark_cannot_read(tmp_path: Path) -> None:
    """A v3 nanosecond table cross-reads as expected SPARK-CANNOT-READ (R-002)."""
    pytest.importorskip("pyspark")
    warehouse = tmp_path
    repark = _repark_session("xread-ns", {V3_ALLOW_CREATE_KEY: "true"})
    try:
        repark.register_memory_catalog("ice", warehouse)
        repark.sql("CREATE NAMESPACE ice.ns")
        repark.sql(
            "CREATE TABLE ice.ns.nsts (id INT, ts timestamp_ns, tz timestamptz_ns) "
            "USING iceberg TBLPROPERTIES ('format-version' = '3')"
        )
        repark.sql(
            "INSERT INTO ice.ns.nsts SELECT 1, "
            "CAST('2024-01-15 10:20:30.123456789' AS timestamp_ns), "
            "CAST('2024-01-15 10:20:30.123456789' AS timestamptz_ns)"
        ).collect()
        repark.sql(
            "INSERT INTO ice.ns.nsts SELECT 2, "
            "CAST('2023-06-30 12:34:56.987654321' AS timestamp_ns), "
            "CAST('2023-06-30 12:34:56.987654321' AS timestamptz_ns)"
        ).collect()
        count = repark.sql("SELECT COUNT(*) AS n FROM ice.ns.nsts").to_arrow()
        assert count.column("n").to_pylist() == [2]
        rows = repark.sql(
            "SELECT id, CAST(ts AS STRING) AS ts, CAST(tz AS STRING) AS tz "
            "FROM ice.ns.nsts ORDER BY id"
        ).to_arrow()
        print(rows.to_pylist())
        spark = _spark_session(warehouse, "xread_ns")
        outcomes = cross_read_local_tables(repark, "ice", spark, "xread_ns", "ns", ["nsts"])
        print(format_outcome_table(outcomes))
        assert len(outcomes) == 1
        outcome = outcomes[0]
        assert outcome.has_ns_columns
        assert outcome.status == SPARK_CANNOT_READ
        assert outcome.spark_error_class is not None
        condition = outcome.spark_error_condition or ""
        print(
            f"spark error: {outcome.spark_error_class} {condition} {outcome.spark_error_first_line}"
        )
    finally:
        repark.stop()


class S3AsLocal:
    def __init__(self, doc: str) -> None:
        self.doc = doc
        self.seen: list[str] = []

    def from_uri(self, location: str) -> tuple[pafs.FileSystem, str]:
        self.seen.append(location)
        return pafs.LocalFileSystem(), self.doc


def ns_metadata_doc() -> dict[str, Any]:
    return {
        "current-schema-id": 0,
        "schemas": [
            {
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": False, "type": "long"},
                    {"id": 2, "name": "ts", "required": False, "type": "timestamp_ns"},
                ],
            }
        ],
    }


def write_metadata_doc(tmp_path: Path) -> Path:
    doc = tmp_path / "v1.metadata.json"
    doc.write_text(json.dumps(ns_metadata_doc()), encoding="utf-8")
    return doc


def test_metadata_ns_check_reads_a_bare_local_path(tmp_path: Path) -> None:
    assert metadata_has_ns_types(str(write_metadata_doc(tmp_path))) is True


def test_metadata_ns_check_reads_a_relative_local_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    doc = write_metadata_doc(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert metadata_has_ns_types(doc.name) is True


def test_metadata_ns_check_reads_a_file_uri(tmp_path: Path) -> None:
    assert metadata_has_ns_types(write_metadata_doc(tmp_path).as_uri()) is True


def test_metadata_ns_check_reads_an_s3_location_through_its_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = S3AsLocal(str(write_metadata_doc(tmp_path)))
    monkeypatch.setattr(pafs, "FileSystem", fake)
    assert metadata_has_ns_types("s3://bucket/metadata/v1.metadata.json") is True
    assert fake.seen == ["s3://bucket/metadata/v1.metadata.json"]
