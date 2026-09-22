"""ICE-WRITE-OPTIONS-1 — DataFrame write options on Iceberg writes.

Offline tier compares RePark with the recorded Spark 4.1.2 + Iceberg 1.11.0 fixture
(``ice_write_options_1_spark_oracle.json``). Live tier (``REPARK_PARITY_LIVE=1``)
re-runs the record drivers and checks the fixture cell by cell.

pins: ice-write-options-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import warnings
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession, _native
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    UnsupportedOperationException,
)

CATALOG = "ice_write_options_1"
NS = "ns"
_FIXTURE = Path(__file__).resolve().parent / "ice_write_options_1_spark_oracle.json"
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_SPARK_APP_KEYS = {
    "app-id",
    "app-name",
    "engine-name",
    "engine-version",
    "iceberg-version",
    "spark.app.id",
}
_COLLISION_FIELDS = (
    "key",
    "value",
    "snapshots_before",
    "summary_value_for_key",
    "summary_keys",
    "rows",
)
_COLLISION_CELLS = tuple(f"COLL-{index:02d}" for index in range(9))
_ENGINE_RESERVED_CELLS = {"COLL-01-engine-name", "COLL-02-engine-version"}


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog + namespace (local, AWS-free)."""
    session = ReparkSession.builder.appName("pytest-ice-write-options-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NS}")
    return session


def _fixture_cell(cell_id: str) -> dict[str, Any]:
    cells = json.loads(_FIXTURE.read_text(encoding="utf-8"))["cells"]
    for cell in cells:
        if cell["id"] == cell_id:
            return cell
    raise AssertionError(f"fixture cell {cell_id} missing")


def _latest_summary(spark: ReparkSession, table: str) -> dict[str, str]:
    rows = (
        spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1")
        .to_arrow()
        .to_pylist()
    )
    assert rows, f"no snapshots on {table}"
    return dict(rows[0]["summary"])


def _snapshot_count(spark: ReparkSession, table: str) -> int:
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").to_arrow().to_pylist()
    return int(rows[0]["n"])


def _data_suffixes(spark: ReparkSession, table: str) -> list[str]:
    rows = spark.sql(f"SELECT file_path FROM {table}.files").to_arrow().to_pylist()
    return sorted(str(row["file_path"]).rsplit(".", 1)[-1] for row in rows)


def _seed(spark: ReparkSession, table: str, partitioned: bool = False) -> None:
    part = " PARTITIONED BY (name)" if partitioned else ""
    spark.sql(f"CREATE TABLE {CATALOG}.{NS}.{table} (id BIGINT, name STRING) USING iceberg{part}")
    spark.sql(
        f"INSERT INTO {CATALOG}.{NS}.{table} "
        "SELECT * FROM (VALUES (0, 'name-0'), (1, 'name-1')) AS t(id, name)"
    )


def _frame(spark: ReparkSession, n: int = 2) -> Any:
    values = ", ".join(f"({i}, 'name-{i}')" for i in range(n))
    return spark.sql(f"SELECT * FROM (VALUES {values}) AS t(id, name)")


def test_snapshot_property_append(spark: ReparkSession) -> None:
    """SNAP-01: the prefix is stripped; the pair lands in the append summary."""
    _seed(spark, "snap_prop")
    table = f"{CATALOG}.{NS}.snap_prop"
    _frame(spark).writeTo(table).option("snapshot-property.run_id", "abc-123").append()
    summary = _latest_summary(spark, table)
    assert summary["run_id"] == "abc-123"
    assert "snapshot-property.run_id" not in summary
    assert _fixture_cell("SNAP-01-run-id-append")["summary"]["run_id"] == "abc-123"


def test_snapshot_property_two_props(spark: ReparkSession) -> None:
    """SNAP-02: two properties land together with exact values."""
    _seed(spark, "snap_two")
    table = f"{CATALOG}.{NS}.snap_two"
    (
        _frame(spark)
        .writeTo(table)
        .option("snapshot-property.run_id", "abc-123")
        .option("snapshot-property.pipeline.batch", "7")
        .append()
    )
    summary = _latest_summary(spark, table)
    assert summary["run_id"] == "abc-123"
    assert summary["pipeline.batch"] == "7"


def test_snapshot_property_utf8_v2(spark: ReparkSession) -> None:
    """SNAP-08: UTF-8 key and value ride the out-of-band channel byte-exact (V2)."""
    _seed(spark, "snap_utf8")
    table = f"{CATALOG}.{NS}.snap_utf8"
    (_frame(spark).writeTo(table).option("snapshot-property.café-🎉", "naïve 🎉").append())
    assert _latest_summary(spark, table)["café-🎉"] == "naïve 🎉"


def test_snapshot_property_utf8_v1(spark: ReparkSession) -> None:
    """SNAP-09: UTF-8 key and value ride the out-of-band channel byte-exact (V1)."""
    _seed(spark, "snap_utf8_v1")
    table = f"{CATALOG}.{NS}.snap_utf8_v1"
    (
        _frame(spark)
        .write.format("iceberg")
        .option("snapshot-property.café", "naïve 🎉")
        .insertInto(table)
    )
    assert _latest_summary(spark, table)["café"] == "naïve 🎉"


def test_user_typed_insert_options_not_honoured(spark: ReparkSession) -> None:
    """SQL-02: user-typed OPTIONS on SQL INSERT fails to parse (main behaviour)."""
    _seed(spark, "sql_smuggle")
    table = f"{CATALOG}.{NS}.sql_smuggle"
    with pytest.raises(ParseException):
        spark.sql(
            f"INSERT INTO {table} OPTIONS('snapshot-property.run_id'='smuggled') "
            "SELECT * FROM (VALUES (9, 'name-9')) AS t(id, name)"
        )
    assert "run_id" not in _latest_summary(spark, table)


def _table_properties(warehouse: Path, table: str) -> dict[str, str]:
    """Property map of the newest metadata JSON under the warehouse root."""
    metas = sorted(warehouse.rglob(f"{table}/metadata/*.metadata.json"))
    assert metas, f"no metadata found for {table}"
    meta = json.loads(metas[-1].read_text(encoding="utf-8"))
    return {str(key): str(value) for key, value in meta.get("properties", {}).items()}


def test_user_typed_ctas_options_land_as_properties(spark: ReparkSession, tmp_path: Path) -> None:
    """SQL-03 (D-5, 2026-09-21): WITH refuses; user-typed OPTIONS on CTAS stores both keys."""
    table = f"{CATALOG}.{NS}.sql_ctas_opt"
    with pytest.raises(UnsupportedOperationException, match="not supported for Iceberg"):
        spark.sql(
            f"CREATE TABLE {table} WITH ('a'='b') AS SELECT * FROM (VALUES (1, 'x')) AS t(id, name)"
        )
    spark.sql(
        f"CREATE TABLE {table} USING iceberg OPTIONS('a'='b') "
        "AS SELECT * FROM (VALUES (1, 'x')) AS t(id, name)"
    )
    props = _table_properties(tmp_path, "sql_ctas_opt")
    assert props["a"] == "b"
    assert props["option.a"] == "b"
    arrow = spark.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow()
    assert arrow.column("id").to_pylist() == [1]


def test_snapshot_property_dyn_overwrite(spark: ReparkSession) -> None:
    """SNAP-03: the property lands on the dynamic-overwrite (replace-partitions) commit."""
    _seed(spark, "snap_part", partitioned=True)
    table = f"{CATALOG}.{NS}.snap_part"
    (_frame(spark).writeTo(table).option("snapshot-property.run_id", "dyn-1").overwritePartitions())
    assert _latest_summary(spark, table)["run_id"] == "dyn-1"
    assert _fixture_cell("SNAP-03-dyn-overwrite")["summary"]["run_id"] == "dyn-1"


def test_snapshot_property_create_replace(spark: ReparkSession) -> None:
    """SNAP-04: the property lands on the CTAS snapshot, which is the only snapshot."""
    table = f"{CATALOG}.{NS}.snap_create"
    (_frame(spark, 4).writeTo(table).option("snapshot-property.run_id", "ctas-1").createOrReplace())
    assert _latest_summary(spark, table)["run_id"] == "ctas-1"
    assert _snapshot_count(spark, table) == 1
    assert _fixture_cell("SNAP-04-create-replace")["snapshot_count"] == 1


def test_create_replace_existing_single_snapshot(spark: ReparkSession) -> None:
    """SNAP-13: OPTIONS replace on an existing table adds one snapshot (P-04)."""
    _seed(spark, "snap_replace_once")
    table = f"{CATALOG}.{NS}.snap_replace_once"
    before = _snapshot_count(spark, table)
    (
        _frame(spark, 4)
        .writeTo(table)
        .option("snapshot-property.run_id", "replace-1")
        .createOrReplace()
    )
    assert _snapshot_count(spark, table) == before + 1
    assert _latest_summary(spark, table)["run_id"] == "replace-1"


def test_snapshot_property_v1_ctas(spark: ReparkSession) -> None:
    """SNAP-05: V1 saveAsTable carries the property onto the CTAS snapshot."""
    table = f"{CATALOG}.{NS}.snap_v1ctas"
    (
        _frame(spark, 4)
        .write.format("iceberg")
        .option("snapshot-property.run_id", "v1-ctas-1")
        .saveAsTable(table)
    )
    assert _latest_summary(spark, table)["run_id"] == "v1-ctas-1"


def test_snapshot_property_v1_insertinto(spark: ReparkSession) -> None:
    """SNAP-06: V1 insertInto carries the property onto the append snapshot."""
    _seed(spark, "snap_v1ins")
    table = f"{CATALOG}.{NS}.snap_v1ins"
    (
        _frame(spark)
        .write.format("iceberg")
        .option("snapshot-property.run_id", "v1-ins-1")
        .insertInto(table)
    )
    assert _latest_summary(spark, table)["run_id"] == "v1-ins-1"


def test_snapshot_property_not_carried(spark: ReparkSession) -> None:
    """SNAP-07: the property is per-snapshot; a later plain append lacks it."""
    _seed(spark, "snap_stale")
    table = f"{CATALOG}.{NS}.snap_stale"
    _frame(spark).writeTo(table).option("snapshot-property.run_id", "first").append()
    _frame(spark).writeTo(table).append()
    assert "run_id" not in _latest_summary(spark, table)


def test_write_format_parquet(spark: ReparkSession) -> None:
    """FORMAT-01: explicit parquet writes parquet files."""
    _seed(spark, "fmt_parquet")
    table = f"{CATALOG}.{NS}.fmt_parquet"
    _frame(spark).writeTo(table).option("write-format", "parquet").append()
    assert set(_data_suffixes(spark, table)) == {"parquet"}


def test_write_format_orc_writes(spark: ReparkSession) -> None:
    """FORMAT-02: write-format orc lands ORC data files."""
    spark.sql(f"CREATE TABLE {CATALOG}.{NS}.fmt_orc (id BIGINT, name STRING) USING iceberg")
    table = f"{CATALOG}.{NS}.fmt_orc"
    _frame(spark).writeTo(table).option("write-format", "orc").append()
    assert set(_data_suffixes(spark, table)) == {"orc"}
    formats = spark.sql(f"SELECT file_format FROM {table}.files").to_arrow().to_pylist()
    assert formats
    assert {str(row["file_format"]) for row in formats} == {"ORC"}


def test_write_format_avro_writes(spark: ReparkSession) -> None:
    """FORMAT-03: write-format avro lands AVRO data files."""
    spark.sql(f"CREATE TABLE {CATALOG}.{NS}.fmt_avro (id BIGINT, name STRING) USING iceberg")
    table = f"{CATALOG}.{NS}.fmt_avro"
    _frame(spark).writeTo(table).option("write-format", "avro").append()
    assert set(_data_suffixes(spark, table)) == {"avro"}
    formats = spark.sql(f"SELECT file_format FROM {table}.files").to_arrow().to_pylist()
    assert formats
    assert {str(row["file_format"]) for row in formats} == {"AVRO"}


def test_write_format_bogus_refuses(spark: ReparkSession) -> None:
    """FORMAT-04: an unknown format is refused like Spark's IllegalArgumentException."""
    _seed(spark, "fmt_bogus")
    table = f"{CATALOG}.{NS}.fmt_bogus"
    with pytest.raises(AnalysisException, match="Invalid file format"):
        _frame(spark).writeTo(table).option("write-format", "bogus").append()
    assert "Invalid file format" in _fixture_cell("FORMAT-04-bogus")["error"]["message"]


def test_target_size_option_accepted(spark: ReparkSession) -> None:
    """OPT-01: a tiny target size is accepted and commits every row with the property."""
    spark.sql(f"CREATE TABLE {CATALOG}.{NS}.opt_size (id BIGINT, name STRING) USING iceberg")
    table = f"{CATALOG}.{NS}.opt_size"
    _frame(spark, 20).writeTo(table).append()
    (
        _frame(spark, 20)
        .writeTo(table)
        .option("target-file-size-bytes", "1024")
        .option("snapshot-property.run_id", "size-1")
        .append()
    )
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == 40
    assert _latest_summary(spark, table)["run_id"] == "size-1"


def test_target_size_bogus_refuses(spark: ReparkSession) -> None:
    """P2-17: a non-numeric target size is refused (Spark: NumberFormatException)."""
    _seed(spark, "size_bad")
    table = f"{CATALOG}.{NS}.size_bad"
    with pytest.raises(AnalysisException, match="target-file-size-bytes"):
        _frame(spark).writeTo(table).option("target-file-size-bytes", "abc").append()


def test_compression_codec_gzip_footer(spark: ReparkSession) -> None:
    """OPT-02: compression-codec=gzip lands in the written parquet footers."""
    import pyarrow.parquet as pa_pq

    spark.sql(f"CREATE TABLE {CATALOG}.{NS}.opt_codec (id BIGINT, name STRING) USING iceberg")
    table = f"{CATALOG}.{NS}.opt_codec"
    _frame(spark).writeTo(table).option("compression-codec", "gzip").append()
    rows = spark.sql(f"SELECT file_path FROM {table}.files").to_arrow().to_pylist()
    assert rows
    for row in rows:
        path = str(row["file_path"])
        local = path[7:] if path.startswith("file://") else path
        codec = pa_pq.read_metadata(local).row_group(0).column(0).compression
        assert codec == "GZIP"


def test_compression_codec_bogus_refuses(spark: ReparkSession) -> None:
    """An unknown codec is refused naming the accepted spellings."""
    _seed(spark, "codec_bad")
    table = f"{CATALOG}.{NS}.codec_bad"
    with pytest.raises(AnalysisException, match="compression-codec"):
        _frame(spark).writeTo(table).option("compression-codec", "bogus").append()


def test_compression_level_zstd_accepted(spark: ReparkSession) -> None:
    """P2-02: zstd + level is accepted and commits the rows."""
    _seed(spark, "opt_level")
    table = f"{CATALOG}.{NS}.opt_level"
    (
        _frame(spark)
        .writeTo(table)
        .option("compression-codec", "zstd")
        .option("compression-level", "1")
        .append()
    )
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == 4


def test_compression_level_alone_accepted(spark: ReparkSession) -> None:
    """P2-01: a lone compression-level is accepted and commits the rows."""
    _seed(spark, "level_alone")
    table = f"{CATALOG}.{NS}.level_alone"
    _frame(spark).writeTo(table).option("compression-level", "5").append()
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == 4


def test_compression_gzip_level_refuses(spark: ReparkSession) -> None:
    """OPT-03: Spark fails gzip + integer level; RePark refuses the combo too."""
    assert "ZlibCompressor" in _fixture_cell("OPT-03-level")["error"]["message"]
    _seed(spark, "gzip_level")
    table = f"{CATALOG}.{NS}.gzip_level"
    with pytest.raises(AnalysisException, match="compression-level"):
        (
            _frame(spark)
            .writeTo(table)
            .option("compression-codec", "gzip")
            .option("compression-level", "1")
            .append()
        )
    assert _snapshot_count(spark, table) == 1


def test_distribution_modes_accepted(spark: ReparkSession) -> None:
    """OPT-04/P2-09: hash and none commit the same rows."""
    for name in ("dist_hash", "dist_none"):
        _seed(spark, name)
    table_hash = f"{CATALOG}.{NS}.dist_hash"
    table_none = f"{CATALOG}.{NS}.dist_none"
    _frame(spark, 4).writeTo(table_hash).option("distribution-mode", "hash").append()
    _frame(spark, 4).writeTo(table_none).option("distribution-mode", "none").append()
    for table in (table_hash, table_none):
        rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
        assert int(rows[0]["n"]) == 6


def test_distribution_bogus_refuses(spark: ReparkSession) -> None:
    """P2-08: an unknown distribution mode is refused like Spark's IllegalArgumentException."""
    _seed(spark, "dist_bad")
    table = f"{CATALOG}.{NS}.dist_bad"
    with pytest.raises(AnalysisException, match="distribution mode"):
        _frame(spark).writeTo(table).option("distribution-mode", "bogus").append()
    assert "Invalid distribution mode" in _fixture_cell("P2-08-dist-bogus")["error"]["message"]


def test_fanout_accepted(spark: ReparkSession) -> None:
    """OPT-05/P2-10/P2-11: fanout true/false/garbage commit the rows, as Spark does."""
    _seed(spark, "fanout", partitioned=True)
    table = f"{CATALOG}.{NS}.fanout"
    for value in ("true", "false", "bogus"):
        (
            _frame(spark, 1)
            .writeTo(table)
            .option("fanout-enabled", value)
            .option("snapshot-property.run_id", f"fan-{value}")
            .append()
        )
    assert _latest_summary(spark, table)["run_id"] == "fan-bogus"
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == 5


def test_isolation_serializable_overlap_commits_divergence(spark: ReparkSession) -> None:
    """OPT-06 divergence: Spark refuses serializable overlap; RePark commits it.

    The option is honoured (serializable validation runs against the base snapshot);
    only the strictness differs — Spark trips on its own files, the fork's OCC finds
    no concurrent commit. Recorded in ICE-WRITE-OPTIONS-1 residuals.
    """
    assert "ValidationException" in _fixture_cell("OPT-06-isolation")["error"]["message"]
    _seed(spark, "opt_iso", partitioned=True)
    table = f"{CATALOG}.{NS}.opt_iso"
    (
        _frame(spark)
        .writeTo(table)
        .option("isolation-level", "serializable")
        .option("snapshot-property.run_id", "ser-1")
        .overwritePartitions()
    )
    assert _latest_summary(spark, table)["run_id"] == "ser-1"
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == 2


def test_isolation_snapshot_overlap_succeeds(spark: ReparkSession) -> None:
    """P2-13: snapshot isolation lets the overlapping overwrite commit with the property."""
    _seed(spark, "iso_snap", partitioned=True)
    table = f"{CATALOG}.{NS}.iso_snap"
    (
        _frame(spark)
        .writeTo(table)
        .option("isolation-level", "snapshot")
        .option("snapshot-property.run_id", "snap-1")
        .overwritePartitions()
    )
    assert _latest_summary(spark, table)["run_id"] == "snap-1"


def test_isolation_append_ignored(spark: ReparkSession) -> None:
    """P2-12: isolation on a plain append is accepted and ignored, as Spark does."""
    _seed(spark, "iso_app")
    table = f"{CATALOG}.{NS}.iso_app"
    (_frame(spark).writeTo(table).option("isolation-level", "serializable").append())
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == 4


def test_isolation_bogus_refuses(spark: ReparkSession) -> None:
    """OPT-07: an unknown isolation level is refused like Spark's IllegalArgumentException."""
    _seed(spark, "iso_bad", partitioned=True)
    table = f"{CATALOG}.{NS}.iso_bad"
    with pytest.raises(AnalysisException, match="solation level"):
        (_frame(spark).writeTo(table).option("isolation-level", "bogus").overwritePartitions())
    assert "Invalid isolation level" in _fixture_cell("OPT-07-isolation-bad")["error"]["message"]


def test_check_options_accepted(spark: ReparkSession) -> None:
    """OPT-08/OPT-09/P2-06/P2-07: the check options commit the rows on both engines."""
    _seed(spark, "checks", partitioned=True)
    table = f"{CATALOG}.{NS}.checks"
    (
        _frame(spark)
        .writeTo(table)
        .option("check-nullability", "false")
        .option("check-ordering", "false")
        .append()
    )
    (_frame(spark).writeTo(table).option("check-ordering", "true").append())
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == 6


def test_unknown_option_ignored(spark: ReparkSession) -> None:
    """UNKNOWN-01: an unknown key commits and leaves no summary trace, as Spark does."""
    _seed(spark, "opt_unknown")
    table = f"{CATALOG}.{NS}.opt_unknown"
    _frame(spark).writeTo(table).option("repark-totally-unknown-key", "zzz").append()
    summary = _latest_summary(spark, table)
    assert "repark-totally-unknown-key" not in summary
    assert "repark-totally-unknown-key" not in _fixture_cell("UNKNOWN-01")["summary"]
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == 4


def test_prop_key_case_lowered(spark: ReparkSession) -> None:
    """P2-16: Spark lower-cases the suffix; RePark answers the same key."""
    _seed(spark, "prop_case")
    table = f"{CATALOG}.{NS}.prop_case"
    _frame(spark).writeTo(table).option("SNAPSHOT-PROPERTY.UPPER_KEY", "v").append()
    summary = _latest_summary(spark, table)
    assert summary.get("upper_key") == "v"
    assert "upper_key" in _fixture_cell("P2-16-prop-case")["summary"]


def test_empty_suffix_key(spark: ReparkSession) -> None:
    """P2-18: Spark commits the empty suffix as an empty-string summary key."""
    _seed(spark, "empty_key")
    table = f"{CATALOG}.{NS}.empty_key"
    _frame(spark).writeTo(table).option("snapshot-property.", "v").append()
    assert _latest_summary(spark, table).get("") == "v"
    assert "" in _fixture_cell("P2-18-empty-suffix")["summary"]


def test_overwrite_condition_refusal_with_options(spark: ReparkSession) -> None:
    """P2-14: conditional overwrite stays a loud refusal; options never reach a commit."""
    from repark import functions as fns

    _seed(spark, "cond")
    table = f"{CATALOG}.{NS}.cond"
    with pytest.raises(UnsupportedOperationException, match="overwrite"):
        (
            _frame(spark)
            .writeTo(table)
            .option("snapshot-property.run_id", "cond-1")
            .overwrite(fns.col("id") < 100)
        )
    assert _snapshot_count(spark, table) == 1


def test_v1_append_mode_property(spark: ReparkSession) -> None:
    """P2-19: V1 saveAsTable in append mode carries the property."""
    _seed(spark, "v1_app")
    table = f"{CATALOG}.{NS}.v1_app"
    (
        _frame(spark)
        .write.format("iceberg")
        .option("snapshot-property.run_id", "v1-app-1")
        .mode("append")
        .saveAsTable(table)
    )
    assert _latest_summary(spark, table)["run_id"] == "v1-app-1"


def test_sql_insert_carries_no_property(spark: ReparkSession) -> None:
    """SQL-00/SQL-01: no session conf reaches the SQL-door summary (C-006 measurement)."""
    _seed(spark, "sql_door")
    table = f"{CATALOG}.{NS}.sql_door"
    spark.sql(f"INSERT INTO {table} SELECT * FROM (VALUES (9, 'name-9')) AS t(id, name)")
    summary = _latest_summary(spark, table)
    assert "run_id" not in summary
    assert "run_id" not in _fixture_cell("SQL-01-set-conf")["summary"]


def test_no_option_warning(spark: ReparkSession) -> None:
    """C-005: honoured options warn no one; the process-once UserWarning is gone."""
    _seed(spark, "no_warn")
    table = f"{CATALOG}.{NS}.no_warn"
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        (
            _frame(spark)
            .writeTo(table)
            .option("snapshot-property.run_id", "w-1")
            .option("repark-totally-unknown-key", "zzz")
            .append()
        )
    assert _latest_summary(spark, table)["run_id"] == "w-1"


def test_empty_append_with_props_stamps_summary(spark: ReparkSession) -> None:
    """An option-carrying empty append still stamps the properties (Java BatchAppend)."""
    _seed(spark, "empty_app")
    table = f"{CATALOG}.{NS}.empty_app"
    empty = spark.sql("SELECT * FROM (VALUES (0, 'x')) AS t(id, name) WHERE false")
    before = _snapshot_count(spark, table)
    empty.writeTo(table).option("snapshot-property.run_id", "empty-1").append()
    assert _snapshot_count(spark, table) == before + 1
    assert _latest_summary(spark, table)["run_id"] == "empty-1"


def _stable_projection(cell: dict[str, Any]) -> dict[str, Any]:
    summary = {
        key: value for key, value in cell.get("summary", {}).items() if key not in _SPARK_APP_KEYS
    }
    files = [
        (entry["suffix"], entry["file_format"], entry["records"]) for entry in cell.get("files", [])
    ]
    error = cell.get("error")
    return {
        "summary": summary,
        "files": sorted(files),
        "snapshot_count": cell.get("snapshot_count"),
        "error_class": error["class"] if error else None,
        "collision": {field: cell.get(field) for field in _COLLISION_FIELDS},
        "collision_message": error["message"] if error and cell["id"] in _collision_ids() else None,
    }


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_cells_reproduce_fixture(tmp_path: Path) -> None:
    """Live tier: re-run the record drivers and check the fixture cell by cell."""
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

    assert "4.1_2.13:1.11.0" in ICEBERG_SPARK_RUNTIME_GAV
    out = tmp_path / "live.json"
    env = dict(os.environ)
    for script in (
        "_record_ice_write_options_1_oracle.py",
        "_record_ice_write_options_2_oracle.py",
        "_record_ice_write_options_3_oracle.py",
    ):
        proc = subprocess.run(
            [sys.executable, str(Path(__file__).resolve().parent / script), "--out", str(out)],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parents[3]),
            env=env,
            timeout=1200,
        )
        assert proc.returncode == 0, proc.stderr[-3000:]
    recorded = {cell["id"]: cell for cell in json.loads(out.read_text(encoding="utf-8"))["cells"]}
    expected = {
        cell["id"]: cell for cell in json.loads(_FIXTURE.read_text(encoding="utf-8"))["cells"]
    }
    assert set(recorded) == set(expected)
    for cell_id, want in expected.items():
        assert _stable_projection(recorded[cell_id]) == _stable_projection(want), cell_id


def test_snapshot_property_added_records_refuses(spark: ReparkSession) -> None:
    """SNAP-10: a colliding engine metric key refuses like Spark (Q-20c-5)."""
    _seed(spark, "snap_reserved")
    table = f"{CATALOG}.{NS}.snap_reserved"
    before = _snapshot_count(spark, table)
    with pytest.raises(IllegalArgumentException, match="Multiple entries with same key"):
        (_frame(spark).writeTo(table).option("snapshot-property.added-records", "999").append())
    assert _snapshot_count(spark, table) == before


def test_snapshot_property_operation_dropped(spark: ReparkSession) -> None:
    """SNAP-11: a user operation never replaces the engine value (Q-20c-5)."""
    _seed(spark, "snap_opdrop")
    table = f"{CATALOG}.{NS}.snap_opdrop"
    (_frame(spark).writeTo(table).option("snapshot-property.operation", "stolen-op").append())
    assert _latest_summary(spark, table).get("operation") != "stolen-op"


def test_snapshot_property_engine_operation_id_ours(spark: ReparkSession) -> None:
    """SNAP-12: engine.operation-id always carries the engine UUID (Q-20c-5)."""
    _seed(spark, "snap_opid")
    table = f"{CATALOG}.{NS}.snap_opid"
    (
        _frame(spark)
        .writeTo(table)
        .option("snapshot-property.engine.operation-id", "stolen-id")
        .append()
    )
    assert _latest_summary(spark, table)["engine.operation-id"] != "stolen-id"


def _channel(spark: ReparkSession, sql: str, options: dict[str, str]) -> None:
    _native.session_sql_with_write_options(spark._inner, sql, options)


def test_merge_refuses_write_options(spark: ReparkSession) -> None:
    """V-03: MERGE refuses a non-empty options map instead of dropping it (Q-21c-5)."""
    _seed(spark, "merge_opt")
    table = f"{CATALOG}.{NS}.merge_opt"
    before = _snapshot_count(spark, table)
    with pytest.raises(AnalysisException, match="MERGE INTO does not support write options"):
        _channel(
            spark,
            f"MERGE INTO {table} AS t USING (SELECT 9 AS id, 'name-9' AS name) AS s "
            "ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
            {"snapshot-property.run_id": "merge-1"},
        )
    assert _snapshot_count(spark, table) == before


def test_insert_by_name_append_refuses_write_options(spark: ReparkSession) -> None:
    """V-03: an INSERT ... BY NAME append refuses a non-empty options map (Q-21c-5)."""
    _seed(spark, "byname_app")
    table = f"{CATALOG}.{NS}.byname_app"
    before = _snapshot_count(spark, table)
    with pytest.raises(AnalysisException, match="BY NAME does not support write options"):
        _channel(
            spark,
            f"INSERT INTO {table} BY NAME SELECT 'name-9' AS name, 9 AS id",
            {"snapshot-property.run_id": "byname-1"},
        )
    assert _snapshot_count(spark, table) == before


def test_insert_by_name_overwrite_honours_write_options(spark: ReparkSession) -> None:
    """V-03: an INSERT OVERWRITE ... BY NAME carries the snapshot property it was given."""
    _seed(spark, "byname_ow")
    table = f"{CATALOG}.{NS}.byname_ow"
    _channel(
        spark,
        f"INSERT OVERWRITE {table} BY NAME SELECT 'name-9' AS name, 9 AS id",
        {"snapshot-property.run_id": "byname-ow-1"},
    )
    assert _latest_summary(spark, table)["run_id"] == "byname-ow-1"


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM {t}",
        "DELETE FROM {t} WHERE id = 0",
        "UPDATE {t} SET name = 'x' WHERE id = 0",
        "TRUNCATE TABLE {t}",
        "ALTER TABLE {t} SET TBLPROPERTIES ('k' = 'v')",
        "ALTER TABLE {t} ADD PARTITION FIELD id",
        "ALTER TABLE {t} ADD COLUMN s.z INT",
        "DROP TABLE {t}",
        "DROP NAMESPACE {c}.{n}",
        "CREATE NAMESPACE {c}.other_ns",
        "CALL {c}.system.expire_snapshots(table => '{n}.arms')",
    ],
)
def test_non_write_arms_refuse_write_options(spark: ReparkSession, statement: str) -> None:
    """V-03 sweep: every router arm that cannot honour the options map refuses it."""
    _seed(spark, "arms")
    table = f"{CATALOG}.{NS}.arms"
    before = _snapshot_count(spark, table)
    with pytest.raises(AnalysisException, match="does not support write options"):
        _channel(
            spark,
            statement.format(t=table, c=CATALOG, n=NS),
            {"snapshot-property.run_id": "arm-1"},
        )
    assert _snapshot_count(spark, table) == before
    assert spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()[0]["n"] == 2
    namespaces = spark.sql(f"SHOW NAMESPACES IN {CATALOG}").to_arrow().to_pylist()
    assert all("other_ns" not in str(row) for row in namespaces)


def _non_iceberg_view(spark: ReparkSession, name: str) -> list[dict[str, Any]]:
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).createOrReplaceTempView(name)
    return spark.sql(f"SELECT * FROM {name} ORDER BY id").to_arrow().to_pylist()


@pytest.mark.parametrize(
    ("statement", "context"),
    [
        (
            "INSERT OVERWRITE {v} SELECT 9 AS id, 'z' AS name WHERE false",
            "INSERT OVERWRITE on a non-Iceberg target does not support write options",
        ),
        (
            "INSERT OVERWRITE {v} DEFAULT VALUES",
            "INSERT OVERWRITE without a source does not support write options",
        ),
    ],
)
def test_non_iceberg_overwrite_refuses_write_options(
    spark: ReparkSession, statement: str, context: str
) -> None:
    """V-03: the empty-source wipe and no-source passthrough refuse on a non-Iceberg target."""
    view = "ow_non_iceberg"
    before = _non_iceberg_view(spark, view)
    with pytest.raises(AnalysisException, match=context):
        _channel(spark, statement.format(v=view), {"snapshot-property.run_id": "ow-1"})
    assert spark.sql(f"SELECT * FROM {view} ORDER BY id").to_arrow().to_pylist() == before


def _collision_ids() -> set[str]:
    cells = json.loads(_FIXTURE.read_text(encoding="utf-8"))["cells"]
    return {cell["id"] for cell in cells if cell["id"].startswith(_COLLISION_CELLS)}


@pytest.mark.parametrize("cell_id", sorted(_collision_ids()))
def test_snapshot_property_collision_cells(spark: ReparkSession, cell_id: str) -> None:
    """V-04: a user extra refuses iff the engine computed that key for this append (COLL-*)."""
    cell = _fixture_cell(cell_id)
    key, value = cell["key"], cell["value"]
    table = f"{CATALOG}.{NS}.coll_{cell_id[5:7]}"
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1)")
    assert _snapshot_count(spark, table) == cell["snapshots_before"]
    writer = spark.range(2, 4).toDF("id").writeTo(table).option(f"snapshot-property.{key}", value)
    if cell["error"] is not None:
        with pytest.raises(IllegalArgumentException) as refused:
            writer.append()
        message = str(refused.value)
        if cell_id in _ENGINE_RESERVED_CELLS:
            head, _, tail = cell["error"]["message"].partition(" and ")
            assert head.rsplit("=", 1)[0] + "=" in message
            assert f" and {tail}" in message
        else:
            assert cell["error"]["message"] in message
        assert _snapshot_count(spark, table) == cell["snapshot_count"]
        return
    writer.append()
    assert _snapshot_count(spark, table) == cell["snapshot_count"]
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().to_pylist()
    assert int(rows[0]["n"]) == cell["rows"]
    landed = _latest_summary(spark, table).get(key)
    if key == "engine.operation-id":
        assert cell["summary_value_for_key"] == value
        assert landed not in (None, value)
    else:
        assert landed == cell["summary_value_for_key"]


def test_table_level_gzip_with_option_codec_refuses(spark: ReparkSession, tmp_path: Path) -> None:
    """V-02: table compression-level plus option gzip refuses before any file (L-04 twin)."""
    _seed(spark, "gzip_table_level")
    table = f"{CATALOG}.{NS}.gzip_table_level"
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.parquet.compression-level' = '1')")
    before = _snapshot_count(spark, table)
    files_before = sorted(tmp_path.rglob("*.parquet"))
    with pytest.raises(AnalysisException, match="compression-level"):
        _frame(spark).writeTo(table).option("compression-codec", "gzip").append()
    assert _snapshot_count(spark, table) == before
    assert sorted(tmp_path.rglob("*.parquet")) == files_before
