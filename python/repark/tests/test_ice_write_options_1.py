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

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException

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


def test_write_format_orc_refuses(spark: ReparkSession) -> None:
    """FORMAT-02: orc has no RePark writer — a typed refusal naming the registry row."""
    _seed(spark, "fmt_orc")
    table = f"{CATALOG}.{NS}.fmt_orc"
    with pytest.raises(UnsupportedOperationException, match="ICE-WRITE-OPTIONS-1"):
        _frame(spark).writeTo(table).option("write-format", "orc").append()
    assert _snapshot_count(spark, table) == 1


def test_write_format_avro_refuses(spark: ReparkSession) -> None:
    """FORMAT-03: avro has no RePark writer — a typed refusal naming the registry row."""
    _seed(spark, "fmt_avro")
    table = f"{CATALOG}.{NS}.fmt_avro"
    with pytest.raises(UnsupportedOperationException, match="ICE-WRITE-OPTIONS-1"):
        _frame(spark).writeTo(table).option("write-format", "avro").append()
    assert _snapshot_count(spark, table) == 1


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
