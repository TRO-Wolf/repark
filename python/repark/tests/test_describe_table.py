"""Facade pins for DESCRIBE [TABLE] [EXTENDED|FORMATTED] on Iceberg tables."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live describe oracle is skipped (CI is JVM-free)"

CATALOG = "mem"
NAMESPACE = "dsns1"
TABLE = "t1"

PLAIN_ROWS: list[tuple[str, str, str | None]] = [
    ("id", "bigint", "the row identifier"),
    ("name", "string", None),
    ("ts", "timestamp", None),
    ("", "", ""),
    ("# Partitioning", "", ""),
    ("Part 0", "days(ts)", ""),
]

EXTENDED_HEAD: list[tuple[str, str, str | None]] = [
    *PLAIN_ROWS,
    ("", "", ""),
    ("# Metadata Columns", "", ""),
    ("_spec_id", "int", ""),
    ("_partition", "struct<ts_day:date>", ""),
    ("_file", "string", ""),
    ("_pos", "bigint", ""),
    ("_deleted", "boolean", ""),
    ("", "", ""),
    ("# Detailed Table Information", "", ""),
]


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-describe-table").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    session.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.{TABLE} (name STRING, ts TIMESTAMP) "
        "USING iceberg PARTITIONED BY (days(ts)) TBLPROPERTIES ('k'='v')"
    )
    session.sql(
        f"ALTER TABLE {CATALOG}.{NAMESPACE}.{TABLE} "
        "ADD COLUMN id BIGINT COMMENT 'the row identifier' FIRST"
    )
    return session


def _rows(spark: ReparkSession, sql: str) -> list[tuple[str, str, str | None]]:
    table = spark.sql(sql).to_arrow()
    columns = [table.column(name).to_pylist() for name in table.schema.names]
    return list(zip(*columns, strict=True))


def test_describe_table_plain_matches_spark_rows(spark: ReparkSession) -> None:
    """D-2: plain DESCRIBE returns the step-1 capture rows with Spark nullability."""
    table = spark.sql(f"DESCRIBE {CATALOG}.{NAMESPACE}.{TABLE}").to_arrow()
    assert table.schema.names == ["col_name", "data_type", "comment"]
    assert [field.type for field in table.schema] == [pa.string()] * 3
    assert [field.nullable for field in table.schema] == [False, False, True]
    assert _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.{TABLE}") == PLAIN_ROWS
    assert _rows(spark, f"DESCRIBE TABLE {CATALOG}.{NAMESPACE}.{TABLE}") == PLAIN_ROWS


def test_describe_table_extended_sections(spark: ReparkSession) -> None:
    """D-2: EXTENDED adds metadata and detail sections in Spark order."""
    rows = _rows(spark, f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.{TABLE}")
    assert rows[: len(EXTENDED_HEAD)] == EXTENDED_HEAD
    detail = rows[len(EXTENDED_HEAD) :]
    assert detail[0] == ("Name", f"{CATALOG}.{NAMESPACE}.{TABLE}", "")
    assert detail[1] == ("Type", "MANAGED", "")
    assert detail[2][0] == "Location"
    assert detail[2][1].endswith(f"/{NAMESPACE}/{TABLE}")
    assert detail[2][2] == ""
    assert detail[3] == ("Provider", "iceberg", "")
    assert detail[4][0] == "Owner"
    assert detail[4][1] != ""
    assert detail[4][2] == ""
    assert detail[5] == ("Table Properties", "[current-snapshot-id=none,k=v]", "")
    assert detail[6] == ("Statistics", "0 bytes, 0 rows", None)


def test_describe_formatted_is_extended(spark: ReparkSession) -> None:
    """D-6: FORMATTED output is byte-identical to EXTENDED."""
    assert _rows(spark, f"DESCRIBE TABLE FORMATTED {CATALOG}.{NAMESPACE}.{TABLE}") == _rows(
        spark, f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.{TABLE}"
    )


def test_describe_missing_table_analysis_exception(spark: ReparkSession) -> None:
    """D-4: a missing table raises AnalysisException with Spark's condition text."""
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE TABLE {CATALOG}.{NAMESPACE}.no_such_table")
    assert "[TABLE_OR_VIEW_NOT_FOUND]" in str(excinfo.value)
    assert f"`{CATALOG}`.`{NAMESPACE}`.`no_such_table`" in str(excinfo.value)


def test_describe_temp_view_falls_through(spark: ReparkSession) -> None:
    """D-1: DESCRIBE of a temp view keeps DataFusion's shape, not the table path."""
    spark.sql("SELECT 1 AS a, 'x' AS b").createOrReplaceTempView("src_view")
    described = spark.sql("DESCRIBE src_view").to_arrow()
    assert described.schema.names[0] != "col_name"
    assert described.num_rows == 2


def test_describe_table_properties_redacted(spark: ReparkSession) -> None:
    """D-5: secret table properties redact in Table Properties and leak nothing."""
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.creds (id BIGINT) USING iceberg")
    spark.sql(
        f"ALTER TABLE {CATALOG}.{NAMESPACE}.creds "
        "SET TBLPROPERTIES ('secret_token' = 'hunter2', 'k' = 'v')"
    )
    extended = _rows(spark, f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.creds")
    properties = next(value for name, value, _ in extended if name == "Table Properties")
    assert "*********(redacted)" in properties
    assert "hunter2" not in properties
    assert "k=v" in properties


def test_describe_table_statistics_counts_written_rows(spark: ReparkSession) -> None:
    """Statistics and snapshot id move with the current snapshot after a write."""
    spark.sql(
        f"INSERT INTO {CATALOG}.{NAMESPACE}.{TABLE} VALUES "
        "(1, 'a', TIMESTAMP '2026-01-01 00:00:00'), (2, 'b', TIMESTAMP '2026-01-02 00:00:00')"
    )
    extended = _rows(spark, f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.{TABLE}")
    statistics = next(value for name, value, _ in extended if name == "Statistics")
    assert statistics.endswith("2 rows")
    assert not statistics.startswith("0 bytes")
    properties = next(value for name, value, _ in extended if name == "Table Properties")
    assert "current-snapshot-id=none" not in properties


def test_describe_table_show_truncate_false_prints(
    spark: ReparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    """Owner call: show(truncate=False) prints the full table with untruncated cells."""
    spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.{TABLE}").show(truncate=False)
    captured = capsys.readouterr().out
    assert "col_name" in captured
    assert "the row identifier" in captured
    assert "# Detailed Table Information" in captured
    assert "|" in captured


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_describe_table_live_matches_capture_and_repark(tmp_path: Path) -> None:
    """Live leg: the step-1 capture re-measures equal; repark matches it row for row."""
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    owned = SparkSession.getActiveSession() is None
    oracle = live_parity.build_spark_iceberg_engine(tmp_path / "spark-wh")
    session = oracle.session
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS local.dsns1")
        session.sql(
            "CREATE TABLE local.dsns1.t1 "
            "(id BIGINT COMMENT 'the row identifier', name STRING, ts TIMESTAMP) "
            "USING iceberg PARTITIONED BY (days(ts)) TBLPROPERTIES ('k'='v')"
        )
        live_plain = [tuple(row) for row in session.sql("DESCRIBE local.dsns1.t1").collect()]
        live_schema = session.sql("DESCRIBE local.dsns1.t1").schema
        live_extended = [
            tuple(row) for row in session.sql("DESCRIBE TABLE EXTENDED local.dsns1.t1").collect()
        ]
        live_formatted = [
            tuple(row) for row in session.sql("DESCRIBE TABLE FORMATTED local.dsns1.t1").collect()
        ]
    finally:
        if owned:
            session.stop()

    assert [field.nullable for field in live_schema] == [False, False, True]
    assert live_plain == PLAIN_ROWS
    assert live_formatted == live_extended
    assert live_extended[: len(EXTENDED_HEAD)] == EXTENDED_HEAD

    engine = ReparkSession.builder.appName("pytest-describe-table-live").getOrCreate()
    engine.register_memory_catalog("livecheck", tmp_path / "repark-wh")
    try:
        engine.sql("CREATE NAMESPACE livecheck.dsns1")
        engine.sql(
            "CREATE TABLE livecheck.dsns1.t1 (name STRING, ts TIMESTAMP) "
            "USING iceberg PARTITIONED BY (days(ts)) TBLPROPERTIES ('k'='v')"
        )
        engine.sql(
            "ALTER TABLE livecheck.dsns1.t1 ADD COLUMN id BIGINT COMMENT 'the row identifier' FIRST"
        )
        repark_extended = _rows(engine, "DESCRIBE TABLE EXTENDED livecheck.dsns1.t1")
    finally:
        engine.stop()

    assert repark_extended[: len(EXTENDED_HEAD)] == live_extended[: len(EXTENDED_HEAD)]
    base = len(EXTENDED_HEAD)
    assert repark_extended[base] == ("Name", "livecheck.dsns1.t1", "")
    assert live_extended[base] == ("Name", "local.dsns1.t1", "")
    assert repark_extended[base + 1] == ("Type", "MANAGED", "")
    assert live_extended[base + 1] == ("Type", "MANAGED", "")
    assert repark_extended[base + 3] == ("Provider", "iceberg", "")
    assert live_extended[base + 3] == ("Provider", "iceberg", "")
    assert repark_extended[base + 6] == ("Statistics", "0 bytes, 0 rows", None)
    assert live_extended[base + 6] == ("Statistics", "0 bytes, 0 rows", None)
    for offset in (2, 4):
        assert repark_extended[base + offset][1] != ""
        assert live_extended[base + offset][1] != ""
    assert "k=v" in repark_extended[base + 5][1]
    assert "current-snapshot-id=none" in repark_extended[base + 5][1]
    assert "k=v" in live_extended[base + 5][1]
    assert "current-snapshot-id=none" in live_extended[base + 5][1]
