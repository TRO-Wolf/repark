"""Facade pins for DESCRIBE [TABLE] [EXTENDED|FORMATTED] on Iceberg tables."""

from __future__ import annotations

import os
import re
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
    assert table.schema == pa.schema(
        [
            pa.field("col_name", pa.string(), nullable=False),
            pa.field("data_type", pa.string(), nullable=False),
            pa.field("comment", pa.string(), nullable=True),
        ]
    )
    assert _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.{TABLE}") == PLAIN_ROWS
    assert _rows(spark, f"DESCRIBE TABLE {CATALOG}.{NAMESPACE}.{TABLE}") == PLAIN_ROWS


def test_describe_table_extended_sections(spark: ReparkSession) -> None:
    """D-2: EXTENDED adds metadata and detail sections in Spark order."""
    rows = _rows(spark, f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.{TABLE}")
    assert rows[: len(EXTENDED_HEAD)] == EXTENDED_HEAD
    detail = rows[len(EXTENDED_HEAD) :]
    assert len(detail) == 7
    assert detail[0:2] == [
        ("Name", f"{CATALOG}.{NAMESPACE}.{TABLE}", ""),
        ("Type", "MANAGED", ""),
    ]
    assert re.fullmatch(r"(?:file:)?/.+/dsns1/t1", detail[2][1])
    assert detail[2] == ("Location", detail[2][1], "")
    assert detail[3:] == [
        ("Provider", "iceberg", ""),
        ("Owner", "unknown", ""),
        (
            "Table Properties",
            "[current-snapshot-id=none,format=iceberg/parquet,format-version=2,k=v,"
            "write.parquet.compression-codec=zstd]",
            "",
        ),
        ("Statistics", "0 bytes, 0 rows", None),
    ]


def test_describe_formatted_is_extended(spark: ReparkSession) -> None:
    """D-6: FORMATTED output is byte-identical to EXTENDED."""
    assert _rows(spark, f"DESCRIBE TABLE FORMATTED {CATALOG}.{NAMESPACE}.{TABLE}") == _rows(
        spark, f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.{TABLE}"
    )


def test_describe_missing_table_analysis_exception(spark: ReparkSession) -> None:
    """D-4: a missing table raises AnalysisException with Spark's condition text."""
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE TABLE {CATALOG}.{NAMESPACE}.no_such_table")
    assert excinfo.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert excinfo.value.getSqlState() == "42P01"
    assert str(excinfo.value) == (
        "Error during planning: "
        f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{CATALOG}`.`{NAMESPACE}`."
        "`no_such_table` cannot be found. Verify the spelling and correctness of the schema and "
        "catalog. If you did not qualify the name with a schema, verify the current_schema() "
        "output, or qualify the name with the correct schema and catalog. To tolerate the error "
        "on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    )


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
    assert properties == (
        "[current-snapshot-id=none,format=iceberg/parquet,format-version=2,k=v,"
        "secret_token=*********(redacted),write.parquet.compression-codec=zstd]"
    )


def test_describe_table_statistics_counts_written_rows(spark: ReparkSession) -> None:
    """Statistics and snapshot id move with the current snapshot after a write."""
    spark.sql(
        f"INSERT INTO {CATALOG}.{NAMESPACE}.{TABLE} VALUES "
        "(1, 'a', TIMESTAMP '2026-01-01 00:00:00'), (2, 'b', TIMESTAMP '2026-01-02 00:00:00')"
    )
    extended = _rows(spark, f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.{TABLE}")
    statistics = next(value for name, value, _ in extended if name == "Statistics")
    assert re.fullmatch(r"[0-9]+ bytes, 2 rows", statistics)
    properties = next(value for name, value, _ in extended if name == "Table Properties")
    assert re.fullmatch(
        r"\[current-snapshot-id=[0-9]+,format=iceberg/parquet,format-version=2,"
        r"write.parquet.compression-codec=zstd\]",
        properties,
    )


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

    oracle = live_parity.build_spark_iceberg_engine(tmp_path / "spark-wh")
    session = oracle.session
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
    assert re.fullmatch(r"(?:file:)?/.+/dsns1/t1", repark_extended[base + 2][1])
    assert re.fullmatch(r"(?:file:)?/.+/dsns1/t1", live_extended[base + 2][1])
    assert repark_extended[base + 4] == ("Owner", "unknown", "")
    assert live_extended[base + 4][0] == "Owner"
    assert re.fullmatch(r"[A-Za-z0-9_.-]+", live_extended[base + 4][1])
    assert live_extended[base + 4][2] == ""
    expected_properties = (
        "Table Properties",
        "[current-snapshot-id=none,format=iceberg/parquet,format-version=2,k=v,"
        "write.parquet.compression-codec=zstd]",
        "",
    )
    assert repark_extended[base + 5] == expected_properties
    assert live_extended[base + 5] == expected_properties
