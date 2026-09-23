"""Facade pins for DESCRIBE [TABLE] [EXTENDED|FORMATTED] on Iceberg tables."""

from __future__ import annotations

import os
import re
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException

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
    """Return the Arrow rows for one DESCRIBE statement."""
    table = spark.sql(sql).to_arrow()
    columns = [table.column(name).to_pylist() for name in table.schema.names]
    return list(zip(*columns, strict=True))


def _create_describe_error_table(spark: ReparkSession) -> str:
    """Create the measured DESCRIBE error fixture table."""
    table = "describe_errors"
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.{table} "
        "(id BIGINT, s STRUCT<a: INT>, `we ird` STRING) USING iceberg"
    )
    return table


def _assert_describe_parse_error(
    spark: ReparkSession,
    sql: str,
    condition: str,
    sqlstate: str,
    message: str,
) -> None:
    """Assert one Spark-shaped DESCRIBE parse error."""
    with pytest.raises(ParseException) as raised:
        spark.sql(sql)
    assert raised.value.getCondition() == condition
    assert raised.value.getSqlState() == sqlstate
    assert str(raised.value) == message


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


def test_describe_identity_partition_uses_spark_partition_information(
    spark: ReparkSession,
) -> None:
    identity_table = "identity_partition"
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.{identity_table} (id BIGINT, data STRING) "
        "USING iceberg PARTITIONED BY (data)"
    )
    expected = [
        ("id", "bigint", None),
        ("data", "string", None),
        ("# Partition Information", "", ""),
        ("# col_name", "data_type", "comment"),
        ("data", "string", None),
    ]
    plain = _rows(spark, f"DESCRIBE TABLE {CATALOG}.{NAMESPACE}.{identity_table}")
    extended = _rows(spark, f"DESCRIBE TABLE EXTENDED {CATALOG}.{NAMESPACE}.{identity_table}")
    assert plain == expected
    assert extended[: len(expected)] == expected


def test_describe_table_column_matches_spark_rows(spark: ReparkSession) -> None:
    """DESCRIBE TABLE column returns Spark's non-null info-name/value rows."""
    table = spark.sql(f"DESCRIBE TABLE {CATALOG}.{NAMESPACE}.{TABLE} id").to_arrow()
    assert table.schema.names == ["info_name", "info_value"]
    assert [field.type for field in table.schema] == [pa.string()] * 2
    assert [field.nullable for field in table.schema] == [False, False]
    assert _rows(spark, f"DESCRIBE TABLE {CATALOG}.{NAMESPACE}.{TABLE} id") == [
        ("col_name", "id"),
        ("data_type", "bigint"),
        ("comment", "the row identifier"),
    ]


def test_describe_table_column_number_is_parse_exception(spark: ReparkSession) -> None:
    """A numeric column tail keeps Spark's parse error class and text."""
    table = _create_describe_error_table(spark)
    _assert_describe_parse_error(
        spark,
        f"DESCRIBE {CATALOG}.{NAMESPACE}.{table} 1",
        "PARSE_SYNTAX_ERROR",
        "42601",
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '1': extra input '1'. SQLSTATE: 42601",
    )


def test_describe_table_column_trailing_word_is_parse_exception(spark: ReparkSession) -> None:
    """A trailing column word keeps Spark's parse error class and text."""
    table = _create_describe_error_table(spark)
    _assert_describe_parse_error(
        spark,
        f"DESCRIBE {CATALOG}.{NAMESPACE}.{table} id extra",
        "PARSE_SYNTAX_ERROR",
        "42601",
        "[PARSE_SYNTAX_ERROR] Syntax error at or near 'extra': extra input 'extra'. "
        "SQLSTATE: 42601",
    )


def test_describe_table_column_unclosed_quote_is_parse_exception(spark: ReparkSession) -> None:
    """An unclosed column quote keeps Spark's lexer parse error class and text."""
    table = _create_describe_error_table(spark)
    _assert_describe_parse_error(
        spark,
        f"DESCRIBE {CATALOG}.{NAMESPACE}.{table} 'id",
        "PARSE_SYNTAX_ERROR",
        "42601",
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601",
    )


@pytest.mark.parametrize(
    "_case_id,sql,near",
    [
        ("d_blk_lead_unclosed_quote", "/* c */ DESCRIBE sc.sales.pc 'x", "'"),
        ("d_line_lead_unclosed_quote", "-- c\nDESCRIBE sc.sales.pc 'x", "'"),
        ("d_blk_mid_unclosed_quote", "DESCRIBE /* c */ sc.sales.pc 'x", "'"),
        ("d_plain_unclosed_quote", "DESCRIBE sc.sales.pc 'x", "'"),
        ("d_blk_apos_unclosed_quote", "DESCRIBE sc.sales.pc /* it's */ 'x", "'"),
        ("d_blk_apos_lead_unclosed_quote", "/* it's */ DESCRIBE sc.sales.pc 'x", "'"),
        ("d_line_apos_unclosed_bt", "DESCRIBE sc.sales.pc -- it's\n`x", "`"),
        ("d_blk_lead_unclosed_dquote", '/* c */ DESC sc.sales.pc "x', '"'),
    ],
    ids=lambda value: value,
)
def test_describe_comment_prefix_unclosed_quotes_keep_parse_messages(
    spark: ReparkSession, _case_id: str, sql: str, near: str
) -> None:
    """Comment markers do not hide an unclosed DESCRIBE table quote."""
    _assert_describe_parse_error(
        spark,
        sql,
        "PARSE_SYNTAX_ERROR",
        "42601",
        f"[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601",
    )


@pytest.mark.parametrize(
    "sql",
    [
        "/* c */ DESCRIBE mem.dsns1.t1",
        "-- c\nDESCRIBE mem.dsns1.t1",
        "DESCRIBE /* c */ mem.dsns1.t1",
        "/* ' */ DESCRIBE mem.dsns1.t1",
    ],
    ids=["d_blk_lead_ok", "d_line_lead_ok", "d_blk_mid_ok", "d_blk_quote_in_comment_only"],
)
def test_describe_comments_keep_valid_table_answers(spark: ReparkSession, sql: str) -> None:
    """Comment prefixes and quote-only comments keep valid table rows."""
    assert _rows(spark, sql) == PLAIN_ROWS


def test_describe_namespace_unclosed_quote_keeps_non_table_path(spark: ReparkSession) -> None:
    """An unclosed quote after NAMESPACE does not enter the table error path."""
    with pytest.raises(ParseException) as raised:
        spark.sql("/* c */ DESCRIBE NAMESPACE mem.dsns1 'x")
    assert str(raised.value) == (
        'SQL error: TokenizerError("Unterminated string literal at Line: 1, Column: 38")'
    )


def test_describe_unclosed_comment_keeps_tokenizer_outcome(spark: ReparkSession) -> None:
    """An unclosed block comment stays with the tokenizer error path."""
    with pytest.raises(ParseException) as raised:
        spark.sql("/* c DESCRIBE mem.dsns1.t1 'x")
    assert str(raised.value) == (
        'SQL error: TokenizerError("Unexpected EOF while in a multi-line comment at Line: 1, '
        'Column: 30")'
    )


def test_describe_table_column_partition_is_parse_exception(spark: ReparkSession) -> None:
    """A partition column clause keeps Spark's unsupported parse error."""
    table = _create_describe_error_table(spark)
    _assert_describe_parse_error(
        spark,
        f"DESCRIBE {CATALOG}.{NAMESPACE}.{table} PARTITION (id=1) id",
        "UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_PARTITION",
        "0A000",
        "[UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_PARTITION] The feature is not supported: "
        "DESC TABLE COLUMN for a specific partition. SQLSTATE: 0A000",
    )


def test_describe_table_unclosed_table_quote_is_parse_exception(spark: ReparkSession) -> None:
    """An unclosed table quote keeps Spark's lexer parse error class and text."""
    _assert_describe_parse_error(
        spark,
        f"DESCRIBE '{CATALOG}.{NAMESPACE}.{TABLE}",
        "PARSE_SYNTAX_ERROR",
        "42601",
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601",
    )


def test_describe_table_backtick_column_matches_spark_rows(spark: ReparkSession) -> None:
    """A backticked column keeps its embedded space in DESCRIBE rows."""
    table = _create_describe_error_table(spark)
    assert _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.{table} `we ird`") == [
        ("col_name", "we ird"),
        ("data_type", "string"),
        ("comment", "NULL"),
    ]


def test_describe_table_extended_column_matches_plain_rows(spark: ReparkSession) -> None:
    """EXTENDED DESCRIBE with a column keeps the three column rows only."""
    table = _create_describe_error_table(spark)
    assert _rows(spark, f"DESCRIBE EXTENDED {CATALOG}.{NAMESPACE}.{table} id") == [
        ("col_name", "id"),
        ("data_type", "bigint"),
        ("comment", "NULL"),
    ]


def test_describe_table_column_bare_name_uses_default_namespace(spark: ReparkSession) -> None:
    """A bare DESCRIBE table name plus column resolves through session defaults."""
    spark.sql(f"USE {CATALOG}.{NAMESPACE}").to_arrow()
    assert _rows(spark, f"DESCRIBE TABLE {TABLE} id") == [
        ("col_name", "id"),
        ("data_type", "bigint"),
        ("comment", "the row identifier"),
    ]


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
    assert detail[3] == ("Provider", "iceberg", "")
    assert detail[4][0] == "Owner"
    assert re.fullmatch(r"[A-Za-z0-9_.-]+", detail[4][1])
    assert detail[4][2] == ""
    assert detail[5:] == [
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
    assert re.fullmatch(r"[1-9][0-9]* bytes, 2 rows", statistics)
    properties = next(value for name, value, _ in extended if name == "Table Properties")
    assert re.fullmatch(
        r"\[current-snapshot-id=[0-9]+,format=iceberg/parquet,format-version=2,"
        r"k=v,write.parquet.compression-codec=zstd\]",
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
    assert repark_extended[base + 4][0] == "Owner"
    assert re.fullmatch(r"[A-Za-z0-9_.-]+", repark_extended[base + 4][1])
    assert repark_extended[base + 4][2] == ""
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
