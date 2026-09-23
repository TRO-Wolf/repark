from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException

CATALOG = "mem"
NAMESPACE = "scns1"
TABLE = "sc1"
QUALIFIED = f"{CATALOG}.{NAMESPACE}.{TABLE}"
INVALID_SHOW_CREATE_TABLE_MESSAGE = (
    "[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: SHOW CREATE TABLE is not valid. "
    "SQLSTATE: 42601"
)
INVALID_SHOW_CREATE_TABLE_RENDERED_MESSAGE = (
    f'SQL error: ParserError("{INVALID_SHOW_CREATE_TABLE_MESSAGE}")'
)
UNCLOSED_BRACKETED_COMMENT_MESSAGE = (
    "[UNCLOSED_BRACKETED_COMMENT] Found an unclosed bracketed comment. "
    "Please, append */ at the end of the comment. SQLSTATE: 42601"
)
UNCLOSED_BRACKETED_COMMENT_RENDERED_MESSAGE = (
    f'SQL error: ParserError("{UNCLOSED_BRACKETED_COMMENT_MESSAGE}")'
)

COMMENTED_SHOW_CREATE_REFUSALS = [
    ("blk_lead_unclosed", f"/* c */ SHOW CREATE TABLE `{CATALOG}.{NAMESPACE}"),
    ("blk_mid_unclosed", f"SHOW /* c */ CREATE TABLE `{CATALOG}.{NAMESPACE}"),
    ("blk_mid2_unclosed", f"SHOW CREATE /* c */ TABLE `{CATALOG}.{NAMESPACE}"),
    ("blk_after_table_unclosed", f"SHOW CREATE TABLE /* c */ `{CATALOG}.{NAMESPACE}"),
    ("line_lead_unclosed", f"-- c\nSHOW CREATE TABLE `{CATALOG}.{NAMESPACE}"),
    ("line_mid_unclosed", f"SHOW -- c\nCREATE TABLE `{CATALOG}.{NAMESPACE}"),
    (
        "nested_blk_unclosed",
        f"/* a /* b */ c */ SHOW CREATE TABLE `{CATALOG}.{NAMESPACE}",
    ),
    ("blk_lead_trailing", f"/* c */ SHOW CREATE TABLE {QUALIFIED} extra"),
    ("blk_mid_trailing", f"SHOW /* c */ CREATE TABLE {QUALIFIED} extra"),
    ("blk_lead_bare", "/* c */ SHOW CREATE TABLE"),
]


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-show-create-table").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    session.sql(
        f"CREATE TABLE {QUALIFIED} (id BIGINT NOT NULL COMMENT 'c', data STRING) USING iceberg "
        "PARTITIONED BY (bucket(4, id)) TBLPROPERTIES ('k'='v')"
    )
    return session


def _location(spark: ReparkSession) -> str:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {QUALIFIED}").collect()
    return next(row[1] for row in rows if row[0] == "Location")


def test_show_create_table_answers_spark_text(spark: ReparkSession) -> None:
    frame = spark.sql(f"SHOW CREATE TABLE {QUALIFIED}")
    assert frame.columns == ["createtab_stmt"]
    rows = frame.collect()
    assert len(rows) == 1
    assert rows[0][0] == (
        f"CREATE TABLE {QUALIFIED} (\n"
        "  id BIGINT NOT NULL COMMENT 'c',\n"
        "  data STRING)\n"
        "USING iceberg\n"
        "PARTITIONED BY (bucket(4, id))\n"
        f"LOCATION '{_location(spark)}'\n"
        "TBLPROPERTIES (\n"
        "  'current-snapshot-id' = 'none',\n"
        "  'format' = 'iceberg/parquet',\n"
        "  'format-version' = '2',\n"
        "  'k' = 'v',\n"
        "  'write.parquet.compression-codec' = 'zstd')\n"
    )


def test_show_create_as_serde_has_spark_error_contract(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"SHOW CREATE TABLE {QUALIFIED} AS SERDE").collect()
    assert caught.value.getCondition() == "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE"
    assert caught.value.getSqlState() == "0A000"
    assert str(caught.value) == (
        "Error during planning: [NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] SHOW CREATE TABLE AS "
        "SERDE is not supported for v2 tables. SQLSTATE: 0A000"
    )


def test_show_create_missing_table_has_spark_error_contract(spark: ReparkSession) -> None:
    missing = f"{CATALOG}.{NAMESPACE}.nope"
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"SHOW CREATE TABLE {missing}").collect()
    assert caught.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert caught.value.getSqlState() == "42P01"
    assert str(caught.value) == (
        "Error during planning: "
        f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{CATALOG}`.`{NAMESPACE}`.`nope` cannot "
        "be found. Verify the spelling and correctness of the schema and catalog. If you did "
        "not qualify the name with a schema, verify the current_schema() output, or qualify the "
        "name with the correct schema and catalog. To tolerate the error on drop use DROP VIEW "
        "IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    )


def test_show_create_without_a_name_has_spark_error_contract(spark: ReparkSession) -> None:
    with pytest.raises(ParseException) as caught:
        spark.sql("SHOW CREATE TABLE").collect()
    assert caught.value.getCondition() == "INVALID_STATEMENT_OR_CLAUSE"
    assert caught.value.getSqlState() == "42601"
    assert str(caught.value) == (INVALID_SHOW_CREATE_TABLE_RENDERED_MESSAGE)


@pytest.mark.parametrize(
    ("label", "statement"),
    [
        ("blk_lead_ok", f"/* c */ SHOW CREATE TABLE {QUALIFIED}"),
        ("blk_mid_ok", f"SHOW /* c */ CREATE TABLE {QUALIFIED}"),
    ],
)
def test_show_create_comments_match_the_uncommented_answer(
    spark: ReparkSession, label: str, statement: str
) -> None:
    expected = spark.sql(f"SHOW CREATE TABLE {QUALIFIED}")
    actual = spark.sql(statement)
    assert actual.columns == expected.columns, label
    assert actual.collect() == expected.collect(), label


@pytest.mark.parametrize(("label", "statement"), COMMENTED_SHOW_CREATE_REFUSALS)
def test_show_create_comments_keep_the_spark_parse_contract(
    spark: ReparkSession, label: str, statement: str
) -> None:
    with pytest.raises(ParseException) as caught:
        spark.sql(statement).collect()
    assert caught.value.getCondition() == "INVALID_STATEMENT_OR_CLAUSE", label
    assert caught.value.getSqlState() == "42601", label
    assert str(caught.value) == INVALID_SHOW_CREATE_TABLE_RENDERED_MESSAGE, label


def test_show_create_unclosed_comment_keeps_the_current_parse_outcome(
    spark: ReparkSession,
) -> None:
    statement = f"/* c SHOW CREATE TABLE {QUALIFIED}"
    with pytest.raises(ParseException) as caught:
        spark.sql(statement).collect()
    assert caught.value.getCondition() is None
    assert caught.value.getSqlState() is None
    assert str(caught.value) == (
        'SQL error: TokenizerError("Unexpected EOF while in a multi-line comment at Line: 1, '
        'Column: 37")'
    )


@pytest.mark.parametrize(
    "statement",
    [
        "SHOW CREATE TABLE /* c sc.sales.t",
        "SHOW CREATE TABLE sc.sales.t /* c",
        "SHOW CREATE TABLE sc.sales.t /*",
        "SHOW CREATE TABLE sc.sales.t AS SERDE /* c",
    ],
)
def test_show_create_unclosed_bracketed_comment_has_spark_parse_contract(
    spark: ReparkSession, statement: str
) -> None:
    with pytest.raises(ParseException) as caught:
        spark.sql(statement).collect()
    assert caught.value.getCondition() == "UNCLOSED_BRACKETED_COMMENT"
    assert caught.value.getSqlState() == "42601"
    assert str(caught.value) == UNCLOSED_BRACKETED_COMMENT_RENDERED_MESSAGE


def test_show_create_unclosed_comment_before_table_keyword_falls_through(
    spark: ReparkSession,
) -> None:
    with pytest.raises(ParseException) as caught:
        spark.sql("SHOW CREATE /* c TABLE sc.sales.t").collect()
    assert caught.value.getCondition() is None
    assert caught.value.getSqlState() is None
    assert str(caught.value) == (
        'SQL error: TokenizerError("Unexpected EOF while in a multi-line comment at Line: 1, '
        'Column: 34")'
    )


def test_show_create_multi_statement_has_spark_parse_contract(spark: ReparkSession) -> None:
    with pytest.raises(ParseException) as caught:
        spark.sql(f"SHOW CREATE TABLE {QUALIFIED}; SELECT 1").collect()
    assert caught.value.getCondition() == "INVALID_STATEMENT_OR_CLAUSE"
    assert caught.value.getSqlState() == "42601"
    assert str(caught.value) == INVALID_SHOW_CREATE_TABLE_RENDERED_MESSAGE


def test_show_tables_comment_near_miss_matches_its_uncommented_answer(spark: ReparkSession) -> None:
    expected = spark.sql(f"SHOW TABLES IN {CATALOG}.{NAMESPACE}")
    actual = spark.sql(f"/* c */ SHOW TABLES IN {CATALOG}.{NAMESPACE}")
    assert actual.columns == expected.columns
    assert actual.collect() == expected.collect()
