from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException

CATALOG = "mem"
NAMESPACE = "scns1"
TABLE = "sc1"
QUALIFIED = f"{CATALOG}.{NAMESPACE}.{TABLE}"


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
    assert str(caught.value) == (
        'SQL error: ParserError("[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: SHOW '
        'CREATE TABLE is not valid. SQLSTATE: 42601")'
    )
