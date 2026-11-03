from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException, ParseException
from repark.spark.session import _reset_active_session_for_tests

GRAMMAR_CONDITIONS = ("PARSE_SYNTAX_ERROR", "INVALID_IDENTIFIER")
SNAPSHOT_ID_REFUSAL = (
    "Time travel option `snapshot-id` is no longer supported, "
    "use Spark built-in `versionAsOf` instead"
)
Door = Callable[[ReparkSession, str], Any]


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[ReparkSession]:
    session = (
        ReparkSession.builder.appName("ident-dialect-doors")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(tmp_path / "hd"))
        .getOrCreate()
    )
    session.register_memory_catalog("mem", str(tmp_path / "mem"))
    for catalog in ("hd", "mem"):
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
        session.sql(f"CREATE TABLE {catalog}.ns.t (id INT) USING iceberg")
        session.sql(f"INSERT INTO {catalog}.ns.t VALUES (1), (2)")
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


def _source(spark: ReparkSession) -> Any:
    return spark.sql("SELECT 3 AS id")


def _merge(spark: ReparkSession, name: str) -> Any:
    return _source(spark).mergeInto(name, "id").whenNotMatched().insertAll().merge()


DOORS: dict[str, Door] = {
    "read_table": lambda spark, name: spark.read.table(name).collect(),
    "spark_table": lambda spark, name: spark.table(name).collect(),
    "write_to": lambda spark, name: _source(spark).writeTo(name).append(),
    "insert_into": lambda spark, name: _source(spark).write.insertInto(name),
    "save_as_table": lambda spark, name: _source(spark).write.mode("append").saveAsTable(name),
    "merge_into": _merge,
    "table_exists": lambda spark, name: spark.catalog.tableExists(name),
    "list_columns": lambda spark, name: spark.catalog.listColumns(name),
    "get_table": lambda spark, name: spark.catalog.getTable(name),
    "cache_table": lambda spark, name: spark.catalog.cacheTable(name),
    "is_cached": lambda spark, name: spark.catalog.isCached(name),
    "uncache_table": lambda spark, name: spark.catalog.uncacheTable(name),
    "refresh_table": lambda spark, name: spark.catalog.refreshTable(name),
    "recover_partitions": lambda spark, name: spark.catalog.recoverPartitions(name),
    "create_table": lambda spark, name: spark.catalog.createTable(
        name, schema=_source(spark).schema
    ),
}
SPELLINGS = {
    "dq": ('hd.ns."t"', "[PARSE_SYNTAX_ERROR] Syntax error at or near '\"t\"'"),
    "unicode": (
        "hd.ns.ü1",
        "[INVALID_IDENTIFIER] The unquoted identifier ü1 is invalid and must be back quoted "
        "as: `ü1`",
    ),
}


def _rows(spark: ReparkSession) -> list[int]:
    return sorted(row[0] for row in spark.sql("SELECT id FROM hd.ns.t").collect())


@pytest.mark.parametrize("spelling", sorted(SPELLINGS))
@pytest.mark.parametrize("door", sorted(DOORS))
def test_a_table_door_refuses_a_quoted_or_non_ascii_identifier_like_spark(
    spark: ReparkSession, door: str, spelling: str
) -> None:
    name, refusal = SPELLINGS[spelling]
    with pytest.raises(ParseException) as caught:
        DOORS[door](spark, name)
    assert refusal in str(caught.value)
    assert _rows(spark) == [1, 2]


@pytest.mark.parametrize("door", sorted(DOORS))
def test_a_table_door_reads_a_back_quoted_identifier(spark: ReparkSession, door: str) -> None:
    try:
        DOORS[door](spark, "hd.ns.`t`")
    except Exception as error:
        assert not any(condition in str(error) for condition in GRAMMAR_CONDITIONS), str(error)


@pytest.mark.parametrize("door", ["read_table", "spark_table"])
def test_a_read_door_answers_the_back_quoted_and_dotted_spellings_like_spark(
    spark: ReparkSession, door: str
) -> None:
    assert len(DOORS[door](spark, "hd.ns.`t`")) == 2
    assert len(DOORS[door](spark, "hd.ns.t.snapshots")) == 1


@pytest.mark.parametrize("catalog", ["hd", "mem"])
def test_load_reports_the_legacy_snapshot_option_before_the_identifier_grammar(
    spark: ReparkSession, catalog: str
) -> None:
    reader = spark.read.format("iceberg").option("snapshot-id", 1)
    with pytest.raises(IllegalArgumentException) as caught:
        reader.load(f'{catalog}.ns."missing$all_files"').collect()
    assert SNAPSHOT_ID_REFUSAL in str(caught.value)


def test_load_refuses_a_numeric_identifier_at_parse(spark: ReparkSession) -> None:
    with pytest.raises(ParseException):
        spark.read.format("iceberg").load("123").collect()
