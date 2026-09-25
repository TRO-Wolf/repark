from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import ParseException, PySparkException


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-ice-typed-partition-refusals").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    return session


def _refusal(spark: ReparkSession, statement: str) -> PySparkException:
    with pytest.raises(PySparkException) as caught:
        spark.sql(statement).collect()
    return caught.value


@pytest.mark.parametrize(
    ("partitioning", "expected_type", "message"),
    [
        pytest.param(
            "(p MAP<STRING,INT>)",
            PySparkException,
            "DataInvalid => Cannot partition by non-primitive source field: 'map'.",
            id="map",
        ),
        pytest.param(
            "(p STRUCT<a: INT, b: STRING>)",
            PySparkException,
            "DataInvalid => Cannot partition by non-primitive source field: 'struct<intstring>'.",
            id="struct-two-fields",
        ),
        pytest.param(
            "(p STRING DEFAULT 'x')",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'DEFAULT'. SQLSTATE: 42601",
            id="default",
        ),
        pytest.param(
            "(p STRING NOT NULL DEFAULT 'a' COMMENT 'c')",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'DEFAULT'. SQLSTATE: 42601",
            id="not-null-default-comment",
        ),
    ],
)
def test_typed_partition_column_shapes_refuse_like_spark(
    spark: ReparkSession, partitioning: str, expected_type: type[Exception], message: str
) -> None:
    caught = _refusal(
        spark, f"CREATE TABLE sc.ns.pr (id BIGINT) USING iceberg PARTITIONED BY {partitioning}"
    )
    assert type(caught) is expected_type
    assert str(caught) == message
    assert not spark.catalog.tableExists("sc.ns.pr")


def test_ctas_mixing_untyped_and_typed_partition_elements_answers_the_mix_text(
    spark: ReparkSession,
) -> None:
    caught = _refusal(
        spark,
        "CREATE TABLE sc.ns.pm USING iceberg PARTITIONED BY (id, p STRING) "
        "AS SELECT 1 AS id, 'x' AS p",
    )
    assert type(caught) is ParseException
    assert str(caught) == (
        'SQL error: ParserError("Operation not allowed: PARTITION BY: Cannot mix partition '
        'expressions and partition columns:\\nExpressions: id\\nColumns: p string.")'
    )
    assert not spark.catalog.tableExists("sc.ns.pm")


@pytest.mark.parametrize(
    ("columns", "partitioning", "pair"),
    [
        pytest.param("(id BIGINT, data STRING)", "(DATA STRING)", "data and DATA", id="declared"),
        pytest.param("(id BIGINT)", "(p STRING, P INT)", "p and P", id="typed"),
    ],
)
def test_typed_partition_columns_differing_by_case_reach_the_fork_under_case_sensitive(
    spark: ReparkSession, columns: str, partitioning: str, pair: str
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        caught = _refusal(
            spark,
            f"CREATE TABLE sc.ns.pcs {columns} USING iceberg PARTITIONED BY {partitioning}",
        )
    finally:
        spark.conf.set("spark.sql.caseSensitive", "false")
    assert type(caught) is PySparkException
    assert str(caught) == f"DataInvalid => Cannot build lower case index: {pair} collide"
    assert not spark.catalog.tableExists("sc.ns.pcs")
