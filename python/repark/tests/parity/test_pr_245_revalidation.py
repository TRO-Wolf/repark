"""PR-245 public-door revalidation against the pinned PySpark 4.1.2 measurements.

pins: pr-245-revalidation/C-001, C-002, C-003, C-010
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import sql as repark_sql
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as F  # noqa: N812 - PySpark convention


@pytest.fixture
def spark() -> ReparkSession:
    """Create the facade session used by the revalidation pins."""
    session = ReparkSession.builder.appName("pytest-pr-245").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    """Collect a frame through the Arrow path."""
    return frame.to_arrow()  # type: ignore[attr-defined]


def test_pr245_spark_literal_classes_and_error_position(spark: ReparkSession) -> None:
    r"""Preserve escapes, raw strings, adjacency, comments, and the lexer error position."""
    table = _table(
        spark.sql(
            "SELECT /* lead */ '\\d' AS unknown, '\\\\d' AS regex, "
            "r'\\d' AS raw_value, 'ab' 'cd' AS adjacent_value, 'a\\nb' AS newline"
        )
    )
    assert table.to_pylist() == [
        {
            "unknown": "d",
            "regex": r"\d",
            "raw_value": r"\d",
            "adjacent_value": "abcd",
            "newline": "a\nb",
        }
    ]
    assert [field.type for field in table.schema] == [pa.string()] * 5
    with pytest.raises((AnalysisException, PySparkException)) as caught:
        spark.sql("SELECT 1 AS ok,\n'a\\' AS broken").to_arrow()
    message = str(caught.value)
    assert "Line: 2" in message and "Column: 1" in message


@pytest.mark.parametrize(
    ("sql", "line", "column"),
    [
        ("SELECT '\\u0061' + )", 1, 19),
        ("SELECT '\\u0061' AS ok,\n  1 + )", 2, 7),
        ("SELECT '\\u0061' AS ok,\n  '\\u0062' + )", 2, 14),
        ("SELECT '\\n' AS shifted, )", 1, 25),
        ("SELECT '\\u0027' AS expanded, '\\u0061' AS shrunk, )", 1, 50),
        ("SELECT '\\u0061' +", 1, 18),
    ],
)
def test_pr245_downstream_error_uses_original_sql_position(
    spark: ReparkSession, sql: str, line: int, column: int
) -> None:
    """Map downstream parser locations through length changes and EOF to the caller's SQL."""
    with pytest.raises((AnalysisException, PySparkException)) as caught:
        spark.sql(sql).to_arrow()
    message = str(caught.value)
    assert f"Line: {line}" in message
    assert f"Column: {column}" in message


def test_pr245_expanding_error_positions_survive_prior_direct_parser_error(
    spark: ReparkSession,
) -> None:
    """Keep expanded-source positions after the same session returns a direct parser error."""
    cases = [
        ("SELECT '\\u0061' + )", 19),
        ("SELECT '\\n' AS shifted, )", 25),
        ("SELECT '\\u0027' AS expanded, '\\u0061' AS shrunk, )", 50),
    ]
    for sql, column in cases:
        with pytest.raises((AnalysisException, PySparkException)) as caught:
            spark.sql(sql).to_arrow()
        message = str(caught.value)
        assert "Line: 1" in message
        assert f"Column: {column}" in message


def test_pr245_binary_legal_values_types_and_illegal_contract(spark: ReparkSession) -> None:
    """Preserve legal binary values and types, and refuse each illegal source class."""
    table = _table(
        spark.sql(
            "SELECT CAST('abc' AS BINARY) AS cast_value, "
            "TRY_CAST('abc' AS BINARY) AS try_value, CAST(NULL AS BINARY) AS null_value, "
            "hex(CAST('\\t' AS BINARY)) AS tab_hex"
        )
    )
    assert table.to_pylist() == [
        {"cast_value": b"abc", "try_value": b"abc", "null_value": None, "tab_hex": "09"}
    ]
    assert [field.type for field in table.schema] == [
        pa.binary(),
        pa.binary(),
        pa.binary(),
        pa.string(),
    ]
    cases = [
        ("CAST(CAST(1 AS INT) AS BINARY)", "CAST_WITH_CONF_SUGGESTION", "INT"),
        ("TRY_CAST(CAST(1 AS INT) AS BINARY)", "CAST_WITHOUT_SUGGESTION", "INT"),
        ("CAST(CAST(1.5 AS DECIMAL(3,1)) AS BINARY)", "CAST_WITHOUT_SUGGESTION", "DECIMAL"),
        ("CAST(true AS BINARY)", "CAST_WITHOUT_SUGGESTION", "BOOLEAN"),
        ("CAST(DATE '2024-01-01' AS BINARY)", "CAST_WITHOUT_SUGGESTION", "DATE"),
    ]
    for expression, condition, source in cases:
        with pytest.raises((AnalysisException, PySparkException)) as caught:
            spark.sql(f"SELECT {expression} AS value").to_arrow()
        message = str(caught.value)
        assert condition in message and source in message


def test_pr245_ansi_and_facade_controls(spark: ReparkSession) -> None:
    r"""Keep ANSI literals unchanged and facade DataFrame/SQL binary results equal."""
    ansi = _table(repark_sql(r"SELECT '\d' AS value"))
    assert ansi.column("value").to_pylist() == [r"\d"]
    assert ansi.schema.field("value").type == pa.string()
    facade = _table(spark.range(1).select(F.lit("abc").cast("binary").alias("value")))
    sql = _table(spark.sql("SELECT CAST('abc' AS BINARY) AS value"))
    assert facade.column("value").to_pylist() == sql.column("value").to_pylist() == [b"abc"]
    assert facade.schema.field("value").type == sql.schema.field("value").type == pa.binary()
