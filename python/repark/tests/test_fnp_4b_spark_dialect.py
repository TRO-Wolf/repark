"""FNP-4B door pins — double-quoted STRING literals and Spark expression strings.

Oracle: PySpark 4.1.2 fixtures-batch1 cells BL9-* and FNP4B-* (see the unit ledger
``task/ledgers/staging/fnp-4b-ledger.md``). Every pin collects on the Arrow path
(value AND type/nullability), on BOTH doors where the shape rule applies
(``spark.sql`` and ``F.expr`` / ``filter`` / ``where`` / ``selectExpr``).

pins: fnp-4b/C-001, C-003
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.dataframe import DataFrame


@pytest.fixture
def spark() -> ReparkSession:
    """A facade session for the FNP-4B door pins."""
    session = ReparkSession.builder.appName("pytest-fnp-4b-doors").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def frame(spark: ReparkSession) -> DataFrame:
    """The two-row frame the FNP4B oracle cells were measured against."""
    return spark.createDataFrame([("x", 1, 2), ("y", 2, 3)], ["name", "ID", "my col"])


def _table(result: DataFrame) -> pa.Table:
    """Collect a frame on the Arrow path (value AND type)."""
    return result.to_arrow()


def test_sql_double_quoted_literal_is_a_string(spark: ReparkSession) -> None:
    """BL9-0: ``SELECT "abc"`` is the STRING ``abc``."""
    table = _table(spark.sql('SELECT "abc" AS s'))
    assert table.column("s").to_pylist() == ["abc"]
    assert pa.types.is_string(table.schema.field("s").type)
    assert table.schema.field("s").nullable is False


def test_sql_double_quoted_escape_quote(spark: ReparkSession) -> None:
    """BL9-1: ``SELECT "a\\"b"`` is ``a"b``."""
    table = _table(spark.sql(r'SELECT "a\"b" AS s'))
    assert table.column("s").to_pylist() == ['a"b']
    assert pa.types.is_string(table.schema.field("s").type)


def test_sql_double_quoted_backslash_n_has_length_3(spark: ReparkSession) -> None:
    """BL9-2: ``length("a\\nb")`` is 3."""
    table = _table(spark.sql(r'SELECT length("a\nb") AS n'))
    assert table.column("n").to_pylist() == [3]
    assert table.schema.field("n").nullable is False


def test_sql_double_quoted_escaped_squote(spark: ReparkSession) -> None:
    """BL9-3: ``SELECT "it\\'s"`` is ``it's``."""
    table = _table(spark.sql(r'SELECT "it\'s" AS s'))
    assert table.column("s").to_pylist() == ["it's"]


def test_sql_double_quoted_concat(spark: ReparkSession) -> None:
    """BL9-5: ``SELECT "a" || "b"`` is ``ab``."""
    table = _table(spark.sql('SELECT "a" || "b" AS c'))
    assert table.column("c").to_pylist() == ["ab"]


def test_sql_double_quoted_string_int_compare(spark: ReparkSession) -> None:
    """BL9-6: ``SELECT "1" = 1`` is true (Spark ``(1 = 1)``, nullable)."""
    table = _table(spark.sql('SELECT "1" = 1 AS b'))
    assert table.column("b").to_pylist() == [True]
    assert pa.types.is_boolean(table.schema.field("b").type)
    assert table.schema.field("b").nullable is True


def test_sql_single_quoted_backslash_quote_still_works(spark: ReparkSession) -> None:
    """BL9-4: ``SELECT 'it\\"s'`` is ``it"s`` (SQP-1 behavior, unchanged)."""
    table = _table(spark.sql(r"SELECT 'it\"s' AS s"))
    assert table.column("s").to_pylist() == ['it"s']


def test_filter_double_quoted_string_literal(spark: ReparkSession, frame: DataFrame) -> None:
    """FNP4B-filter-dq: ``filter('name = "x"')`` keeps the ``x`` row."""
    table = _table(frame.filter('name = "x"'))
    assert table.to_pylist() == [{"name": "x", "ID": 1, "my col": 2}]


def test_where_double_quoted_string_literal(spark: ReparkSession, frame: DataFrame) -> None:
    """FNP4B-filter-dq through ``where``: same row as ``filter``."""
    table = _table(frame.where('name = "x"'))
    assert table.to_pylist() == [{"name": "x", "ID": 1, "my col": 2}]


def test_filter_double_quoted_id_string_raises_on_int_compare(
    spark: ReparkSession, frame: DataFrame
) -> None:
    """FNP4B-filter-dq-ID: ``filter('"ID" = 1')`` is loud, never a row match."""
    with pytest.raises((AnalysisException, PySparkException), match=r"ID"):
        frame.filter('"ID" = 1').to_arrow()


def test_select_expr_double_quoted_literal(spark: ReparkSession, frame: DataFrame) -> None:
    """FNP4B-selectExpr-dq: ``selectExpr('"abc" AS s')`` is ``abc`` on both rows."""
    table = _table(frame.selectExpr('"abc" AS s'))
    assert table.column("s").to_pylist() == ["abc", "abc"]
    assert pa.types.is_string(table.schema.field("s").type)
    assert table.schema.field("s").nullable is False


def test_expr_double_quoted_literal(spark: ReparkSession) -> None:
    """FNP4B-expr-dq through ``F.expr``: ``"abc"`` is the STRING ``abc``."""
    table = _table(spark.range(1).select(F.expr('"abc"').alias("s")))
    assert table.column("s").to_pylist() == ["abc"]
    assert pa.types.is_string(table.schema.field("s").type)


def test_sql_backtick_ident_with_double_quoted_subquery(spark: ReparkSession) -> None:
    """FNP4B-sql-bt-dq: backtick qualifier over a double-quoted string subquery."""
    table = _table(spark.sql('SELECT `a b`.c FROM (SELECT "v" AS c) AS `a b`'))
    assert table.column("c").to_pylist() == ["v"]
    assert pa.types.is_string(table.schema.field("c").type)
    assert table.schema.field("c").nullable is False


def test_filter_backtick_ident(spark: ReparkSession, frame: DataFrame) -> None:
    """FNP4B-filter-bt: ``filter('`my col` > 2')`` keeps the ``y`` row."""
    table = _table(frame.filter("`my col` > 2"))
    assert table.to_pylist() == [{"name": "y", "ID": 2, "my col": 3}]


def test_filter_lowercase_name_binds_uppercase_column(
    spark: ReparkSession, frame: DataFrame
) -> None:
    """FNP4B-filter-case: ``filter('id > 1')`` binds ``ID`` and keeps ``y``."""
    table = _table(frame.filter("id > 1"))
    assert table.to_pylist() == [{"name": "y", "ID": 2, "my col": 3}]


def test_select_expr_backtick_ident(spark: ReparkSession, frame: DataFrame) -> None:
    """FNP4B-selectExpr-bt: values and BIGINT type; the display name is observed."""
    table = _table(frame.selectExpr("`my col` + 1"))
    assert table.column(0).to_pylist() == [3, 4]
    assert pa.types.is_int64(table.schema.field(0).type)


def test_select_expr_lambda(spark: ReparkSession, frame: DataFrame) -> None:
    """FNP4B-selectExpr-lambda: ``transform`` over a lambda reaches both rows."""
    table = _table(frame.selectExpr("transform(array(1,2), x -> x + 1) t"))
    assert table.column("t").to_pylist() == [[2, 3], [2, 3]]
    assert table.schema.field("t").nullable is False


def test_expr_lambda(spark: ReparkSession) -> None:
    """FNP4B-expr-lambda: values and array type; the display name is observed."""
    table = _table(spark.range(1).select(F.expr("filter(array(1,2,3), x -> x > 1)")))
    assert table.column(0).to_pylist() == [[2, 3]]
    assert pa.types.is_list(table.schema.field(0).type)


def test_expr_column_reference_binds_the_frame_column(
    spark: ReparkSession, frame: DataFrame
) -> None:
    """FNP4B-expr-col: ``F.expr('ID + 1')`` answers ``[2, 3]`` as BIGINT."""
    table = _table(frame.select(F.expr("ID + 1")))
    assert table.column(0).to_pylist() == [2, 3]
    assert pa.types.is_int64(table.schema.field(0).type)


def test_expr_backtick_column_reference_binds_the_frame_column(
    spark: ReparkSession, frame: DataFrame
) -> None:
    """FNP4B-expr-bt: ``F.expr('`my col` * 2')`` answers ``[4, 6]`` as BIGINT."""
    table = _table(frame.select(F.expr("`my col` * 2")))
    assert table.column(0).to_pylist() == [4, 6]
    assert pa.types.is_int64(table.schema.field(0).type)
