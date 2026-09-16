"""Oracle pins for DOOR-CONVERGE-2b (PySpark 4.1.2).

Oracle fixtures (the spec — read first):
`/tmp/oc-worker/qc-oracle/fixtures-batch16-dc2rest-setansi.json` (Q16-0…39),
`/tmp/oc-worker/pc-oracle/fixtures-batch2.json` (DIV-ceil-*, DIV-round-*,
DIV-like-*, DIV-slice-*, DIV-size-*, R-ceil-neg-scale, R-round-int,
R-date-part-sec, R-array-types),
`/tmp/oc-worker/pc-oracle/fixtures-batch3.json` (PG-is-distinct, PG-values-alias,
PG-map-access) and `/tmp/oc-worker/pc-oracle/fixtures-batch7.json`
(N7-0…N7-32 element `containsNull` and outer nullability).

pins: door-converge-2b/C-001, C-002, C-003, C-004, C-005, C-006

Every value cell asserts the Spark VALUE, the Arrow TYPE (including element
`containsNull`, normalized to the `element` field name) and the outer
NULLABILITY on the SQL door. Facade legs repeat representative shapes through
`F.*` where the facade signature already carries the arity (F.bround and
three-argument F.like/F.ilike are run-17a hand-offs per the ledger).

Column-nullability spellings: cells whose fixture marks the input column
non-nullable are spelled against a `VALUES` view (literal rows produce
non-nullable columns, PG-values-alias); cells whose fixture input is nullable
use the `createDataFrame` view `t`.
"""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pytest

from repark.errors import AnalysisException, PySparkException
from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


@pytest.fixture
def spark() -> ReparkSession:
    """Per-test local session (no AWS)."""
    return ReparkSession.builder.appName("dc2b").getOrCreate()


def _norm(data_type: pa.DataType) -> pa.DataType:
    """Rename list element fields to `element` (Spark-visible shape only)."""
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        element = data_type.value_field
        field = pa.field("element", _norm(element.type)).with_nullable(element.nullable)
        return pa.list_(field)
    return data_type


def _list(inner: pa.DataType, contains_null: bool) -> pa.DataType:
    return pa.list_(pa.field("element", inner, nullable=contains_null))


def _sql_cell(spark: ReparkSession, sql: str) -> tuple[pa.DataType, bool, list]:
    """Return (normalized Arrow type, outer nullable, column values) for one cell."""
    table = spark.sql(sql).to_arrow()
    field = table.schema[0]
    return _norm(field.type), field.nullable, table.column(0).to_pylist()


def _check_cell(
    spark: ReparkSession, sql: str, value: list, arrow_type: pa.DataType, nullable: bool
) -> None:
    """Assert value, Arrow type and nullability of one SQL-door oracle cell."""
    actual_type, actual_nullable, actual_value = _sql_cell(spark, sql)
    assert actual_value == value
    assert actual_type == arrow_type
    assert actual_nullable == nullable


@pytest.fixture
def views(spark: ReparkSession) -> None:
    """`t` holds nullable columns; `v` holds non-nullable VALUES columns."""
    spark.createDataFrame(
        [(2.5, 125, Decimal("12345.6789"), "a%c", [1, 2], [None, 3], ["x", "y"], 5, None)],
        "d double, b bigint, w decimal(38,4), s string, a array<int>, an array<int>, "
        "sa array<string>, n int, allnull array<int>",
    ).createOrReplaceTempView("t")
    spark.sql("SELECT * FROM (VALUES ('a%c', 125)) AS nn(s, b)").createOrReplaceTempView("v")


_SQL_VALUES: dict[str, tuple[str, list, pa.DataType, bool]] = {
    "Q16-0": ("SELECT bround(2.5, 0)", [Decimal("2")], pa.decimal128(2, 0), True),
    "Q16-1": ("SELECT bround(3.5, 0)", [Decimal("4")], pa.decimal128(2, 0), True),
    "Q16-2": ("SELECT bround(-2.5, 0)", [Decimal("-2")], pa.decimal128(2, 0), True),
    "Q16-3": ("SELECT bround(d, 0) FROM t", [2.0], pa.float64(), True),
    "Q16-4": ("SELECT bround(2.345, 2)", [Decimal("2.34")], pa.decimal128(4, 2), True),
    "Q16-5": ("SELECT bround(1250, -2)", [1200], pa.int32(), True),
    "Q16-6": ("SELECT bround(CAST(1350 AS INT), -2)", [1400], pa.int32(), True),
    "Q16-7": ("SELECT round(b, -1) FROM t", [130], pa.int64(), True),
    "Q16-8": ("SELECT round(b, -3) FROM t", [0], pa.int64(), True),
    "Q16-9": ("SELECT ceil(b, -1) FROM t", [Decimal("130")], pa.decimal128(21, 0), True),
    "Q16-10": ("SELECT floor(b, -2) FROM t", [Decimal("100")], pa.decimal128(21, 0), True),
    "Q16-11": ("SELECT round(w, 2) FROM t", [Decimal("12345.68")], pa.decimal128(37, 2), True),
    "Q16-12": ("SELECT round(w, -2) FROM t", [Decimal("12300")], pa.decimal128(35, 0), True),
    "Q16-13": ("SELECT ceil(w, 1) FROM t", [Decimal("12345.7")], pa.decimal128(36, 1), True),
    "Q16-14": ("SELECT floor(w, -3) FROM t", [Decimal("12000")], pa.decimal128(35, 0), True),
    "Q16-15": ("SELECT round(CAST(2.5 AS FLOAT), 0)", [3.0], pa.float32(), True),
    "Q16-16": ("SELECT round(d, -1) FROM t", [0.0], pa.float64(), True),
    "Q16-17": ("SELECT ceil(d, 0) FROM t", [Decimal("3")], pa.decimal128(16, 0), True),
    "Q16-18": (
        "SELECT floor(CAST(-0.5 AS DOUBLE), 0)",
        [Decimal("-1")],
        pa.decimal128(16, 0),
        True,
    ),
    "Q16-19": (
        "SELECT ceil(CAST(NULL AS DECIMAL(4,2)), 1)",
        [None],
        pa.decimal128(4, 1),
        True,
    ),
    "Q16-20": (
        "SELECT round(1234.5, CAST(NULL AS INT))",
        [None],
        pa.decimal128(5, 0),
        True,
    ),
    "Q16-21": (
        "SELECT date_part('SECOND', TIMESTAMP'2024-03-05 01:02:03.123456')",
        [Decimal("3.123456")],
        pa.decimal128(8, 6),
        False,
    ),
    "Q16-22": (
        "SELECT date_part('SECOND', CAST(NULL AS TIMESTAMP))",
        [None],
        pa.decimal128(8, 6),
        True,
    ),
    "Q16-23": (
        "SELECT extract(SECOND FROM CAST(NULL AS TIMESTAMP))",
        [None],
        pa.decimal128(8, 6),
        True,
    ),
    "Q16-24": (
        "SELECT date_part('MINUTE', TIMESTAMP'2024-03-05 01:02:03.123456')",
        [2],
        pa.int32(),
        False,
    ),
    "Q16-25": (
        "SELECT date_part('seconds', DATE'2024-01-01')",
        [Decimal("0.000000")],
        pa.decimal128(8, 6),
        False,
    ),
    "Q16-26": ("SELECT array(1, 2L)", [[1, 2]], _list(pa.int64(), False), False),
    "Q16-27": (
        "SELECT array(CAST(1 AS TINYINT), CAST(2 AS SMALLINT))",
        [[1, 2]],
        _list(pa.int16(), False),
        False,
    ),
    "Q16-28": (
        "SELECT array(1, 2.5)",
        [[Decimal("1.0"), Decimal("2.5")]],
        _list(pa.decimal128(11, 1), False),
        False,
    ),
    "Q16-29": (
        "SELECT array(1.5, 2.25)",
        [[Decimal("1.50"), Decimal("2.25")]],
        _list(pa.decimal128(3, 2), False),
        False,
    ),
    "Q16-30": ("SELECT array(1, NULL, 3)", [[1, None, 3]], _list(pa.int32(), True), False),
    "Q16-31": (
        "SELECT slice(array(CAST(1 AS SMALLINT), 2), 1, 1)",
        [[1]],
        _list(pa.int32(), False),
        False,
    ),
    "Q16-32": ("SELECT like(s, 'a/%c', '/') FROM v", [True], pa.bool_(), False),
    "Q16-33": ("SELECT like(s, 'a!%c', '!') FROM v", [True], pa.bool_(), False),
    "Q16-34": ("SELECT ilike('A%C', 'a/%c', '/')", [True], pa.bool_(), False),
    "Q16-37": ("SELECT like(NULL, 'a', '/')", [None], pa.bool_(), True),
    "Q16-38": ("SELECT s LIKE 'a/%c' ESCAPE '/' FROM v", [True], pa.bool_(), False),
    "Q16-39": ("SELECT NOT like(s, 'x%', '/') FROM v", [True], pa.bool_(), False),
    "DIV-ceil-0": ("SELECT ceil(1.25)", [Decimal("2")], pa.decimal128(2, 0), True),
    "DIV-ceil-1": (
        "SELECT ceil(CAST(1.25 AS DECIMAL(4,2)))",
        [Decimal("2")],
        pa.decimal128(3, 0),
        True,
    ),
    "DIV-ceil-2": ("SELECT ceil(-1.5D)", [-1], pa.int64(), True),
    "DIV-ceil-3": ("SELECT ceil(1.2345, 2)", [Decimal("1.24")], pa.decimal128(4, 2), True),
    "DIV-ceil-4": ("SELECT floor(-1.2345, 2)", [Decimal("-1.24")], pa.decimal128(4, 2), True),
    "DIV-ceil-5": ("SELECT ceiling(2.1D)", [3], pa.int64(), True),
    "DIV-ceil-6": (
        "SELECT floor(CAST(-1.25 AS DECIMAL(4,2)))",
        [Decimal("-2")],
        pa.decimal128(3, 0),
        True,
    ),
    "DIV-round-0": ("SELECT round(2.5D)", [3.0], pa.float64(), True),
    "DIV-round-1": ("SELECT round(-2.5D)", [-3.0], pa.float64(), True),
    "DIV-round-2": ("SELECT round(2.345, 2)", [Decimal("2.35")], pa.decimal128(4, 2), True),
    "DIV-round-3": (
        "SELECT round(CAST(2.5 AS DECIMAL(3,1)))",
        [Decimal("3")],
        pa.decimal128(3, 0),
        True,
    ),
    "DIV-round-4": ("SELECT round(1234.5, -2)", [Decimal("1200")], pa.decimal128(5, 0), True),
    "DIV-round-5": ("SELECT round(0.125D, 2)", [0.13], pa.float64(), True),
    "DIV-like-0": ("SELECT 'a_c' like 'a\\_c'", [True], pa.bool_(), False),
    "DIV-like-1": ("SELECT like('a%c', 'a/%c', '/')", [True], pa.bool_(), False),
    "DIV-like-2": ("SELECT 'abc' like 'A%'", [False], pa.bool_(), False),
    "DIV-like-3": ("SELECT 'abc' ilike 'A%'", [True], pa.bool_(), False),
    "DIV-like-4": ("SELECT ilike('A_C', 'a/_c', '/')", [True], pa.bool_(), False),
    "DIV-slice-0": (
        "SELECT slice(array(1,2,3), 1, 2)",
        [[1, 2]],
        _list(pa.int32(), False),
        False,
    ),
    "DIV-slice-1": (
        "SELECT slice(array(1,2,3), -2, 2)",
        [[2, 3]],
        _list(pa.int32(), False),
        False,
    ),
    "DIV-size-0": ("SELECT size(array(1,2))", [2], pa.int32(), False),
    "DIV-size-2": ("SELECT size(map(1,2))", [1], pa.int32(), False),
    "R-ceil-a": ("SELECT ceil(1234.5, -2)", [Decimal("1300")], pa.decimal128(5, 0), True),
    "R-ceil-b": (
        "SELECT floor(CAST(1.5 AS DOUBLE), 1)",
        [Decimal("1.5")],
        pa.decimal128(17, 1),
        True,
    ),
    "R-ceil-c": (
        "SELECT ceil(CAST(5 AS INT), -1)",
        [Decimal("10")],
        pa.decimal128(11, 0),
        True,
    ),
    "R-round-a": ("SELECT round(CAST(125 AS INT), -1)", [130], pa.int32(), True),
    "R-round-b": ("SELECT round(CAST(125 AS BIGINT), -1)", [130], pa.int64(), True),
    "R-round-d": ("SELECT round(CAST(2.5 AS FLOAT))", [3.0], pa.float32(), True),
    "R-date-sec-b": (
        "SELECT extract(SECOND FROM TIMESTAMP'2024-03-05 01:02:03.5')",
        [Decimal("3.500000")],
        pa.decimal128(8, 6),
        False,
    ),
    "R-array-a": ("SELECT array(1, 2)", [[1, 2]], _list(pa.int32(), False), False),
    "R-array-b": ("SELECT array(1L)", [[1]], _list(pa.int64(), False), False),
    "R-array-c": (
        "SELECT array(1.5)",
        [[Decimal("1.5")]],
        _list(pa.decimal128(2, 1), False),
        False,
    ),
    "R-array-d": (
        "SELECT slice(array(1,2,3), 1, 1)",
        [[1]],
        _list(pa.int32(), False),
        False,
    ),
    "PG-is-distinct": ("SELECT NULL IS DISTINCT FROM 1", [True], pa.bool_(), False),
    "PG-not-distinct": (
        "SELECT NULL IS NOT DISTINCT FROM NULL",
        [True],
        pa.bool_(),
        False,
    ),
    "PG-map-access-a": ("SELECT map('k', 1)['k']", [1], pa.int32(), True),
    "PG-map-access-b": ("SELECT array(1,2)[0]", [1], pa.int32(), False),
    "N7-0": ("SELECT array(1, 2)", [[1, 2]], _list(pa.int32(), False), False),
    "N7-1": ("SELECT array(1, NULL)", [[1, None]], _list(pa.int32(), True), False),
    "N7-2": (
        "SELECT array()",
        [[]],
        pa.list_(pa.field("element", pa.null()).with_nullable(False)),
        False,
    ),
    "N7-3": (
        "SELECT array(CAST(NULL AS INT))",
        [[None]],
        _list(pa.int32(), True),
        False,
    ),
    "N7-4": ("SELECT array('a', 'b')", [["a", "b"]], _list(pa.string(), False), False),
    "N7-5": (
        "SELECT slice(array(1,2,3), 1, 2)",
        [[1, 2]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-6": ("SELECT slice(a, 1, 1) FROM t", [[1]], _list(pa.int32(), True), True),
    "N7-7": ("SELECT slice(an, 1, 1) FROM t", [[None]], _list(pa.int32(), True), True),
    "N7-8": ("SELECT array_repeat(1, 2)", [[1, 1]], _list(pa.int32(), False), False),
    "N7-9": ("SELECT array_repeat(n, 2) FROM t", [[5, 5]], _list(pa.int32(), True), False),
    "N7-10": (
        "SELECT array_repeat(NULL, 2)",
        [[None, None]],
        _list(pa.null(), True),
        False,
    ),
    "N7-11": (
        "SELECT array_append(array(1,2), 3)",
        [[1, 2, 3]],
        _list(pa.int32(), True),
        False,
    ),
    "N7-12": (
        "SELECT array_append(a, NULL) FROM t",
        [[1, 2, None]],
        _list(pa.int32(), True),
        True,
    ),
    "N7-13": (
        "SELECT array_insert(array(1,2), 1, 0)",
        [[0, 1, 2]],
        _list(pa.int32(), True),
        False,
    ),
    "N7-14": (
        "SELECT flatten(array(array(1), array(2)))",
        [[1, 2]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-15": (
        "SELECT array_distinct(array(1,1,2))",
        [[1, 2]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-16": (
        "SELECT concat(array(1), array(2))",
        [[1, 2]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-17": ("SELECT sequence(1, 3)", [[1, 2, 3]], _list(pa.int32(), False), False),
    "N7-18": ("SELECT split('a,b', ',')", [["a", "b"]], _list(pa.string(), False), False),
    "N7-19": (
        "SELECT array_remove(array(1,2), 1)",
        [[2]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-20": (
        "SELECT array_union(array(1), array(2))",
        [[1, 2]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-21": (
        "SELECT array_compact(array(1, NULL))",
        [[1]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-22": ("SELECT reverse(array(1,2))", [[2, 1]], _list(pa.int32(), False), False),
    "N7-23": (
        "SELECT array_sort(array(2,1))",
        [[1, 2]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-29": ("SELECT a FROM t", [[1, 2]], _list(pa.int32(), True), True),
    "N7-30": ("SELECT an FROM t", [[None, 3]], _list(pa.int32(), True), True),
    "N7-25": (
        "SELECT transform(array(1,2), x -> x + 1)",
        [[2, 3]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-26": (
        "SELECT filter(array(1,2), x -> x > 1)",
        [[2]],
        _list(pa.int32(), False),
        False,
    ),
    "N7-27": ("SELECT map_keys(map(1,'a'))", [[1]], _list(pa.int32(), True), False),
    "N7-28": ("SELECT map_values(map(1,'a'))", [["a"]], _list(pa.string(), True), False),
    "N7-31": ("SELECT array_contains(array(1,2), 1)", [True], pa.bool_(), False),
    "N7-32": ("SELECT size(array(1,2))", [2], pa.int32(), False),
}


@pytest.mark.parametrize("cell", sorted(_SQL_VALUES))
def test_sql_cell(spark: ReparkSession, views: None, cell: str) -> None:
    """Each oracle cell pins value + Arrow type + nullability on the SQL door."""
    sql, value, arrow_type, nullable = _SQL_VALUES[cell]
    _check_cell(spark, sql, value, arrow_type, nullable)


def test_shuffle_preserves_type_and_nullability(spark: ReparkSession) -> None:
    """pins: door-converge-2b/C-005 — N7-24 (order is random; set and shape are pinned)."""
    actual_type, actual_nullable, actual_value = _sql_cell(spark, "SELECT shuffle(array(1,2))")
    assert actual_type == _list(pa.int32(), False)
    assert actual_nullable is False
    assert sorted(actual_value[0]) == [1, 2]


def test_pg_values_alias_columns_non_nullable(spark: ReparkSession) -> None:
    """pins: door-converge-2b/C-005 — literal VALUES rows yield non-null columns."""
    table = spark.sql("SELECT * FROM VALUES (1, 'a') AS v(x, y)").to_arrow()
    assert table.schema.field("x").type == pa.int32()
    assert not table.schema.field("x").nullable
    assert table.schema.field("y").type == pa.string()
    assert not table.schema.field("y").nullable
    assert table.to_pylist() == [{"x": 1, "y": "a"}]


_ERROR_CELLS: dict[str, tuple[str, str]] = {
    "Q16-35": ("SELECT like(s, 'a%c', 'ab') FROM v", "INVALID_ESCAPE_CHAR"),
    "Q16-36": ("SELECT like(s, 'a%c', '') FROM v", "INVALID_ESCAPE_CHAR"),
    "slice-start-zero": (
        "SELECT slice(array(1,2,3), 0, 1)",
        "INVALID_PARAMETER_VALUE.START",
    ),
    "slice-negative-length": (
        "SELECT slice(array(1,2,3), 2, -1)",
        "INVALID_PARAMETER_VALUE.LENGTH",
    ),
    "extract-epoch": (
        "SELECT date_part('epoch', TIMESTAMP'2024-03-05 01:02:03')",
        "INVALID_EXTRACT_FIELD",
    ),
    "extract-nanosecond": (
        "SELECT date_part('nanosecond', DATE'2024-03-05')",
        "INVALID_EXTRACT_FIELD",
    ),
    "extract-unknown": (
        "SELECT date_part('fizz', DATE'2024-03-05')",
        "INVALID_EXTRACT_FIELD",
    ),
}


@pytest.mark.parametrize("cell", sorted(_ERROR_CELLS))
def test_sql_error_cell(spark: ReparkSession, views: None, cell: str) -> None:
    """Refusals carry the Spark error class on the SQL door."""
    sql, condition = _ERROR_CELLS[cell]
    with pytest.raises((AnalysisException, PySparkException), match=condition):
        spark.sql(sql).to_arrow()


def test_facade_round_ceil_floor_scale(spark: ReparkSession, views: None) -> None:
    """pins: door-converge-2b/C-001, C-002 — facade arms reach the Spark kernels."""
    df = spark.sql("SELECT w, d FROM t")
    table = df.select(
        F.round(F.col("w"), 2).alias("r"),
        F.ceil(F.col("w")).alias("c"),
        F.floor(F.col("w")).alias("f"),
    ).to_arrow()
    assert table.column("r").to_pylist() == [Decimal("12345.68")]
    assert table.schema.field("r").type == pa.decimal128(37, 2)
    assert table.column("c").to_pylist() == [Decimal("12346")]
    assert table.schema.field("c").type == pa.decimal128(35, 0)
    assert table.column("f").to_pylist() == [Decimal("12345")]
    assert table.schema.field("f").type == pa.decimal128(35, 0)


def test_facade_like_two_arg(spark: ReparkSession, views: None) -> None:
    """pins: door-converge-2b/C-006 — facade 2-arg like/ilike reach the kernel."""
    df = spark.sql("SELECT s FROM t")
    table = df.select(
        F.like(F.col("s"), F.lit("a/%c")).alias("m"),
        F.ilike(F.col("s"), F.lit("A/%C")).alias("i"),
    ).to_arrow()
    assert table.column("m").to_pylist() == [False]
    assert table.column("i").to_pylist() == [False]


def test_facade_date_part(spark: ReparkSession, views: None) -> None:
    """pins: door-converge-2b/C-003 — facade date_part carries fractional seconds."""
    df = spark.sql("SELECT TIMESTAMP'2024-03-05 01:02:03.5' AS x")
    table = df.select(
        F.date_part(F.lit("SECOND"), F.col("x")).alias("v"),
    ).to_arrow()
    assert table.schema.field("v").type == pa.decimal128(8, 6)
    assert table.column("v").to_pylist() == [Decimal("3.500000")]


def test_facade_array_shapes(spark: ReparkSession, views: None) -> None:
    """pins: door-converge-2b/C-004, C-005 — facade collection arms share kernels."""
    df = spark.sql("SELECT a, an FROM t")
    table = df.select(
        F.array(F.lit(1), F.lit(2)).alias("mk"),
        F.slice(F.col("a"), F.lit(1), F.lit(1)).alias("sl"),
        F.array_repeat(F.lit(1), F.lit(2)).alias("rp"),
        F.array_distinct(F.col("a")).alias("di"),
        F.array_compact(F.col("an")).alias("cp"),
        F.array_union(F.col("a"), F.col("an")).alias("un"),
        F.array_remove(F.col("a"), F.lit(2)).alias("rm"),
        F.map_values(F.create_map(F.lit("k"), F.lit(1))).alias("mv"),
        F.map_keys(F.create_map(F.lit("k"), F.lit(1))).alias("mkk"),
    ).to_arrow()
    expected = {
        "mk": [1, 2],
        "sl": [1],
        "rp": [1, 1],
        "di": [1, 2],
        "cp": [3],
        "un": [1, 2, None, 3],
        "rm": [1],
        "mv": [1],
        "mkk": ["k"],
    }
    for name, value in expected.items():
        assert table.column(name).to_pylist() == [value], name
    assert not table.schema.field("mk").type.value_field.nullable
    assert table.schema.field("un").type.value_field.nullable
    assert table.schema.field("mkk").type.value_field.nullable
