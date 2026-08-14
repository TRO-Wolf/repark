"""Facade tests for the Column / expression surface (WG1).

Every op is exercised against the real native engine (``maturin develop``), and each behavior is
pinned to an **exact Spark-semantics fixture**. The ``test_parity_*`` goldens are recorded
differentially from **live PySpark 4.1.2** (``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``, Spark 4 needs
Java 17 — the stale "not recordable here" note predated the zulu-17 install) and compared through
``repark_parity.assert_frames_equal`` (name + Arrow type + field nullability + bit-exact value).

A parity pin is genuine only when BOTH engines infer the pinned type/nullability, so parity inputs
are built with ``createDataFrame`` (Python ``int``→bigint/int64, ``float``→double, all nullable) —
NOT inline SQL ``VALUES``, which Spark types differently (int32/non-null, DECIMAL literals) while
repark lowers through DataFusion to int64/nullable/double. Pinning the latter would record repark's
own output as "Spark" (the cycle-2 F1/C1 mispin class — see ``task/lessons.md`` 2026-07-19).
"""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pytest

from repark import Column, ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark import types as T  # noqa: N812 — PySpark idiom: `import ...types as T`
from repark.errors import AnalysisException
from repark_parity import assert_frames_equal


@pytest.fixture
def spark() -> ReparkSession:
    """A default session (PySpark ``SparkSession.builder.getOrCreate()``)."""
    return ReparkSession.builder.appName("pytest-columns").getOrCreate()


def _rows(df: object) -> list[dict[str, object]]:
    """Collect a DataFrame to a row-order-insensitive, sorted list of dicts."""
    table = df.to_arrow()  # type: ignore[attr-defined]
    rows = table.to_pylist()
    return sorted(rows, key=lambda row: tuple((key, str(value)) for key, value in row.items()))


# ==================================================================================================
# types.py — the seven Spark type objects → canonical engine strings
# ==================================================================================================


def test_type_objects_map_to_engine_strings() -> None:
    # Risk: a wrong type mapping casts to the wrong Arrow type, corrupting values silently.
    assert T.StringType()._engine_type() == "string"
    assert T.IntegerType()._engine_type() == "int"
    assert T.DoubleType()._engine_type() == "double"
    assert T.BooleanType()._engine_type() == "boolean"
    assert T.DateType()._engine_type() == "date"
    assert T.TimestampType()._engine_type() == "timestamp"
    assert T.DecimalType(10, 4)._engine_type() == "decimal(10,4)"
    assert T.DecimalType()._engine_type() == "decimal(10,0)"  # PySpark defaults


def test_type_object_equality_and_repr() -> None:
    assert T.StringType() == T.StringType()
    assert T.DecimalType(10, 4) == T.DecimalType(10, 4)
    assert T.DecimalType(10, 4) != T.DecimalType(10, 2)
    assert T.IntegerType() != T.DoubleType()
    assert repr(T.DecimalType(10, 4)) == "DecimalType(10,4)"


# ==================================================================================================
# functions.py — construction (col / lit / expr)
# ==================================================================================================


def test_col_and_lit_are_columns(spark: ReparkSession) -> None:
    assert isinstance(F.col("a"), Column)
    assert isinstance(F.lit(1), Column)
    df = spark.sql("SELECT 1 AS a").withColumn("b", F.lit(7))
    assert _rows(df) == [{"a": 1, "b": 7}]


def test_lit_covers_scalar_kinds(spark: ReparkSession) -> None:
    # Risk: Python bool is an int subclass — lit(True) must be boolean, not integer 1.
    df = (
        spark.sql("SELECT 1 AS keep")
        .withColumn("i", F.lit(42))
        .withColumn("f", F.lit(2.5))
        .withColumn("s", F.lit("hi"))
        .withColumn("b", F.lit(True))
        .withColumn("n", F.lit(None))
    )
    table = df.to_arrow()
    assert table.column("i").to_pylist() == [42]
    assert table.column("f").to_pylist() == [2.5]
    assert table.column("s").to_pylist() == ["hi"]
    assert table.column("b").to_pylist() == [True]
    assert pa.types.is_boolean(table.schema.field("b").type)
    assert table.column("n").to_pylist() == [None]


def test_lit_rejects_unsupported_type(spark: ReparkSession) -> None:
    # X1 census: lit(list) is now supported (F.array expansion); pin a still-unsupported type.
    with pytest.raises(TypeError, match="lit"):
        F.lit(object())


def test_expr_resolves_column_free_sql(spark: ReparkSession) -> None:
    # A column-free SQL expression resolves eagerly (DataFusion built-ins + literals).
    df = spark.sql("SELECT 1 AS keep").withColumn("s", F.expr("1 + 1"))
    assert _rows(df) == [{"keep": 1, "s": 2}]


def test_expr_referencing_a_column_raises(spark: ReparkSession) -> None:
    # Documented WG1 boundary: column-referencing expr() has no schema to bind to (raises loudly);
    # the DataFrame-bound expr path lands with the date-function group. WG-3: this is now an
    # AnalysisException (the PySpark-faithful type — was a bare ValueError); it still fails loudly,
    # and AnalysisException subclasses RuntimeError so `except RuntimeError` callers are unaffected.
    with pytest.raises(AnalysisException):
        F.expr("a + 1")


# ==================================================================================================
# column.py — operators (arithmetic / comparison / logical)
# ==================================================================================================


def test_arithmetic_operators(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 10 AS a, 3 AS b")
    out = (
        df.withColumn("add", F.col("a") + F.col("b"))
        .withColumn("sub", F.col("a") - F.col("b"))
        .withColumn("mul", F.col("a") * F.col("b"))
        .withColumn("scal", F.col("a") + 5)
        .withColumn("rscal", 100 - F.col("a"))
    )
    row = out.to_arrow().to_pylist()[0]
    assert (row["add"], row["sub"], row["mul"], row["scal"], row["rscal"]) == (13, 7, 30, 15, 90)


def test_division_is_float(spark: ReparkSession) -> None:
    # U2: SQL literals `7.0`/`2.0` are DECIMAL(2,1); DataFrame Column `/` still yields
    # float64 3.5 (the analyzer's SQL `/` rewrite is not this path).
    df = spark.sql("SELECT 7.0 AS a, 2.0 AS b").withColumn("d", F.col("a") / F.col("b"))
    table = df.to_arrow()
    assert table.schema.field("a").type == pa.decimal128(2, 1)
    assert table.schema.field("b").type == pa.decimal128(2, 1)
    assert pa.types.is_floating(table.schema.field("d").type)
    assert table.column("d").to_pylist() == [3.5]


def test_sql_float_literal_division_is_decimal(spark: ReparkSession) -> None:
    # A7 grant: `SELECT 7.0 / 2.0` is Spark decimal `/` after U4b — (2,1)/(2,1) → (8,6).
    table = spark.sql("SELECT 7.0 / 2.0 AS d").to_arrow()
    assert table.schema.field("d").type == pa.decimal128(8, 6)
    assert table.column("d").to_pylist() == [Decimal("3.500000")]


def test_division_of_integers_is_double(spark: ReparkSession) -> None:
    # PySpark `/` is ALWAYS true division: 7 / 2 == 3.5 (DoubleType), never integer-truncating
    # division (which is the separate `//` / `div`). Integer operands must promote to double.
    df = spark.sql("SELECT 7 AS a, 2 AS b").withColumn("d", F.col("a") / F.col("b"))
    column = df.to_arrow().column("d")
    assert column.to_pylist() == [3.5]
    assert pa.types.is_floating(column.type)


def test_comparison_operators_produce_boolean_columns(spark: ReparkSession) -> None:
    # Risk: `==` must build a boolean Column, not evaluate to a Python bool.
    predicate = F.col("a") == F.lit(2)
    assert isinstance(predicate, Column)
    df = spark.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(a)")
    assert _rows(df.filter(F.col("a") >= 2)) == [{"a": 2}, {"a": 3}]
    assert _rows(df.filter(F.col("a") < 2)) == [{"a": 1}]
    assert _rows(df.filter(F.col("a") != 2)) == [{"a": 1}, {"a": 3}]
    assert _rows(df.filter(F.col("a") <= 2)) == [{"a": 1}, {"a": 2}]
    assert _rows(df.filter(F.col("a") > 2)) == [{"a": 3}]


def test_logical_operators(spark: ReparkSession) -> None:
    df = spark.sql("SELECT * FROM (VALUES (1, 5), (2, 6), (3, 7)) AS t(a, b)")
    # (a > 1) AND (b < 7)  → row (2, 6)
    both = df.filter((F.col("a") > 1) & (F.col("b") < 7))
    assert _rows(both) == [{"a": 2, "b": 6}]
    # (a == 1) OR (a == 3)
    either = df.filter((F.col("a") == 1) | (F.col("a") == 3))
    assert _rows(either) == [{"a": 1, "b": 5}, {"a": 3, "b": 7}]
    # NOT (a == 2)
    negated = df.filter(~(F.col("a") == 2))
    assert _rows(negated) == [{"a": 1, "b": 5}, {"a": 3, "b": 7}]


def test_python_bool_on_column_raises() -> None:
    # Risk: without the PySpark __bool__ guard, `and`/`or`/`not`/`if` on a Column resolve via
    # object truthiness and silently DROP predicates — wrong rows, no error.
    with pytest.raises(ValueError, match="Cannot convert column into bool"):
        bool(F.col("a") > 1)
    with pytest.raises(ValueError, match="Cannot convert column into bool"):
        _ = (F.col("a") > 1) and (F.col("b") > 2)  # the classic migrating-script mistake
    with pytest.raises(ValueError, match="Cannot convert column into bool"):
        _ = (F.col("a") > 1) or (F.col("b") > 2)
    with pytest.raises(ValueError, match="Cannot convert column into bool"):
        if F.col("a"):  # pragma: no cover — the raise IS the assertion
            pass
    with pytest.raises(ValueError, match="Cannot convert column into bool"):
        # list.__contains__ probes membership via `==` → Column → bool → must raise, not be truthy.
        _ = F.col("x") in [1, 2]


def test_in_operator_against_column_raises() -> None:
    # PySpark parity: `value in column` raises loudly (membership belongs in a filter/WHERE).
    with pytest.raises(ValueError, match="Cannot apply 'in' operator against a column"):
        _ = 1 in F.col("x")


# ==================================================================================================
# column.py — alias / cast
# ==================================================================================================


def test_alias_renames(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a").select(F.col("a").alias("renamed"))
    assert df.to_arrow().column_names == ["renamed"]


@pytest.mark.parametrize(
    ("data_type", "arrow_predicate"),
    [
        (T.StringType(), pa.types.is_string),
        (T.IntegerType(), pa.types.is_int32),
        (T.DoubleType(), pa.types.is_float64),
        (T.BooleanType(), pa.types.is_boolean),
        (T.DateType(), pa.types.is_date32),
        (T.TimestampType(), pa.types.is_timestamp),
    ],
)
def test_cast_to_type_object(
    spark: ReparkSession, data_type: T.DataType, arrow_predicate: object
) -> None:
    # Cast a string literal that each target can parse; assert the resulting Arrow type.
    source = "2024-03-15" if isinstance(data_type, (T.DateType, T.TimestampType)) else "1"
    if isinstance(data_type, T.BooleanType):
        source = "true"
    df = spark.sql(f"SELECT '{source}' AS v").withColumn("c", F.col("v").cast(data_type))
    field_type = df.to_arrow().schema.field("c").type
    assert arrow_predicate(field_type)  # type: ignore[operator]


def test_cast_to_decimal_preserves_precision_scale(spark: ReparkSession) -> None:
    # Risk: DECIMAL precision/scale drift is a silent P&L bug — pin the exact Arrow decimal type.
    df = spark.sql("SELECT '12.3456' AS v").withColumn("c", F.col("v").cast(T.DecimalType(10, 4)))
    field_type = df.to_arrow().schema.field("c").type
    assert pa.types.is_decimal(field_type)
    assert field_type.precision == 10
    assert field_type.scale == 4


def test_cast_accepts_string_type_spec(spark: ReparkSession) -> None:
    df = spark.sql("SELECT '5' AS v").withColumn("c", F.col("v").cast("int"))
    assert pa.types.is_int32(df.to_arrow().schema.field("c").type)


def test_cast_accepts_long_and_bigint_type_spec(spark: ReparkSession) -> None:
    # R2 enabler: the PySpark integer-width spellings cast to Int64 (no `types` object emits them,
    # but `.cast("long")` / `.cast("bigint")` are PySpark idioms and the na-fill width-preserving
    # path needs Int64). A float source truncates toward zero (Arrow cast), like Spark.
    for spec in ("long", "bigint"):
        df = spark.sql("SELECT 2.9 AS v").withColumn("c", F.col("v").cast(spec))
        table = df.to_arrow()
        assert pa.types.is_int64(table.schema.field("c").type), f"cast('{spec}') → int64"
        assert table.column("c").to_pylist() == [2], f"cast('{spec}') truncates 2.9 → 2"


# ==================================================================================================
# functions.py — coalesce / concat / current_timestamp
# ==================================================================================================


def test_coalesce_fills_nulls(spark: ReparkSession) -> None:
    df = spark.sql("SELECT * FROM (VALUES ('x'), (NULL)) AS t(name)").withColumn(
        "m", F.coalesce(F.col("name"), F.lit("unknown"))
    )
    assert _rows(df.select("m")) == [{"m": "unknown"}, {"m": "x"}]


def test_concat_joins_strings(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 'a' AS x, 'b' AS y").withColumn(
        "j", F.concat(F.col("x"), F.lit("_"), F.col("y"))
    )
    assert df.to_arrow().column("j").to_pylist() == ["a_b"]


def test_concat_propagates_null(spark: ReparkSession) -> None:
    # PySpark `concat` returns NULL if ANY argument is NULL (DataFusion's concat instead skips
    # nulls as empty strings — the engine must guard against that divergence).
    df = spark.sql("SELECT 'a' AS x, CAST(NULL AS STRING) AS y").withColumn(
        "j", F.concat(F.col("x"), F.col("y"))
    )
    assert df.to_arrow().column("j").to_pylist() == [None]


def test_sql_concat_coalesce_null_returns_utf8_not_view(spark: ReparkSession) -> None:
    """SQL ``concat(coalesce(NULL, ''), …)`` must not plan/kernel Utf8/Utf8View-mismatch.

    TPC-DS Q5/Q80/Q84 (D2): datafusion-spark's concat promised Utf8 but the DF kernel
    returned Utf8View after coalesce folded a null to Utf8View(\"\"). repark-functions
    SparkConcat coerces args to Utf8 and always emits Utf8 (Arrow string, not view).
    """
    table = spark.sql(
        "SELECT concat(concat(coalesce(CAST(NULL AS VARCHAR), ''), ', '), 'Ann') AS name"
    ).to_arrow()
    field = table.schema.field("name")
    assert pa.types.is_string(field.type), f"expected Utf8 string, got {field.type!r}"
    assert not pa.types.is_string_view(field.type)
    assert table.column("name").to_pylist() == [", Ann"]


def test_sql_concat_any_null_propagates(spark: ReparkSession) -> None:
    """SQL-path concat (not only Column API) must match Spark any-NULL → NULL."""
    table = spark.sql("SELECT concat('a', CAST(NULL AS VARCHAR), 'b') AS j").to_arrow()
    assert table.column("j").to_pylist() == [None]


def test_sql_concat_stringifies_non_string_args(spark: ReparkSession) -> None:
    """Spark SQL ``concat`` stringifies non-string args (``concat(1, 2)`` → ``'12'``)."""
    table = spark.sql("SELECT concat(1, 2) AS j").to_arrow()
    field = table.schema.field("j")
    assert pa.types.is_string(field.type), f"expected Utf8 string, got {field.type!r}"
    assert table.column("j").to_pylist() == ["12"]


def test_sql_concat_array_any_null_propagates_per_row(spark: ReparkSession) -> None:
    """SQL multi-row any-NULL → NULL via ReparkSession (registry SparkConcat Apply path).

    Column API embeds DF concat + CASE (separate from the UDF overwrite). This pin is the
    shipping SQL surface used by TPC-DS — mutation-proofs per-row null mask under the facade.
    """
    table = spark.sql(
        """
        SELECT concat(a, b) AS j FROM (VALUES
            ('x', CAST(NULL AS VARCHAR)),
            ('y', 'z'),
            (CAST(NULL AS VARCHAR), 'w')
        ) AS t(a, b)
        """
    ).to_arrow()
    field = table.schema.field("j")
    assert pa.types.is_string(field.type), f"expected Utf8 string, got {field.type!r}"
    assert not pa.types.is_string_view(field.type)
    assert table.column("j").to_pylist() == [None, "yz", None]


def test_current_timestamp_is_a_timestamp(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a").withColumn("ts", F.current_timestamp())
    assert pa.types.is_timestamp(df.to_arrow().schema.field("ts").type)


def test_current_timestamp_camelcase_alias() -> None:
    assert F.currentTimestamp is F.current_timestamp


# ==================================================================================================
# dataframe.py — transform surface
# ==================================================================================================


def test_with_column_and_camelcase_alias(spark: ReparkSession) -> None:
    from repark import DataFrame

    assert DataFrame.withColumn is DataFrame.with_column
    df = spark.sql("SELECT 1 AS a").withColumn("b", F.col("a") + 1)
    assert _rows(df) == [{"a": 1, "b": 2}]


def test_filter_where_column_and_sql(spark: ReparkSession) -> None:
    df = spark.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(a)")
    assert _rows(df.filter(F.col("a") > 1)) == [{"a": 2}, {"a": 3}]
    assert _rows(df.where(F.col("a") > 1)) == [{"a": 2}, {"a": 3}]
    # SQL-string predicate resolves against the frame's own schema.
    assert _rows(df.filter("a <= 2")) == [{"a": 1}, {"a": 2}]
    assert _rows(df.where("a = 3")) == [{"a": 3}]


def test_filter_rejects_bad_type(spark: ReparkSession) -> None:
    with pytest.raises(TypeError, match="NOT_COLUMN_OR_STR"):
        spark.sql("SELECT 1 AS a").filter(123)  # type: ignore[arg-type]


def test_select_by_name_and_by_column(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a, 2 AS b, 3 AS c")
    assert df.select("a", "c").to_arrow().column_names == ["a", "c"]
    assert df.select(F.col("b").alias("bb")).to_arrow().column_names == ["bb"]


def test_drop_removes_columns(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS a, 2 AS b, 3 AS c").drop("b")
    assert df.to_arrow().column_names == ["a", "c"]
    # Dropping an absent column is a no-op (Spark semantics).
    assert df.drop("nonexistent").to_arrow().column_names == ["a", "c"]


def test_order_by_desc_and_asc(spark: ReparkSession) -> None:
    df = spark.sql("SELECT * FROM (VALUES (1), (3), (2)) AS t(a)")
    desc = df.orderBy(F.col("a").desc()).to_arrow().column("a").to_pylist()
    assert desc == [3, 2, 1]
    asc = df.sort(F.col("a").asc()).to_arrow().column("a").to_pylist()
    assert asc == [1, 2, 3]
    # Bare name defaults to ascending; the `ascending` keyword flips it.
    assert df.orderBy("a").to_arrow().column("a").to_pylist() == [1, 2, 3]
    assert df.orderBy("a", ascending=False).to_arrow().column("a").to_pylist() == [3, 2, 1]


def test_order_by_nulls_ordering(spark: ReparkSession) -> None:
    # Spark: ascending → nulls first, descending → nulls last.
    df = spark.sql("SELECT * FROM (VALUES (2), (NULL), (1)) AS t(a)")
    assert df.orderBy(F.col("a").asc()).to_arrow().column("a").to_pylist() == [None, 1, 2]
    assert df.orderBy(F.col("a").desc()).to_arrow().column("a").to_pylist() == [2, 1, None]


def test_join_inner_merges_key(spark: ReparkSession) -> None:
    left = spark.sql("SELECT * FROM (VALUES (1, 100), (2, 200)) AS l(k, lv)")
    right = spark.sql("SELECT * FROM (VALUES (1, 11), (3, 33)) AS r(k, rv)")
    joined = left.join(right, on="k", how="inner")
    # A single merged key column (Spark semantics), matched rows only.
    assert joined.to_arrow().column_names == ["k", "lv", "rv"]
    assert _rows(joined) == [{"k": 1, "lv": 100, "rv": 11}]


def test_join_left_keeps_unmatched(spark: ReparkSession) -> None:
    left = spark.sql("SELECT * FROM (VALUES (1, 100), (2, 200)) AS l(k, lv)")
    right = spark.sql("SELECT * FROM (VALUES (1, 11)) AS r(k, rv)")
    joined = left.join(right, on="k", how="left")
    assert _rows(joined) == [{"k": 1, "lv": 100, "rv": 11}, {"k": 2, "lv": 200, "rv": None}]


def test_join_on_list_of_keys(spark: ReparkSession) -> None:
    left = spark.sql("SELECT * FROM (VALUES (1, 'a', 100)) AS l(k1, k2, lv)")
    right = spark.sql("SELECT * FROM (VALUES (1, 'a', 11)) AS r(k1, k2, rv)")
    joined = left.join(right, on=["k1", "k2"], how="inner")
    assert joined.to_arrow().column_names == ["k1", "k2", "lv", "rv"]
    assert _rows(joined) == [{"k1": 1, "k2": "a", "lv": 100, "rv": 11}]


def test_join_on_column_condition_keeps_all_columns(spark: ReparkSession) -> None:
    left = spark.sql("SELECT * FROM (VALUES (1, 100), (2, 200)) AS l(lk, lv)")
    right = spark.sql("SELECT * FROM (VALUES (1, 11)) AS r(rk, rv)")
    joined = left.join(right, on=(F.col("lk") == F.col("rk")), how="inner")
    # Expression joins keep both key columns (Spark semantics).
    assert set(joined.to_arrow().column_names) == {"lk", "lv", "rk", "rv"}
    assert _rows(joined) == [{"lk": 1, "lv": 100, "rk": 1, "rv": 11}]


def test_join_rejects_unsupported_how(spark: ReparkSession) -> None:
    left = spark.sql("SELECT 1 AS k")
    right = spark.sql("SELECT 1 AS k")
    with pytest.raises(ValueError, match="unsupported join type"):
        left.join(right, on="k", how="cross")


# ==================================================================================================
# Spark-parity fixtures — goldens recorded from live PySpark 4.1.2 through the differential core.
# Inputs use createDataFrame so BOTH engines genuinely infer the pinned type + nullability; a parity
# pin over inline SQL VALUES would record repark's own int64/nullable/double shape as "Spark" (see
# the module docstring + task/lessons.md 2026-07-19).
# ==================================================================================================


def test_parity_coalesce_cast_chain(spark: ReparkSession) -> None:
    # Mirrors the source publish job's withColumns shape: coalesce a nullable column then cast.
    # createDataFrame (NOT inline VALUES) so both engines infer id=int64/nullable and
    # name=string/nullable (live PySpark 4.1.2). An inline VALUES source would type id as int32 /
    # non-null on Spark, making the int64/nullable golden a repark-only pin.
    source = spark.createDataFrame([(1, "x"), (2, None), (3, "z")], ["id", "name"])
    result = (
        source.withColumn("clean_name", F.coalesce(F.col("name"), F.lit("unknown")))
        .withColumn("id_str", F.col("id").cast(T.StringType()))
        .select("id", "clean_name", "id_str")
    )
    # `clean_name` is non-nullable: Spark's coalesce with a non-null literal fallback can never be
    # null, so the golden pins that nullability — and the harness now compares field nullability as
    # part of the schema signature, so the engine must reproduce this Spark-parity guarantee.
    golden = pa.table(
        [
            pa.array([1, 2, 3], pa.int64()),
            pa.array(["x", "unknown", "z"], pa.string()),
            pa.array(["1", "2", "3"], pa.string()),
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("clean_name", pa.string(), nullable=False),
                pa.field("id_str", pa.string(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_filter_orderby_ordered(spark: ReparkSession) -> None:
    # orderBy pins row order → an order-sensitive differential. createDataFrame (NOT inline VALUES)
    # so both engines infer id=int64/nullable and amt=double (live PySpark 4.1.2). An inline VALUES
    # source would type id as int32/non-null and the literal 5.0 as DECIMAL(2,1) on Spark, so the
    # int64/double golden would be a repark-only pin.
    source = spark.createDataFrame([(1, 5.0), (2, 9.0), (3, 1.0)], ["id", "amt"])
    result = source.filter(F.col("id") >= 2).orderBy(F.col("amt").desc()).select("id", "amt")
    golden = pa.table(
        {
            "id": pa.array([2, 3], pa.int64()),
            "amt": pa.array([9.0, 1.0], pa.float64()),
        }
    )
    assert_frames_equal(result.to_arrow(), golden, order_sensitive=True)


def test_parity_integer_division_is_double(spark: ReparkSession) -> None:
    # Spark `/` on integers yields DoubleType (true division): 7/2 == 3.5, 9/2 == 4.5. Pin both
    # the value and the float64 type through the differential core, on integer inputs (the class
    # of input that would truncate under DataFusion's native integer `/`).
    source = spark.sql("SELECT * FROM (VALUES (7, 2), (9, 2)) AS t(a, b)")
    result = source.withColumn("d", F.col("a") / F.col("b")).select("d")
    golden = pa.table({"d": pa.array([3.5, 4.5], pa.float64())})
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_concat_null_propagates(spark: ReparkSession) -> None:
    # Spark concat: any-null → NULL. Row 1 (both non-null) concatenates; row 2 (a null arg) → NULL.
    source = spark.sql("SELECT * FROM (VALUES ('a', 'b'), ('c', CAST(NULL AS STRING))) AS t(x, y)")
    result = source.withColumn("j", F.concat(F.col("x"), F.col("y"))).select("j")
    golden = pa.table({"j": pa.array(["ab", None], pa.string())})
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_end_to_end_chain(spark: ReparkSession) -> None:
    # A full chain: withColumn (arithmetic + cast), filter, join, orderBy — a GENUINE cross-engine
    # parity pin. createDataFrame (NOT inline VALUES) so both engines infer id=int64/nullable and
    # label=string/nullable (live PySpark 4.1.2); an inline VALUES source would type id as int32 /
    # non-null and label as non-null on Spark. The two frames are registered as temp views so the
    # by-name join gets distinct qualifiers — repark materializes an inline createDataFrame as a
    # single-alias subquery, so two of them share a qualifier and cannot be joined directly yet
    # (recorded in task/todo.md).
    facts = spark.createDataFrame([(1, 10.0), (2, 20.0), (3, 30.0)], ["id", "amt"])
    dims = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "label"])
    facts.createOrReplaceTempView("e2e_facts")
    dims.createOrReplaceTempView("e2e_dims")
    facts = spark.sql("SELECT * FROM e2e_facts")
    dims = spark.sql("SELECT * FROM e2e_dims")
    result = (
        facts.withColumn("amt_x2", (F.col("amt") * 2).cast(T.IntegerType()))
        .filter(F.col("id") > 1)
        .join(dims, on="id", how="inner")
        .orderBy(F.col("id").asc())
        .select("id", "label", "amt_x2")
    )
    golden = pa.table(
        {
            "id": pa.array([2, 3], pa.int64()),
            "label": pa.array(["B", "C"], pa.string()),
            "amt_x2": pa.array([40, 60], pa.int32()),
        }
    )
    assert_frames_equal(result.to_arrow(), golden, order_sensitive=True)
