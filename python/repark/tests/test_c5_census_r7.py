"""C5 census-r7 — Column access surface + sameSemantics + when type-gate pins.

Hour-0 classic FAIL-VALUE/FAIL-MISSING clusters shipped outside P5/H2/R2/M8/U11
regions. Pins exercise Arrow collect value+type (docs/testing.md), not show-only.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark.errors import PySparkTypeError
from repark.spark.column import Column
from repark.spark.functions import col, lit, trim, upper, when
from repark.spark.session import ReparkSession
from repark.spark.types import IntegerType, LongType


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-c5-census-r7").getOrCreate()
    yield session
    session.stop()


# ==================================================================================================
# Column.getItem / getField / __getattr__ (Apache test_access_nested_types family)
# ==================================================================================================


def test_column_getitem_getitem_array_and_map(spark: ReparkSession) -> None:
    """Array int + map str via getItem; values and Arrow types."""
    frame = spark.createDataFrame([([1, 2], {"k": "v"})], ["l", "d"])
    row = frame.select(frame.l.getItem(0).alias("a0"), frame.d.getItem("k").alias("mk")).collect()[
        0
    ]
    assert row["a0"] == 1
    assert row["mk"] == "v"
    table = frame.select(
        frame.l.getItem(0).alias("a0"), frame.d.getItem("k").alias("mk")
    ).to_arrow()
    assert table.column("a0")[0].as_py() == 1
    assert table.column("mk")[0].as_py() == "v"
    assert pa.types.is_integer(table.column("a0").type)
    assert pa.types.is_string(table.column("mk").type) or pa.types.is_large_string(
        table.column("mk").type
    )


def test_column_getfield_and_getattr_struct(spark: ReparkSession) -> None:
    """Struct field via getField and attribute syntax (df.r.b / getField)."""
    frame = spark.range(1).selectExpr("named_struct('a', 1, 'b', 'b') as r")
    via_getfield = frame.select(frame.r.getField("b").alias("fb")).collect()[0][0]
    via_getattr = frame.select(frame.r.b.alias("fb")).collect()[0][0]
    via_getitem = frame.select(frame.r["b"].alias("fb")).collect()[0][0]
    assert via_getfield == "b"
    assert via_getattr == "b"
    assert via_getitem == "b"
    table = frame.select(frame.r.getField("b").alias("fb")).to_arrow()
    assert table.column("fb")[0].as_py() == "b"
    assert pa.types.is_string(table.column("fb").type) or pa.types.is_large_string(
        table.column("fb").type
    )


# ==================================================================================================
# Column.try_cast (Apache test_cast_str_representation + null-on-fail)
# ==================================================================================================


def test_try_cast_display_and_null_on_fail(spark: ReparkSession) -> None:
    """Display form TRY_CAST(...); bad string → NULL; good string → int; Arrow types."""
    assert str(col("a").try_cast("int")) == "Column<'TRY_CAST(a AS INT)'>"
    assert str(col("a").try_cast("INT")) == "Column<'TRY_CAST(a AS INT)'>"
    assert str(col("a").try_cast(IntegerType())) == "Column<'TRY_CAST(a AS INT)'>"
    assert str(col("a").try_cast(LongType())) == "Column<'TRY_CAST(a AS BIGINT)'>"

    frame = spark.createDataFrame([(2, "123"), (5, "Bob"), (3, None)], ["age", "name"])
    out = frame.select(frame.name.try_cast(LongType()).alias("n")).collect()
    assert out[0]["n"] == 123
    assert out[1]["n"] is None
    assert out[2]["n"] is None
    table = frame.select(frame.name.try_cast("double").alias("n")).to_arrow()
    values = [table.column("n")[i].as_py() for i in range(table.num_rows)]
    assert values[0] == 123.0
    assert values[1] is None
    assert values[2] is None
    assert pa.types.is_floating(table.column("n").type)
    long_table = frame.select(frame.name.try_cast(LongType()).alias("n")).to_arrow()
    assert pa.types.is_int64(long_table.column("n").type)
    assert long_table.column("n")[0].as_py() == 123
    assert long_table.column("n")[1].as_py() is None


def test_cast_and_try_cast_reject_non_datatype_or_str() -> None:
    """Apache test_cast_negative grammar: int → NOT_DATATYPE_OR_STR (cast + try_cast)."""
    for method_name in ("cast", "try_cast"):
        with pytest.raises(PySparkTypeError) as caught:
            getattr(col("a"), method_name)(123)
        assert caught.value.getCondition() == "NOT_DATATYPE_OR_STR"
        params = caught.value.getMessageParameters()
        assert params is not None
        assert params.get("arg_name") == "dataType"
        assert params.get("arg_type") == "int"


# ==================================================================================================
# Column.transform (Apache test_transform)
# ==================================================================================================


def test_column_transform_chain(spark: ReparkSession) -> None:
    """Built-in + lambda transform chains."""
    frame = spark.createDataFrame([("  hello  ",), ("  world  ",)], ["text"])
    result = frame.select(frame.text.transform(trim).transform(upper).alias("result")).collect()
    assert result[0][0] == "HELLO"
    assert result[1][0] == "WORLD"

    nums = spark.createDataFrame([(10,), (20,), (30,)], ["value"])
    chained = nums.select(
        nums.value.transform(lambda c: c + 5)
        .transform(lambda c: c * 2)
        .transform(lambda c: c - 10)
        .alias("v")
    ).collect()
    assert [row["v"] for row in chained] == [20, 40, 60]
    table = nums.select(nums.value.transform(lambda c: c + 1).alias("v")).to_arrow()
    assert table.column("v")[0].as_py() == 11
    assert pa.types.is_integer(table.column("v").type)


def test_column_transform_type_gates() -> None:
    """Non-callable f → NOT_CALLABLE; non-Column return → NOT_COLUMN."""
    with pytest.raises(PySparkTypeError) as caught:
        col("x").transform(123)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_CALLABLE"
    params = caught.value.getMessageParameters()
    assert params is not None
    assert params.get("arg_name") == "f"
    assert params.get("arg_type") == "int"

    with pytest.raises(PySparkTypeError) as caught_result:
        col("x").transform(lambda _c: 5)  # type: ignore[arg-type, return-value]
    assert caught_result.value.getCondition() == "NOT_COLUMN"
    result_params = caught_result.value.getMessageParameters()
    assert result_params is not None
    assert result_params.get("arg_name") == "f(column)"
    assert result_params.get("arg_type") == "int"


# ==================================================================================================
# F.when type gate (Apache test_when)
# ==================================================================================================


def test_when_str_condition_raises_not_column() -> None:
    with pytest.raises(PySparkTypeError) as caught:
        when("id", 1)
    assert caught.value.getCondition() == "NOT_COLUMN"
    params = caught.value.getMessageParameters()
    assert params is not None
    assert params.get("arg_name") == "condition"
    assert params.get("arg_type") == "str"


def test_when_chained_str_condition_raises_not_column() -> None:
    """Column.when chain also type-gates condition (sibling of F.when)."""
    with pytest.raises(PySparkTypeError) as caught:
        when(col("id") > 0, 1).when("bad", 2)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_COLUMN"
    params = caught.value.getMessageParameters()
    assert params is not None
    assert params.get("arg_name") == "condition"
    assert params.get("arg_type") == "str"


def test_when_column_condition_still_works(spark: ReparkSession) -> None:
    frame = spark.range(3).select(when(col("id") > 1, lit("hi")).otherwise(lit("lo")).alias("w"))
    values = [row["w"] for row in frame.collect()]
    assert values == ["lo", "lo", "hi"]


# ==================================================================================================
# DataFrame.sameSemantics (Apache test_same_semantics_error)
# ==================================================================================================


def test_same_semantics_non_dataframe_raises(spark: ReparkSession) -> None:
    with pytest.raises(PySparkTypeError) as caught:
        spark.range(10).sameSemantics(1)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_DATAFRAME"
    params = caught.value.getMessageParameters()
    assert params is not None
    assert params.get("arg_name") == "other"
    assert params.get("arg_type") == "int"
    # Snake-case alias is the same method object surface.
    with pytest.raises(PySparkTypeError) as caught_snake:
        spark.range(10).same_semantics(1)  # type: ignore[arg-type]
    assert caught_snake.value.getCondition() == "NOT_DATAFRAME"


def test_same_semantics_identity_true(spark: ReparkSession) -> None:
    left = spark.range(5)
    right = spark.range(5)
    # Best-effort native-handle identity (not Catalyst isomorphism).
    assert left.sameSemantics(left) is True
    assert left.same_semantics(left) is True
    # Two independent range(5) frames do not share `_inner` → False.
    assert left.sameSemantics(right) is False
    assert isinstance(left.sameSemantics(right), bool)


def test_column_getitem_aliases_are_columns() -> None:
    """Surface types: getItem / getField return Column (no missing-attr)."""
    assert isinstance(col("l").getItem(0), Column)
    assert isinstance(col("r").getField("b"), Column)
    assert isinstance(col("r").x, Column)  # __getattr__ field path
