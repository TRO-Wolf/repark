"""FN-E — collections facade wrappers (value + Arrow type).

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow path
(``to_arrow()``): value AND type. Alias names resolve and share a behavior case
with their canonical. ``get`` pins the 0-based vs SQL ``element_at`` 1-based
hazard (index 0 / 1 / NULL).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException, PySparkValueError
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-e").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _is_string(arrow_type: pa.DataType) -> bool:
    return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)


# Aliases: resolve + one behavior case


def test_cardinality_and_array_size_alias_of_size(spark: ReparkSession) -> None:
    assert callable(F.cardinality)
    assert callable(F.array_size)
    frame = spark.sql("SELECT array(10, 20, 30) AS a, map(1, 'x', 2, 'y') AS m")
    table = _table(
        frame.select(
            F.cardinality("a").alias("ca"),
            F.array_size("a").alias("sa"),
            F.size("a").alias("za"),
            F.cardinality("m").alias("cm"),
            F.array_size("m").alias("sm"),
            F.size("m").alias("zm"),
        )
    )
    assert table.column("ca").to_pylist() == table.column("sa").to_pylist() == [3]
    assert table.column("za").to_pylist() == [3]
    assert table.column("cm").to_pylist() == table.column("sm").to_pylist() == [2]
    assert table.column("zm").to_pylist() == [2]
    assert table.schema.field("ca").type == table.schema.field("za").type
    assert table.schema.field("sa").type == table.schema.field("za").type
    assert pa.types.is_integer(table.schema.field("ca").type)


def test_array_agg_alias_of_collect_list(spark: ReparkSession) -> None:
    assert callable(F.array_agg)
    frame = spark.createDataFrame([(1,), (None,), (2,), (1,)], ["x"])
    table = _table(frame.agg(F.array_agg("x").alias("aa"), F.collect_list("x").alias("cl")))
    array_agg_values = list(table.column("aa").to_pylist()[0])
    collect_list_values = list(table.column("cl").to_pylist()[0])
    assert None not in array_agg_values
    assert sorted(array_agg_values) == sorted(collect_list_values) == [1, 1, 2]
    assert pa.types.is_list(table.schema.field("aa").type)
    assert table.schema.field("aa").type == table.schema.field("cl").type


# SHIMs


def test_named_struct_fields_value_and_type(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(7, "z")], ["x", "y"])
    table = _table(frame.select(F.named_struct("a", "x", "b", "y").alias("s")))
    assert table.to_pylist() == [{"s": {"a": 7, "b": "z"}}]
    field_type = table.schema.field("s").type
    assert pa.types.is_struct(field_type)
    assert field_type.field("a").name == "a"
    assert pa.types.is_integer(field_type.field("a").type)
    assert _is_string(field_type.field("b").type)


def test_named_struct_rejects_odd_length() -> None:
    with pytest.raises(PySparkValueError, match="even number"):
        F.named_struct("a", 1, "b")


def test_map_contains_key(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT map(1, 'a', 2, 'b') AS m")
    table = _table(
        frame.select(
            F.map_contains_key("m", 2).alias("hit"),
            F.map_contains_key("m", 9).alias("miss"),
        )
    )
    assert table.column("hit").to_pylist() == [True]
    assert table.column("miss").to_pylist() == [False]
    assert pa.types.is_boolean(table.schema.field("hit").type)


def test_array_append_and_prepend(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT array(1, 2, 3) AS a")
    table = _table(
        frame.select(
            F.array_append("a", 4).alias("ap"),
            F.array_prepend("a", 0).alias("pp"),
        )
    )
    assert table.column("ap").to_pylist() == [[1, 2, 3, 4]]
    assert table.column("pp").to_pylist() == [[0, 1, 2, 3]]
    assert pa.types.is_list(table.schema.field("ap").type)
    assert pa.types.is_integer(table.schema.field("ap").type.value_type)


def test_array_append_null_array_and_null_element(spark: ReparkSession) -> None:
    null_array = _table(
        spark.sql("SELECT CAST(NULL AS ARRAY<INT>) AS a").select(
            F.array_append("a", 4).alias("ap"),
            F.array_prepend("a", 4).alias("pp"),
        )
    )
    assert null_array.column("ap").to_pylist() == [None]
    assert null_array.column("pp").to_pylist() == [None]
    assert pa.types.is_list(null_array.schema.field("ap").type)

    with_null = _table(
        spark.sql("SELECT array(1, 2) AS a").select(F.array_append("a", None).alias("ap"))
    )
    assert with_null.column("ap").to_pylist() == [[1, 2, None]]


def test_arrays_overlap(spark: ReparkSession) -> None:
    yes = _table(
        spark.sql("SELECT array(1, 2, 3) AS a, array(3, 4) AS b").select(
            F.arrays_overlap("a", "b").alias("o")
        )
    )
    no = _table(
        spark.sql("SELECT array(1, 2) AS a, array(3, 4) AS b").select(
            F.arrays_overlap("a", "b").alias("o")
        )
    )
    nulls_only = _table(
        spark.sql(
            "SELECT array(1, CAST(NULL AS INT)) AS a, array(CAST(NULL AS INT), 2) AS b"
        ).select(F.arrays_overlap("a", "b").alias("o"))
    )
    null_array = _table(
        spark.sql("SELECT CAST(NULL AS ARRAY<INT>) AS a, array(1) AS b").select(
            F.arrays_overlap("a", "b").alias("o")
        )
    )
    assert yes.column("o").to_pylist() == [True]
    assert no.column("o").to_pylist() == [False]
    assert nulls_only.column("o").to_pylist() == [False]
    assert null_array.column("o").to_pylist() == [None]
    assert pa.types.is_boolean(yes.schema.field("o").type)


# SEMANTIC-HAZARD: get is 0-based; SQL element_at is 1-based and rejects 0


def test_get_is_zero_based_vs_sql_element_at(spark: ReparkSession) -> None:
    """Index 0 / 1 / NULL: ``get`` is Spark 0-based; SQL ``element_at`` is 1-based."""
    frame = spark.sql("SELECT array(10, 20, 30) AS a")
    table = _table(
        frame.select(
            F.get("a", 0).alias("g0"),
            F.get("a", 1).alias("g1"),
            F.get("a", None).alias("gn"),
        )
    )
    assert table.column("g0").to_pylist() == [10]
    assert table.column("g1").to_pylist() == [20]
    assert table.column("gn").to_pylist() == [None]
    assert pa.types.is_integer(table.schema.field("g0").type)
    assert table.schema.field("g0").type == table.schema.field("g1").type

    sql = _table(
        spark.sql(
            "SELECT element_at(array(10, 20, 30), 1) AS e1, "
            "element_at(array(10, 20, 30), CAST(NULL AS INT)) AS en"
        )
    )
    assert sql.column("e1").to_pylist() == [10]
    assert sql.column("en").to_pylist() == [None]
    assert sql.schema.field("e1").type == table.schema.field("g0").type

    with pytest.raises(PySparkException, match="INVALID_INDEX_OF_ZERO"):
        spark.sql("SELECT element_at(array(10, 20, 30), 0)").to_arrow()


def test_get_map_by_key(spark: ReparkSession) -> None:
    """Map lookup through ``get`` still works — but the key must be a Column.

    PySpark 4.1.2 ``get(col, index)`` is ``ColumnOrName`` (it only wraps a bare ``int`` in
    ``lit``), so a bare ``str`` is a **column name**, not a map key — the opposite of what
    the same call means on Spark. ``F.element_at`` keeps the literal-key convenience (W1).
    """
    frame = spark.sql("SELECT map('k', 1, 'z', 2) AS m, 'z' AS which")
    table = _table(
        frame.select(
            F.get("m", F.lit("z")).alias("hit"),
            F.get("m", F.lit("no")).alias("miss"),
            F.get("m", "which").alias("by_column_name"),
        )
    )
    assert table.column("hit").to_pylist() == [2]
    assert table.column("miss").to_pylist() == [None]
    assert table.column("by_column_name").to_pylist() == [2]
    assert pa.types.is_integer(table.schema.field("hit").type)
