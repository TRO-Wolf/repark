"""X3 census pins — DataFrame error-class seed + high-leverage surface (test_dataframe).

Apache suite rows gated here: error kwargs / getQueryContext, drop(Column), join(on=None),
sample seed stability, session.conf, count(star), table(None), show diagnostics, toDF types.
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout

import pytest

from repark import DataFrame, ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkTypeError,
)
from repark.spark.row import Row
from repark.spark.storage import StorageLevel


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-x3-dataframe").getOrCreate()
    yield session
    session.stop()


# Error kwargs / getQueryContext seed (check_error surface)


def test_pyspark_type_error_errorclass_kwargs_and_query_context() -> None:
    params = {"arg_name": "tableName", "arg_type": "NoneType"}
    error = PySparkTypeError(
        errorClass="NOT_STR",
        messageParameters=params,
    )
    assert error.getCondition() == "NOT_STR"
    assert error.getErrorClass() == "NOT_STR"
    assert error.getMessageParameters() == {
        "arg_name": "tableName",
        "arg_type": "NoneType",
    }
    assert error.getQueryContext() == []
    assert error.getSqlState() is None
    assert isinstance(error, TypeError)
    assert isinstance(error, Exception)
    # Caller / returned-dict mutation must not corrupt the stored parameters (octo X3 C2).
    params["arg_name"] = "MUTATED"
    returned = error.getMessageParameters()
    assert returned is not None
    returned["arg_type"] = "MUTATED"
    assert error.getMessageParameters() == {
        "arg_name": "tableName",
        "arg_type": "NoneType",
    }


def test_table_none_raises_not_str(spark: ReparkSession) -> None:
    with pytest.raises(PySparkTypeError) as caught:
        spark.table(None)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_STR"
    assert caught.value.getMessageParameters() == {
        "arg_name": "tableName",
        "arg_type": "NoneType",
    }
    assert caught.value.getQueryContext() == []


def test_where_int_raises_not_column_or_str(spark: ReparkSession) -> None:
    with pytest.raises(PySparkTypeError) as caught:
        spark.range(3).where(10)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_COLUMN_OR_STR"
    assert caught.value.getMessageParameters() == {
        "arg_name": "condition",
        "arg_type": "int",
    }


def test_colregex_int_raises_not_str(spark: ReparkSession) -> None:
    with pytest.raises(PySparkTypeError) as caught:
        spark.range(3).colRegex(10)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_STR"
    assert caught.value.getMessageParameters() == {
        "arg_name": "colName",
        "arg_type": "int",
    }


def test_with_columns_renamed_tuple_raises_not_dict(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Alice", 50)], ["name", "age"])
    with pytest.raises(PySparkTypeError) as caught:
        frame.withColumnsRenamed(("name", "x"))  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_DICT"
    assert caught.value.getMessageParameters() == {
        "arg_name": "colsMap",
        "arg_type": "tuple",
    }


def test_drop_duplicates_str_raises_not_list_or_tuple(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Alice", 50)], ["name", "age"])
    with pytest.raises(PySparkTypeError) as caught:
        frame.dropDuplicates("name")  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_LIST_OR_TUPLE"
    assert caught.value.getMessageParameters() == {
        "arg_name": "subset",
        "arg_type": "str",
    }


def test_drop_duplicates_empty_subset_is_full_distinct(spark: ReparkSession) -> None:
    """Empty subset must not hit DataFusion empty ORDER BY (octo X3 C3)."""
    frame = spark.createDataFrame(
        [("Alice", 50), ("Alice", 50), ("Bob", 60)],
        ["name", "age"],
    )
    out = frame.dropDuplicates([])
    assert out.count() == 2
    assert set(out.columns) == {"name", "age"}


def test_drop_duplicates_none_element_raises_not_str(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Alice", 50)], ["name", "age"])
    with pytest.raises(PySparkTypeError) as caught:
        frame.dropDuplicates([None])  # type: ignore[list-item]
    assert caught.value.getCondition() == "NOT_STR"
    assert caught.value.getMessageParameters() == {
        "arg_name": "subset",
        "arg_type": "NoneType",
    }


def test_show_bool_n_raises_not_int(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("foo",)])
    with pytest.raises(PySparkTypeError) as caught:
        frame.show(True)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_INT"
    assert caught.value.getMessageParameters() == {
        "arg_name": "n",
        "arg_type": "bool",
    }


def test_show_bad_vertical_and_truncate(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("foo",)])
    with pytest.raises(PySparkTypeError) as caught:
        frame.show(vertical="foo")  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_BOOL"
    with pytest.raises(PySparkTypeError) as caught_trunc:
        frame.show(truncate="foo")  # type: ignore[arg-type]
    assert caught_trunc.value.getCondition() == "NOT_BOOL"
    # Digit string truncate is accepted (Apache test_df_show).
    with redirect_stdout(io.StringIO()):
        frame.show(n=5, truncate="1", vertical=False)


def test_sample_missing_args_error_class(spark: ReparkSession) -> None:
    with pytest.raises(PySparkTypeError) as caught:
        spark.range(1).sample()
    assert caught.value.getCondition() == "NOT_BOOL_OR_FLOAT_OR_INT"
    with pytest.raises(TypeError):
        spark.range(1).sample("a")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        spark.range(1).sample(seed="abc")  # type: ignore[arg-type]
    with pytest.raises(IllegalArgumentException):
        spark.range(1).sample(-1.0).count()


def test_sample_positional_fraction_seed_overload(spark: ReparkSession) -> None:
    """sample(fraction, seed) must not treat the seed as fraction (octo X3 C1)."""
    ids_seed7_a = {row[0] for row in spark.range(200).sample(0.3, 7).collect()}
    ids_seed7_b = {row[0] for row in spark.range(200).sample(0.3, 7).collect()}
    ids_seed8 = {row[0] for row in spark.range(200).sample(0.3, 8).collect()}
    assert ids_seed7_a == ids_seed7_b
    assert ids_seed7_a != ids_seed8
    # Overload must accept the call (not TypeError); count is finite.
    assert spark.range(100).sample(0.3, 7).count() > 0
    # Bool-first form with explicit seed.
    assert (
        spark.range(100).sample(False, 0.5, 3).count()
        == spark.range(100).sample(False, 0.5, 3).count()
    )


def test_to_df_none_raises_not_list_of_str(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("John", 30), ("Alice", 25)])
    with pytest.raises(PySparkTypeError) as caught:
        frame.toDF("key", None)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_LIST_OF_STR"
    renamed = frame.toDF("key", "value")
    assert renamed.schema.simpleString() == "struct<key:string,value:bigint>"


# Surface unblocks


def test_drop_accepts_column(spark: ReparkSession) -> None:
    frame = spark.createDataFrame(
        [("A", 50, "Y"), ("B", 60, "Y")],
        ["name", "age", "active"],
    )
    assert frame.drop(F.col("name")).columns == ["age", "active"]
    assert frame.drop(F.col("name"), F.col("age")).columns == ["active"]


def test_join_on_none_cross_and_invalid_how(spark: ReparkSession) -> None:
    left = spark.range(1).toDF("a")
    right = spark.range(1).toDF("b")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    assert left.join(right, how="inner").collect() == [Row(a=0, b=0)]
    spark.conf.set("spark.sql.crossJoin.enabled", "false")
    with pytest.raises(AnalysisException):
        left.join(right, how="inner").collect()
    with pytest.raises(AnalysisException):
        left.join(right, how="invalid-join-type")
    # how=cross always allowed (no conf gate); conf=false does not block crossJoin.
    assert left.crossJoin(right).count() == 1
    assert left.join(right, how="cross").count() == 1


def test_join_builder_conf_cross_join_disabled() -> None:
    """Builder .config(crossJoin=false) must gate join(on=None) (octo X3 C1)."""
    session = (
        ReparkSession.builder.appName("pytest-x3-builder-cross")
        .config("spark.sql.crossJoin.enabled", "false")
        .getOrCreate()
    )
    try:
        assert session.conf.get("spark.sql.crossJoin.enabled") == "false"
        left = session.range(1).toDF("a")
        right = session.range(1).toDF("b")
        with pytest.raises(AnalysisException):
            left.join(right).collect()
        assert left.crossJoin(right).count() == 1
    finally:
        session.stop()


def test_sample_without_seed_is_action_stable(spark: ReparkSession) -> None:
    sampled = spark.range(10000).sample(0.1)
    counts = [sampled.count() for _ in range(5)]
    assert len(set(counts)) == 1
    assert counts[0] > 0
    # Positional fraction+seed form (Spark sample(0.1, 7)).
    seeded = spark.range(1000).sample(0.1, 7)
    assert seeded.count() == spark.range(1000).sample(0.1, 7).count()
    # Default plan seed is 42 (stable bake-in, not process-global RNG leak).
    default_ids = {row[0] for row in spark.range(200).sample(0.2).collect()}
    seed42_ids = {row[0] for row in spark.range(200).sample(0.2, 42).collect()}
    assert default_ids == seed42_ids


def test_explain_extended_does_not_execute_plan(spark: ReparkSession) -> None:
    """extended=True must not map to EXPLAIN ANALYZE (octo X3 hang fix residual pin)."""
    import io
    import time
    from contextlib import redirect_stdout

    # range(10e10) would hang for minutes if ANALYZE executed; budget << 1s.
    started = time.perf_counter()
    with redirect_stdout(io.StringIO()):
        spark.range(10_000_000_000).explain(True)
    assert time.perf_counter() - started < 2.0


def test_random_split_seed_sensitivity(spark: ReparkSession) -> None:
    """Seeded randomSplit must diverge across seeds (octo X3 C6)."""
    left7 = {row[0] for row in spark.range(200).randomSplit([0.5, 0.5], seed=7)[0].collect()}
    left8 = {row[0] for row in spark.range(200).randomSplit([0.5, 0.5], seed=8)[0].collect()}
    assert left7 != left8
    # Same seed is stable.
    left7_b = {row[0] for row in spark.range(200).randomSplit([0.5, 0.5], seed=7)[0].collect()}
    assert left7 == left7_b


def test_storage_level_equals_pyspark_shape(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(2, 2), (3, 3)])
    assert frame.storageLevel == StorageLevel.NONE
    frame.cache()
    assert frame.storageLevel == StorageLevel.MEMORY_AND_DISK_DESER
    frame.unpersist()
    assert frame.storageLevel == StorageLevel.NONE
    # Duck-type equality vs a foreign object with the same flags.
    foreign = type(
        "ForeignLevel",
        (),
        {
            "useDisk": False,
            "useMemory": False,
            "useOffHeap": False,
            "deserialized": False,
            "replication": 1,
        },
    )()
    assert frame.storageLevel == foreign


def test_count_star_column_forms(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([{"a": 1, "b": "v"}])
    assert frame.select(F.count("*")).columns == ["count(1)"]
    assert frame.select(F.count(F.col("*"))).columns == ["count(1)"]
    assert frame.select(F.count(frame["*"])).columns == ["count(1)"]
    nested = frame.select(F.struct("a", "b").alias("s"))
    assert nested.select(F.count(nested["*"])).columns == ["count(1)"]
    assert nested.select(F.count(F.col("*"))).collect()[0][0] == 1
    # Field names must follow struct args (not DataFusion c0/c1) — octo X3 C4.
    arrow_table = nested.to_arrow()
    struct_type = arrow_table.schema.field("s").type
    assert struct_type.names == ["a", "b"]
    assert nested.collect()[0][0] == {"a": 1, "b": "v"}


def test_session_conf_set_get_unset(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.crossJoin.enabled", False)
    assert spark.conf.get("spark.sql.crossJoin.enabled") == "false"
    spark.conf.unset("spark.sql.crossJoin.enabled")
    assert spark.conf.get("spark.sql.crossJoin.enabled", "true") == "true"


def test_isinstance_dataframe_and_union_classmethod(spark: ReparkSession) -> None:
    frame = spark.range(1)
    assert isinstance(frame, DataFrame)
    assert DataFrame.union(frame, frame).collect() == [Row(id=0), Row(id=0)]
