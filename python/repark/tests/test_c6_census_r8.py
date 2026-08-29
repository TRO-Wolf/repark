"""C6 mechanism pins: Catalog.registerFunction / functionExists + UDF.deterministic."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import SparkSession
from repark.spark.functions import udf
from repark.spark.types import IntegerType, StringType


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    session = SparkSession.builder.master("local[1]").appName("test-c6-census").getOrCreate()
    yield session
    session.stop()


def test_catalog_register_function_sql_and_dataframe(spark: SparkSession) -> None:
    """Catalog.registerFunction aliases spark.udf.register (expand FAIL-MISSING cluster)."""
    double = spark.catalog.registerFunction(
        "double_int",
        lambda value: value + value,
        IntegerType(),
    )
    assert double is not None
    assert "double_int" in spark._udf_registry()

    # SQL rewrite path (U8).
    row = spark.sql("SELECT double_int(1) AS d").collect()[0]
    assert row.d == 2

    frame = spark.range(1, 4).select(double("id").alias("d"))
    assert sorted(frame.to_arrow().column("d").to_pylist()) == [2, 4, 6]


def test_catalog_register_function_camel_case_alias(spark: SparkSession) -> None:
    """camelCase registerFunction is the same method as snake_case (class-level alias)."""
    from repark.spark.catalog import Catalog

    # Catalog is reconstructed per ``spark.catalog`` access — compare unbound methods.
    assert Catalog.registerFunction is Catalog.register_function
    spark.catalog.registerFunction("strlen_c6", lambda text: len(text), IntegerType())
    assert spark.sql("SELECT strlen_c6('ab') AS n").collect()[0].n == 2


def test_catalog_function_exists_registry_only(spark: SparkSession) -> None:
    """functionExists probes the session UDF registry (not CREATE FUNCTION / JVM)."""
    from repark.spark.catalog import Catalog

    assert Catalog.functionExists is Catalog.function_exists
    assert spark.catalog.functionExists("func1") is False
    assert spark.catalog.functionExists("default.func1") is False
    assert spark.catalog.functionExists("spark_catalog.default.func1") is False
    assert spark.catalog.functionExists("func1", "default") is False

    spark.catalog.registerFunction("func1", lambda value: value, StringType())
    assert spark.catalog.functionExists("func1") is True
    assert spark.catalog.functionExists("default.func1") is True
    assert spark.catalog.functionExists("spark_catalog.default.func1") is True
    assert spark.catalog.functionExists("func1", "default") is True
    # Case-insensitive registry probe.
    assert spark.catalog.functionExists("FUNC1") is True
    assert spark.catalog.functionExists("missing_fn") is False


def test_udf_deterministic_default_and_as_nondeterministic() -> None:
    """UserDefinedFunction.deterministic defaults True; asNondeterministic flips False."""
    marked = udf(lambda value: value, IntegerType())
    assert marked.deterministic is True
    marked.asNondeterministic()
    assert marked.deterministic is False


def test_catalog_register_preserves_nondeterministic(spark: SparkSession) -> None:
    """registerFunction preserves asNondeterministic flag on the returned callable."""
    random_udf = udf(lambda value: value, IntegerType()).asNondeterministic()
    assert random_udf.deterministic is False
    registered = spark.catalog.registerFunction("rand_identity", random_udf)
    assert registered.deterministic is False
    assert spark.catalog.functionExists("rand_identity") is True
