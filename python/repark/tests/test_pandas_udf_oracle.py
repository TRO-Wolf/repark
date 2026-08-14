"""Live PySpark 4.1.2 oracle for pandas_udf (U7 SCALAR + M5 SCALAR_ITER / GROUPED_AGG).

Records values, null handling, type coercion, multi-arg, iterator form, pure
grouped-agg, and error surfacing. Skips cleanly when pyspark/JVM is unavailable
so JVM-free ``make py-test-facade`` stays green.

Not Apache ``test_pandas_udf*`` census claims (charter OUT).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

_ZULU = Path("/usr/lib/jvm/zulu-17-amd64")
if _ZULU.is_dir():
    os.environ["JAVA_HOME"] = str(_ZULU)
    os.environ["PATH"] = f"{_ZULU / 'bin'}:{os.environ.get('PATH', '')}"

import pyspark  # noqa: E402 — after JAVA_HOME

if tuple(int(part) for part in pyspark.__version__.split(".")[:2]) < (4, 1):
    pytest.skip(
        f"pandas_udf oracle needs PySpark >= 4.1, got {pyspark.__version__}",
        allow_module_level=True,
    )

from pyspark.sql import SparkSession as PySparkSession  # noqa: E402
from pyspark.sql.functions import PandasUDFType as SparkPandasUDFType  # noqa: E402
from pyspark.sql.functions import pandas_udf as spark_pandas_udf  # noqa: E402

from repark import SparkSession  # noqa: E402
from repark.errors import PySparkException  # noqa: E402
from repark.spark.functions import PandasUDFType, col, pandas_udf  # noqa: E402

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from pandas_udf_oracle_funcs import (  # noqa: E402
    add_long,
    boom,
    double_long,
    double_long_iter,
    mean_double_agg,
    null_safe_double,
    upper_str,
)


def _multiset(rows: list[dict[str, Any]]) -> list[tuple[tuple[str, Any], ...]]:
    def cell(value: Any) -> Any:
        if value is None:
            return ("null",)
        if isinstance(value, float) and value != value:
            return ("nan",)
        return ("v", value)

    packed = [tuple(sorted((key, cell(val)) for key, val in row.items())) for row in rows]
    return sorted(packed)


@pytest.fixture(scope="module")
def spark_oracle() -> Iterator[Any]:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    try:
        session = (
            PySparkSession.builder.master("local[1]")
            .appName("repark-pandas-udf-oracle")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.executorEnv.PYTHONPATH", _TESTS_DIR)
            .getOrCreate()
        )
    except Exception as error:
        pytest.skip(f"PySpark gateway unavailable for pandas_udf oracle: {error}")
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def spark_repark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]").appName("repark-pandas-udf-oracle").getOrCreate()
    )
    yield session
    session.stop()


def test_oracle_pandas_udf_double_values(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(1,), (2,), (3,)]
    jvm_fn = spark_pandas_udf("long")(double_long)
    repark_fn = pandas_udf("long")(double_long)
    jvm = spark_oracle.createDataFrame(data, "x INT")
    repark = spark_repark.createDataFrame(data, "x INT")
    jvm_rows = [row.asDict() for row in jvm.select(jvm_fn("x").alias("y")).collect()]
    repark_rows = repark.select(repark_fn(col("x")).alias("y")).to_arrow().to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_pandas_udf_nulls(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(1,), (None,), (3,)]
    jvm_fn = spark_pandas_udf("long")(null_safe_double)
    repark_fn = pandas_udf("long")(null_safe_double)
    jvm = spark_oracle.createDataFrame(data, "x INT")
    repark = spark_repark.createDataFrame(data, "x INT")
    jvm_rows = [row.asDict() for row in jvm.select(jvm_fn("x").alias("y")).collect()]
    repark_rows = repark.select(repark_fn(col("x")).alias("y")).to_arrow().to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_pandas_udf_type_coercion_long(
    spark_oracle: Any, spark_repark: SparkSession
) -> None:
    data = [(1,), (2,)]
    jvm_fn = spark_pandas_udf("long")(double_long)
    repark_fn = pandas_udf("long")(double_long)
    jvm = spark_oracle.createDataFrame(data, "x INT")
    repark = spark_repark.createDataFrame(data, "x INT")
    jvm_schema = jvm.select(jvm_fn("x").alias("y")).schema
    repark_table = repark.select(repark_fn(col("x")).alias("y")).to_arrow()
    jvm_type = jvm_schema[0].dataType.simpleString()
    # Spark long → bigint; repark Arrow int64 with bigint/long simpleString.
    assert jvm_type in {"bigint", "long"}
    assert repark_table.schema.field("y").type.bit_width == 64
    assert repark_table.column("y").to_pylist() == [
        row.y for row in jvm.select(jvm_fn("x").alias("y")).collect()
    ]


def test_oracle_pandas_udf_multi_arg(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(1, 10), (2, 20)]
    jvm_fn = spark_pandas_udf("long")(add_long)
    repark_fn = pandas_udf("long")(add_long)
    jvm = spark_oracle.createDataFrame(data, "a INT, b INT")
    repark = spark_repark.createDataFrame(data, "a INT, b INT")
    jvm_rows = [row.asDict() for row in jvm.select(jvm_fn("a", "b").alias("s")).collect()]
    repark_rows = repark.select(repark_fn(col("a"), col("b")).alias("s")).to_arrow().to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_pandas_udf_string(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [("hi",), ("Yo",)]
    jvm_fn = spark_pandas_udf("string")(upper_str)
    repark_fn = pandas_udf("string")(upper_str)
    jvm = spark_oracle.createDataFrame(data, "s STRING")
    repark = spark_repark.createDataFrame(data, "s STRING")
    jvm_rows = [row.asDict() for row in jvm.select(jvm_fn("s").alias("u")).collect()]
    repark_rows = repark.select(repark_fn(col("s")).alias("u")).to_arrow().to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_pandas_udf_error_surfacing(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(1,)]
    jvm_fn = spark_pandas_udf("long")(boom)
    repark_fn = pandas_udf("long")(boom)
    jvm = spark_oracle.createDataFrame(data, "x INT")
    repark = spark_repark.createDataFrame(data, "x INT")
    with pytest.raises(Exception, match="oracle-udf-boom"):
        jvm.select(jvm_fn("x").alias("y")).collect()
    with pytest.raises(PySparkException, match="oracle-udf-boom"):
        repark.select(repark_fn(col("x")).alias("y")).to_arrow()


def test_oracle_pandas_udf_with_column(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(1, "a"), (2, "b")]
    jvm_fn = spark_pandas_udf("long")(double_long)
    repark_fn = pandas_udf("long")(double_long)
    jvm = spark_oracle.createDataFrame(data, "x INT, s STRING")
    repark = spark_repark.createDataFrame(data, "x INT, s STRING")
    jvm_rows = [row.asDict() for row in jvm.withColumn("y", jvm_fn("x")).collect()]
    repark_rows = repark.withColumn("y", repark_fn(col("x"))).to_arrow().to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_pandas_udf_scalar_iter(spark_oracle: Any, spark_repark: SparkSession) -> None:
    """M5 SCALAR_ITER: Iterator[Series] → Iterator[Series] vs live PySpark 4.1.2."""
    data = [(1,), (2,), (3,)]
    jvm_fn = spark_pandas_udf("long", SparkPandasUDFType.SCALAR_ITER)(double_long_iter)
    repark_fn = pandas_udf("long", PandasUDFType.SCALAR_ITER)(double_long_iter)
    jvm = spark_oracle.createDataFrame(data, "x INT")
    repark = spark_repark.createDataFrame(data, "x INT")
    jvm_rows = [row.asDict() for row in jvm.select(jvm_fn("x").alias("y")).collect()]
    repark_rows = repark.select(repark_fn(col("x")).alias("y")).to_arrow().to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_pandas_udf_grouped_agg(spark_oracle: Any, spark_repark: SparkSession) -> None:
    """M5 pure GROUPED_AGG vs live PySpark 4.1.2 (value + type on Arrow path)."""
    data = [("a", 1.0), ("a", 3.0), ("b", 10.0), ("b", 20.0)]
    jvm_fn = spark_pandas_udf("double", SparkPandasUDFType.GROUPED_AGG)(mean_double_agg)
    repark_fn = pandas_udf("double", PandasUDFType.GROUPED_AGG)(mean_double_agg)
    jvm = spark_oracle.createDataFrame(data, "k STRING, v DOUBLE")
    repark = spark_repark.createDataFrame(data, "k STRING, v DOUBLE")
    jvm_rows = [row.asDict() for row in jvm.groupBy("k").agg(jvm_fn("v").alias("m")).collect()]
    repark_table = repark.groupBy("k").agg(repark_fn("v").alias("m")).to_arrow()
    repark_rows = repark_table.to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)
    # Type pin: double / float64 on Arrow path (not show-only).
    assert repark_table.schema.field("m").type.bit_width == 64
