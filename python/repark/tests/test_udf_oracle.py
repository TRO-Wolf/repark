"""Live PySpark 4.1.2 oracle for classic scalar Python udf (U8).

Records values, null handling, type coercion, multi-arg, register+SQL, and error
surfacing. Skips cleanly when pyspark/JVM is unavailable so JVM-free
``make py-test-facade`` stays green.
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
        f"udf oracle needs PySpark >= 4.1, got {pyspark.__version__}",
        allow_module_level=True,
    )

from pyspark.sql import SparkSession as PySparkSession  # noqa: E402
from pyspark.sql.functions import col as spark_col  # noqa: E402
from pyspark.sql.functions import udf as spark_udf  # noqa: E402
from pyspark.sql.types import LongType as SparkLongType  # noqa: E402
from pyspark.sql.types import StringType as SparkStringType  # noqa: E402

from repark import SparkSession  # noqa: E402
from repark.errors import PySparkException  # noqa: E402
from repark.functions import col as repark_col  # noqa: E402
from repark.functions import udf  # noqa: E402
from repark.types import LongType, StringType  # noqa: E402

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
# Spark workers re-import picklable helpers — tests dir must be on PYTHONPATH.
_existing_pythonpath = os.environ.get("PYTHONPATH", "")
if _TESTS_DIR not in _existing_pythonpath.split(os.pathsep):
    if not _existing_pythonpath:
        os.environ["PYTHONPATH"] = _TESTS_DIR
    else:
        os.environ["PYTHONPATH"] = f"{_TESTS_DIR}{os.pathsep}{_existing_pythonpath}"

from udf_oracle_funcs import (  # noqa: E402
    add_long,
    boom,
    double_long,
    null_safe_double,
    upper_str,
)


def _rows(frame: Any) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in frame.collect()]


def _multiset(rows: list[dict[str, Any]]) -> list[tuple[tuple[str, Any], ...]]:
    def cell(value: Any) -> Any:
        if value is None:
            return ("null",)
        return ("v", value)

    packed = [tuple(sorted((key, cell(val)) for key, val in row.items())) for row in rows]
    return sorted(packed)


@pytest.fixture(scope="module")
def spark_oracle() -> Iterator[Any]:
    try:
        session = (
            PySparkSession.builder.master("local[1]")
            .appName("repark-python-udf-oracle")
            .getOrCreate()
        )
    except Exception as error:
        pytest.skip(f"PySpark gateway unavailable for udf oracle: {error}")
    yield session
    session.stop()


@pytest.fixture
def spark_repark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]").appName("repark-python-udf-oracle").getOrCreate()
    )
    yield session
    session.stop()


def test_oracle_udf_double_values(spark_oracle: Any, spark_repark: SparkSession) -> None:
    jvm_fn = spark_udf(double_long, SparkLongType())
    repark_fn = udf(double_long, LongType())
    data = [(1,), (2,), (3,)]
    jvm = spark_oracle.createDataFrame(data, "a long").select(jvm_fn("a").alias("b"))
    repark = spark_repark.createDataFrame(data, "a long").select(repark_fn("a").alias("b"))
    assert _multiset(_rows(jvm)) == _multiset(_rows(repark))


def test_oracle_udf_nulls(spark_oracle: Any, spark_repark: SparkSession) -> None:
    jvm_fn = spark_udf(null_safe_double, SparkLongType())
    repark_fn = udf(null_safe_double, LongType())
    data = [(1,), (None,), (3,)]
    jvm = spark_oracle.createDataFrame(data, "a long").select(jvm_fn("a").alias("b"))
    repark = spark_repark.createDataFrame(data, "a long").select(repark_fn("a").alias("b"))
    assert _multiset(_rows(jvm)) == _multiset(_rows(repark))


def test_oracle_udf_type_coercion_long(spark_oracle: Any, spark_repark: SparkSession) -> None:
    jvm_fn = spark_udf(double_long, SparkLongType())
    repark_fn = udf(double_long, LongType())
    data = [(10,), (20,)]
    jvm = spark_oracle.createDataFrame(data, "a long").select(jvm_fn("a").alias("b"))
    repark = spark_repark.createDataFrame(data, "a long").select(repark_fn("a").alias("b"))
    assert _multiset(_rows(jvm)) == _multiset(_rows(repark))
    # Arrow type pin on repark path (value AND type — never only show).
    arrow = repark.to_arrow()
    assert arrow.schema.field("b").type.bit_width == 64


def test_oracle_udf_multi_arg(spark_oracle: Any, spark_repark: SparkSession) -> None:
    jvm_fn = spark_udf(add_long, SparkLongType())
    repark_fn = udf(add_long, LongType())
    data = [(1, 10), (2, 20)]
    jvm = spark_oracle.createDataFrame(data, "a long, b long").select(jvm_fn("a", "b").alias("s"))
    repark = spark_repark.createDataFrame(data, "a long, b long").select(
        repark_fn("a", "b").alias("s")
    )
    assert _multiset(_rows(jvm)) == _multiset(_rows(repark))


def test_oracle_udf_string(spark_oracle: Any, spark_repark: SparkSession) -> None:
    jvm_fn = spark_udf(upper_str, SparkStringType())
    repark_fn = udf(upper_str, StringType())
    data = [("ab",), ("Cd",)]
    jvm = spark_oracle.createDataFrame(data, "s string").select(jvm_fn("s").alias("u"))
    repark = spark_repark.createDataFrame(data, "s string").select(repark_fn("s").alias("u"))
    assert _multiset(_rows(jvm)) == _multiset(_rows(repark))


def test_oracle_udf_error_surfacing(spark_oracle: Any, spark_repark: SparkSession) -> None:
    jvm_fn = spark_udf(boom, SparkLongType())
    repark_fn = udf(boom, LongType())
    data = [(1,)]
    jvm_df = spark_oracle.createDataFrame(data, "a long").select(jvm_fn("a"))
    repark_df = spark_repark.createDataFrame(data, "a long").select(repark_fn("a"))
    with pytest.raises(Exception, match="oracle-udf-boom"):
        jvm_df.collect()
    with pytest.raises(PySparkException, match="oracle-udf-boom"):
        repark_df.collect()


def test_oracle_udf_with_column(spark_oracle: Any, spark_repark: SparkSession) -> None:
    jvm_fn = spark_udf(double_long, SparkLongType())
    repark_fn = udf(double_long, LongType())
    data = [(1,), (2,)]
    jvm = spark_oracle.createDataFrame(data, "a long").withColumn("b", jvm_fn(spark_col("a")))
    repark = spark_repark.createDataFrame(data, "a long").withColumn(
        "b", repark_fn(repark_col("a"))
    )
    assert _multiset([{"b": r["b"]} for r in _rows(jvm)]) == _multiset(
        [{"b": r["b"]} for r in _rows(repark)]
    )


def test_oracle_udf_register_sql(spark_oracle: Any, spark_repark: SparkSession) -> None:
    spark_oracle.udf.register("oracle_double", double_long, SparkLongType())
    spark_repark.udf.register("oracle_double", double_long, LongType())
    data = [(1,), (2,)]
    spark_oracle.createDataFrame(data, "a long").createOrReplaceTempView("t_oracle_udf")
    spark_repark.createDataFrame(data, "a long").createOrReplaceTempView("t_oracle_udf")
    jvm = spark_oracle.sql("SELECT oracle_double(a) AS b FROM t_oracle_udf")
    repark = spark_repark.sql("SELECT oracle_double(a) AS b FROM t_oracle_udf")
    assert _multiset(_rows(jvm)) == _multiset(_rows(repark))
