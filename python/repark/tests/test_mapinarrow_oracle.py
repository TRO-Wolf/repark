"""Live PySpark 4.1.2 oracle for DataFrame.mapInArrow (U-SPIKE-MAPINARROW).

Named deliverable. Records values, empty-input, empty-iterator-from-func, schema-mismatch
class, multi-batch row multiset. Batch boundaries are non-contractual.

Requires Java 17+ (prefers ``/usr/lib/jvm/zulu-17-amd64``). Skips cleanly when the gateway
cannot launch so JVM-free ``make py-test-facade`` stays green.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

# Prefer the recorded oracle JVM even when ambient JAVA_HOME is Java 11.
_ZULU = Path("/usr/lib/jvm/zulu-17-amd64")
if _ZULU.is_dir():
    os.environ["JAVA_HOME"] = str(_ZULU)
    os.environ["PATH"] = f"{_ZULU / 'bin'}:{os.environ.get('PATH', '')}"

import pyspark  # noqa: E402 — after JAVA_HOME

if tuple(int(part) for part in pyspark.__version__.split(".")[:2]) < (4, 1):
    pytest.skip(
        f"mapInArrow oracle needs PySpark >= 4.1, got {pyspark.__version__}",
        allow_module_level=True,
    )

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import IntegerType, StructField, StructType  # noqa: E402

# Ensure picklable helpers are importable by Spark workers.
_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from mapinarrow_oracle_funcs import (  # noqa: E402
    double_int_batch,
    drop_all_batches,
    wrong_type_batches,
)


@pytest.fixture(scope="module")
def spark_oracle() -> Iterator[SparkSession]:
    # Workers must use the same interpreter that has pyarrow (mapInArrow dependency).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("repark-mapinarrow-oracle")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.executorEnv.PYTHONPATH", _TESTS_DIR)
            .config("spark.driver.extraJavaOptions", "")
            .getOrCreate()
        )
    except Exception as error:
        pytest.skip(f"PySpark gateway unavailable for mapInArrow oracle: {error}")
    try:
        yield session
    finally:
        session.stop()


def test_oracle_mapinarrow_values(spark_oracle: SparkSession) -> None:
    frame = spark_oracle.createDataFrame([(1,), (2,), (3,)], "x INT")
    schema = StructType([StructField("x", IntegerType(), True)])
    out = frame.mapInArrow(double_int_batch, schema)
    rows = sorted(row.x for row in out.collect())
    assert rows == [2, 4, 6]


def test_oracle_mapinarrow_empty_input(spark_oracle: SparkSession) -> None:
    frame = spark_oracle.createDataFrame([], "x INT")
    schema = StructType([StructField("x", IntegerType(), True)])
    out = frame.mapInArrow(double_int_batch, schema)
    assert out.collect() == []
    assert out.schema.names == ["x"]


def test_oracle_mapinarrow_empty_iterator_from_func(spark_oracle: SparkSession) -> None:
    frame = spark_oracle.createDataFrame([(1,), (2,)], "x INT")
    schema = StructType([StructField("x", IntegerType(), True)])
    out = frame.mapInArrow(drop_all_batches, schema)
    assert out.collect() == []


def test_oracle_mapinarrow_schema_mismatch_class(spark_oracle: SparkSession) -> None:
    """Type mismatch vs declared schema — Spark is loud; name mismatch is positional (not loud)."""
    frame = spark_oracle.createDataFrame([(1,)], "x INT")
    schema = StructType([StructField("x", IntegerType(), True)])
    out = frame.mapInArrow(wrong_type_batches, schema)
    with pytest.raises(Exception) as caught:
        out.collect()
    err = caught.value
    print("ORACLE_SCHEMA_MISMATCH_CLASS", type(err).__module__, type(err).__name__)
    print("ORACLE_SCHEMA_MISMATCH_MSG", str(err)[:800])
    assert err is not None


def test_oracle_mapinarrow_multi_batch_row_multiset(spark_oracle: SparkSession) -> None:
    frame = spark_oracle.range(0, 50).selectExpr("cast(id as int) as x").repartition(4)
    schema = StructType([StructField("x", IntegerType(), True)])
    out = frame.mapInArrow(double_int_batch, schema)
    rows = sorted(row.x for row in out.collect())
    assert rows == [i * 2 for i in range(50)]
