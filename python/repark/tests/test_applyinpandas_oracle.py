"""Live PySpark 4.1.2 oracle for GroupedData.applyInPandas (U6 named deliverable).

Records values, empty input, empty groups (no key present → no func call), null keys,
multi-key, and schema-mismatch class. Skips cleanly when pyspark/JVM is unavailable so
JVM-free ``make py-test-facade`` stays green.
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
        f"applyInPandas oracle needs PySpark >= 4.1, got {pyspark.__version__}",
        allow_module_level=True,
    )

from pyspark.sql import SparkSession as PySparkSession  # noqa: E402

from repark import SparkSession  # noqa: E402
from repark.errors import PySparkException  # noqa: E402

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from applyinpandas_oracle_funcs import count_rows, sum_v, sum_v_global  # noqa: E402


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
            .appName("repark-applyinpandas-oracle")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.executorEnv.PYTHONPATH", _TESTS_DIR)
            .getOrCreate()
        )
    except Exception as error:
        pytest.skip(f"PySpark gateway unavailable for applyInPandas oracle: {error}")
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def spark_repark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]").appName("repark-applyinpandas-oracle").getOrCreate()
    )
    yield session
    session.stop()


def test_oracle_applyinpandas_single_key_sum(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(1, 10), (2, 20), (1, 30), (2, 5)]
    jvm = spark_oracle.createDataFrame(data, "k INT, v INT")
    repark = spark_repark.createDataFrame(data, "k INT, v INT")
    jvm_rows = [
        row.asDict() for row in jvm.groupBy("k").applyInPandas(sum_v, "k INT, total INT").collect()
    ]
    repark_rows = (
        repark.groupBy("k").applyInPandas(sum_v, "k INT, total INT").to_arrow().to_pylist()
    )
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_applyinpandas_multi_key(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(1, "a", 1), (1, "b", 2), (1, "a", 3), (2, "a", 4)]
    schema = "k INT, g STRING, v INT"
    out_schema = "k INT, g STRING, total INT"
    jvm = spark_oracle.createDataFrame(data, schema)
    repark = spark_repark.createDataFrame(data, schema)
    jvm_rows = [
        row.asDict() for row in jvm.groupBy("k", "g").applyInPandas(sum_v, out_schema).collect()
    ]
    repark_rows = repark.groupBy("k", "g").applyInPandas(sum_v, out_schema).to_arrow().to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_applyinpandas_null_keys(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(None, 1), (1, 2), (None, 3), (1, 4)]
    jvm = spark_oracle.createDataFrame(data, "k INT, v INT")
    repark = spark_repark.createDataFrame(data, "k INT, v INT")
    jvm_rows = [
        row.asDict() for row in jvm.groupBy("k").applyInPandas(sum_v, "k INT, total INT").collect()
    ]
    repark_rows = (
        repark.groupBy("k").applyInPandas(sum_v, "k INT, total INT").to_arrow().to_pylist()
    )
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_applyinpandas_empty_input(spark_oracle: Any, spark_repark: SparkSession) -> None:
    jvm = spark_oracle.createDataFrame([], "k INT, v INT")
    repark = spark_repark.createDataFrame([], "k INT, v INT")
    jvm_rows = [
        row.asDict() for row in jvm.groupBy("k").applyInPandas(sum_v, "k INT, total INT").collect()
    ]
    repark_rows = (
        repark.groupBy("k").applyInPandas(sum_v, "k INT, total INT").to_arrow().to_pylist()
    )
    assert jvm_rows == []
    assert repark_rows == []


def test_oracle_applyinpandas_global_groupby(spark_oracle: Any, spark_repark: SparkSession) -> None:
    data = [(1, 10), (2, 20)]
    jvm = spark_oracle.createDataFrame(data, "k INT, v INT")
    repark = spark_repark.createDataFrame(data, "k INT, v INT")
    jvm_rows = [
        row.asDict() for row in jvm.groupBy().applyInPandas(sum_v_global, "total INT").collect()
    ]
    repark_rows = repark.groupBy().applyInPandas(sum_v_global, "total INT").to_arrow().to_pylist()
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_applyinpandas_count_rows_multi_key(
    spark_oracle: Any, spark_repark: SparkSession
) -> None:
    data = [(1, "x", 1), (1, "x", 2), (1, "y", 3)]
    schema = "k INT, g STRING, v INT"
    out_schema = "k INT, g STRING, n INT"
    jvm = spark_oracle.createDataFrame(data, schema)
    repark = spark_repark.createDataFrame(data, schema)
    jvm_rows = [
        row.asDict()
        for row in jvm.groupBy("k", "g").applyInPandas(count_rows, out_schema).collect()
    ]
    repark_rows = (
        repark.groupBy("k", "g").applyInPandas(count_rows, out_schema).to_arrow().to_pylist()
    )
    assert _multiset(repark_rows) == _multiset(jvm_rows)


def test_oracle_applyinpandas_schema_mismatch_class(spark_repark: SparkSession) -> None:
    """Schema mismatch is loud on repark (Spark also fails; exception class may differ)."""

    def wrong(pdf: Any) -> Any:
        import pandas as pd

        return pd.DataFrame({"nope": [1]})

    frame = spark_repark.createDataFrame([(1, 1)], "k INT, v INT")
    out = frame.groupBy("k").applyInPandas(wrong, "k INT, total INT")
    with pytest.raises(PySparkException, match="schema mismatch") as caught:
        out.collect()
    err = caught.value
    print("ORACLE_SCHEMA_MISMATCH_CLASS", type(err).__module__, type(err).__name__)
    print("ORACLE_SCHEMA_MISMATCH_MSG", str(err)[:800])


def test_oracle_applyinpandas_empty_wrong_columns_class(spark_repark: SparkSession) -> None:
    """Empty wrong column names are loud (Spark RESULT_COLUMN_NAMES_MISMATCH class)."""

    def empty_wrong(_pdf: Any) -> Any:
        import pandas as pd

        return pd.DataFrame({"wrong": pd.Series(dtype="int32")})

    frame = spark_repark.createDataFrame([(1, 1), (2, 2)], "k INT, v INT")
    out = frame.groupBy("k").applyInPandas(empty_wrong, "k INT, total INT")
    with pytest.raises(PySparkException, match=r"schema mismatch.*Unexpected: wrong") as caught:
        out.collect()
    print("ORACLE_EMPTY_WRONG_CLASS", type(caught.value).__name__)
    print("ORACLE_EMPTY_WRONG_MSG", str(caught.value)[:400])
