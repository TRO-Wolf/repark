"""Integer `+` / `-` / `*` overflow pins (F-Y10-1).

Live Spark 4.1.2 (zulu-17) is the oracle. Shared-raise under default ANSI;
two's-complement wrap and Arrow type when ``spark.sql.ansi.enabled=false``.

pins: f-y10-1-int-overflow/C-002
"""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.types import IntegerType, LongType, StructField, StructType


def _spark() -> ReparkSession:
    """Default session (ANSI ON)."""
    return ReparkSession.builder.appName("integer-overflow-parity").getOrCreate()


def _spark_legacy() -> ReparkSession:
    """Session with ``spark.sql.ansi.enabled=false``."""
    return ReparkSession.builder.config("spark.sql.ansi.enabled", "false").getOrCreate()


def test_int32_add_max_plus_one_raises_under_default_ansi() -> None:
    """CAST INT MAX + 1 raises ARITHMETIC_OVERFLOW (shared-raise with Spark)."""
    spark = _spark()
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        spark.sql("SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS v").to_arrow()


def test_int32_add_cast_plus_literal_raises_under_default_ansi() -> None:
    """CAST(INT) + 1 stays INT and raises; it must not widen to int64."""
    spark = _spark()
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        spark.sql("SELECT CAST(2147483647 AS INT) + 1 AS v").to_arrow()


def test_int32_add_max_plus_one_wraps_when_ansi_false() -> None:
    """ansi=false wraps to Int32 MIN, matching Spark."""
    spark = _spark_legacy()
    table = spark.sql("SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS v").to_arrow()
    assert str(table.schema.field("v").type) == "int32"
    assert table.column("v").to_pylist() == [-2147483648]


def test_int32_add_cast_plus_literal_wraps_int32_when_ansi_false() -> None:
    """CAST(INT) + 1 under ansi=false stays Int32 wrap, not int64 widen."""
    spark = _spark_legacy()
    table = spark.sql("SELECT CAST(2147483647 AS INT) + 1 AS v").to_arrow()
    assert str(table.schema.field("v").type) == "int32"
    assert table.column("v").to_pylist() == [-2147483648]


def test_int32_sub_min_minus_one_raises_under_default_ansi() -> None:
    """INT MIN - 1 raises ARITHMETIC_OVERFLOW."""
    spark = _spark()
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        spark.sql("SELECT CAST(-2147483648 AS INT) - CAST(1 AS INT) AS v").to_arrow()


def test_int32_mul_max_times_two_raises_under_default_ansi() -> None:
    """INT MAX * 2 raises ARITHMETIC_OVERFLOW."""
    spark = _spark()
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        spark.sql("SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS v").to_arrow()


def test_int32_mul_max_times_two_wraps_when_ansi_false() -> None:
    """ansi=false INT MAX * 2 wraps to -2."""
    spark = _spark_legacy()
    table = spark.sql("SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS v").to_arrow()
    assert str(table.schema.field("v").type) == "int32"
    assert table.column("v").to_pylist() == [-2]


def test_int64_add_max_plus_one_raises_under_default_ansi() -> None:
    """BIGINT MAX + 1 raises long ARITHMETIC_OVERFLOW."""
    spark = _spark()
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        spark.sql("SELECT CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT) AS v").to_arrow()


def test_int64_add_max_plus_one_wraps_when_ansi_false() -> None:
    """ansi=false BIGINT MAX + 1 wraps to Int64 MIN."""
    spark = _spark_legacy()
    table = spark.sql(
        "SELECT CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT) AS v"
    ).to_arrow()
    assert str(table.schema.field("v").type) == "int64"
    assert table.column("v").to_pylist() == [-9223372036854775808]


def test_int32_add_control_stays_int32() -> None:
    """Non-overflowing INT add keeps Int32 value and type."""
    spark = _spark()
    table = spark.sql("SELECT CAST(2147483646 AS INT) + CAST(1 AS INT) AS v").to_arrow()
    assert str(table.schema.field("v").type) == "int32"
    assert table.column("v").to_pylist() == [2147483647]


def test_facade_int32_add_cols_raises_under_default_ansi() -> None:
    """Facade col(int32)+col(int32) at the boundary raises."""
    spark = _spark()
    schema = StructType(
        [
            StructField("a", IntegerType(), False),
            StructField("b", IntegerType(), False),
        ]
    )
    frame = spark.createDataFrame([(2147483647, 1)], schema)
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        frame.select((F.col("a") + F.col("b")).alias("v")).to_arrow()


def test_facade_int32_add_python_lit_raises_under_default_ansi() -> None:
    """Facade col(int32)+1 raises; Python lit(1) must not widen to int64."""
    spark = _spark()
    schema = StructType([StructField("a", IntegerType(), False)])
    frame = spark.createDataFrame([(2147483647,)], schema)
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        frame.select((F.col("a") + 1).alias("v")).to_arrow()


def test_facade_int32_add_python_lit_wraps_when_ansi_false() -> None:
    """Facade col(int32)+1 under ansi=false wraps as Int32."""
    spark = _spark_legacy()
    schema = StructType([StructField("a", IntegerType(), False)])
    frame = spark.createDataFrame([(2147483647,)], schema)
    table = frame.select((F.col("a") + 1).alias("v")).to_arrow()
    assert str(table.schema.field("v").type) == "int32"
    assert table.column("v").to_pylist() == [-2147483648]


def test_facade_int64_add_cols_raises_under_default_ansi() -> None:
    """Facade col(int64)+col(int64) at the boundary raises."""
    spark = _spark()
    schema = StructType(
        [
            StructField("a", LongType(), False),
            StructField("b", LongType(), False),
        ]
    )
    frame = spark.createDataFrame([(9223372036854775807, 1)], schema)
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        frame.select((F.col("a") + F.col("b")).alias("v")).to_arrow()
