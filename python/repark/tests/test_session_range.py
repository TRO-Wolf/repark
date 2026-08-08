"""X1 — SparkSession.range parity pins (Apache DataFrameTests.test_range + forms)."""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException
from repark.session import _reset_active_session_for_tests


@pytest.fixture
def spark() -> ReparkSession:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-range").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def test_range_end_only(spark: ReparkSession) -> None:
    """range(end) → 0 .. end-1."""
    assert spark.range(3).count() == 3
    assert [row.id for row in spark.range(3).collect()] == [0, 1, 2]


def test_range_apache_counts(spark: ReparkSession) -> None:
    """Apache DataFrameTests.test_range count pins (exclusive end, negative step, large step)."""
    assert spark.range(1, 1).count() == 0
    assert spark.range(1, 0, -1).count() == 1
    assert spark.range(0, 1 << 40, 1 << 39).count() == 2
    assert spark.range(-2).count() == 0
    assert spark.range(3).count() == 3


def test_range_start_end_step(spark: ReparkSession) -> None:
    """range(start, end, step) arithmetic sequence, exclusive end."""
    assert [row.id for row in spark.range(1, 7, 2).collect()] == [1, 3, 5]
    assert [row.id for row in spark.range(1, 0, -1).collect()] == [1]


def test_range_column_name_is_id(spark: ReparkSession) -> None:
    assert spark.range(2).columns == ["id"]


def test_range_step_zero_raises(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="step"):
        spark.range(0, 10, 0)


def test_range_num_partitions_accepted(spark: ReparkSession) -> None:
    """numPartitions is API-parity accepted (ignored on single-node backend)."""
    assert spark.range(0, 5, 1, numPartitions=4).count() == 5


def test_range_float_end_coerced(spark: ReparkSession) -> None:
    """Spark coerces float bounds via int() — range(10e0) → 10."""
    assert spark.range(10.0).count() == 10


def test_range_empty_and_negative_step_values(spark: ReparkSession) -> None:
    """Empty-path SQL + negative-step multisets (octo C1 mutation pins)."""
    assert spark.range(1, 1).collect() == []
    assert spark.range(5, 0, 1).count() == 0
    assert spark.range(0, 5, -1).count() == 0
    assert [row.id for row in spark.range(5, 0, -2).collect()] == [5, 3, 1]
    assert [row.id for row in spark.range(0, 10, 3).collect()] == [0, 3, 6, 9]


def test_range_bool_and_num_partitions_validation(spark: ReparkSession) -> None:
    """bool is rejected (int subclass trap); numPartitions < 1 raises."""
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError, match="int or float"):
        spark.range(True)  # type: ignore[arg-type]
    with pytest.raises(IllegalArgumentException, match="numPartitions"):
        spark.range(0, 5, 1, numPartitions=0)


def test_range_float_step_truncated_and_physical_int64(spark: ReparkSession) -> None:
    """Float step uses int() (Spark Long coerce); Arrow id stays int64 (octo C2)."""
    import pyarrow as pa

    assert [row.id for row in spark.range(0, 5, 1.9).collect()] == [0, 1, 2, 3, 4]
    table = spark.range(3).to_arrow()
    assert pa.types.is_int64(table.schema.field("id").type)
    # X2 remap: Arrow Int64 surfaces as LongType → "bigint" (Spark parity).
    assert spark.range(1).dtypes == [("id", "bigint")]


def test_range_after_stop_raises(spark: ReparkSession) -> None:
    """Stopped session refuses range (octo C3)."""
    spark.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        spark.range(3)
