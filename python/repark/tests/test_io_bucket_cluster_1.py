"""io-bucket-cluster-1 pins: the bucketBy / sortBy / clusterBy writer surface."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    PySparkNotImplementedError,
    PySparkTypeError,
)

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_reader_writer_oracle.json").read_text(encoding="utf-8")
)["cells"]


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A per-test facade session on the default memory catalog."""
    session = ReparkSession.builder.appName("pytest-io-bucket-cluster-1").getOrCreate()
    yield session
    session.stop()


def _kv_frame(spark: ReparkSession) -> Any:
    """The (key, a, b) frame the oracle cells ran against."""
    return spark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")


def _assert_error_cell(error: BaseException, cell_id: str) -> None:
    """Compare a raised error against one recorded error oracle cell."""
    cell = _ORACLE[cell_id]["error"]
    assert type(error).__name__ == cell["raises"]
    if cell["condition"] is not None:
        assert error.getCondition() == cell["condition"]
    if cell["params"] is not None:
        assert error.getMessageParameters() == cell["params"]
    recorded_lines = cell["message"].splitlines()
    actual_lines = str(error).splitlines()
    assert actual_lines[0] == recorded_lines[0]
    if len(recorded_lines) == 1:
        assert str(error) == cell["message"]


def test_bucket_by_rejects_non_int_num_buckets(spark: ReparkSession) -> None:
    """bucketBy numBuckets that is not an int raises NOT_INT at the call (bucketBy_bad_num).

    pins: io-bucket-cluster-1/C-001
    """
    frame = _kv_frame(spark)
    with pytest.raises(PySparkTypeError) as raised:
        frame.write.bucketBy("x", "a")
    _assert_error_cell(raised.value, "bucketBy_bad_num")
    for bad_num in (None, 1.5):
        with pytest.raises(PySparkTypeError) as raised_other:
            frame.write.bucketBy(bad_num, "a")
        assert raised_other.value.getCondition() == "NOT_INT"
        assert raised_other.value.getMessageParameters() == {
            "arg_name": "numBuckets",
            "arg_type": type(bad_num).__name__,
        }
        assert str(raised_other.value) == (
            f"[NOT_INT] Argument `numBuckets` should be an int, got {type(bad_num).__name__}."
        )


def test_bucket_by_accepts_list_first_column_and_returns_writer(
    spark: ReparkSession,
) -> None:
    """A list first column is accepted and both spellings return the writer (bucketBy_list).

    pins: io-bucket-cluster-1/C-001
    """
    frame = _kv_frame(spark)
    listed = frame.write.bucketBy(2, ["a", "b"])
    assert type(listed).__name__ == _ORACLE["bucketBy_list"]["result"]["value"]
    writer = frame.write
    assert writer.bucketBy(2, "a") is writer
    assert writer.bucket_by(2, ["a", "b"], "key") is writer


def test_bucketed_path_save_refused(spark: ReparkSession, tmp_path: Path) -> None:
    """Every path-based save with bucketBy raises _LEGACY_ERROR_TEMP_1312 (bucketBy_save).

    pins: io-bucket-cluster-1/C-001
    """
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.write.bucketBy(2, "a").parquet(str(tmp_path / "bk1"))
    _assert_error_cell(raised.value, "bucketBy_save")
    for call in (
        lambda: frame.write.bucketBy(2, "a").save(str(tmp_path / "bk2")),
        lambda: frame.write.bucketBy(2, ["a"]).csv(str(tmp_path / "bk3")),
        lambda: frame.write.bucketBy(2, "a").json(str(tmp_path / "bk4")),
        lambda: frame.write.bucketBy(2, "a").format("text").save(str(tmp_path / "bk5")),
    ):
        with pytest.raises(AnalysisException) as raised_arm:
            call()
        _assert_error_cell(raised_arm.value, "bucketBy_save")


def test_sort_by_without_bucket_by_refused(spark: ReparkSession, tmp_path: Path) -> None:
    """sortBy without bucketBy refuses at the action; the cell measured saveAsTable.

    pins: io-bucket-cluster-1/C-001
    """
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.write.sortBy("a").saveAsTable("sb_t")
    _assert_error_cell(raised.value, "sortBy_without_bucketBy")
    with pytest.raises(AnalysisException) as raised_snake:
        frame.write.sort_by("a").saveAsTable("sb_t2")
    _assert_error_cell(raised_snake.value, "sortBy_without_bucketBy")
    with pytest.raises(AnalysisException) as raised_path:
        frame.write.sortBy("a").parquet(str(tmp_path / "sb1"))
    _assert_error_cell(raised_path.value, "sortBy_without_bucketBy")


def test_bucket_by_count_bounds_refused_at_save(spark: ReparkSession) -> None:
    """numBuckets <= 0 or > 100000 raises INVALID_BUCKET_COUNT at the save (bucketBy_zero).

    pins: io-bucket-cluster-1/C-001
    """
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.write.bucketBy(0, "a").saveAsTable("bk_zero")
    _assert_error_cell(raised.value, "bucketBy_zero")
    for bad_count in (-1, 100001):
        with pytest.raises(AnalysisException) as raised_other:
            frame.write.bucketBy(bad_count, "a").saveAsTable("bk_zero")
        assert raised_other.value.getCondition() == "INVALID_BUCKET_COUNT"
        assert raised_other.value.getMessageParameters() == {
            "bucketingMaxBuckets": "100000",
            "numBuckets": str(bad_count),
        }
        assert str(raised_other.value) == (
            f"[INVALID_BUCKET_COUNT] Number of buckets should be greater than 0 but less "
            f"than or equal to bucketing.maxBuckets (`100000`). Got `{bad_count}`. "
            "SQLSTATE: 22003"
        )


def test_bucket_by_missing_column_refused(spark: ReparkSession) -> None:
    """A bucket column absent from the frame raises COLUMN_NOT_DEFINED_IN_TABLE.

    pins: io-bucket-cluster-1/C-001
    """
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.write.bucketBy(2, "zz").saveAsTable("bk_missing")
    _assert_error_cell(raised.value, "bucketBy_missing_col")
    assert not spark.catalog.tableExists("bk_missing")


def test_bucket_by_save_as_table_refused_ruling_r1(spark: ReparkSession) -> None:
    """Ruling R-1: a valid bucketed saveAsTable refuses NOT_IMPLEMENTED (bucketBy_saveAsTable).

    Spark records Hive bucket files and DESCRIBE EXTENDED rows; repark has no Hive bucketing.
    pins: io-bucket-cluster-1/C-001
    """
    frame = _kv_frame(spark)
    with pytest.raises(PySparkNotImplementedError) as raised:
        frame.write.bucketBy(2, "a").sortBy("b").saveAsTable("bk_t")
    feature = "bucketBy on an Iceberg table (use writeTo(...).partitionedBy(F.bucket(n, col)))"
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": feature}
    assert str(raised.value) == f"[NOT_IMPLEMENTED] {feature} is not implemented."
    assert not spark.catalog.tableExists("bk_t")


def test_cluster_by_path_save_writes_and_ignores_clustering(
    spark: ReparkSession,
    tmp_path: Path,
) -> None:
    """A path save with clusterBy answers None and writes (clusterBy_save)."""
    frame = _kv_frame(spark)
    destination = tmp_path / "cl1"
    assert frame.write.clusterBy("a").parquet(str(destination)) is None
    assert frame.write.cluster_by("a").parquet(str(tmp_path / "cl2")) is None
    assert spark.read.parquet(str(destination)).to_arrow().num_rows == 2


def test_cluster_by_save_as_table_refused_ruling_r2(spark: ReparkSession) -> None:
    """Ruling R-2: clusterBy saveAsTable refuses NOT_IMPLEMENTED (clusterBy_saveAsTable).

    Spark records clustering columns in the table; repark does not.
    pins: io-bucket-cluster-1/C-002
    """
    frame = _kv_frame(spark)
    with pytest.raises(PySparkNotImplementedError) as raised:
        frame.write.clusterBy("a").saveAsTable("cl_t")
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "clusterBy on an Iceberg table"}
    assert (
        str(raised.value) == "[NOT_IMPLEMENTED] clusterBy on an Iceberg table is not implemented."
    )
    assert not spark.catalog.tableExists("cl_t")
    with pytest.raises(PySparkNotImplementedError):
        frame.write.cluster_by("a").saveAsTable("cl_t")


def test_cluster_by_conflicts_refused_at_save_as_table(spark: ReparkSession) -> None:
    """clusterBy with partitionBy / bucketBy refuses at the save, both call orders.

    pins: io-bucket-cluster-1/C-002
    """
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised_partitioned:
        frame.write.clusterBy("a").partitionBy("b").saveAsTable("cl_t2")
    _assert_error_cell(raised_partitioned.value, "clusterBy_with_partitionBy")
    with pytest.raises(AnalysisException) as raised_partitioned_reverse:
        frame.write.partitionBy("b").clusterBy("a").saveAsTable("cl_t2")
    _assert_error_cell(raised_partitioned_reverse.value, "clusterBy_with_partitionBy")
    with pytest.raises(AnalysisException) as raised_bucketed:
        frame.write.clusterBy("a").bucketBy(2, "b").saveAsTable("cl_t3")
    _assert_error_cell(raised_bucketed.value, "clusterBy_with_bucketBy")
    with pytest.raises(AnalysisException) as raised_bucketed_reverse:
        frame.write.bucketBy(2, "b").clusterBy("a").saveAsTable("cl_t3")
    _assert_error_cell(raised_bucketed_reverse.value, "clusterBy_with_bucketBy")


def test_v2_cluster_by_returns_writer(spark: ReparkSession) -> None:
    """writeTo().clusterBy(col, *cols) returns the V2 writer (v2_clusterBy_type).

    pins: io-bucket-cluster-1/C-002
    """
    frame = _kv_frame(spark)
    writer = frame.writeTo("v2c").clusterBy("a")
    assert type(writer).__name__ == _ORACLE["v2_clusterBy_type"]["result"]["value"]
    assert frame.writeTo("v2c").cluster_by("a", "b") is not None


def test_v2_cluster_by_with_partitioned_by_refused_at_actions(spark: ReparkSession) -> None:
    """V2 clusterBy + partitionedBy refuses at create / replace / createOrReplace.

    pins: io-bucket-cluster-1/C-002
    """
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.writeTo("v2c2").partitionedBy("b").clusterBy("a").create()
    _assert_error_cell(raised.value, "v2_clusterBy_partitionedBy")
    with pytest.raises(AnalysisException) as raised_replace:
        frame.writeTo("v2c3").partitionedBy("b").clusterBy("a").replace()
    _assert_error_cell(raised_replace.value, "v2_clusterBy_partitionedBy")
    with pytest.raises(AnalysisException) as raised_or_replace:
        frame.writeTo("v2c4").partitionedBy("b").cluster_by("a").createOrReplace()
    _assert_error_cell(raised_or_replace.value, "v2_clusterBy_partitionedBy")


def test_v2_cluster_by_create_refused_ruling_r2(spark: ReparkSession) -> None:
    """Ruling R-2 at V2 create: NOT_IMPLEMENTED (v2_clusterBy_create_session_catalog).

    Spark answered None by creating a non-Iceberg session-catalog table; repark refuses.
    pins: io-bucket-cluster-1/C-002
    """
    frame = _kv_frame(spark)
    with pytest.raises(PySparkNotImplementedError) as raised:
        frame.writeTo("v2c").clusterBy("a").create()
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "clusterBy on an Iceberg table"}
    assert not spark.catalog.tableExists("v2c")
    with pytest.raises(PySparkNotImplementedError):
        frame.writeTo("v2c5").cluster_by("a").createOrReplace()


def test_partitioned_save_as_table_still_writes(spark: ReparkSession) -> None:
    """Regression guard: partitionBy saveAsTable CTAS and partition-filtered reads stand.

    pins: io-bucket-cluster-1/C-004
    """
    frame = _kv_frame(spark)
    frame.write.partitionBy("a").saveAsTable("reg_parted")
    rows = spark.sql("SELECT key FROM reg_parted WHERE a = 3 ORDER BY key").to_arrow().to_pylist()
    assert rows == [{"key": "y"}]
