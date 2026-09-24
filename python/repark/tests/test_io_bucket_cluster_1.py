"""io-bucket-cluster-1 pins: the bucketBy / sortBy / clusterBy writer surface."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import repark.spark.functions as F  # noqa: N812
from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
)

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_reader_writer_oracle.json").read_text(encoding="utf-8")
)["cells"]
_U7_MEASURED: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_write_df_1_spark_oracle.json").read_text(encoding="utf-8")
)["measured"]


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
    assert writer.bucket_by(2, ["a", "b"]) is writer


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
    """A bucket column absent from the frame answers as Spark's Iceberg catalog (Ruling Q1).

    A new table raises ``_LEGACY_ERROR_TEMP_3060`` with the frame's printSchema tree; an append
    onto an existing table answers the layout mismatch. The recorded ``bucketBy_missing_col``
    cell ran on Spark's Hive session catalog.
    pins: u7-write-df/C-015
    """
    frame = spark.createDataFrame(
        [(7, "g", "x"), (8, "h", "w")], "id BIGINT, data STRING, cat STRING"
    )
    with pytest.raises(AnalysisException) as raised:
        frame.write.bucketBy(4, "nope").saveAsTable("bk_missing")
    expected = _U7_MEASURED["bucketBy_new_missing_col"]["error"]
    assert str(raised.value) == expected["message"]
    assert raised.value.getCondition() == expected["condition"]
    assert not spark.catalog.tableExists("bk_missing")
    spark.sql(
        "CREATE TABLE bk_missing (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (cat, bucket(4, id))"
    )
    with pytest.raises(IllegalArgumentException) as mismatch:
        frame.write.bucketBy(4, "nope").mode("append").saveAsTable("bk_missing")
    assert (
        str(mismatch.value) == (_U7_MEASURED["bucket_existing_missing_append"]["error"]["message"])
    )


def test_bucket_by_sort_by_save_as_table_refuses_like_iceberg(spark: ReparkSession) -> None:
    """A sorted bucketed saveAsTable refuses as Spark's Iceberg catalog does (R-1 retired).

    The recorded ``bucketBy_saveAsTable`` cell ran on Spark's Hive session catalog; on an
    Iceberg catalog Spark cannot convert ``sorted_bucket`` into a partition transform.
    pins: u7-write-df/C-008
    """
    frame = _kv_frame(spark)
    with pytest.raises(IllegalArgumentException) as raised:
        frame.write.bucketBy(2, "a").sortBy("b").saveAsTable("bk_t")
    assert str(raised.value) == (
        "Cannot convert transform with more than one column reference: sorted_bucket(a, 2, b)"
    )
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


def test_cluster_by_empty_call_refuses_at_the_call(spark: ReparkSession) -> None:
    """Empty clusterBy raises Spark's bare assert at the call; no table, state kept.

    pins: io-bucket-cluster-1/C-005
    """
    frame = _kv_frame(spark)
    for call in (
        lambda: frame.write.clusterBy(),
        lambda: frame.write.clusterBy([]),
        lambda: frame.write.clusterBy(()),
    ):
        with pytest.raises(
            AssertionError, match=r"clusterBy needs one or more clustering columns\."
        ):
            call()
    assert not spark.catalog.tableExists("empty_cl")
    writer = frame.write
    writer.clusterBy("a")
    with pytest.raises(AssertionError, match=r"clusterBy needs one or more clustering columns\."):
        writer.clusterBy()
    with pytest.raises(PySparkNotImplementedError) as raised_kept:
        writer.saveAsTable("cl_cleared")
    assert raised_kept.value.getCondition() == "NOT_IMPLEMENTED"
    assert not spark.catalog.tableExists("cl_cleared")


def test_bucket_by_list_with_extra_cols_refuses(spark: ReparkSession) -> None:
    """A list/tuple first column with extra cols raises CANNOT_SET_TOGETHER at the call.

    pins: io-bucket-cluster-1/C-005
    """
    frame = _kv_frame(spark)
    with pytest.raises(PySparkValueError) as raised:
        frame.write.bucketBy(2, ["a"], "b")
    assert raised.value.getCondition() == "CANNOT_SET_TOGETHER"
    assert raised.value.getMessageParameters() == {"arg_list": "`col` of type list and `cols`"}
    assert (
        str(raised.value)
        == "[CANNOT_SET_TOGETHER] `col` of type list and `cols` should not be set together."
    )
    with pytest.raises(PySparkValueError) as raised_tuple:
        frame.write.bucketBy(2, ("a", "b"), "key")
    assert raised_tuple.value.getMessageParameters() == {
        "arg_list": "`col` of type tuple and `cols`"
    }
    with pytest.raises(PySparkValueError) as raised_sort:
        frame.write.sortBy(["a"], "b")
    assert raised_sort.value.getCondition() == "CANNOT_SET_TOGETHER"
    with pytest.raises(PySparkValueError) as raised_snake:
        frame.write.bucket_by(2, ["a"], "b")
    assert raised_snake.value.getCondition() == "CANNOT_SET_TOGETHER"


def test_bucket_by_non_str_names_refused(spark: ReparkSession) -> None:
    """Non-str column names raise NOT_LIST_OF_STR at the call; an empty list is IndexError.

    pins: io-bucket-cluster-1/C-005
    """
    frame = _kv_frame(spark)
    for bad_col, arg_type in ((1, "int"), (None, "NoneType"), (F.col("a"), "Column")):
        with pytest.raises(PySparkTypeError) as raised_col:
            frame.write.bucketBy(2, bad_col)
        assert raised_col.value.getCondition() == "NOT_LIST_OF_STR"
        assert raised_col.value.getMessageParameters() == {
            "arg_name": "col",
            "arg_type": arg_type,
        }
    with pytest.raises(PySparkTypeError) as raised_sort:
        frame.write.sortBy(1)
    assert raised_sort.value.getMessageParameters() == {"arg_name": "col", "arg_type": "int"}
    with pytest.raises(PySparkTypeError) as raised_extra:
        frame.write.bucketBy(2, "a", 1)
    assert raised_extra.value.getMessageParameters() == {"arg_name": "cols", "arg_type": "int"}
    with pytest.raises(PySparkTypeError) as raised_member:
        frame.write.bucketBy(2, ["a", 1])
    assert raised_member.value.getMessageParameters() == {"arg_name": "cols", "arg_type": "int"}
    for call in (
        lambda: frame.write.bucketBy(2, []),
        lambda: frame.write.sortBy([]),
    ):
        with pytest.raises(IndexError, match="list index out of range"):
            call()


def test_bucketed_and_sorted_path_save_refused_1313(
    spark: ReparkSession,
    tmp_path: Path,
) -> None:
    """A path save with bucketBy AND sortBy raises _LEGACY_ERROR_TEMP_1313 (L-004).

    pins: io-bucket-cluster-1/C-005
    """
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.write.bucketBy(2, "a").sortBy("b").parquet(str(tmp_path / "bs1"))
    assert raised.value.getCondition() == "_LEGACY_ERROR_TEMP_1313"
    assert raised.value.getMessageParameters() == {"operation": "save"}
    assert str(raised.value) == "'save' does not support bucketBy and sortBy right now."
    for call in (
        lambda: frame.write.bucketBy(2, "a").sortBy("b").save(str(tmp_path / "bs2")),
        lambda: frame.write.bucketBy(2, "a").sortBy("b").csv(str(tmp_path / "bs3")),
        lambda: frame.write.bucketBy(2, "a").sortBy("b").json(str(tmp_path / "bs4")),
        lambda: frame.write.bucketBy(2, "a").sortBy("b").format("text").save(str(tmp_path / "bs5")),
    ):
        with pytest.raises(AnalysisException) as raised_arm:
            call()
        assert raised_arm.value.getCondition() == "_LEGACY_ERROR_TEMP_1313"
        assert str(raised_arm.value) == "'save' does not support bucketBy and sortBy right now."
    with pytest.raises(AnalysisException) as raised_bucket_only:
        frame.write.bucketBy(2, "a").parquet(str(tmp_path / "b1"))
    _assert_error_cell(raised_bucket_only.value, "bucketBy_save")


def test_insert_into_refuses_bucketing(spark: ReparkSession) -> None:
    """insertInto refuses bucketing (Ruling R-3); clusterBy still writes there (Q-1).

    pins: io-bucket-cluster-1/C-005
    """
    frame = _kv_frame(spark)
    frame.write.saveAsTable("q1_t")
    with pytest.raises(AnalysisException) as raised:
        frame.write.bucketBy(2, "a").insertInto("q1_t")
    assert raised.value.getCondition() == "_LEGACY_ERROR_TEMP_1312"
    assert raised.value.getMessageParameters() == {"operation": "insertInto"}
    assert str(raised.value) == "'insertInto' does not support bucketBy right now."
    with pytest.raises(AnalysisException) as raised_both:
        frame.write.bucketBy(2, "a").sortBy("b").insertInto("q1_t")
    assert raised_both.value.getCondition() == "_LEGACY_ERROR_TEMP_1313"
    assert str(raised_both.value) == "'insertInto' does not support bucketBy and sortBy right now."
    with pytest.raises(AnalysisException) as raised_sort:
        frame.write.sortBy("a").insertInto("q1_t")
    _assert_error_cell(raised_sort.value, "sortBy_without_bucketBy")
    assert frame.write.clusterBy("a").insertInto("q1_t") is None
    assert spark.sql("SELECT key FROM q1_t").to_arrow().num_rows == 4


def test_partitioned_save_as_table_still_writes(spark: ReparkSession) -> None:
    """Regression guard: partitionBy saveAsTable CTAS and partition-filtered reads stand.

    pins: io-bucket-cluster-1/C-004
    """
    frame = _kv_frame(spark)
    frame.write.partitionBy("a").saveAsTable("reg_parted")
    rows = spark.sql("SELECT key FROM reg_parted WHERE a = 3 ORDER BY key").to_arrow().to_pylist()
    assert rows == [{"key": "y"}]


def test_not_list_of_str_message_and_extra_first_order(spark: ReparkSession) -> None:
    """NOT_LIST_OF_STR has Spark's sentence, extra cols first. pins: io-bucket-cluster-1/C-006"""
    frame = spark.createDataFrame([("x", 1, 2)], "key string, a int, b int")
    with pytest.raises(PySparkTypeError) as first_bad:
        frame.write.bucketBy(2, 1)
    assert (
        str(first_bad.value) == "[NOT_LIST_OF_STR] Argument `col` should be a list[str], got int."
    )
    with pytest.raises(PySparkTypeError) as extra_bad:
        frame.write.sortBy("a", None)
    assert (
        str(extra_bad.value)
        == "[NOT_LIST_OF_STR] Argument `cols` should be a list[str], got NoneType."
    )
    with pytest.raises(PySparkTypeError) as both_bad:
        frame.write.bucketBy(2, 1, 2)
    assert both_bad.value.getMessageParameters() == {"arg_name": "cols", "arg_type": "int"}
