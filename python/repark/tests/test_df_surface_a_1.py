"""DF-SURFACE-A-1 step 1 — oracle pins for the nine-name DataFrame surface."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark.functions import col
from repark.spark.session import SparkSession
from repark.spark.types import (
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

_ORACLE = json.loads((Path(__file__).parent / "facade_dataframe_surface_oracle.json").read_text())


def _cell(name: str) -> dict:
    """Return the recorded PySpark 4.1.2 oracle cell."""
    return _ORACLE["cells"][name]


def _kv(spark: ReparkSession):
    """Build the oracle's two-row ``key string, a int, b int`` frame."""
    return spark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")


def _assert_frame_matches_cell(frame: object, cell_id: str) -> None:
    """Assert columns, schema simpleString, and Row reprs equal the oracle cell."""
    result = _cell(cell_id)["result"]
    assert frame.columns == result["columns"]
    assert frame.schema.simpleString() == result["schema"]
    assert [repr(row) for row in frame.collect()] == result["rows"]


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test."""
    session = ReparkSession.builder.appName("pytest-df-surface-a-1").getOrCreate()
    yield session
    session.stop()


def test_to_reorders_casts_and_wins_schema_spelling(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-001 — cells to_reorder_cast, to_case, to_narrow."""
    frame = _kv(spark)
    reordered = frame.to(
        StructType([StructField("b", LongType()), StructField("key", StringType())])
    )
    _assert_frame_matches_cell(reordered, "to_reorder_cast")
    assert reordered is not frame
    _assert_frame_matches_cell(frame.to(StructType([StructField("A", IntegerType())])), "to_case")
    _assert_frame_matches_cell(frame.to(StructType([StructField("a", ShortType())])), "to_narrow")


def test_to_missing_nullable_field_fills_null(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-001 — cell to_missing."""
    _assert_frame_matches_cell(
        _kv(spark).to(StructType([StructField("zz", LongType())])), "to_missing"
    )


def test_to_nullable_source_into_required_field_refuses(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-001 — cell to_nullability."""
    with pytest.raises(AnalysisException) as caught:
        _kv(spark).to(StructType([StructField("a", IntegerType(), False)]))
    assert str(caught.value) == _cell("to_nullability")["error"]["message"]


def test_to_refuses_string_to_int_store_assignment(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-001 — cell to_bad_cast."""
    with pytest.raises(AnalysisException) as caught:
        _kv(spark).to(StructType([StructField("key", IntegerType())]))
    assert str(caught.value) == _cell("to_bad_cast")["error"]["message"]


def test_to_non_struct_schema_raises_not_struct(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-001 — cell to_not_schema, DF-TO-1 declared error-class answer."""
    with pytest.raises(PySparkTypeError) as caught:
        _kv(spark).to("a int")
    assert caught.value.getCondition() == "NOT_STRUCT"
    assert caught.value.getMessageParameters() == {
        "arg_name": "schema",
        "arg_type": "str",
    }


def test_with_metadata_replaces_metadata_and_keeps_order(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-002 — cells withMetadata, _replaces, _order."""
    frame = _kv(spark)
    stamped = frame.withMetadata("a", {"k": "v"})
    assert stamped.schema["a"].metadata == {"k": "v"}
    restamped = stamped.withMetadata("a", {"j": 1})
    assert restamped.schema["a"].metadata == {"j": 1}
    assert stamped.columns == [
        item["value"] for item in _cell("withMetadata_order")["result"]["items"]
    ]


def test_with_metadata_not_dict_raises(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-002 — cell withMetadata_not_dict."""
    with pytest.raises(PySparkTypeError) as caught:
        _kv(spark).withMetadata("a", "x")
    assert caught.value.getCondition() == "NOT_DICT"
    assert caught.value.getMessageParameters()["arg_name"] == "metadata"


def test_with_metadata_unknown_column_raises(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-002 — cell withMetadata_missing."""
    with pytest.raises(AnalysisException) as caught:
        _kv(spark).withMetadata("zz", {"k": "v"})
    assert "zz" in str(caught.value)


def test_register_temp_table_warns_registers_and_returns_none(
    spark: ReparkSession,
) -> None:
    """pins: df-surface-a-1/C-003 — cells registerTempTable, registerTempTable_return."""
    with pytest.warns(FutureWarning, match="Deprecated in 2.0"):
        result = _kv(spark).registerTempTable("rtt")
    assert result is None
    _assert_frame_matches_cell(spark.table("rtt"), "registerTempTable")


def test_register_temp_table_replaces_existing_view(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-003 — cell registerTempTable_replace."""
    with pytest.warns(FutureWarning):
        _kv(spark).registerTempTable("rtt3")
    with pytest.warns(FutureWarning):
        spark.range(1).registerTempTable("rtt3")
    _assert_frame_matches_cell(spark.table("rtt3"), "registerTempTable_replace")


def test_checkpoint_returns_new_frame_with_same_rows(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-003 — cells checkpoint_with_dir, checkpoint_lazy (DF-CHECKPOINT-1)."""
    frame = _kv(spark)
    eager = frame.checkpoint()
    assert eager is not frame
    _assert_frame_matches_cell(eager, "checkpoint_with_dir")
    lazy = frame.checkpoint(eager=False)
    assert lazy is not frame
    _assert_frame_matches_cell(lazy, "checkpoint_lazy")


def test_spark_session_returns_owning_session(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-003 — cells sparkSession_is, sparkSession_type."""
    frame = _kv(spark)
    assert frame.sparkSession is spark
    assert type(frame.sparkSession) is SparkSession


def test_is_local_false_on_every_measured_shape(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-surface-a-1/C-004 — cells isLocal_*."""
    assert _kv(spark).isLocal() is False
    assert spark.range(3).isLocal() is False
    assert spark.sql("select 1").isLocal() is False
    assert spark.sql("select * from values (1) t(a)").isLocal() is False
    assert _kv(spark).filter("a > 1").isLocal() is False
    assert spark.createDataFrame([], "a int").isLocal() is False
    parquet_dir = str(tmp_path / "pq")
    _kv(spark).coalesce(1).write.parquet(parquet_dir)
    assert spark.read.parquet(parquet_dir).isLocal() is False


def test_input_files_parquet_returns_file_uris(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-surface-a-1/C-004 — cell inputFiles_parquet."""
    parquet_dir = str(tmp_path / "pq")
    _kv(spark).coalesce(1).write.parquet(parquet_dir)
    files = spark.read.parquet(parquet_dir).inputFiles()
    assert len(files) == 1
    assert files[0].startswith("file://")
    assert files[0].endswith(".parquet")
    assert Path(files[0].split("://", 1)[1]).is_absolute()


def test_input_files_union_dedupes(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-surface-a-1/C-004 — cell inputFiles_union."""
    parquet_dir = str(tmp_path / "pq")
    _kv(spark).coalesce(1).write.parquet(parquet_dir)
    unioned = spark.read.parquet(parquet_dir).union(spark.read.parquet(parquet_dir))
    assert len(unioned.inputFiles()) == _cell("inputFiles_union")["result"]["value"]


def test_input_files_filter_keeps_scanned_files(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-surface-a-1/C-004 — cell inputFiles_csv_filter."""
    parquet_dir = str(tmp_path / "pq")
    _kv(spark).coalesce(1).write.parquet(parquet_dir)
    filtered = spark.read.parquet(parquet_dir).filter("a > 100")
    assert len(filtered.inputFiles()) == _cell("inputFiles_csv_filter")["result"]["value"]


def test_input_files_local_frame_is_empty(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-004 — cell inputFiles_local."""
    assert _kv(spark).inputFiles() == []


def test_execution_info_raises_classic_operation_error(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-005 — cell executionInfo."""
    with pytest.raises(PySparkValueError) as caught:
        _ = _kv(spark).executionInfo
    assert caught.value.getCondition() == "CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF"
    assert str(caught.value) == _cell("executionInfo")["error"]["message"]
    assert caught.value.getMessageParameters() == {"member": "queryExecution"}


def test_semantic_hash_plan_fingerprints(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-005 — cells semanticHash_*."""
    assert isinstance(_kv(spark).semanticHash(), int)
    first = spark.range(3).filter("id > 1")
    second = spark.range(3).filter("id > 1")
    assert first.semanticHash() == second.semanticHash()
    alias_a = spark.range(3).select(col("id").alias("a"))
    alias_b = spark.range(3).select(col("id").alias("b"))
    assert alias_a.semanticHash() == alias_b.semanticHash()
    assert spark.range(3).semanticHash() != spark.range(4).semanticHash()
    order_a = spark.range(3).filter("id > 1").filter("id < 3")
    order_b = spark.range(3).filter("id < 3").filter("id > 1")
    assert order_a.semanticHash() != order_b.semanticHash()


def test_semantic_hash_stable_within_process(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-005 — same frame hashes identically on repeat calls."""
    frame = spark.range(3).filter("id > 1")
    assert frame.semanticHash() == frame.semanticHash()
