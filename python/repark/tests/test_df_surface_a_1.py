"""DF-SURFACE-A-1 — oracle pins for the seven-name DataFrame surface."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark.functions import col
from repark.spark.session import SparkSession
from repark.spark.types import (
    ArrayType,
    BinaryType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
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
    """pins: df-surface-a-1/C-001 — cells to_reorder_cast, to_case."""
    frame = _kv(spark)
    reordered = frame.to(
        StructType([StructField("b", LongType()), StructField("key", StringType())])
    )
    _assert_frame_matches_cell(reordered, "to_reorder_cast")
    assert reordered is not frame
    _assert_frame_matches_cell(frame.to(StructType([StructField("A", IntegerType())])), "to_case")


def test_to_narrow_reports_logical_width_1(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-001 — cell to_narrow; LOGICAL-WIDTH-1 codifies today's int answer."""
    narrowed = _kv(spark).to(StructType([StructField("a", ShortType())]))
    assert narrowed.columns == _cell("to_narrow")["result"]["columns"]
    assert [repr(row) for row in narrowed.collect()] == _cell("to_narrow")["result"]["rows"]
    assert narrowed.schema.simpleString() == "struct<a:int>"


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


def test_to_store_assignment_atomic_to_string(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-001: atomic→string incl. Spark's doctest long→string."""
    df = spark.createDataFrame([("a", 1)], ["i", "j"])
    out = df.to(StructType([StructField("j", StringType()), StructField("i", StringType())]))
    assert out.schema.simpleString() == "struct<j:string,i:string>"
    assert [repr(row) for row in out.collect()] == ["Row(j='1', i='a')"]
    bools = spark.createDataFrame([(True,)], "f boolean")
    assert bools.to(StructType([StructField("f", StringType())])).collect()[0][0] == "true"
    dates = spark.createDataFrame([(date(2024, 6, 15),)], "d date")
    assert dates.to(StructType([StructField("d", StringType())])).collect()[0][0] == "2024-06-15"
    decs = spark.createDataFrame([(Decimal("1.50"),)], "d decimal(10,2)")
    assert decs.to(StructType([StructField("d", StringType())])).collect()[0][0] == "1.50"


def test_to_binary_follows_reported_schema_df_to_binary_1(spark: ReparkSession) -> None:
    """to() keeps bytes under a string field for a binary column. pins: df-surface-a-1/C-008"""
    frame = spark.createDataFrame([(b"hi",)], "b binary")
    assert frame.schema.simpleString() == "struct<b:string>"
    as_string = frame.to(StructType([StructField("b", StringType())]))
    assert as_string.schema.simpleString() == "struct<b:string>"
    assert as_string.collect()[0][0] == b"hi"
    with pytest.raises(AnalysisException) as caught:
        frame.to(StructType([StructField("b", BinaryType())])).collect()
    assert "INVALID_COLUMN_OR_FIELD_DATA_TYPE" in str(caught.value)


def test_to_store_assignment_refusals(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-001: string/boolean→numeric and decimal narrowing refuse."""
    bools = spark.createDataFrame([(True,)], "f boolean")
    with pytest.raises(AnalysisException, match="INVALID_COLUMN_OR_FIELD_DATA_TYPE"):
        bools.to(StructType([StructField("f", IntegerType())]))
    decs = spark.createDataFrame([(Decimal("1.50"),)], "d decimal(10,2)")
    with pytest.raises(AnalysisException, match="INVALID_COLUMN_OR_FIELD_DATA_TYPE"):
        decs.to(StructType([StructField("d", DecimalType(4, 1))]))
    widened = decs.to(StructType([StructField("d", DecimalType(12, 3))]))
    assert widened.schema.simpleString() == "struct<d:decimal(12,3)>"


def test_to_nested_identity_roundtrip(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-002: to(df.schema) works for struct/array/map columns."""
    nested = spark.createDataFrame([((1, "z"),)], "s struct<x:int,y:string>")
    assert nested.to(nested.schema).collect() == nested.collect()
    arr = spark.createDataFrame([([1, 2],)], "xs array<int>")
    assert arr.to(arr.schema).collect() == arr.collect()
    mapped = spark.createDataFrame([({"k": 1},)], "m map<string,int>")
    assert mapped.to(mapped.schema).collect() == mapped.collect()


def test_to_nested_struct_reconcile(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-002: inner reorder/drop/null-fill/missing-non-nullable."""
    nested = spark.createDataFrame([((1, "z"),)], "s struct<x:int,y:string>")
    inner = StructType([StructField("y", StringType()), StructField("x", IntegerType())])
    reordered = nested.to(StructType([StructField("s", inner)]))
    assert reordered.schema.simpleString() == "struct<s:struct<y:string,x:int>>"
    assert [repr(row) for row in reordered.collect()] == ["Row(s={'y': 'z', 'x': 1})"]
    target = StructType([StructField("x", IntegerType()), StructField("w", IntegerType())])
    filled = nested.to(StructType([StructField("s", target)]))
    assert filled.schema.simpleString() == "struct<s:struct<x:int,w:int>>"
    assert [repr(row) for row in filled.collect()] == ["Row(s={'x': 1, 'w': None})"]
    required = StructType([StructField("x", IntegerType()), StructField("w", IntegerType(), False)])
    with pytest.raises(AnalysisException, match="NULLABLE_COLUMN_OR_FIELD"):
        nested.to(StructType([StructField("s", required)]))


def test_to_nested_array_map_element_cast(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-002: array/map element casts through transform arms."""
    arr = spark.createDataFrame([([1, 2],)], "xs array<int>")
    widened = arr.to(StructType([StructField("xs", ArrayType(LongType()))]))
    assert widened.schema.simpleString() == "struct<xs:array<bigint>>"
    assert widened.collect() == arr.collect()
    mapped = spark.createDataFrame([({"k": 1},)], "m map<string,int>")
    remapped = mapped.to(StructType([StructField("m", MapType(StringType(), LongType()))]))
    assert remapped.schema.simpleString() == "struct<m:map<string,bigint>>"
    assert remapped.collect() == mapped.collect()


def test_to_nested_inner_bad_cast_refuses(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-002: inner string→int refused with the field path."""
    nested = spark.createDataFrame([((1, "z"),)], "s struct<x:int,y:string>")
    bad = StructType([StructField("s", StructType([StructField("y", IntegerType())]))])
    with pytest.raises(AnalysisException, match="INVALID_COLUMN_OR_FIELD_DATA_TYPE"):
        nested.to(bad)


def test_with_metadata_dropped_at_stamp_df_metadata_1(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — DF-METADATA-1 codifies loss; cells withMetadata, _replaces."""
    stamped = _kv(spark).withMetadata("a", {"k": "v"})
    assert stamped.schema["a"].metadata == {}
    restamped = stamped.withMetadata("a", {"j": 1})
    assert restamped.schema["a"].metadata == {}
    assert stamped.columns == [
        item["value"] for item in _cell("withMetadata_order")["result"]["items"]
    ]


def test_with_metadata_dropped_across_positions_df_metadata_1(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """pins: df-surface-a-1/C-008 — DF-METADATA-1 loss per position (no engine field metadata)."""
    stamped = _kv(spark).withMetadata("a", {"k": "v"})
    assert stamped.filter("a > 0").schema["a"].metadata == {}
    assert stamped.select("key", "a").schema["a"].metadata == {}
    assert stamped.withColumn("c", col("a")).schema["a"].metadata == {}
    other = spark.createDataFrame([("x",), ("y",)], "key string")
    assert stamped.join(other, "key").schema["a"].metadata == {}
    assert stamped.union(_kv(spark)).schema["a"].metadata == {}
    cached = stamped.cache()
    cached.collect()
    assert cached.schema["a"].metadata == {}
    parquet_dir = str(tmp_path / "meta_pq")
    stamped.write.parquet(parquet_dir)
    assert spark.read.parquet(parquet_dir).schema["a"].metadata == {}


def test_to_metadata_arms_drop_df_metadata_1(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-010/DF-METADATA-1: source-keep and target-override lose."""
    stamped = _kv(spark).withMetadata("a", {"src": "1"})
    kept = stamped.to(StructType([StructField("a", IntegerType())]))
    assert kept.schema["a"].metadata == {}
    overridden = stamped.to(StructType([StructField("a", IntegerType(), metadata={"tgt": "2"})]))
    assert overridden.schema["a"].metadata == {}


def test_with_metadata_not_dict_raises(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-002 — cell withMetadata_not_dict."""
    with pytest.raises(PySparkTypeError) as caught:
        _kv(spark).withMetadata("a", "x")
    assert caught.value.getCondition() == "NOT_DICT"
    assert caught.value.getMessageParameters()["arg_name"] == "metadata"


def test_with_metadata_unknown_column_raises_unresolved(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-008: cell withMetadata_missing class + condition."""
    with pytest.raises(AnalysisException) as caught:
        _kv(spark).withMetadata("zz", {"k": "v"})
    assert "zz" in str(caught.value)
    assert caught.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert caught.value.getErrorClass() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert caught.value.getMessageParameters() == {
        "objectName": "`zz`",
        "proposal": "`key`, `a`, `b`",
    }
    assert caught.value.getSqlState() == "42703"


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


def test_spark_session_survives_new_session_promotion(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-008 — L-005: the owning session, not the promoted active one."""
    frame = _kv(spark)
    other = spark.newSession()
    try:
        other.range(1).collect()
        assert frame.sparkSession is spark
        assert frame.sparkSession is not other
    finally:
        other.stop()


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


def test_execution_info_raises_classic_operation_error(spark: ReparkSession) -> None:
    """pins: df-surface-a-1/C-005 — cell executionInfo."""
    with pytest.raises(PySparkValueError) as caught:
        _ = _kv(spark).executionInfo
    assert caught.value.getCondition() == "CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF"
    assert str(caught.value) == _cell("executionInfo")["error"]["message"]
    assert caught.value.getMessageParameters() == {"member": "queryExecution"}
