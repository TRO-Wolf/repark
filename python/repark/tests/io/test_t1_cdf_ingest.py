"""createDataFrame ingestion parity (dict key-union + nested List/Struct).

Divergence-class pins: ``collect`` / ``to_arrow`` value AND Arrow type, per entry point (dict
list, polars, pandas). Live Spark 4.1.2 oracle; synthetic Orders-shaped fixtures only.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkTypeError, PySparkValueError
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType


@pytest.fixture
def spark() -> ReparkSession:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-t1-cdf-ingest").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


# Dict key-union (Spark 4.1.2 oracle)
@pytest.mark.parametrize(
    ("rows", "expected_columns"),
    [
        (
            [{"c": 1, "a": 2}, {"b": 3, "a": 4}, {"d": 5, "c": 6}],
            ["a", "c", "b", "d"],
        ),
        (
            [{"z": 1, "a": 2, "m": 3}, {"b": 4}],
            ["a", "m", "z", "b"],
        ),
        (
            [{"b": 1}, {"a": 2, "c": 3}],
            ["b", "a", "c"],
        ),
    ],
)
def test_dict_key_union_order_first_row_sorted_then_append(
    spark: ReparkSession,
    rows: list[dict[str, int]],
    expected_columns: list[str],
) -> None:
    """Oracle key-union order: sorted first-row keys, then append newly seen (sorted per row)."""
    table = spark.createDataFrame(rows).to_arrow()
    assert table.column_names == expected_columns
    for name in table.column_names:
        assert table.schema.field(name).type == pa.int64(), name
    # Full value pin for the primary three-row oracle case.
    if expected_columns == ["a", "c", "b", "d"]:
        assert table.to_pylist() == [
            {"a": 2, "c": 1, "b": None, "d": None},
            {"a": 4, "c": None, "b": 3, "d": None},
            {"a": None, "c": 6, "b": None, "d": 5},
        ]
    # collect path: same column order as to_arrow (dataframe.columns is the contract)
    frame = spark.createDataFrame(rows)
    assert frame.columns == expected_columns
    collected = frame.collect()
    assert len(collected) == len(rows)


def test_dict_key_union_empty_first_mapping_null_fills(spark: ReparkSession) -> None:
    """Empty first dict still key-unions later keys; missing → null."""
    table = spark.createDataFrame([{}, {"a": 1}, {"a": 2, "b": 3}]).to_arrow()
    assert table.column_names == ["a", "b"]
    assert table.to_pylist() == [
        {"a": None, "b": None},
        {"a": 1, "b": None},
        {"a": 2, "b": 3},
    ]
    assert table.schema.field("a").type == pa.int64()
    table2 = spark.createDataFrame([{"a": 1}, {}]).to_arrow()
    assert table2.to_pylist() == [{"a": 1}, {"a": None}]


def test_dict_key_union_null_fill_and_type_widening(spark: ReparkSession) -> None:
    """Field absent in row 1 still types from later non-null cells; earlier rows null-fill."""
    rows = [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b", "score": 3.5},
        {"id": 3, "extra": "only-row3"},
    ]
    table = spark.createDataFrame(rows).to_arrow()
    assert table.column_names == ["id", "name", "score", "extra"]
    assert table.to_pylist() == [
        {"id": 1, "name": "a", "score": None, "extra": None},
        {"id": 2, "name": "b", "score": 3.5, "extra": None},
        {"id": 3, "name": None, "score": None, "extra": "only-row3"},
    ]
    assert table.schema.field("id").type == pa.int64()
    assert (
        pa.types.is_floating(table.schema.field("score").type)
        or table.schema.field("score").type == pa.float64()
    )
    assert pa.types.is_string(table.schema.field("extra").type) or pa.types.is_large_string(
        table.schema.field("extra").type
    )
    # collect path (value)
    collected = spark.createDataFrame(rows).collect()
    assert collected[0]["score"] is None
    assert collected[1]["score"] == 3.5
    assert collected[2]["extra"] == "only-row3"


def test_dict_key_union_orders_shaped_synthetic(spark: ReparkSession) -> None:
    """Synthetic Orders-shaped ragged optional fields (invented IDs only)."""
    # repark defaults inferNestedDictAsStruct to true; the conf off keeps the PySpark-default
    # MAP cell path.
    spark.conf.set("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false")
    rows = [
        {
            "OrderId": 9001,
            "Symbol": "TEST",
            "Quantity": 100,
            "Legs": [{"LegId": 1, "Side": "Buy", "Qty": 50}],
        },
        {
            "OrderId": 9002,
            "Symbol": "TEST",
            "Quantity": 50,
            "StopPrice": 12.5,
            "AdvancedOptions": {"Duration": "DAY"},
            "Legs": [{"LegId": 2, "Side": "Sell", "Qty": 25}],
        },
        {
            "OrderId": 9003,
            "Symbol": "TEST",
            "ConditionalOrders": [{"Type": "trail", "Offset": 0.5}],
            "Quantity": 10,
        },
    ]
    table = spark.createDataFrame(rows).to_arrow()
    assert table.column_names[0:4] == ["Legs", "OrderId", "Quantity", "Symbol"]
    assert "StopPrice" in table.column_names
    assert "AdvancedOptions" in table.column_names
    assert "ConditionalOrders" in table.column_names
    rows_out = table.to_pylist()
    assert rows_out[0]["OrderId"] == 9001
    assert rows_out[0]["StopPrice"] is None
    assert rows_out[1]["StopPrice"] == 12.5
    assert rows_out[2]["ConditionalOrders"] is not None
    assert rows_out[2]["Legs"] is None
    assert table.schema.field("OrderId").type == pa.int64()
    assert pa.types.is_floating(table.schema.field("StopPrice").type)
    # schema=None dict path: nested list-of-dicts → list<map> (Spark array<map>; not struct).
    # Typed ArrayType(StructType) path is pinned separately.
    legs_type = table.schema.field("Legs").type
    assert pa.types.is_list(legs_type) or pa.types.is_large_list(legs_type)
    assert pa.types.is_map(legs_type.value_type)


def test_dict_structtype_schema_null_fill_drops_extras(spark: ReparkSession) -> None:
    """Explicit StructType: missing → null; extra keys dropped (Spark oracle)."""
    schema = StructType(
        [
            StructField("OrderId", LongType(), True),
            StructField("Status", StringType(), True),
            StructField("StopPrice", DoubleType(), True),
        ]
    )
    rows = [
        {"OrderId": 1, "Status": "A"},
        {"OrderId": 2, "Status": "B", "StopPrice": 1.5},
        {"OrderId": 3, "Status": "C", "StopPrice": 2.0, "ExtraField": "drop"},
    ]
    table = spark.createDataFrame(rows, schema=schema).to_arrow()
    assert table.column_names == ["OrderId", "Status", "StopPrice"]
    assert table.to_pylist() == [
        {"OrderId": 1, "Status": "A", "StopPrice": None},
        {"OrderId": 2, "Status": "B", "StopPrice": 1.5},
        {"OrderId": 3, "Status": "C", "StopPrice": 2.0},
    ]
    assert table.schema.field("OrderId").type == pa.int64()
    assert pa.types.is_floating(table.schema.field("StopPrice").type)


def test_dict_structtype_nested_array_struct_value_and_type(spark: ReparkSession) -> None:
    """Explicit nested ArrayType(StructType) dict path — value+type."""
    schema = StructType(
        [
            StructField("OrderId", LongType(), True),
            StructField(
                "Legs",
                ArrayType(
                    StructType(
                        [
                            StructField("LegId", LongType(), True),
                            StructField("Side", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    rows = [
        {"OrderId": 1, "Legs": [{"LegId": 1, "Side": "Buy"}]},
        {"OrderId": 2, "Legs": None},
    ]
    table = spark.createDataFrame(rows, schema=schema).to_arrow()
    assert table.column_names == ["OrderId", "Legs"]
    assert table.schema.field("OrderId").type == pa.int64()
    legs_type = table.schema.field("Legs").type
    assert pa.types.is_list(legs_type) or pa.types.is_large_list(legs_type)
    assert pa.types.is_struct(legs_type.value_type)
    assert table.to_pylist() == [
        {"OrderId": 1, "Legs": [{"LegId": 1, "Side": "Buy"}]},
        {"OrderId": 2, "Legs": None},
    ]
    collected = spark.createDataFrame(rows, schema=schema).collect()
    assert collected[0]["OrderId"] == 1
    assert collected[0]["Legs"][0]["Side"] == "Buy"
    assert collected[1]["Legs"] is None


def test_row_key_mismatch_still_refuses(spark: ReparkSession) -> None:
    """Row lists stay fail-loud (Spark STRUCT_ARRAY_LENGTH_MISMATCH class)."""
    from repark import Row

    with pytest.raises(PySparkValueError, match=r"missing field|unexpected field"):
        spark.createDataFrame([Row(a=1, b=2), Row(a=3, c=4)])


def test_dict_int_float_same_key_refuses_merge(spark: ReparkSession) -> None:
    """Spark CANNOT_MERGE_TYPE: LongType + DoubleType on the same inferred column."""
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DoubleType"):
        spark.createDataFrame([{"a": 1}, {"a": 2.5}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DoubleType"):
        spark.createDataFrame([{"a": 1.5}, {"a": 2}])


def test_dict_list_int_float_elements_refuse_merge(spark: ReparkSession) -> None:
    """Nested list Long+Double must refuse — not truncate 1.5→1."""
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DoubleType"):
        spark.createDataFrame([{"v": [1, 2]}, {"v": [1.5, 2.5]}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DoubleType"):
        spark.createDataFrame([{"v": [1, 1.5]}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DoubleType"):
        spark.createDataFrame([{"v": [1]}, {"v": [2.5]}])


def test_dict_int_decimal_same_key_refuses_merge(spark: ReparkSession) -> None:
    """Spark CANNOT_MERGE_TYPE: LongType + DecimalType — no silent 2.5→2."""
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DecimalType"):
        spark.createDataFrame([{"a": 1}, {"a": Decimal("2.5")}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DecimalType"):
        spark.createDataFrame([{"a": Decimal("2.5")}, {"a": 1}])
    # Exact integral Decimal still refuses (type merge, not value-domain).
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DecimalType"):
        spark.createDataFrame([{"a": 1}, {"a": Decimal("2")}])


def test_dict_decimal_double_same_key_refuses_merge(spark: ReparkSession) -> None:
    """Spark CANNOT_MERGE_TYPE: DecimalType ↔ DoubleType."""
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|DecimalType|DoubleType"):
        spark.createDataFrame([{"a": Decimal("2.5")}, {"a": 1.5}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|DecimalType|DoubleType"):
        spark.createDataFrame([{"a": 1.5}, {"a": Decimal("2.5")}])


def test_dict_float_bool_same_key_refuses_merge(spark: ReparkSession) -> None:
    """Spark CANNOT_MERGE_TYPE: DoubleType + BooleanType — no True→1.0."""
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|DoubleType|BooleanType"):
        spark.createDataFrame([{"a": 1.5}, {"a": True}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|BooleanType|DoubleType"):
        spark.createDataFrame([{"a": True}, {"a": 1.5}])


def test_dict_int_bool_same_key_refuses_merge(spark: ReparkSession) -> None:
    """Spark CANNOT_MERGE_TYPE: LongType ↔ BooleanType."""
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|BooleanType"):
        spark.createDataFrame([{"a": 1}, {"a": True}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|BooleanType|LongType"):
        spark.createDataFrame([{"a": True}, {"a": 1}])


def test_dict_list_int_decimal_elements_refuse_merge(spark: ReparkSession) -> None:
    """Nested list/map Long+Decimal must refuse — not truncate via pa.array."""
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DecimalType"):
        spark.createDataFrame([{"v": [1]}, {"v": [Decimal("2.5")]}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DecimalType"):
        spark.createDataFrame([{"v": [1, Decimal("2.5")]}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|DecimalType"):
        spark.createDataFrame([{"m": {"k": 1}}, {"m": {"k": Decimal("2.5")}}])


def test_dict_timestamp_numeric_same_key_refuses_merge(spark: ReparkSession) -> None:
    """Spark CANNOT_MERGE_TYPE: TimestampType + Long/Double — no epoch coercion."""
    stamp = datetime(2020, 1, 1)
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|TimestampType|LongType"):
        spark.createDataFrame([{"a": stamp}, {"a": 1}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|LongType|TimestampType"):
        spark.createDataFrame([{"a": 1}, {"a": stamp}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|TimestampType|DoubleType"):
        spark.createDataFrame([{"a": stamp}, {"a": 1.5}])


def test_dict_date_numeric_and_timestamp_refuse_merge(spark: ReparkSession) -> None:
    """Spark CANNOT_MERGE_TYPE: DateType + Long/Timestamp — no day-epoch."""
    day = date(2020, 1, 1)
    stamp = datetime(2020, 1, 1)
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|DateType|LongType"):
        spark.createDataFrame([{"a": day}, {"a": 1}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|DateType|TimestampType"):
        spark.createDataFrame([{"a": day}, {"a": stamp}])
    with pytest.raises(PySparkTypeError, match=r"CANNOT_MERGE_TYPE|TimestampType|DateType"):
        spark.createDataFrame([{"a": stamp}, {"a": day}])


def test_dict_int_string_same_key_refuses_not_silent_string_coerce(
    spark: ReparkSession,
) -> None:
    """Residual: Spark stringifies int+str same key; repark fails loud (not absorbed)."""
    with pytest.raises(PySparkTypeError):
        spark.createDataFrame([{"a": 1}, {"a": "x"}])


def test_dict_name_list_schema_ragged_keys_length_bind(spark: ReparkSession) -> None:
    """Residual: name-list schema length-binds first-row keys (not Spark null-fill).

    Spark ``schema=['a','b']`` + ``[{a:1},{b:2}]`` null-fills; StructType is the null-fill
    explicit-schema path here.
    """
    with pytest.raises(PySparkValueError, match=r"schema length|partially overlaps|column count"):
        spark.createDataFrame([{"a": 1}, {"b": 2}], schema=["a", "b"])


# Polars nested List(Struct) + pandas Arrow nested
def test_polars_list_struct_nested_roundtrip_value_and_type(spark: ReparkSession) -> None:
    """Polars List(Struct) via Arrow path — collect/to_arrow value + type."""
    pl = pytest.importorskip("polars")
    frame = pl.DataFrame(
        {
            "order_id": [9001, 9002],
            "legs": [
                [{"leg_id": 1, "side": "Buy", "qty": 50}],
                [{"leg_id": 2, "side": "Sell", "qty": 25}],
            ],
        }
    )
    table = spark.createDataFrame(frame).to_arrow()
    assert table.column_names == ["order_id", "legs"]
    assert table.schema.field("order_id").type == pa.int64()
    legs_type = table.schema.field("legs").type
    assert pa.types.is_list(legs_type) or pa.types.is_large_list(legs_type)
    value_type = legs_type.value_type
    assert pa.types.is_struct(value_type)
    field_names = {field.name for field in value_type}
    assert {"leg_id", "side", "qty"} <= field_names
    pylist = table.to_pylist()
    assert pylist[0]["order_id"] == 9001
    assert pylist[0]["legs"][0]["leg_id"] == 1
    assert pylist[0]["legs"][0]["side"] == "Buy"
    assert pylist[1]["legs"][0]["qty"] == 25
    # collect entry point
    collected = spark.createDataFrame(frame).collect()
    assert collected[0]["order_id"] == 9001
    assert collected[1]["legs"][0]["side"] == "Sell"


def test_polars_struct_column_roundtrip(spark: ReparkSession) -> None:
    """Top-level polars Struct column lands via Arrow."""
    pl = pytest.importorskip("polars")
    frame = pl.DataFrame(
        {
            "id": [1],
            "meta": [{"duration": "DAY", "route": "SMART"}],
        }
    )
    table = spark.createDataFrame(frame).to_arrow()
    assert table.schema.field("id").type == pa.int64()
    assert pa.types.is_struct(table.schema.field("meta").type)
    assert table.to_pylist()[0]["meta"]["duration"] == "DAY"
    collected = spark.createDataFrame(frame).collect()
    assert collected[0]["meta"]["duration"] == "DAY"


def test_polars_binary_time_still_refuse(spark: ReparkSession) -> None:
    """Retained refuse: Binary / Time (engine cannot represent on CDF path)."""
    pl = pytest.importorskip("polars")
    with pytest.raises(PySparkTypeError, match=r"binary|Binary"):
        spark.createDataFrame(pl.DataFrame({"c": pl.Series("c", [b"x"], dtype=pl.Binary)}))
    with pytest.raises(PySparkTypeError, match=r"time|Time"):
        import datetime as dt

        spark.createDataFrame(pl.DataFrame({"c": pl.Series("c", [dt.time(12, 0)], dtype=pl.Time)}))


def test_pandas_arrow_list_struct_nested_roundtrip(spark: ReparkSession) -> None:
    """pandas ArrowDtype list<struct> via pa.Table.from_pandas — value + type."""
    pd = pytest.importorskip("pandas")
    list_struct = pa.list_(
        pa.struct(
            [
                ("leg_id", pa.int64()),
                ("side", pa.string()),
            ]
        )
    )
    series = pd.Series(
        [[{"leg_id": 10, "side": "Buy"}], [{"leg_id": 20, "side": "Sell"}]],
        dtype=pd.ArrowDtype(list_struct),
    )
    pdf = pd.DataFrame({"order_id": pd.Series([1, 2], dtype="int64[pyarrow]"), "legs": series})
    table = spark.createDataFrame(pdf).to_arrow()
    assert table.column_names == ["order_id", "legs"]
    assert table.schema.field("order_id").type == pa.int64()
    legs_type = table.schema.field("legs").type
    assert pa.types.is_list(legs_type) or pa.types.is_large_list(legs_type)
    assert pa.types.is_struct(legs_type.value_type)
    assert table.to_pylist()[0]["legs"][0]["leg_id"] == 10
    assert table.to_pylist()[1]["legs"][0]["side"] == "Sell"
    # collect entry point (divergence-class: value per entry point)
    collected = spark.createDataFrame(pdf).collect()
    assert collected[0]["order_id"] == 1
    assert collected[0]["legs"][0]["leg_id"] == 10
    assert collected[1]["legs"][0]["side"] == "Sell"


# Wrapped JSON {"Orders":[...]} → json.load + createDataFrame
def test_wrapped_json_object_via_json_load_dict_path(spark: ReparkSession, tmp_path: Path) -> None:
    """``read.json`` is NDJSON; object wrapper goes via json.load + dict key-union."""
    payload = {
        "Orders": [
            {"OrderId": 7001, "Status": "Filled", "Quantity": 10},
            {"OrderId": 7002, "Status": "Open", "StopPrice": 42.5},
        ]
    }
    path = tmp_path / "orders_wrap.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with path.open(encoding="utf-8") as handle:
        loaded = json.load(handle)
    assert isinstance(loaded, dict) and "Orders" in loaded
    table = spark.createDataFrame(loaded["Orders"]).to_arrow()
    assert table.column_names == ["OrderId", "Quantity", "Status", "StopPrice"]
    assert table.to_pylist() == [
        {"OrderId": 7001, "Quantity": 10, "Status": "Filled", "StopPrice": None},
        {"OrderId": 7002, "Quantity": None, "Status": "Open", "StopPrice": 42.5},
    ]
    assert table.schema.field("OrderId").type == pa.int64()
    assert pa.types.is_floating(table.schema.field("StopPrice").type)


def test_legacy_first_element_conf_coerces_float_into_long_array(spark: ReparkSession) -> None:
    """The numeric-merge refuse is conf-aware.

    With ``spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled=true`` Spark infers
    the element type from the FIRST element and truncate-coerces later numerics (Apache
    ``test_infer_nested_array_element_type_with_struct``); with the conf off (default) the
    CANNOT_MERGE_TYPE refuse stands.
    """
    key = "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled"
    spark.conf.set(key, "true")
    try:
        frame = spark.createDataFrame([[[[1, 1.0]]]])
        arrow = frame.to_arrow()
        element = arrow.schema.field(arrow.column_names[0]).type
        assert pa.types.is_list(element) and pa.types.is_list(element.value_type)
        assert pa.types.is_int64(element.value_type.value_type)
        assert arrow.to_pylist()[0][arrow.column_names[0]] == [[1, 1]]
    finally:
        spark.conf.unset(key)
    with pytest.raises(PySparkTypeError, match="Can not merge type LongType and DoubleType"):
        spark.createDataFrame([[[[1, 1.0]]]])
