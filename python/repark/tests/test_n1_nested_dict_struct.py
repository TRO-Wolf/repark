"""r23b N1 — ``spark.sql.pyspark.inferNestedDictAsStruct.enabled`` (SPARK-35929).

Live Spark 4.1.2 oracle matrix recorded in ``task/n1-nested-dict-struct-ledger.md``
before product code. Pins: value **and** Arrow type on ``collect`` / ``to_arrow`` for
both conf states. Synthetic Orders-shaped fixtures only (Q10 field lists; invented values).

Q8 regression pins:
  (a) conf-false byte-identity with r22 T1 map behavior
  (b) sparse-vector ``{size,indices,values}`` path conf-invariant
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkTypeError
from repark.row import Row
from repark.session import _reset_active_session_for_tests
from repark.types import LongType, MapType, StringType, StructField, StructType

CONF = "spark.sql.pyspark.inferNestedDictAsStruct.enabled"

# Q10 Orders shape — field names only; values invented (no brokerage literals).
_LEG_FIELDS = (
    "OpenOrClose",
    "QuantityOrdered",
    "ExecQuantity",
    "QuantityRemaining",
    "BuyOrSell",
    "Symbol",
    "AssetType",
    "ExecutionPrice",
)
_COND_FIELDS = ("Relationship", "OrderID")


@pytest.fixture
def spark() -> ReparkSession:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-n1-nested-dict-struct").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _synthetic_leg() -> dict[str, object]:
    return {
        "OpenOrClose": "O",
        "QuantityOrdered": 10,
        "ExecQuantity": 0,
        "QuantityRemaining": 10,
        "BuyOrSell": "Buy",
        "Symbol": "AAA",
        "AssetType": "EQ",
        "ExecutionPrice": 1.5,
    }


def _synthetic_cond() -> dict[str, object]:
    return {"Relationship": "OCO", "OrderID": 99}


# ==================================================================================================
# Conf registration / entry points
# ==================================================================================================


def test_conf_default_false_in_sqlconf_defaults(spark: ReparkSession) -> None:
    """Default is false via ``_SQLCONF_DEFAULTS`` (get without prior set)."""
    assert spark.conf.get(CONF) == "false"
    # getAll is a property (PySpark camelCase), not a method.
    assert spark.conf.getAll[CONF] == "false"


def test_conf_set_and_builder_config() -> None:
    """Honor ``conf.set`` and builder ``.config`` (shared runtime store)."""
    _reset_active_session_for_tests()
    via_builder = ReparkSession.builder.appName("n1-builder").config(CONF, "true").getOrCreate()
    try:
        assert via_builder.conf.get(CONF) == "true"
    finally:
        via_builder.stop()
        _reset_active_session_for_tests()

    session = ReparkSession.builder.appName("n1-conf-set").getOrCreate()
    try:
        assert session.conf.get(CONF) == "false"
        session.conf.set(CONF, "true")
        assert session.conf.get(CONF) == "true"
        session.conf.set(CONF, False)
        assert session.conf.get(CONF) == "false"
    finally:
        session.stop()
        _reset_active_session_for_tests()


# ==================================================================================================
# Q8 (a): conf-false byte-identity with r22 T1 map behavior
# ==================================================================================================


def test_q8a_conf_false_list_of_dict_is_map(spark: ReparkSession) -> None:
    """conf false/unset: list-of-dict cell → list<map>; mixed values stringify (r22 T1)."""
    for conf_val in (None, "false"):
        if conf_val is None:
            # unset / default
            pass
        else:
            spark.conf.set(CONF, conf_val)
        data = [
            Row(f1=[{"payment": 200.5, "name": "A"}], f2=[1, 2]),
            Row(f1=[{"payment": 100.5, "name": "B"}], f2=[2, 3]),
        ]
        frame = spark.createDataFrame(data)
        table = frame.to_arrow()
        f1_type = table.schema.field("f1").type
        assert pa.types.is_list(f1_type) or pa.types.is_large_list(f1_type)
        assert pa.types.is_map(f1_type.value_type), f"conf={conf_val!r}: {f1_type}"
        # Mixed double+string map values → string (Spark 4.1.2 / r22 T1).
        assert pa.types.is_string(f1_type.value_type.item_type) or pa.types.is_large_string(
            f1_type.value_type.item_type
        )
        rows = table.to_pylist()
        first_map = rows[0]["f1"][0]
        if isinstance(first_map, list):
            first_map = dict(first_map)
        assert first_map["payment"] == "200.5"
        assert first_map["name"] == "A"
        collected = frame.collect()
        cell0 = collected[0]["f1"][0]
        if not isinstance(cell0, dict):
            cell0 = dict(cell0)
        assert cell0["name"] == "A"
        assert cell0["payment"] == "200.5"


def test_q8a_conf_false_unnested_dict_cell_is_map(spark: ReparkSession) -> None:
    """conf false: un-nested dict column value → MapType (not struct)."""
    spark.conf.set(CONF, "false")
    frame = spark.createDataFrame([Row(m={"a": 1, "b": 2}), Row(m={"a": 3, "c": 4})])
    table = frame.to_arrow()
    m_type = table.schema.field("m").type
    assert pa.types.is_map(m_type)
    assert pa.types.is_integer(m_type.item_type)
    assert table.to_pylist() == [
        {"m": [("a", 1), ("b", 2)]},
        {"m": [("a", 3), ("c", 4)]},
    ] or _map_pylist_as_dicts(table) == [
        {"m": {"a": 1, "b": 2}},
        {"m": {"a": 3, "c": 4}},
    ]


def _map_pylist_as_dicts(table: pa.Table) -> list[dict[str, object]]:
    """Normalize Arrow map pylist (list-of-pairs or dict) for assertions."""
    out: list[dict[str, object]] = []
    for row in table.to_pylist():
        cell = row["m"]
        if isinstance(cell, list):
            out.append({"m": dict(cell)})
        else:
            out.append({"m": cell})
    return out


# ==================================================================================================
# Q8 (b): sparse-vector path conf-invariant
# ==================================================================================================


@pytest.mark.parametrize("conf_val", ["false", "true"])
def test_q8b_sparse_vector_exact_keys_conf_invariant(spark: ReparkSession, conf_val: str) -> None:
    """Exact ``{size,indices,values}`` sparse struct is conf-invariant (Q8)."""
    spark.conf.set(CONF, conf_val)
    frame = spark.createDataFrame([Row(v={"size": 3, "indices": [0, 2], "values": [1.0, 2.0]})])
    table = frame.to_arrow()
    v_type = table.schema.field("v").type
    assert pa.types.is_struct(v_type), f"conf={conf_val}: {v_type}"
    names = [field.name for field in v_type]
    assert names == ["size", "indices", "values"]
    assert pa.types.is_int32(v_type.field("size").type)
    assert pa.types.is_list(v_type.field("indices").type) or pa.types.is_large_list(
        v_type.field("indices").type
    )
    assert pa.types.is_int32(v_type.field("indices").type.value_type)
    assert pa.types.is_floating(v_type.field("values").type.value_type)
    row0 = table.to_pylist()[0]["v"]
    assert row0["size"] == 3
    assert list(row0["indices"]) == [0, 2]
    assert list(row0["values"]) == [1.0, 2.0]
    collected = frame.collect()[0]["v"]
    assert collected["size"] == 3


# ==================================================================================================
# conf true: nested dict cells → StructType
# ==================================================================================================


def test_conf_true_list_of_dict_is_struct(spark: ReparkSession) -> None:
    """Apache test_infer_nested_dict_as_struct shape (local rows, not RDD)."""
    spark.conf.set(CONF, "true")
    data = [
        Row(f1=[{"payment": 200.5, "name": "A"}], f2=[1, 2]),
        Row(f1=[{"payment": 100.5, "name": "B"}], f2=[2, 3]),
    ]
    frame = spark.createDataFrame(data)
    table = frame.to_arrow()
    f1_type = table.schema.field("f1").type
    assert pa.types.is_list(f1_type)
    assert pa.types.is_struct(f1_type.value_type), f1_type
    field_names = [field.name for field in f1_type.value_type]
    assert field_names == ["payment", "name"]
    assert pa.types.is_floating(f1_type.value_type.field("payment").type)
    assert pa.types.is_string(f1_type.value_type.field("name").type) or pa.types.is_large_string(
        f1_type.value_type.field("name").type
    )
    rows = table.to_pylist()
    assert rows[0]["f1"][0]["payment"] == 200.5
    assert rows[0]["f1"][0]["name"] == "A"
    assert rows[1]["f1"][0]["payment"] == 100.5
    collected = frame.collect()
    assert collected[0]["f1"][0]["payment"] == 200.5
    assert collected[0]["f1"][0]["name"] == "A"


def test_conf_true_array_ragged_keys_null_fill(spark: ReparkSession) -> None:
    """Apache test_infer_array_element_type_with_struct: union keys, null-fill."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(f1=[{"payment": 200.5}, {"name": "A"}])])
    table = frame.to_arrow()
    f1_type = table.schema.field("f1").type
    assert pa.types.is_struct(f1_type.value_type)
    names = [field.name for field in f1_type.value_type]
    assert names == ["payment", "name"]
    rows = table.to_pylist()
    assert rows[0]["f1"][0] == {"payment": 200.5, "name": None}
    assert rows[0]["f1"][1] == {"payment": None, "name": "A"}
    collected = frame.collect()[0]["f1"]
    assert collected[0]["payment"] == 200.5
    assert collected[0]["name"] is None
    assert collected[1]["payment"] is None
    assert collected[1]["name"] == "A"


def test_conf_true_unnested_dict_cell_struct_union(spark: ReparkSession) -> None:
    """Un-nested dict column value → struct; multi-row field union."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(m={"a": 1, "b": 2}), Row(m={"a": 3, "c": 4})])
    table = frame.to_arrow()
    m_type = table.schema.field("m").type
    assert pa.types.is_struct(m_type)
    assert [field.name for field in m_type] == ["a", "b", "c"]
    assert table.to_pylist() == [
        {"m": {"a": 1, "b": 2, "c": None}},
        {"m": {"a": 3, "b": None, "c": 4}},
    ]
    collected = frame.collect()
    assert collected[0]["m"]["a"] == 1
    assert collected[0]["m"]["c"] is None
    assert collected[1]["m"]["c"] == 4


def test_conf_true_dict_in_dict(spark: ReparkSession) -> None:
    """Nested dict-in-dict → nested struct (not map of map)."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(m={"outer": {"payment": 200.5, "name": "A"}})])
    table = frame.to_arrow()
    m_type = table.schema.field("m").type
    assert pa.types.is_struct(m_type)
    outer = m_type.field("outer").type
    assert pa.types.is_struct(outer)
    assert [field.name for field in outer] == ["payment", "name"]
    assert table.to_pylist()[0]["m"]["outer"]["payment"] == 200.5
    assert frame.collect()[0]["m"]["outer"]["name"] == "A"


def test_conf_true_empty_then_nonempty(spark: ReparkSession) -> None:
    """Empty dict first sample still unions later keys under conf true."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(m={}), Row(m={"a": 1})])
    table = frame.to_arrow()
    m_type = table.schema.field("m").type
    assert pa.types.is_struct(m_type)
    assert [field.name for field in m_type] == ["a"]
    assert table.to_pylist() == [{"m": {"a": None}}, {"m": {"a": 1}}]


def test_conf_true_field_order_insertion_not_sorted(spark: ReparkSession) -> None:
    """Cell-struct field order is insertion order (not row key-union sorted)."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(m={"z": 1, "a": 2, "m": 3})])
    m_type = frame.to_arrow().schema.field("m").type
    assert [field.name for field in m_type] == ["z", "a", "m"]


def test_conf_true_multi_row_field_union_order(spark: ReparkSession) -> None:
    """First-row fields first, later new keys appended (Spark _merge_type order)."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(m={"z": 1}), Row(m={"a": 2, "z": 3})])
    m_type = frame.to_arrow().schema.field("m").type
    assert [field.name for field in m_type] == ["z", "a"]
    assert frame.to_arrow().to_pylist() == [
        {"m": {"z": 1, "a": None}},
        {"m": {"z": 3, "a": 2}},
    ]


def test_conf_true_non_string_keys_refuse(spark: ReparkSession) -> None:
    """Non-string dict keys under conf true refuse (Spark field-name assertion)."""
    spark.conf.set(CONF, "true")
    with pytest.raises(PySparkTypeError, match="should be a string"):
        spark.createDataFrame([Row(m={1: "a", 2: "b"})])


def test_conf_false_non_string_keys_map(spark: ReparkSession) -> None:
    """Non-string keys under conf false stay map<bigint, …>."""
    spark.conf.set(CONF, "false")
    frame = spark.createDataFrame([Row(m={1: "a", 2: "b"})])
    m_type = frame.to_arrow().schema.field("m").type
    assert pa.types.is_map(m_type)
    assert pa.types.is_integer(m_type.key_type)


# ==================================================================================================
# Row-dict surface conf-invariant (Q6)
# ==================================================================================================


@pytest.mark.parametrize("conf_val", ["false", "true"])
def test_row_dict_key_union_conf_invariant(spark: ReparkSession, conf_val: str) -> None:
    """Row-as-dict key-union is byte-identical under both conf states (Q6)."""
    spark.conf.set(CONF, conf_val)
    rows = [{"c": 1, "a": 2}, {"b": 3, "a": 4}, {"d": 5, "c": 6}]
    table = spark.createDataFrame(rows).to_arrow()
    assert table.column_names == ["a", "c", "b", "d"]
    assert table.to_pylist() == [
        {"a": 2, "c": 1, "b": None, "d": None},
        {"a": 4, "c": None, "b": 3, "d": None},
        {"a": None, "c": 6, "b": None, "d": 5},
    ]


def test_row_dict_nested_legs_follow_conf(spark: ReparkSession) -> None:
    """Row surface columns stable; nested Legs cell type follows conf."""
    rows = [
        {
            "OrderId": 9001,
            "Legs": [{"OpenOrClose": "O", "QuantityOrdered": 10}],
        }
    ]
    spark.conf.set(CONF, "false")
    t_false = spark.createDataFrame(rows).to_arrow()
    legs_false = t_false.schema.field("Legs").type
    assert pa.types.is_list(legs_false)
    assert pa.types.is_map(legs_false.value_type)

    spark.conf.set(CONF, "true")
    t_true = spark.createDataFrame(rows).to_arrow()
    legs_true = t_true.schema.field("Legs").type
    assert pa.types.is_list(legs_true)
    assert pa.types.is_struct(legs_true.value_type)
    assert [field.name for field in legs_true.value_type] == ["OpenOrClose", "QuantityOrdered"]
    # Row column order still key-union sorted first-row keys.
    assert t_true.column_names == t_false.column_names == ["Legs", "OrderId"]


# ==================================================================================================
# Explicit schema wins (Q11)
# ==================================================================================================


@pytest.mark.parametrize("conf_val", ["false", "true"])
def test_explicit_map_schema_wins(spark: ReparkSession, conf_val: str) -> None:
    spark.conf.set(CONF, conf_val)
    schema = StructType([StructField("m", MapType(StringType(), LongType()))])
    frame = spark.createDataFrame([{"m": {"a": 1}}], schema=schema)
    m_type = frame.to_arrow().schema.field("m").type
    assert pa.types.is_map(m_type)
    assert frame.collect()[0]["m"]["a"] == 1 or dict(frame.collect()[0]["m"])["a"] == 1


@pytest.mark.parametrize("conf_val", ["false", "true"])
def test_explicit_struct_schema_wins(spark: ReparkSession, conf_val: str) -> None:
    spark.conf.set(CONF, conf_val)
    schema = StructType(
        [
            StructField(
                "m",
                StructType(
                    [
                        StructField("a", LongType()),
                        StructField("b", LongType()),
                    ]
                ),
            )
        ]
    )
    frame = spark.createDataFrame([{"m": {"a": 1, "b": 2}}], schema=schema)
    m_type = frame.to_arrow().schema.field("m").type
    assert pa.types.is_struct(m_type)
    assert frame.to_arrow().to_pylist()[0]["m"] == {"a": 1, "b": 2}


# ==================================================================================================
# Orders shape (Q10)
# ==================================================================================================


def test_orders_shape_conf_false_map(spark: ReparkSession) -> None:
    """Orders Legs/ConditionalOrders as cells under conf false → map (oracle map path)."""
    spark.conf.set(CONF, "false")
    legs = [_synthetic_leg()]
    cond = [_synthetic_cond()]
    frame = spark.createDataFrame([Row(order={"Legs": legs, "ConditionalOrders": cond})])
    table = frame.to_arrow()
    order_type = table.schema.field("order").type
    assert pa.types.is_map(order_type), order_type
    # Live Spark: map<string, array<map<string,string>>> (homogeneous array values).
    assert pa.types.is_list(order_type.item_type) or pa.types.is_large_list(order_type.item_type), (
        order_type
    )
    collected = frame.collect()
    assert collected[0]["order"] is not None


def test_orders_shape_conf_true_array_struct(spark: ReparkSession) -> None:
    """Orders Legs/ConditionalOrders under conf true → array<struct<…>> (oracle)."""
    spark.conf.set(CONF, "true")
    legs = [_synthetic_leg()]
    cond = [_synthetic_cond()]
    frame = spark.createDataFrame([Row(order={"Legs": legs, "ConditionalOrders": cond})])
    table = frame.to_arrow()
    order_type = table.schema.field("order").type
    assert pa.types.is_struct(order_type)
    assert [field.name for field in order_type] == ["Legs", "ConditionalOrders"]
    legs_type = order_type.field("Legs").type
    cond_type = order_type.field("ConditionalOrders").type
    assert pa.types.is_list(legs_type)
    assert pa.types.is_struct(legs_type.value_type)
    assert [field.name for field in legs_type.value_type] == list(_LEG_FIELDS)
    assert pa.types.is_list(cond_type)
    assert pa.types.is_struct(cond_type.value_type)
    assert [field.name for field in cond_type.value_type] == list(_COND_FIELDS)
    pylist = table.to_pylist()[0]["order"]
    assert pylist["Legs"][0]["Symbol"] == "AAA"
    assert pylist["Legs"][0]["ExecutionPrice"] == 1.5
    assert pylist["Legs"][0]["QuantityOrdered"] == 10
    assert pylist["ConditionalOrders"][0]["OrderID"] == 99
    collected = frame.collect()[0]["order"]
    assert collected["Legs"][0]["BuyOrSell"] == "Buy"
    assert collected["ConditionalOrders"][0]["Relationship"] == "OCO"


def test_orders_row_dict_legs_under_conf_true(spark: ReparkSession) -> None:
    """Row-dict Orders payload: top-level columns key-union; Legs cell → array<struct>."""
    spark.conf.set(CONF, "true")
    payload = [
        {
            "Legs": [_synthetic_leg()],
            "ConditionalOrders": [_synthetic_cond()],
            "StopPrice": 9.5,
        }
    ]
    frame = spark.createDataFrame(payload)
    table = frame.to_arrow()
    # First-row keys sorted: ConditionalOrders, Legs, StopPrice
    assert "Legs" in table.column_names
    assert "ConditionalOrders" in table.column_names
    assert "StopPrice" in table.column_names
    legs_type = table.schema.field("Legs").type
    assert pa.types.is_list(legs_type)
    assert pa.types.is_struct(legs_type.value_type)
    assert [field.name for field in legs_type.value_type] == list(_LEG_FIELDS)
    assert table.to_pylist()[0]["Legs"][0]["Symbol"] == "AAA"
    assert frame.collect()[0]["StopPrice"] == 9.5


# ==================================================================================================
# Octo C1 — reshape exact-key sparse only; oracle residual pins
# ==================================================================================================


def test_c1_struct_with_indices_field_not_sparse_reshape(spark: ReparkSession) -> None:
    """C1-L-001: conf-true struct with an ``indices`` field is NOT sparse reshape.

    Prior bug: any struct containing field name ``indices`` was rebuilt as
    ``{size,indices,values}`` only → KeyError or silent field drop.
    """
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(v={"name": "x", "indices": [1, 2], "qty": 3})])
    table = frame.to_arrow()
    v_type = table.schema.field("v").type
    assert pa.types.is_struct(v_type)
    assert [field.name for field in v_type] == ["name", "indices", "qty"]
    assert table.to_pylist() == [{"v": {"name": "x", "indices": [1, 2], "qty": 3}}]
    collected = frame.collect()[0]["v"]
    assert collected["name"] == "x"
    assert list(collected["indices"]) == [1, 2]
    assert collected["qty"] == 3


def test_c1_sparse_superset_keeps_extra_under_conf_true(spark: ReparkSession) -> None:
    """C1-L-001: super-set of sparse keys under conf true is plain struct (keeps extra)."""
    spark.conf.set(CONF, "true")
    cell = {"size": 3, "indices": [0], "values": [1.0], "extra": 9}
    frame = spark.createDataFrame([Row(v=cell)])
    table = frame.to_arrow()
    v_type = table.schema.field("v").type
    assert pa.types.is_struct(v_type)
    assert [field.name for field in v_type] == ["size", "indices", "values", "extra"]
    # Must not null-fill ``extra`` via sparse reshape (prior silent drop).
    assert table.to_pylist()[0]["v"]["extra"] == 9
    assert frame.collect()[0]["v"]["extra"] == 9
    # Not the sparse int32 layout (exact-key branch only).
    assert pa.types.is_int64(v_type.field("size").type)


def test_c1_explicit_struct_schema_with_indices_not_sparse(spark: ReparkSession) -> None:
    """C1-L-001 / SAF-001: explicit StructType with ``indices`` field is not sparse reshape."""
    spark.conf.set(CONF, "false")
    schema = StructType(
        [
            StructField(
                "v",
                StructType(
                    [
                        StructField("name", StringType()),
                        StructField("indices", LongType()),
                    ]
                ),
            )
        ]
    )
    frame = spark.createDataFrame([{"v": {"name": "x", "indices": 7}}], schema=schema)
    assert frame.to_arrow().to_pylist() == [{"v": {"name": "x", "indices": 7}}]
    assert frame.collect()[0]["v"]["indices"] == 7


def test_c1_null_only_key_skipped_oracle(spark: ReparkSession) -> None:
    """Oracle: ``{\"a\": None, \"b\": 1}`` → struct only ``b`` (null keys skip type contrib)."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(m={"a": None, "b": 1})])
    m_type = frame.to_arrow().schema.field("m").type
    assert [field.name for field in m_type] == ["b"]
    assert frame.to_arrow().to_pylist() == [{"m": {"b": 1}}]


def test_c1_all_empty_dict_struct(spark: ReparkSession) -> None:
    """Oracle: all-empty ``{}``/``{}`` under conf true → empty struct."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(m={}), Row(m={})])
    m_type = frame.to_arrow().schema.field("m").type
    assert pa.types.is_struct(m_type)
    assert list(m_type) == []
    assert frame.to_arrow().to_pylist() == [{"m": {}}, {"m": {}}]


def test_c1_long_string_field_promotes_to_string(spark: ReparkSession) -> None:
    """Oracle: long+string same field under conf true → string (Spark promote)."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(m={"a": 1}), Row(m={"a": "x"})])
    m_type = frame.to_arrow().schema.field("m").type
    assert pa.types.is_string(m_type.field("a").type) or pa.types.is_large_string(
        m_type.field("a").type
    )
    rows = frame.to_arrow().to_pylist()
    assert rows[0]["m"]["a"] == "1"
    assert rows[1]["m"]["a"] == "x"
    assert frame.collect()[0]["m"]["a"] == "1"


def test_c1_long_double_field_refuses(spark: ReparkSession) -> None:
    """Oracle: long+double same field under conf true → CANNOT_MERGE_TYPE."""
    spark.conf.set(CONF, "true")
    with pytest.raises(PySparkTypeError, match="CANNOT_MERGE_TYPE"):
        spark.createDataFrame([Row(m={"a": 1}), Row(m={"a": 1.5})])


def test_c1_nested_list_of_dict_multi_row_union(spark: ReparkSession) -> None:
    """Nested list-of-dict field unions across rows under conf true."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame(
        [
            Row(m={"items": [{"a": 1}]}),
            Row(m={"items": [{"b": 2}]}),
        ]
    )
    table = frame.to_arrow()
    items_type = table.schema.field("m").type.field("items").type
    assert pa.types.is_list(items_type)
    assert pa.types.is_struct(items_type.value_type)
    assert [field.name for field in items_type.value_type] == ["a", "b"]
    assert table.to_pylist() == [
        {"m": {"items": [{"a": 1, "b": None}]}},
        {"m": {"items": [{"a": None, "b": 2}]}},
    ]


def test_c1_none_key_refuses_under_conf_true(spark: ReparkSession) -> None:
    """C1-L-002: None field name refuses (not silent skip)."""
    spark.conf.set(CONF, "true")
    with pytest.raises(PySparkTypeError, match="should be a string"):
        spark.createDataFrame([Row(m={None: 1, "b": 2})])


# ==================================================================================================
# Octo C2 — nested list element merge + conf strip
# ==================================================================================================


def test_c2_nested_list_of_list_of_dict_field_union(spark: ReparkSession) -> None:
    """C2-L-001: list<list<dict>> merges sibling element struct keys under conf true."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame([Row(f1=[[{"a": 1}], [{"b": 2}]])])
    table = frame.to_arrow()
    f1_type = table.schema.field("f1").type
    assert pa.types.is_list(f1_type)
    assert pa.types.is_list(f1_type.value_type)
    inner = f1_type.value_type.value_type
    assert pa.types.is_struct(inner)
    assert [field.name for field in inner] == ["a", "b"]
    assert table.to_pylist() == [
        {"f1": [[{"a": 1, "b": None}], [{"a": None, "b": 2}]]},
    ]
    collected = frame.collect()[0]["f1"]
    assert collected[0][0]["a"] == 1
    assert collected[1][0]["b"] == 2


def test_c2_nested_list_of_list_multi_row_union(spark: ReparkSession) -> None:
    """C2-L-001: multi-row list<list<dict>> merges element structs across rows."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame(
        [
            Row(f1=[[{"a": 1}]]),
            Row(f1=[[{"b": 2}]]),
        ]
    )
    table = frame.to_arrow()
    inner = table.schema.field("f1").type.value_type.value_type
    assert [field.name for field in inner] == ["a", "b"]
    assert table.to_pylist() == [
        {"f1": [[{"a": 1, "b": None}]]},
        {"f1": [[{"a": None, "b": 2}]]},
    ]


def test_c2_conf_truthy_strips_whitespace(spark: ReparkSession) -> None:
    """C2-Q-001: conf values strip before truthiness (`` true`` / ``true\\n``)."""
    for raw in (" true", "true\n", "TRUE", "1"):
        spark.conf.set(CONF, raw)
        frame = spark.createDataFrame([Row(m={"a": 1})])
        assert pa.types.is_struct(frame.to_arrow().schema.field("m").type), raw


def test_c2_bool_long_field_refuses(spark: ReparkSession) -> None:
    """Boolean + Long same struct field refuses CANNOT_MERGE_TYPE."""
    spark.conf.set(CONF, "true")
    with pytest.raises(PySparkTypeError, match="CANNOT_MERGE_TYPE"):
        spark.createDataFrame([Row(m={"a": True}), Row(m={"a": 1})])


# ==================================================================================================
# Octo C3 — empty-list field then list-of-dict must not stringify
# ==================================================================================================


def test_c3_empty_list_field_then_list_of_dict(spark: ReparkSession) -> None:
    """C3-L-001: struct field empty list then list-of-dict → array<struct>, not stringified."""
    spark.conf.set(CONF, "true")
    frame = spark.createDataFrame(
        [
            Row(m={"xs": []}),
            Row(m={"xs": [{"a": 1, "b": "x"}]}),
        ]
    )
    table = frame.to_arrow()
    xs_type = table.schema.field("m").type.field("xs").type
    assert pa.types.is_list(xs_type)
    assert pa.types.is_struct(xs_type.value_type), xs_type
    assert [field.name for field in xs_type.value_type] == ["a", "b"]
    assert table.to_pylist() == [
        {"m": {"xs": []}},
        {"m": {"xs": [{"a": 1, "b": "x"}]}},
    ]
    assert frame.collect()[1]["m"]["xs"][0]["a"] == 1
    assert frame.collect()[1]["m"]["xs"][0]["b"] == "x"


def test_c3_string_does_not_win_over_struct_field(spark: ReparkSession) -> None:
    """C3-L-001: string field type must not absorb a later struct (CANNOT_MERGE)."""
    spark.conf.set(CONF, "true")
    with pytest.raises(PySparkTypeError, match="CANNOT_MERGE_TYPE"):
        spark.createDataFrame(
            [
                Row(m={"x": "plain"}),
                Row(m={"x": {"a": 1}}),
            ]
        )
