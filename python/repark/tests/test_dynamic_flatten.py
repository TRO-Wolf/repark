"""r24 DF1 — DataFrame.dynamicFlatten / dynamic_flatten (repark-extra).

Semantic pins against the operator-supplied polars ``unnest_lazyframe`` reference
(``specs/dynamic-flatten-reference.md``). Arrow path: value **and** type (never only show).
Synthetic fixtures only.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import (
    ArrayType,
    DoubleType,
    FloatType,
    LongType,
    NullType,
    StringType,
    StructField,
    StructType,
)


def _is_arrow_string_type(data_type: pa.DataType) -> bool:
    """Utf8 / large_string / string_view — CAST(VARCHAR) may emit any of these."""
    return (
        pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_string_view(data_type)
    )


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-df1-dynamic-flatten").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


# ==================================================================================================
# Surface + idempotence
# ==================================================================================================


def test_both_method_names_bound(spark: ReparkSession) -> None:
    """Q26: both ``dynamicFlatten`` and ``dynamic_flatten`` exist and agree."""
    from repark.spark.dataframe import DataFrame

    frame = spark.createDataFrame([(1, "a")], "id INT, name STRING")
    assert hasattr(frame, "dynamicFlatten")
    assert hasattr(frame, "dynamic_flatten")
    # Class-level identity (bound methods are new objects per attribute access).
    assert DataFrame.dynamicFlatten is DataFrame.dynamic_flatten
    flat = frame.dynamic_flatten()
    also = frame.dynamicFlatten()
    assert flat.columns == also.columns == ["id", "name"]
    table = flat.to_arrow()
    assert table.to_pylist() == [{"id": 1, "name": "a"}]
    assert table.schema.field("id").type == pa.int32()
    assert table.schema.field("name").type in (pa.string(), pa.large_string())


def test_idempotent_on_already_flat(spark: ReparkSession) -> None:
    """Already-flat frames are a no-op (schema walk finds no structs/lists)."""
    frame = spark.createDataFrame([(1, 2.5, "x")], "a INT, b DOUBLE, c STRING")
    once = frame.dynamicFlatten()
    twice = once.dynamicFlatten()
    assert once.columns == ["a", "b", "c"]
    assert twice.columns == ["a", "b", "c"]
    assert (
        once.to_arrow().to_pylist()
        == twice.to_arrow().to_pylist()
        == [{"a": 1, "b": 2.5, "c": "x"}]
    )


# ==================================================================================================
# Nested struct-in-struct
# ==================================================================================================


def test_nested_struct_in_struct(spark: ReparkSession) -> None:
    """Recursive struct unnest with parent-path prefix (separator default ``_``)."""
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField(
                "outer",
                StructType(
                    [
                        StructField("label", StringType(), True),
                        StructField(
                            "inner",
                            StructType(
                                [
                                    StructField("x", LongType(), True),
                                    StructField("y", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )
    rows = [
        {"id": 1, "outer": {"label": "L", "inner": {"x": 10, "y": "ten"}}},
        {"id": 2, "outer": {"label": "M", "inner": {"x": 20, "y": "twenty"}}},
    ]
    frame = spark.createDataFrame(rows, schema=schema)
    flat = frame.dynamicFlatten()
    assert flat.columns == ["id", "outer_label", "outer_inner_x", "outer_inner_y"]
    table = flat.orderBy("id").to_arrow()
    assert table.to_pylist() == [
        {"id": 1, "outer_label": "L", "outer_inner_x": 10, "outer_inner_y": "ten"},
        {"id": 2, "outer_label": "M", "outer_inner_x": 20, "outer_inner_y": "twenty"},
    ]
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("outer_inner_x").type == pa.int64()
    assert table.schema.field("outer_label").type in (pa.string(), pa.large_string())
    assert table.schema.field("outer_inner_y").type in (pa.string(), pa.large_string())


def test_null_parent_struct_fields_are_null_not_zero(spark: ReparkSession) -> None:
    """Null parent struct → NULL leaf fields (not type defaults 0/''/False).

    Mutation-proof for C1-L-001: bare ``parent.field`` selectExpr zero-fills null parents
    in the engine; dynamicFlatten must use null-safe projection.
    """
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField(
                "outer",
                StructType(
                    [
                        StructField("x", LongType(), True),
                        StructField("label", StringType(), True),
                        StructField("flag", LongType(), True),  # bool leaf covered via long 0 trap
                    ]
                ),
                True,
            ),
        ]
    )
    rows = [
        {"id": 1, "outer": None},
        {"id": 2, "outer": {"x": 5, "label": "ok", "flag": 1}},
        {"id": 3, "outer": {"x": None, "label": None, "flag": None}},
    ]
    frame = spark.createDataFrame(rows, schema=schema)
    flat = frame.dynamicFlatten().orderBy("id")
    assert flat.columns == ["id", "outer_x", "outer_label", "outer_flag"]
    table = flat.to_arrow()
    assert table.to_pylist() == [
        {"id": 1, "outer_x": None, "outer_label": None, "outer_flag": None},
        {"id": 2, "outer_x": 5, "outer_label": "ok", "outer_flag": 1},
        {"id": 3, "outer_x": None, "outer_label": None, "outer_flag": None},
    ]
    assert table.schema.field("outer_x").type == pa.int64()
    assert table.schema.field("outer_flag").type == pa.int64()
    assert table.schema.field("outer_label").type in (pa.string(), pa.large_string())


def test_null_mid_struct_fields_are_null_not_zero(spark: ReparkSession) -> None:
    """Null intermediate struct after first unnest → NULL leaves (multi-pass)."""
    schema = StructType(
        [
            StructField(
                "o",
                StructType(
                    [
                        StructField(
                            "inner",
                            StructType([StructField("x", LongType(), True)]),
                            True,
                        )
                    ]
                ),
                True,
            )
        ]
    )
    rows = [
        {"o": {"inner": None}},
        {"o": {"inner": {"x": 9}}},
        {"o": None},
    ]
    frame = spark.createDataFrame(rows, schema=schema)
    table = frame.dynamicFlatten().to_arrow()
    # No stable key after flatten — compare as a multiset of leaf values.
    values = sorted(
        row["o_inner_x"] if row["o_inner_x"] is not None else -(10**18) for row in table.to_pylist()
    )
    assert values == [-(10**18), -(10**18), 9]
    assert sum(1 for row in table.to_pylist() if row["o_inner_x"] is None) == 2
    assert table.schema.field("o_inner_x").type == pa.int64()


# ==================================================================================================
# List-of-struct
# ==================================================================================================


def test_list_of_struct_explodes_then_unnests(spark: ReparkSession) -> None:
    """``explode_lists=True``: list-of-struct → struct → unnested with parent prefix."""
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField(
                "legs",
                ArrayType(
                    StructType(
                        [
                            StructField("leg_id", LongType(), True),
                            StructField("side", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    rows = [
        {
            "id": 1,
            "legs": [
                {"leg_id": 1, "side": "Buy"},
                {"leg_id": 2, "side": "Sell"},
            ],
        },
        {"id": 2, "legs": [{"leg_id": 9, "side": "Buy"}]},
    ]
    frame = spark.createDataFrame(rows, schema=schema)
    flat = frame.dynamicFlatten().orderBy("id", "legs_leg_id")
    assert flat.columns == ["id", "legs_leg_id", "legs_side"]
    table = flat.to_arrow()
    assert table.to_pylist() == [
        {"id": 1, "legs_leg_id": 1, "legs_side": "Buy"},
        {"id": 1, "legs_leg_id": 2, "legs_side": "Sell"},
        {"id": 2, "legs_leg_id": 9, "legs_side": "Buy"},
    ]
    assert table.schema.field("legs_leg_id").type == pa.int64()
    assert _is_arrow_string_type(table.schema.field("legs_side").type)


def test_list_of_struct_capitalized_legs_and_sibling_struct(spark: ReparkSession) -> None:
    """createDataFrame ``Legs`` list-of-struct + sibling struct flattens (value + type)."""
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField(
                "Meta",
                StructType([StructField("account", StringType(), True)]),
                True,
            ),
            StructField(
                "Legs",
                ArrayType(
                    StructType(
                        [
                            StructField("leg_id", LongType(), True),
                            StructField("side", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    rows = [
        {
            "id": 1,
            "Meta": {"account": "A"},
            "Legs": [
                {"leg_id": 1, "side": "Buy"},
                {"leg_id": 2, "side": "Sell"},
            ],
        },
        {
            "id": 2,
            "Meta": {"account": "B"},
            "Legs": [{"leg_id": 9, "side": "Buy"}],
        },
    ]
    frame = spark.createDataFrame(rows, schema=schema)
    flat = frame.dynamicFlatten().orderBy("id", "Legs_leg_id")
    assert flat.columns == ["id", "Meta_account", "Legs_leg_id", "Legs_side"]
    table = flat.to_arrow()
    assert table.to_pylist() == [
        {"id": 1, "Meta_account": "A", "Legs_leg_id": 1, "Legs_side": "Buy"},
        {"id": 1, "Meta_account": "A", "Legs_leg_id": 2, "Legs_side": "Sell"},
        {"id": 2, "Meta_account": "B", "Legs_leg_id": 9, "Legs_side": "Buy"},
    ]
    assert table.schema.field("Meta_account").type in (pa.string(), pa.large_string())
    assert table.schema.field("Legs_leg_id").type == pa.int64()
    assert _is_arrow_string_type(table.schema.field("Legs_side").type)


def test_multi_list_serial_explode_order(spark: ReparkSession) -> None:
    """Two list columns explode serially in schema order (cartesian product)."""
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("a", ArrayType(LongType()), True),
            StructField("b", ArrayType(LongType()), True),
        ]
    )
    frame = spark.createDataFrame(
        [{"id": 1, "a": [1, 2], "b": [10, 20]}],
        schema=schema,
    )
    flat = frame.dynamicFlatten().orderBy("a", "b")
    assert flat.columns == ["id", "a", "b"]
    table = flat.to_arrow()
    assert table.to_pylist() == [
        {"id": 1, "a": 1, "b": 10},
        {"id": 1, "a": 1, "b": 20},
        {"id": 1, "a": 2, "b": 10},
        {"id": 1, "a": 2, "b": 20},
    ]
    assert table.schema.field("a").type == pa.int64()
    assert table.schema.field("b").type == pa.int64()


def test_list_explode_preserves_interleaved_column_order(spark: ReparkSession) -> None:
    """Exploded list column stays at its schema index (not appended)."""
    schema = StructType(
        [
            StructField("z", LongType(), True),
            StructField("xs", ArrayType(LongType()), True),
            StructField("m", LongType(), True),
        ]
    )
    frame = spark.createDataFrame(
        [{"z": 1, "xs": [2, 3], "m": 4}],
        schema=schema,
    )
    flat = frame.dynamicFlatten().orderBy("xs")
    assert flat.columns == ["z", "xs", "m"]
    table = flat.to_arrow()
    assert table.to_pylist() == [
        {"z": 1, "xs": 2, "m": 4},
        {"z": 1, "xs": 3, "m": 4},
    ]
    assert table.schema.field("xs").type == pa.int64()


# ==================================================================================================
# Struct-in-list-in-struct
# ==================================================================================================


def test_struct_in_list_in_struct(spark: ReparkSession) -> None:
    """Struct containing a list-of-struct: outer unnest → explode → inner unnest."""
    schema = StructType(
        [
            StructField("order_id", LongType(), False),
            StructField(
                "payload",
                StructType(
                    [
                        StructField("symbol", StringType(), True),
                        StructField(
                            "fills",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("qty", LongType(), True),
                                        StructField("px", LongType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )
    rows = [
        {
            "order_id": 7,
            "payload": {
                "symbol": "AAA",
                "fills": [{"qty": 1, "px": 100}, {"qty": 2, "px": 101}],
            },
        }
    ]
    frame = spark.createDataFrame(rows, schema=schema)
    flat = frame.dynamicFlatten().orderBy("payload_fills_qty")
    assert flat.columns == [
        "order_id",
        "payload_symbol",
        "payload_fills_qty",
        "payload_fills_px",
    ]
    table = flat.to_arrow()
    assert table.to_pylist() == [
        {
            "order_id": 7,
            "payload_symbol": "AAA",
            "payload_fills_qty": 1,
            "payload_fills_px": 100,
        },
        {
            "order_id": 7,
            "payload_symbol": "AAA",
            "payload_fills_qty": 2,
            "payload_fills_px": 101,
        },
    ]
    assert table.schema.field("payload_fills_qty").type == pa.int64()
    assert table.schema.field("payload_fills_px").type == pa.int64()


# ==================================================================================================
# Null-typed list (List(Null) / array<void>)
# ==================================================================================================


def test_drop_null_typed_list(spark: ReparkSession) -> None:
    """``drop_null_lists=True`` drops ``array<void>`` / List(Null) instead of exploding.

    Engine ``make_array()`` yields ``array<Null>`` (void element). Python
    ``ArrayType(NullType())`` via createDataFrame currently coerces to array<string>,
    so the pin uses the SQL path that preserves the null element type.
    """
    frame = spark.sql(
        """
        SELECT 1 AS id, make_array() AS user_properties, 'a' AS keep
        UNION ALL
        SELECT 2, make_array(), 'b'
        """
    )
    props_type = frame.schema["user_properties"].dataType
    assert isinstance(props_type, ArrayType)
    assert isinstance(props_type.elementType, NullType)

    flat = frame.dynamicFlatten()
    assert flat.columns == ["id", "keep"]
    assert "user_properties" not in flat.columns
    table = flat.orderBy("id").to_arrow()
    assert table.to_pylist() == [{"id": 1, "keep": "a"}, {"id": 2, "keep": "b"}]


def test_drop_null_lists_false_keeps_null_list_column(spark: ReparkSession) -> None:
    """``drop_null_lists=False`` keeps the ``array<void>`` column as one null-element row.

    Discriminates THIS PR (SQM #176 V-1/V-2). MEASURED: BASE b628b0f and
    f6aed24 inner-explode the empty void list → ``count()==0``. After the
    untyped ``make_array(NULL)`` arm the row survives with ``props`` NULL.
    Kills: void inner-explode fallback; missing ``make_array(NULL)`` CASE.
    """
    frame = spark.sql("SELECT 1 AS id, make_array() AS props")
    props_type = frame.schema["props"].dataType
    assert isinstance(props_type, ArrayType)
    assert isinstance(props_type.elementType, NullType)
    flat = frame.dynamicFlatten(drop_null_lists=False)
    assert flat.columns == ["id", "props"]
    table = flat.to_arrow()
    assert table.to_pylist() == [{"id": 1, "props": None}]
    assert pa.types.is_null(table.schema.field("props").type)


def test_drop_null_lists_false_void_sibling_keeps_typed_list_rows(
    spark: ReparkSession,
) -> None:
    """Empty ``array<void>`` sibling must not cartesian-drop typed lists (SQM #176 V-2).

    MEASURED on f6aed24: ``{props: [] void, items: [{item_id: SKU}]}`` with
    ``drop_null_lists=False`` (default ``empty_as_null=True``) returned 0 rows;
    default ``drop_null_lists=True`` kept SKU; typed-empty sibling contrast
    kept SKU. After ``make_array(NULL)`` the void-empty row survives.
    Kills: void inner-explode fallback that annihilates sibling lists.
    """
    frame = spark.sql(
        """
        SELECT 1 AS id,
               make_array() AS props,
               make_array(named_struct('item_id', 'SKU')) AS items
        """
    )
    props_type = frame.schema["props"].dataType
    assert isinstance(props_type, ArrayType)
    assert isinstance(props_type.elementType, NullType)

    default_drop = frame.dynamicFlatten()
    assert default_drop.columns == ["id", "items_item_id"]
    assert default_drop.to_arrow().to_pylist() == [{"id": 1, "items_item_id": "SKU"}]

    kept = frame.dynamicFlatten(drop_null_lists=False)
    kept_table = kept.to_arrow()
    kept_rows = kept_table.to_pylist()
    assert len(kept_rows) == 1
    assert kept_rows[0]["items_item_id"] == "SKU"
    assert kept_rows[0]["props"] is None
    assert pa.types.is_null(kept_table.schema.field("props").type)
    assert _is_arrow_string_type(kept_table.schema.field("items_item_id").type)

    # empty_as_null=False: EMPTY void drops (polars ≥2.0), same as typed empty.
    empty_drops = frame.dynamicFlatten(drop_null_lists=False, empty_as_null=False)
    assert empty_drops.count() == 0

    # NULL void + typed items: False still keeps the row (NULL-only CASE).
    # Input type is pinned: a scalar-null props would skip explode_keep_null
    # and stay green on BASE inner-explode (Critic-1 Q-010).
    null_void = spark.sql(
        """
        SELECT 1 AS id,
               CASE WHEN false THEN make_array() END AS props,
               make_array(named_struct('item_id', 'SKU')) AS items
        """
    )
    null_props_type = null_void.schema["props"].dataType
    assert isinstance(null_props_type, ArrayType)
    assert isinstance(null_props_type.elementType, NullType)
    null_kept = null_void.dynamicFlatten(drop_null_lists=False, empty_as_null=False)
    null_table = null_kept.to_arrow()
    null_rows = null_table.to_pylist()
    assert len(null_rows) == 1
    assert null_rows[0]["items_item_id"] == "SKU"
    assert null_rows[0]["props"] is None
    assert pa.types.is_null(null_table.schema.field("props").type)


def test_explode_null_and_empty_array_values_drop_rows(spark: ReparkSession) -> None:
    """Default ``empty_as_null=True`` keeps NULL and EMPTY lists as one null-element row.

    ``empty_as_null=False`` is the polars ≥2.0 default (NULL kept, EMPTY dropped).
    Name kept (flip-don't-delete of the pre-fix inner-explode drop pin).
    """
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("xs", ArrayType(LongType()), True),
        ]
    )
    frame = spark.createDataFrame(
        [
            {"id": 1, "xs": None},
            {"id": 2, "xs": []},
            {"id": 3, "xs": [7, 8]},
        ],
        schema=schema,
    )
    default = frame.dynamicFlatten().orderBy("id")
    table = default.to_arrow()
    assert table.to_pylist() == [
        {"id": 1, "xs": None},
        {"id": 2, "xs": None},
        {"id": 3, "xs": 7},
        {"id": 3, "xs": 8},
    ]
    assert table.schema.field("xs").type == pa.int64()

    dropped_empty = frame.dynamicFlatten(empty_as_null=False).orderBy("id")
    dropped_table = dropped_empty.to_arrow()
    assert dropped_table.to_pylist() == [
        {"id": 1, "xs": None},
        {"id": 3, "xs": 7},
        {"id": 3, "xs": 8},
    ]
    assert dropped_table.schema.field("xs").type == pa.int64()


# ==================================================================================================
# Depth-cap LOUD refuse
# ==================================================================================================


def test_max_depth_refuses_loud_never_silent_truncate(spark: ReparkSession) -> None:
    """``max_depth`` exhaustion with remaining nested work raises AnalysisException."""
    schema = StructType(
        [
            StructField(
                "a",
                StructType(
                    [
                        StructField(
                            "b",
                            StructType([StructField("c", LongType(), True)]),
                            True,
                        )
                    ]
                ),
                True,
            )
        ]
    )
    frame = spark.createDataFrame([{"a": {"b": {"c": 1}}}], schema=schema)
    # depth 1: unnest a → column a_b still struct → refuse
    with pytest.raises(AnalysisException, match=r"DYNAMIC_FLATTEN_MAX_DEPTH|max_depth"):
        frame.dynamicFlatten(max_depth=1).collect()
    # depth 0 with nested work: refuse immediately
    with pytest.raises(AnalysisException, match=r"DYNAMIC_FLATTEN_MAX_DEPTH|max_depth"):
        frame.dynamicFlatten(max_depth=0).collect()
    # ample depth succeeds
    ok = frame.dynamicFlatten(max_depth=5)
    assert ok.columns == ["a_b_c"]
    assert ok.to_arrow().to_pylist() == [{"a_b_c": 1}]


def test_max_depth_type_and_range_gates(spark: ReparkSession) -> None:
    """max_depth type/range refuse LOUD (not silent coerce)."""
    frame = spark.createDataFrame([(1,)], "id INT")
    with pytest.raises(PySparkTypeError, match="max_depth"):
        frame.dynamicFlatten(max_depth=True)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="max_depth"):
        frame.dynamicFlatten(max_depth=1.5)  # type: ignore[arg-type]
    with pytest.raises(PySparkValueError, match="max_depth"):
        frame.dynamicFlatten(max_depth=-1)


def test_bool_flag_type_gates(spark: ReparkSession) -> None:
    """explode_lists / drop_null_lists refuse non-bool (no truthy-string coerce)."""
    frame = spark.createDataFrame([(1,)], "id INT")
    with pytest.raises(PySparkTypeError, match="explode_lists"):
        frame.dynamicFlatten(explode_lists="yes")  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="drop_null_lists"):
        frame.dynamicFlatten(drop_null_lists=1)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="empty_as_null"):
        frame.dynamicFlatten(empty_as_null="yes")  # type: ignore[arg-type]


# ==================================================================================================
# Name-collision: prefix disambiguates; refuse if still collides
# ==================================================================================================


def test_prefix_disambiguates_sibling_struct_fields(spark: ReparkSession) -> None:
    """Two structs with the same inner field name → parent prefix, no collision."""
    schema = StructType(
        [
            StructField(
                "left",
                StructType([StructField("score", LongType(), True)]),
                True,
            ),
            StructField(
                "right",
                StructType([StructField("score", LongType(), True)]),
                True,
            ),
        ]
    )
    frame = spark.createDataFrame(
        [{"left": {"score": 1}, "right": {"score": 2}}],
        schema=schema,
    )
    flat = frame.dynamicFlatten()
    assert flat.columns == ["left_score", "right_score"]
    assert flat.to_arrow().to_pylist() == [{"left_score": 1, "right_score": 2}]


def test_unnest_preserves_interleaved_column_order(spark: ReparkSession) -> None:
    """Struct expansion is in-place (polars select+unnest order), not survivors-first."""
    schema = StructType(
        [
            StructField("z", LongType(), True),
            StructField(
                "a",
                StructType(
                    [
                        StructField("x", LongType(), True),
                        StructField("y", LongType(), True),
                    ]
                ),
                True,
            ),
            StructField("m", LongType(), True),
        ]
    )
    frame = spark.createDataFrame(
        [{"z": 1, "a": {"x": 2, "y": 3}, "m": 4}],
        schema=schema,
    )
    flat = frame.dynamicFlatten()
    assert flat.columns == ["z", "a_x", "a_y", "m"]
    table = flat.to_arrow()
    assert table.to_pylist() == [{"z": 1, "a_x": 2, "a_y": 3, "m": 4}]
    assert table.schema.field("z").type == pa.int64()
    assert table.schema.field("a_x").type == pa.int64()
    assert table.schema.field("m").type == pa.int64()


def test_prefixed_name_collision_with_top_level_refuses(spark: ReparkSession) -> None:
    """Prefixed field still colliding with a surviving top-level column → LOUD refuse (Q25)."""
    schema = StructType(
        [
            StructField("a_x", LongType(), True),
            StructField(
                "a",
                StructType([StructField("x", LongType(), True)]),
                True,
            ),
        ]
    )
    frame = spark.createDataFrame([{"a_x": 1, "a": {"x": 2}}], schema=schema)
    with pytest.raises(AnalysisException, match=r"DYNAMIC_FLATTEN_NAME_COLLISION|collid"):
        frame.dynamicFlatten().collect()


def test_prefixed_name_collision_between_expansions_refuses(spark: ReparkSession) -> None:
    """Two expansions producing the same prefixed name → LOUD refuse."""
    # outer_inner as a sibling column of outer, both yield outer_inner_x style paths:
    # outer.inner_x  → outer_inner_x
    # outer_inner.x  → outer_inner_x
    schema = StructType(
        [
            StructField(
                "outer",
                StructType([StructField("inner_x", LongType(), True)]),
                True,
            ),
            StructField(
                "outer_inner",
                StructType([StructField("x", LongType(), True)]),
                True,
            ),
        ]
    )
    frame = spark.createDataFrame(
        [{"outer": {"inner_x": 1}, "outer_inner": {"x": 2}}],
        schema=schema,
    )
    with pytest.raises(AnalysisException, match=r"DYNAMIC_FLATTEN_NAME_COLLISION|collid"):
        frame.dynamicFlatten().collect()


def test_cross_pass_prefixed_collision_refuses(spark: ReparkSession) -> None:
    """Top-level ``a_b_c`` collides with nested ``a.b.c`` after multi-pass unnest (Q25)."""
    schema = StructType(
        [
            StructField("a_b_c", LongType(), True),
            StructField(
                "a",
                StructType(
                    [
                        StructField(
                            "b",
                            StructType([StructField("c", LongType(), True)]),
                            True,
                        )
                    ]
                ),
                True,
            ),
        ]
    )
    frame = spark.createDataFrame(
        [{"a_b_c": 1, "a": {"b": {"c": 2}}}],
        schema=schema,
    )
    with pytest.raises(AnalysisException, match=r"DYNAMIC_FLATTEN_NAME_COLLISION|collid"):
        frame.dynamicFlatten().collect()


def test_list_explode_then_unnest_collision_with_top_level_refuses(
    spark: ReparkSession,
) -> None:
    """Top-level ``legs_leg_id`` collides after list-of-struct explode + unnest."""
    schema = StructType(
        [
            StructField("legs_leg_id", LongType(), True),
            StructField(
                "legs",
                ArrayType(StructType([StructField("leg_id", LongType(), True)])),
                True,
            ),
        ]
    )
    frame = spark.createDataFrame(
        [{"legs_leg_id": 9, "legs": [{"leg_id": 1}]}],
        schema=schema,
    )
    with pytest.raises(AnalysisException, match=r"DYNAMIC_FLATTEN_NAME_COLLISION|collid"):
        frame.dynamicFlatten().collect()


# ==================================================================================================
# Flags + schema-only walk (no forced collect)
# ==================================================================================================


def test_explode_lists_false_leaves_arrays(spark: ReparkSession) -> None:
    """``explode_lists=False`` flattens structs only; arrays remain."""
    schema = StructType(
        [
            StructField(
                "wrap",
                StructType(
                    [
                        StructField("tag", StringType(), True),
                        StructField("nums", ArrayType(LongType()), True),
                    ]
                ),
                True,
            )
        ]
    )
    frame = spark.createDataFrame(
        [{"wrap": {"tag": "t", "nums": [1, 2]}}],
        schema=schema,
    )
    flat = frame.dynamicFlatten(explode_lists=False)
    assert flat.columns == ["wrap_tag", "wrap_nums"]
    table = flat.to_arrow()
    assert table.to_pylist() == [{"wrap_tag": "t", "wrap_nums": [1, 2]}]
    nums_type = table.schema.field("wrap_nums").type
    assert pa.types.is_list(nums_type) or pa.types.is_large_list(nums_type)


def test_custom_separator(spark: ReparkSession) -> None:
    """Custom separator is used for parent-path prefixes."""
    schema = StructType(
        [
            StructField(
                "s",
                StructType([StructField("f", LongType(), True)]),
                True,
            )
        ]
    )
    frame = spark.createDataFrame([{"s": {"f": 3}}], schema=schema)
    flat = frame.dynamicFlatten(separator=".")
    assert flat.columns == ["s.f"]
    assert flat.to_arrow().to_pylist() == [{"s.f": 3}]


def test_schema_walk_does_not_require_prior_collect(spark: ReparkSession) -> None:
    """dynamicFlatten builds a plan from logical schema alone (lazy-equivalent)."""
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField(
                "nested",
                StructType([StructField("v", LongType(), True)]),
                True,
            ),
        ]
    )
    frame = spark.createDataFrame([{"id": 1, "nested": {"v": 99}}], schema=schema)
    # Touch schema only (analyzed, no row exec), then flatten without intermediate collect.
    assert any(f.name == "nested" for f in frame.schema.fields)
    planned = frame.dynamicFlatten()
    # Still a plan: columns available before any action.
    assert planned.columns == ["id", "nested_v"]
    table = planned.to_arrow()
    assert table.to_pylist() == [{"id": 1, "nested_v": 99}]
    assert table.schema.field("nested_v").type == pa.int64()


def test_dynamic_flatten_plan_build_does_not_force_collect(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan-time dynamicFlatten must not invoke collect / count / to_arrow (C2-Q-003)."""
    from repark.spark.dataframe import DataFrame

    actions: list[str] = []

    def _spy(name: str, original: object) -> object:
        def _wrapped(self: DataFrame, *args: object, **kwargs: object) -> object:
            actions.append(name)
            return original(self, *args, **kwargs)  # type: ignore[operator]

        return _wrapped

    monkeypatch.setattr(DataFrame, "collect", _spy("collect", DataFrame.collect))
    monkeypatch.setattr(DataFrame, "count", _spy("count", DataFrame.count))
    monkeypatch.setattr(DataFrame, "to_arrow", _spy("to_arrow", DataFrame.to_arrow))

    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField(
                "outer",
                StructType(
                    [
                        StructField(
                            "inner",
                            StructType([StructField("x", LongType(), True)]),
                            True,
                        )
                    ]
                ),
                True,
            ),
            StructField(
                "legs",
                ArrayType(StructType([StructField("leg_id", LongType(), True)])),
                True,
            ),
        ]
    )
    frame = spark.createDataFrame(
        [{"id": 1, "outer": {"inner": {"x": 2}}, "legs": [{"leg_id": 3}]}],
        schema=schema,
    )
    planned = frame.dynamicFlatten()
    assert actions == []
    assert planned.columns == ["id", "outer_inner_x", "legs_leg_id"]
    # Action after plan is allowed and expected.
    table = planned.to_arrow()
    assert "to_arrow" in actions
    assert table.to_pylist() == [{"id": 1, "outer_inner_x": 2, "legs_leg_id": 3}]


# ==================================================================================================
# GA4-shaped fixture (empty_as_null both flag states)
# ==================================================================================================


_GA4_VALUE = StructType(
    [
        StructField("string_value", StringType(), True),
        StructField("int_value", LongType(), True),
        StructField("float_value", FloatType(), True),
        StructField("double_value", DoubleType(), True),
    ]
)
_GA4_PARAM = StructType(
    [
        StructField("key", StringType(), True),
        StructField("value", _GA4_VALUE, True),
    ]
)
_GA4_ITEM = StructType(
    [
        StructField("item_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", LongType(), True),
    ]
)
_GA4_DEVICE = StructType(
    [
        StructField("category", StringType(), True),
        StructField(
            "web_info",
            StructType(
                [
                    StructField("hostname", StringType(), True),
                    StructField("browser", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)
_GA4_SCHEMA = StructType(
    [
        StructField("event_name", StringType(), False),
        StructField("event_params", ArrayType(_GA4_PARAM), True),
        StructField("user_properties", ArrayType(_GA4_PARAM), True),
        StructField("items", ArrayType(_GA4_ITEM), True),
        StructField("device", _GA4_DEVICE, True),
    ]
)
_GA4_COLUMNS = [
    "event_name",
    "event_params_key",
    "event_params_value_string_value",
    "event_params_value_int_value",
    "event_params_value_float_value",
    "event_params_value_double_value",
    "user_properties_key",
    "user_properties_value_string_value",
    "user_properties_value_int_value",
    "user_properties_value_float_value",
    "user_properties_value_double_value",
    "items_item_id",
    "items_price",
    "items_quantity",
    "device_category",
    "device_web_info_hostname",
    "device_web_info_browser",
]


def _ga4_param(key: str, string_value: str) -> dict[str, object]:
    return {
        "key": key,
        "value": {
            "string_value": string_value,
            "int_value": None,
            "float_value": None,
            "double_value": None,
        },
    }


def _ga4_rows() -> list[dict[str, object]]:
    """In-test GA4-shaped events: params-only / all-three / NULL-arrays."""
    return [
        {
            "event_name": "page_view",
            "event_params": [_ga4_param("page_location", "https://example.test/")],
            "user_properties": [],
            "items": [],
            "device": {
                "category": "desktop",
                "web_info": {"hostname": "example.test", "browser": "Chrome"},
            },
        },
        {
            "event_name": "purchase",
            "event_params": [_ga4_param("currency", "USD")],
            "user_properties": [_ga4_param("user_id", "u-1")],
            "items": [{"item_id": "SKU-1", "price": 9.99, "quantity": 1}],
            "device": {
                "category": "mobile",
                "web_info": {"hostname": "shop.example.test", "browser": "Safari"},
            },
        },
        {
            "event_name": "session_start",
            "event_params": None,
            "user_properties": None,
            "items": None,
            "device": {
                "category": "desktop",
                "web_info": {"hostname": "example.test", "browser": "Firefox"},
            },
        },
    ]


def test_dynamic_flatten_ga4_empty_as_null_keeps_export_rows(spark: ReparkSession) -> None:
    """GA4-shaped 3-row frame: default empty_as_null keeps all three event_names.

    page_view has EMPTY user_properties/items; session_start has NULL arrays;
    purchase has all three lists non-empty. Default True returns all three
    (the 0-rows class is dead). False returns purchase + session_start only
    (polars ≥2.0: empty drops, NULL keeps).
    """
    frame = spark.createDataFrame(_ga4_rows(), schema=_GA4_SCHEMA)

    default = frame.dynamicFlatten().orderBy("event_name")
    assert default.columns == _GA4_COLUMNS
    default_table = default.to_arrow()
    default_rows = default_table.to_pylist()
    assert [row["event_name"] for row in default_rows] == [
        "page_view",
        "purchase",
        "session_start",
    ]
    by_name = {row["event_name"]: row for row in default_rows}
    assert by_name["page_view"]["items_item_id"] is None
    assert by_name["page_view"]["event_params_key"] == "page_location"
    assert by_name["session_start"]["event_params_key"] is None
    assert by_name["session_start"]["items_item_id"] is None
    assert by_name["session_start"]["user_properties_key"] is None
    assert by_name["purchase"]["items_item_id"] == "SKU-1"
    assert by_name["purchase"]["items_price"] == 9.99
    assert by_name["purchase"]["items_quantity"] == 1

    assert _is_arrow_string_type(default_table.schema.field("items_item_id").type)
    assert default_table.schema.field("items_price").type == pa.float64()
    assert default_table.schema.field("items_quantity").type == pa.int64()
    assert default_table.schema.field("event_params_value_int_value").type == pa.int64()
    assert default_table.schema.field("event_params_value_float_value").type == pa.float32()
    assert default_table.schema.field("event_params_value_double_value").type == pa.float64()
    assert _is_arrow_string_type(default_table.schema.field("device_web_info_hostname").type)

    dropped = frame.dynamicFlatten(empty_as_null=False).orderBy("event_name")
    assert dropped.columns == _GA4_COLUMNS
    dropped_table = dropped.to_arrow()
    dropped_rows = dropped_table.to_pylist()
    dropped_names = [row["event_name"] for row in dropped_rows]
    assert dropped_names == ["purchase", "session_start"]
    dropped_by_name = {row["event_name"]: row for row in dropped_rows}
    assert dropped_by_name["purchase"]["items_item_id"] == "SKU-1"
    assert dropped_by_name["purchase"]["items_price"] == 9.99
    assert dropped_by_name["purchase"]["items_quantity"] == 1
    assert dropped_by_name["session_start"]["items_item_id"] is None
    assert (
        dropped_table.schema.field("items_item_id").type
        == default_table.schema.field("items_item_id").type
    )
    assert dropped_table.schema.field("items_price").type == pa.float64()
