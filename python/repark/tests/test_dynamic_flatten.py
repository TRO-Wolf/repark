"""r24 DF1 — DataFrame.dynamicFlatten / dynamic_flatten (repark-extra).

Semantic pins against the operator-supplied polars ``unnest_lazyframe`` reference
(``specs/dynamic-flatten-reference.md``). Arrow path: value **and** type (never only show).
Synthetic fixtures only.

The planner is native (``repark_core::dynamic_flatten``); this file remains the facade contract.
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

    Engine ``make_array()`` yields ``array<Null>`` (void element). This pin uses the SQL
    path; the createDataFrame door is pinned separately by
    ``test_create_dataframe_honors_requested_void`` (G3b D-5 — it used to silently
    substitute array<string>, it now honors the requested ``array<void>``).
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

    Schema-mapper leg MEASURED on this round's BASE c38578d: the kept flat
    ``props`` column carries the Arrow Debug type key ``'Null'``, which the
    lowercase-only void arm missed, so ``.schema['props'].dataType`` was
    ``StringType()`` and ``.dtypes`` said ``('props', 'string')`` while
    ``to_arrow()`` was already ``pa.null()``. After the ``'Null'`` arm:
    ``NullType()`` / ``('props', 'void')``.
    Kills: the StringType fail-open on the Debug-spelled void key (DF-2 W-1).
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
    # DF-2 W-1: reported schema must agree with the Arrow type, not fail open to string.
    assert isinstance(flat.schema["props"].dataType, NullType)
    assert ("props", "void") in flat.dtypes


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
# G3b: the real GA4 ``items`` element carries its OWN ``item_params`` array-of-struct.
# The fixture used to stop at the scalar fields, which is exactly why the
# array-of-struct-inside-an-array-element-struct spelling defect shipped unpinned.
_GA4_ITEM = StructType(
    [
        StructField("item_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", LongType(), True),
        StructField("item_params", ArrayType(_GA4_PARAM), True),
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
    "items_item_params_key",
    "items_item_params_value_string_value",
    "items_item_params_value_int_value",
    "items_item_params_value_float_value",
    "items_item_params_value_double_value",
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
            "items": [
                {
                    "item_id": "SKU-1",
                    "price": 9.99,
                    "quantity": 1,
                    "item_params": [_ga4_param("item_category", "shoes")],
                }
            ],
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
    # G3b: items[].item_params[] — an array-of-struct nested inside an array-element
    # struct. Red on BASE (AnalysisException type_coercion, "Failed to coerce … CASE WHEN").
    assert by_name["purchase"]["items_item_params_key"] == "item_category"
    assert by_name["purchase"]["items_item_params_value_string_value"] == "shoes"
    assert by_name["page_view"]["items_item_params_key"] is None
    assert by_name["session_start"]["items_item_params_key"] is None

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


def test_dynamic_flatten_array_of_struct_inside_array_element_struct(
    spark: ReparkSession,
) -> None:
    """G3b minimal repro: array-of-struct nested INSIDE an array-element struct.

    This is GA4's real ``items[].item_params[]`` shape reduced to its smallest form.
    Red on BASE (95cfaf9) for BOTH doors — ``dynamicFlatten()`` and a bare
    ``explode_outer`` — with ``AnalysisException type_coercion`` / "Failed to coerce …
    CASE WHEN", because the nested array was spelled postfix (``…[]``) and the engine
    parser migrated the ``[]`` onto the innermost field. Mutation-proof: reverting
    ``_sql_array_of`` to ``f"{inner}[]"`` restores the refuse on both doors.
    """
    param = StructType(
        [
            StructField("key", StringType(), True),
            StructField("value", StructType([StructField("sv", StringType(), True)]), True),
        ]
    )
    item = StructType(
        [
            StructField("item_id", StringType(), True),
            StructField("item_params", ArrayType(param), True),
        ]
    )
    schema = StructType(
        [
            StructField("ev", StringType(), True),
            StructField("items", ArrayType(item), True),
        ]
    )
    rows = [
        ("purchase", [("SKU1", [("color", ("red",)), ("size", ("L",))]), ("SKU2", None)]),
        ("view", None),
    ]
    frame = spark.createDataFrame(rows, schema)

    flat = frame.dynamicFlatten()
    assert flat.columns == [
        "ev",
        "items_item_id",
        "items_item_params_key",
        "items_item_params_value_sv",
    ]
    table = flat.to_arrow()
    assert table.to_pylist() == [
        {
            "ev": "purchase",
            "items_item_id": "SKU1",
            "items_item_params_key": "color",
            "items_item_params_value_sv": "red",
        },
        {
            "ev": "purchase",
            "items_item_id": "SKU1",
            "items_item_params_key": "size",
            "items_item_params_value_sv": "L",
        },
        {
            "ev": "purchase",
            "items_item_id": "SKU2",
            "items_item_params_key": None,
            "items_item_params_value_sv": None,
        },
        {
            "ev": "view",
            "items_item_id": None,
            "items_item_params_key": None,
            "items_item_params_value_sv": None,
        },
    ]
    assert _is_arrow_string_type(table.schema.field("items_item_params_key").type)

    # Second door: bare explode_outer on the same shape (also red on BASE).
    from repark import functions as F  # noqa: N812

    exploded = frame.select(F.explode_outer("items").alias("item")).to_arrow()
    assert exploded.num_rows == 3


def test_dynamic_flatten_scalar_array_inside_array_element_struct(
    spark: ReparkSession,
) -> None:
    """G3b guard: the scalar-inner nested array (``array<struct<x, nums:array<bigint>>>``)
    keeps flattening — the uniform angle spelling must not regress the shape that the
    postfix spelling already handled.
    """
    schema = StructType(
        [
            StructField(
                "a",
                ArrayType(
                    StructType(
                        [
                            StructField("x", StringType(), True),
                            StructField("nums", ArrayType(LongType()), True),
                        ]
                    )
                ),
                True,
            )
        ]
    )
    frame = spark.createDataFrame([([("p", [1, 2]), ("q", None)],)], schema)
    table = frame.dynamicFlatten().to_arrow()
    assert table.to_pylist() == [
        {"a_x": "p", "a_nums": 1},
        {"a_x": "p", "a_nums": 2},
        {"a_x": "q", "a_nums": None},
    ]
    assert table.schema.field("a_nums").type == pa.int64()


def test_dynamic_flatten_map_element_still_refuses_loud(spark: ReparkSession) -> None:
    """G3b honesty rider: shapes that still cannot spell keep the documented LOUD refuse.

    The pre-#176 refuse message class ("cannot resolve SQL element type … cast the array
    or use a supported element type") must survive the spelling fix for map elements —
    the fix widens what spells, it must not silently fail open on what does not.
    """
    from repark import functions as F  # noqa: N812
    from repark.spark.types import MapType

    schema = StructType([StructField("m", ArrayType(MapType(StringType(), StringType())), True)])
    frame = spark.createDataFrame([([{"a": "b"}],)], schema)
    with pytest.raises(AnalysisException) as excinfo:
        frame.select(F.explode_outer("m")).to_arrow()
    message = str(excinfo.value)
    assert "cannot resolve SQL element type" in message
    assert "supported element type" in message


def test_create_dataframe_honors_requested_void(spark: ReparkSession) -> None:
    """G3b D-5: an explicitly requested void / array<void> is HONORED, not substituted.

    Red on BASE (95cfaf9): ``_data_type_to_sql_type`` spelled ``NullType`` as ``VARCHAR``, so
    ``createDataFrame`` returned ``struct<v:string, a:array<string>>`` for the schema below
    with **no** warning or refuse — the only silent schema substitution on the ingest path.
    Mutation-proof: restoring ``return "VARCHAR"`` (or dropping the ``VOID``/``NULL`` entries
    from ``_sql_type_to_arrow``) reds every assertion here.

    The ruling hierarchy was HONOR > refuse-loud > silent-substitute; HONOR is reachable
    because the whole path already carries void: Arrow ``null`` / ``list<item: null>``, the
    ``CAST(NULL AS VOID)`` empty-frame seed, and the DF-2 void machinery below.
    """
    schema = StructType(
        [
            StructField("v", NullType(), True),
            StructField("a", ArrayType(NullType()), True),
        ]
    )
    frame = spark.createDataFrame([(None, [None, None]), (None, None)], schema)

    # 1. The reported schema is the requested schema.
    assert frame.schema.simpleString() == "struct<v:void,a:array<void>>"
    assert isinstance(frame.schema["v"].dataType, NullType)
    element = frame.schema["a"].dataType
    assert isinstance(element, ArrayType)
    assert isinstance(element.elementType, NullType)
    assert frame.dtypes == [("v", "void"), ("a", "array<void>")]

    # 2. Arrow agrees — null / list<item: null>, not string / list<item: string>.
    table = frame.to_arrow()
    assert table.schema.field("v").type == pa.null()
    assert table.schema.field("a").type == pa.list_(pa.null())
    assert table.to_pylist() == [{"v": None, "a": [None, None]}, {"v": None, "a": None}]
    assert frame.count() == 2
    assert frame.collect() == frame.collect()

    # 3. Nested + DDL doors spell it the same way.
    nested = spark.createDataFrame(
        [(([None],),)],
        StructType(
            [StructField("s", StructType([StructField("x", ArrayType(NullType()), True)]), True)]
        ),
    )
    assert nested.schema.simpleString() == "struct<s:struct<x:array<void>>>"
    assert spark.createDataFrame([([None],)], "a ARRAY<VOID>").schema.simpleString() == (
        "struct<a:array<void>>"
    )

    # 4. Empty-frame seed (CAST(NULL AS VOID)) keeps the requested type too.
    assert spark.createDataFrame([], schema).schema.simpleString() == (
        "struct<v:void,a:array<void>>"
    )

    # 5. The DF-2 void machinery still holds on the ingested (not SQL-built) frame.
    assert frame.dynamicFlatten().columns == ["v"]
    kept = frame.dynamicFlatten(drop_null_lists=False)
    assert kept.columns == ["v", "a"]
    # MEASURED: the 2-element void list contributes one null row per element, the NULL
    # array row contributes one — 3 rows, every cell NULL (void has no other value).
    assert kept.to_arrow().to_pylist() == [
        {"v": None, "a": None},
        {"v": None, "a": None},
        {"v": None, "a": None},
    ]

    # 6. Non-void requests are untouched (the substitution is gone, not inverted).
    assert (
        spark.createDataFrame(
            [([None],)], StructType([StructField("a", ArrayType(StringType()), True)])
        ).schema.simpleString()
        == "struct<a:array<string>>"
    )


# ==================================================================================================
# DEFECT-2 — projection over a multi-pass dynamicFlatten (task/c25-bugfix-ledger.md, 2026-08-18)
# ==================================================================================================
#
# The defect (pre-existing, measured twice before the fix): after a flatten that takes 2+ explode
# passes, a projection that DROPPED the output of an explode whose ``Unnest`` sits UNDER another
# ``Unnest`` (an earlier prose said "the LAST explode pass's column" — retracted, the sibling
# exploded second is the trigger here) raised inside DataFusion 54.1's
# ``push_down_leaf_projections``. Two distinct upstream failures on one plan:
#
#   * ``Internal error: Assertion failed: expr.is_empty(): Unnest(…)`` — the rule pushes into a
#     node's inputs with ``node.with_new_exprs(node.expressions(), …)``, and ``Unnest`` reports
#     an exec column from ``expressions()`` while its ``with_new_exprs`` asserts the vector empty;
#   * ``Schema error: Schema contains qualified field name <scratch-view>.id and unqualified field
#     name id which would be ambiguous`` — merging a pushed pass-through column into a projection
#     that already re-aliases the same name puts both spellings into one ``DFSchema``.
#
# Neither is reachable from repark's plan shape, and both are optimizer-only. The fix is a
# SCOPED rule, not a flag: the core session installs DataFusion's own rule list with
# ``push_down_leaf_projections`` wrapped so it declines on the ``Unnest``-carrying plans it
# miscompiles and runs untouched everywhere else (``crates/repark-core/src/session/df_guards.rs``;
# ``datafusion.optimizer.enable_leaf_expression_pushdown`` stays at DataFusion's default, because
# turning it off measured up to ~8x in one run on a filtered wide-struct parquet scan —
# load-sensitive ratio, ledger §3).
# Engine-side pins: the five ``session::df_guard_tests::*leaf_pushdown*`` tests.
# These are the facade halves — they red the moment that wrapper is removed.


def _defect2_frame(spark: ReparkSession, sibling: str):
    """The troubleshooting-section repro shape with a renameable sibling list.

    ``Legs`` is a list-of-struct whose element carries its own ``Fills`` list → two explode
    passes; ``sibling`` is a flat top-level list. Which of the two explodes LAST is decided by
    the column name, which is what made the defect order-dependent.
    """
    rows = [{"id": 1, "Legs": [{"leg_id": 1, "Fills": [{"f": 1.0}]}], sibling: ["a", "b"]}]
    return spark.createDataFrame(rows).dynamicFlatten()


@pytest.mark.parametrize("sibling", ["Tags", "Alpha"])
def test_multi_pass_flatten_every_projection_subset_is_green(
    spark: ReparkSession, sibling: str
) -> None:
    """Every non-empty projection subset works, in BOTH explode orders, value-checked.

    ``sibling="Tags"`` is the order that reded before the fix (the sibling list explodes LAST, so
    every subset dropping it raised); ``sibling="Alpha"`` is the order that always worked. Same
    data, same 15 subsets — the pin is that the two orders are now indistinguishable.

    Values are compared against the whole-frame ``to_arrow`` export (the path that stayed correct
    throughout), so this pins results, not merely "did not raise".
    """
    import itertools

    frame = _defect2_frame(spark, sibling)
    columns = frame.columns
    assert len(columns) == 4
    whole = frame.to_arrow().to_pylist()
    assert len(whole) == 2

    subsets = [
        subset
        for size in range(1, len(columns) + 1)
        for subset in itertools.combinations(columns, size)
    ]
    assert len(subsets) == 15
    for subset in subsets:
        got = frame.select(*subset).to_arrow().to_pylist()
        assert got == [{name: row[name] for name in subset} for row in whole], subset


@pytest.mark.parametrize("sibling", ["Tags", "Alpha"])
def test_multi_pass_flatten_count_and_agg_are_green(spark: ReparkSession, sibling: str) -> None:
    """``count()`` / ``agg`` — the extreme case that projects every column away — both orders.

    Before the fix ``count()`` raised on the ``Tags`` order while the same frame's
    ``to_arrow().num_rows`` returned the right number: the row count was reachable and correct
    on the export path while the cheapest way to ask for it failed.
    """
    from repark import functions as F  # noqa: N812

    frame = _defect2_frame(spark, sibling)
    exported = frame.to_arrow().num_rows
    assert exported == 2
    assert frame.count() == exported
    assert frame.agg(F.count(F.lit(1))).to_arrow().to_pylist() == [{"count(1)": exported}]


def test_ga4_real_shape_flatten_then_project(spark: ReparkSession) -> None:
    """The GA4 ``items[].item_params[]`` frame: flatten, then project every single column.

    MEASURED: this shape did **not** reproduce the defect on the base tree (its last explode pass
    is the one every subset keeps), so it is a coverage pin for the real-world shape rather than a
    second reproduction. It holds the multi-pass real-data path against a regression in the guard.
    """
    from repark import functions as F  # noqa: N812

    frame = spark.createDataFrame(_ga4_rows(), schema=_GA4_SCHEMA).dynamicFlatten()
    assert frame.columns == _GA4_COLUMNS
    whole = frame.to_arrow().to_pylist()
    assert frame.count() == len(whole)
    assert frame.agg(F.count(F.lit(1))).to_arrow().to_pylist() == [{"count(1)": len(whole)}]
    for name in frame.columns:
        got = frame.select(name).to_arrow().to_pylist()
        assert got == [{name: row[name]} for row in whole], name
    # A narrowing multi-column projection that drops the flattened array columns entirely.
    narrow = ("event_name", "device_category", "device_web_info_browser")
    assert frame.select(*narrow).to_arrow().to_pylist() == [
        {name: row[name] for name in narrow} for row in whole
    ]


def test_multi_pass_flatten_cache_is_still_a_plain_pattern(spark: ReparkSession) -> None:
    """The retired workaround still works as an ordinary pattern (it is no longer required).

    ``cache()`` used to be the documented escape hatch for the defect. It is now just caching:
    the cached and uncached frames agree, column for column.
    """
    frame = _defect2_frame(spark, "Tags")
    cached = frame.cache()
    try:
        assert cached.count() == frame.count() == 2
        assert cached.to_arrow().to_pylist() == frame.to_arrow().to_pylist()
        assert (
            cached.select("Legs_Fills_f").to_arrow().to_pylist()
            == frame.select("Legs_Fills_f").to_arrow().to_pylist()
        )
    finally:
        cached.unpersist()
