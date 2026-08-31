"""FNP-4c — Spark higher-order kernels on the facade Column API.

Oracle: live PySpark 4.1.2 (c26-oracle, 2026-08-31). Pins are value AND Arrow
type on ``toArrow()``. Nested higher-order remains the FNP-4a loud refusal.
pins: fnp-4c-higher-order-kernels/C-014, C-015
"""

from __future__ import annotations

import pytest

from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp4c-higher-order").getOrCreate()


def _as_map(value: object) -> dict[object, object] | None:
    """Arrow may export MAP as a list of pairs; Spark's collect is a dict."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    return dict(value)


def _arrow_map_entries(column: object, row: int) -> list[tuple[object, object]] | None:
    """Raw Arrow map entries in stored order (dict equality hides reverse)."""
    combined = column.combine_chunks() if hasattr(column, "combine_chunks") else column
    if combined[row].as_py() is None:
        return None
    offsets = combined.offsets.to_pylist()
    keys = combined.keys.to_pylist()
    values = combined.items.to_pylist()
    start, end = offsets[row], offsets[row + 1]
    return list(zip(keys[start:end], values[start:end], strict=True))


def _arrays():
    spark = _session()
    return spark.sql(
        "SELECT a FROM VALUES "
        "(array(1, 2, 3)), "
        "(array(1, CAST(NULL AS INT), 3)), "
        "(CAST(array() AS ARRAY<INT>)), "
        "(CAST(NULL AS ARRAY<INT>)) "
        "AS t(a)"
    )


def test_transform_unary_matches_spark() -> None:
    """pins: fnp-4c-higher-order-kernels/C-001"""
    out = _arrays().select(F.transform("a", lambda x: x + 1).alias("r")).toArrow()
    assert out.column("r").to_pylist() == [[2, 3, 4], [2, None, 4], [], None]
    assert "list" in str(out.schema.field("r").type).lower()


def test_transform_index_is_zero_based() -> None:
    """pins: fnp-4c-higher-order-kernels/C-001"""
    out = _arrays().select(F.transform("a", lambda x, i: x + i).alias("r")).toArrow()
    assert out.column("r").to_pylist() == [[1, 3, 5], [1, None, 5], [], None]
    indices = (
        _session()
        .sql("SELECT array(10, 20, 30) AS a")
        .select(F.transform("a", lambda x, i: i).alias("r"))
        .toArrow()
    )
    assert indices.column("r").to_pylist() == [[0, 1, 2]]


def test_filter_null_predicate_drops_and_index_arity() -> None:
    """pins: fnp-4c-higher-order-kernels/C-002"""
    frame = _arrays()
    kept = frame.select(F.filter("a", lambda x: x > 1).alias("r")).toArrow()
    assert kept.column("r").to_pylist() == [[2, 3], [3], [], None]
    even = frame.select(F.filter("a", lambda x, i: i % 2 == 0).alias("r")).toArrow()
    assert even.column("r").to_pylist() == [[1, 3], [1, 3], [], None]


def test_forall_three_valued_empty_is_true() -> None:
    """pins: fnp-4c-higher-order-kernels/C-005"""
    frame = _arrays()
    gt0 = frame.select(F.forall("a", lambda x: x > 0).alias("r")).toArrow()
    assert gt0.column("r").to_pylist() == [True, None, True, None]
    assert str(gt0.schema.field("r").type) == "bool"
    gt1 = frame.select(F.forall("a", lambda x: x > 1).alias("r")).toArrow()
    assert gt1.column("r").to_pylist() == [False, False, True, None]


def test_aggregate_and_reduce_match_spark_null_and_empty() -> None:
    """pins: fnp-4c-higher-order-kernels/C-003, C-004"""
    frame = _arrays()
    coalesced = frame.select(
        F.aggregate("a", F.lit(0), lambda acc, x: acc + F.coalesce(x, F.lit(0))).alias("r")
    ).toArrow()
    assert coalesced.column("r").to_pylist() == [6, 4, 0, None]
    assert "int64" in str(coalesced.schema.field("r").type).lower()
    raw = frame.select(F.aggregate("a", F.lit(0), lambda acc, x: acc + x).alias("r")).toArrow()
    assert raw.column("r").to_pylist() == [6, None, 0, None]
    finished = frame.select(
        F.aggregate(
            "a",
            F.lit(0),
            lambda acc, x: acc + F.coalesce(x, F.lit(0)),
            lambda acc: acc * 10,
        ).alias("r")
    ).toArrow()
    assert finished.column("r").to_pylist() == [60, 40, 0, None]
    reduced = frame.select(
        F.reduce("a", F.lit(0), lambda acc, x: acc + F.coalesce(x, F.lit(0))).alias("r")
    ).toArrow()
    assert reduced.column("r").to_pylist() == [6, 4, 0, None]


def test_zip_with_null_pads_the_shorter_array() -> None:
    """pins: fnp-4c-higher-order-kernels/C-006"""
    spark = _session()
    frame = spark.sql(
        "SELECT xs, ys FROM VALUES "
        "(array(1, 3, 5, 8), array(0, 2, 4, 6)), "
        "(array(1, 3, 5), array(0, 2)), "
        "(CAST(array() AS ARRAY<INT>), array(1, 2)), "
        "(CAST(NULL AS ARRAY<INT>), array(1)) "
        "AS t(xs, ys)"
    )
    added = frame.select(
        F.zip_with("xs", "ys", lambda x, y: x + F.coalesce(y, F.lit(0))).alias("r")
    ).toArrow()
    assert added.column("r").to_pylist() == [[1, 5, 9, 14], [1, 5, 5], [None, None], None]
    concat = frame.select(
        F.zip_with("xs", "ys", lambda x, y: F.concat_ws("_", x, y)).alias("r")
    ).toArrow()
    assert concat.column("r").to_pylist() == [
        ["1_0", "3_2", "5_4", "8_6"],
        ["1_0", "3_2", "5"],
        ["1", "2"],
        None,
    ]


def test_map_kernels_match_spark_union_and_errors() -> None:
    """pins: fnp-4c-higher-order-kernels/C-007, C-008, C-009, C-010"""
    spark = _session()
    from repark.spark.types import IntegerType, MapType, StringType, StructField, StructType

    maps = spark.createDataFrame(
        [
            ({"foo": 1, "bar": 2}, {"foo": 10, "baz": 3}),
            ({}, {}),
            (None, {"a": 1}),
        ],
        schema=StructType(
            [
                StructField("m1", MapType(StringType(), IntegerType()), True),
                StructField("m2", MapType(StringType(), IntegerType()), True),
            ]
        ),
    )
    keys = maps.select(F.transform_keys("m1", lambda k, v: F.upper(k)).alias("r")).toArrow()
    assert [_as_map(row) for row in keys.column("r").to_pylist()] == [
        {"BAR": 2, "FOO": 1},
        {},
        None,
    ]
    values = maps.select(F.transform_values("m1", lambda k, v: v + 1).alias("r")).toArrow()
    assert [_as_map(row) for row in values.column("r").to_pylist()] == [
        {"bar": 3, "foo": 2},
        {},
        None,
    ]
    filtered = maps.select(F.map_filter("m1", lambda k, v: v > 1).alias("r")).toArrow()
    assert [_as_map(row) for row in filtered.column("r").to_pylist()] == [
        {"bar": 2},
        {},
        None,
    ]
    zipped = maps.select(
        F.map_zip_with(
            "m1",
            "m2",
            lambda k, v1, v2: F.coalesce(v1, F.lit(0)) + F.coalesce(v2, F.lit(0)),
        ).alias("r")
    ).toArrow()
    assert [_as_map(row) for row in zipped.column("r").to_pylist()] == [
        {"bar": 2, "baz": 3, "foo": 11},
        {},
        None,
    ]
    raw = maps.select(F.map_zip_with("m1", "m2", lambda k, v1, v2: v1).alias("r")).toArrow()
    assert [_as_map(row) for row in raw.column("r").to_pylist()] == [
        {"bar": 2, "baz": None, "foo": 1},
        {},
        None,
    ]
    zipped_entries = _arrow_map_entries(zipped.column("r"), 0)
    assert zipped_entries == [("foo", 11), ("bar", 2), ("baz", 3)]

    with pytest.raises(PySparkException, match="NULL_MAP_KEY"):
        maps.select(F.transform_keys("m1", lambda k, v: F.lit(None).cast("string"))).toArrow()
    with pytest.raises(PySparkException, match="DUPLICATED_MAP_KEY"):
        maps.select(F.transform_keys("m1", lambda k, v: F.lit("same"))).toArrow()


def test_lambda_arity_uses_spark_error_class() -> None:
    """pins: fnp-4c-higher-order-kernels/C-012"""
    frame = _arrays()
    with pytest.raises(AnalysisException, match="DATATYPE_MISMATCH"):
        frame.select(F.filter("a", lambda x: x + 1)).toArrow()


def test_each_name_refuses_wrong_lambda_arity_like_spark() -> None:
    """Spark puts the user arity in expects and the declared arity in got.

    pins: fnp-4c-higher-order-kernels/C-012
    """
    frame = _arrays()
    spark = _session()
    from repark.spark.types import IntegerType, MapType, StringType, StructField, StructType

    maps = spark.createDataFrame(
        [({"foo": 1}, {"bar": 2})],
        schema=StructType(
            [
                StructField("m1", MapType(StringType(), IntegerType()), True),
                StructField("m2", MapType(StringType(), IntegerType()), True),
            ]
        ),
    )
    cases = [
        (
            lambda: frame.select(F.transform("a", lambda x, i, z: x)),
            "expects 3 arguments, but got 1",
        ),
        (
            lambda: frame.select(F.filter("a", lambda x, i, z: x > 0)),
            "expects 3 arguments, but got 1",
        ),
        (lambda: frame.select(F.forall("a", lambda x, i: x > 0)), "expects 2 arguments, but got 1"),
        (
            lambda: frame.select(F.aggregate("a", F.lit(0), lambda acc: acc)),
            "expects 1 arguments, but got 2",
        ),
        (
            lambda: frame.select(F.reduce("a", F.lit(0), lambda acc: acc)),
            "expects 1 arguments, but got 2",
        ),
        (
            lambda: frame.select(F.zip_with("a", "a", lambda x: x)),
            "expects 1 arguments, but got 2",
        ),
        (
            lambda: maps.select(F.transform_keys("m1", lambda k: k)),
            "expects 1 arguments, but got 2",
        ),
        (
            lambda: maps.select(F.transform_values("m1", lambda k: k)),
            "expects 1 arguments, but got 2",
        ),
        (
            lambda: maps.select(F.map_filter("m1", lambda k: F.lit(True))),
            "expects 1 arguments, but got 2",
        ),
        (
            lambda: maps.select(F.map_zip_with("m1", "m2", lambda k, v1: k)),
            "expects 2 arguments, but got 3",
        ),
    ]
    for build, needle in cases:
        with pytest.raises(AnalysisException, match=needle):
            build().toArrow()


def test_zip_with_result_is_nullable_when_the_right_array_is() -> None:
    """pins: fnp-4c-higher-order-kernels/C-006"""
    spark = _session()
    from repark.spark.types import ArrayType, IntegerType, StructField, StructType

    frame = spark.createDataFrame(
        [([1, 2], None)],
        schema=StructType(
            [
                StructField("xs", ArrayType(IntegerType()), False),
                StructField("ys", ArrayType(IntegerType()), True),
            ]
        ),
    )
    out = frame.select(F.zip_with("xs", "ys", lambda x, y: x).alias("r")).toArrow()
    assert out.schema.field("r").nullable
    assert out.column("r").to_pylist() == [None]


def test_nested_higher_order_stays_refused() -> None:
    """pins: fnp-4c-higher-order-kernels/C-011"""
    from repark.errors import UnsupportedOperationException

    frame = _arrays()
    with pytest.raises(UnsupportedOperationException, match="higher-order"):
        frame.select(F.transform("a", lambda x: F.exists("a", lambda y: y > 0))).toArrow()
