"""WIN-SLIDE-1: the thirteen non-retractable aggregates answer over a sliding frame.

pins: win-slide-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

import datetime as dt
import math
from typing import Any, Final

import _live_parity as lp
import pytest

SLIDING_AGGREGATES: Final[tuple[tuple[str, str], ...]] = (
    ("approx_count_distinct", "approx_count_distinct({vi})"),
    ("approx_percentile", "approx_percentile({v}, 0.5)"),
    ("bit_and", "bit_and({vi})"),
    ("bit_or", "bit_or({vi})"),
    ("bool_and", "bool_and({b})"),
    ("bool_or", "bool_or({b})"),
    ("collect_list", "collect_list({v})"),
    ("collect_set", "collect_set({v})"),
    ("corr", "corr({v}, {v2})"),
    ("covar_pop", "covar_pop({v}, {v2})"),
    ("covar_samp", "covar_samp({v}, {v2})"),
    ("percentile_approx", "percentile_approx({v}, 0.5)"),
    ("try_sum", "try_sum({v})"),
)

AGGREGATE_NAMES: Final[tuple[str, ...]] = tuple(name for name, _ in SLIDING_AGGREGATES)

VALUE_COLUMNS: Final[dict[str, str]] = {"v": "v", "v2": "v2", "vi": "vi", "b": "b"}
NULL_COLUMNS: Final[dict[str, str]] = {"v": "vn", "v2": "vn", "vi": "vin", "b": "bn"}

FRAME_SHAPES: Final[tuple[tuple[str, str, dict[str, str]], ...]] = (
    ("rows_both", "ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING", VALUE_COLUMNS),
    ("range_frame", "ORDER BY id RANGE BETWEEN 2 PRECEDING AND CURRENT ROW", VALUE_COLUMNS),
    ("all_null", "ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", NULL_COLUMNS),
    ("empty_frame", "ORDER BY id ROWS BETWEEN 5 PRECEDING AND 4 PRECEDING", VALUE_COLUMNS),
    (
        "partitioned",
        "PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
        VALUE_COLUMNS,
    ),
)

SHAPE_NAMES: Final[tuple[str, ...]] = tuple(name for name, _, _ in FRAME_SHAPES)

WINDOW_SPECS: Final[dict[str, Any]] = {
    "rows_both": lambda w: w.orderBy("id").rowsBetween(-2, 1),
    "range_frame": lambda w: w.orderBy("id").rangeBetween(-2, 0),
    "all_null": lambda w: w.orderBy("id").rowsBetween(-2, 0),
    "empty_frame": lambda w: w.orderBy("id").rowsBetween(-5, -4),
    "partitioned": lambda w: w.partitionBy("g").orderBy("id").rowsBetween(-2, 0),
}

DOOR_BUILDERS: Final[dict[str, Any]] = {
    "approx_count_distinct": lambda f, c: f.approx_count_distinct(c["vi"]),
    "approx_percentile": lambda f, c: f.approx_percentile(c["v"], 0.5),
    "bit_and": lambda f, c: f.bit_and(c["vi"]),
    "bit_or": lambda f, c: f.bit_or(c["vi"]),
    "bool_and": lambda f, c: f.bool_and(f.col(c["b"])),
    "bool_or": lambda f, c: f.bool_or(f.col(c["b"])),
    "collect_list": lambda f, c: f.collect_list(c["v"]),
    "collect_set": lambda f, c: f.collect_set(c["v"]),
    "corr": lambda f, c: f.corr(c["v"], c["v2"]),
    "covar_pop": lambda f, c: f.covar_pop(c["v"], c["v2"]),
    "covar_samp": lambda f, c: f.covar_samp(c["v"], c["v2"]),
    "percentile_approx": lambda f, c: f.percentile_approx(c["v"], 0.5),
    "try_sum": lambda f, c: f.try_sum(c["v"]),
}

FIXTURE_ROWS: Final[tuple[tuple[Any, ...], ...]] = (
    (1, "a", 1.0, 2.0, 6, True, 9223372036854775806),
    (2, "a", None, 3.0, 12, True, 1),
    (3, "a", 3.5, None, None, None, 2),
    (4, "a", -2.0, 1.5, 7, False, 3),
    (5, "b", 4.0, 0.5, 3, True, 9223372036854775807),
    (6, "b", 2.0, 2.5, 5, None, 1),
    (7, "b", None, 4.0, 9, False, 2),
    (8, "b", 5.0, -1.0, 4, True, 3),
)

ROW_COUNT: Final[int] = len(FIXTURE_ROWS)

OVERFLOW_FRAME: Final[str] = "ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"
TAIL_FRAME: Final[str] = "ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"

SPARK_GOLDEN: Final[dict[tuple[str, str], tuple[Any, ...]]] = {
    ("approx_count_distinct", "all_null"): (0, 0, 0, 0, 0, 0, 0, 0),
    ("approx_count_distinct", "empty_frame"): (0, 0, 0, 0, 1, 2, 1, 1),
    ("approx_count_distinct", "partitioned"): (1, 2, 2, 2, 1, 2, 3, 3),
    ("approx_count_distinct", "range_frame"): (1, 2, 2, 2, 2, 3, 3, 3),
    ("approx_count_distinct", "rows_both"): (2, 2, 3, 3, 3, 4, 4, 3),
    ("approx_percentile", "all_null"): (None, None, None, None, None, None, None, None),
    ("approx_percentile", "empty_frame"): (None, None, None, None, 1.0, 1.0, 3.5, -2.0),
    ("approx_percentile", "partitioned"): (1.0, 1.0, 1.0, -2.0, 4.0, 2.0, 2.0, 2.0),
    ("approx_percentile", "range_frame"): (1.0, 1.0, 1.0, -2.0, 3.5, 2.0, 2.0, 2.0),
    ("approx_percentile", "rows_both"): (1.0, 1.0, 1.0, 3.5, 2.0, 2.0, 4.0, 2.0),
    ("bit_and", "all_null"): (None, None, None, None, None, None, None, None),
    ("bit_and", "empty_frame"): (None, None, None, None, 6, 4, 12, 7),
    ("bit_and", "partitioned"): (6, 4, 4, 4, 3, 1, 1, 0),
    ("bit_and", "range_frame"): (6, 4, 4, 4, 3, 1, 1, 0),
    ("bit_and", "rows_both"): (4, 4, 4, 0, 1, 1, 0, 0),
    ("bit_or", "all_null"): (None, None, None, None, None, None, None, None),
    ("bit_or", "empty_frame"): (None, None, None, None, 6, 14, 12, 7),
    ("bit_or", "partitioned"): (6, 14, 14, 15, 3, 7, 15, 13),
    ("bit_or", "range_frame"): (6, 14, 14, 15, 7, 7, 15, 13),
    ("bit_or", "rows_both"): (14, 14, 15, 15, 7, 15, 15, 13),
    ("bool_and", "all_null"): (None, None, None, None, None, None, None, None),
    ("bool_and", "empty_frame"): (None, None, None, None, True, True, True, False),
    ("bool_and", "partitioned"): (True, True, True, False, True, True, False, False),
    ("bool_and", "range_frame"): (True, True, True, False, False, False, False, False),
    ("bool_and", "rows_both"): (True, True, False, False, False, False, False, False),
    ("bool_or", "all_null"): (None, None, None, None, None, None, None, None),
    ("bool_or", "empty_frame"): (None, None, None, None, True, True, True, False),
    ("bool_or", "partitioned"): (True, True, True, True, True, True, True, True),
    ("bool_or", "range_frame"): (True, True, True, True, True, True, True, True),
    ("bool_or", "rows_both"): (True, True, True, True, True, True, True, True),
    ("collect_list", "all_null"): ([], [], [], [], [], [], [], []),
    ("collect_list", "empty_frame"): ([], [], [], [], [1.0], [1.0], [3.5], [3.5, -2.0]),
    ("collect_list", "partitioned"): (
        [1.0],
        [1.0],
        [1.0, 3.5],
        [3.5, -2.0],
        [4.0],
        [4.0, 2.0],
        [4.0, 2.0],
        [2.0, 5.0],
    ),
    ("collect_list", "range_frame"): (
        [1.0],
        [1.0],
        [1.0, 3.5],
        [3.5, -2.0],
        [3.5, -2.0, 4.0],
        [-2.0, 4.0, 2.0],
        [4.0, 2.0],
        [2.0, 5.0],
    ),
    ("collect_list", "rows_both"): (
        [1.0],
        [1.0, 3.5],
        [1.0, 3.5, -2.0],
        [3.5, -2.0, 4.0],
        [3.5, -2.0, 4.0, 2.0],
        [-2.0, 4.0, 2.0],
        [4.0, 2.0, 5.0],
        [2.0, 5.0],
    ),
    ("collect_set", "all_null"): ([], [], [], [], [], [], [], []),
    ("collect_set", "empty_frame"): ([], [], [], [], [1.0], [1.0], [3.5], [3.5, -2.0]),
    ("collect_set", "partitioned"): (
        [1.0],
        [1.0],
        [3.5, 1.0],
        [3.5, -2.0],
        [4.0],
        [2.0, 4.0],
        [2.0, 4.0],
        [2.0, 5.0],
    ),
    ("collect_set", "range_frame"): (
        [1.0],
        [1.0],
        [3.5, 1.0],
        [3.5, -2.0],
        [3.5, -2.0, 4.0],
        [-2.0, 2.0, 4.0],
        [2.0, 4.0],
        [2.0, 5.0],
    ),
    ("collect_set", "rows_both"): (
        [1.0],
        [3.5, 1.0],
        [3.5, -2.0, 1.0],
        [3.5, -2.0, 4.0],
        [3.5, -2.0, 2.0, 4.0],
        [-2.0, 2.0, 4.0],
        [2.0, 4.0, 5.0],
        [2.0, 5.0],
    ),
    ("corr", "all_null"): (None, None, None, None, None, None, None, None),
    ("corr", "empty_frame"): (None, None, None, None, None, None, None, None),
    ("corr", "partitioned"): (None, None, None, None, None, -1.0, -1.0, -1.0),
    ("corr", "range_frame"): (None, None, None, None, -1.0, -0.32732683535398854, -1.0, -1.0),
    ("corr", "rows_both"): (
        None,
        None,
        1.0,
        -1.0,
        -0.32732683535398854,
        -0.32732683535398854,
        -0.9941916256019201,
        -1.0,
    ),
    ("covar_pop", "all_null"): (None, None, None, None, None, None, None, None),
    ("covar_pop", "empty_frame"): (None, None, None, None, 0.0, 0.0, None, 0.0),
    ("covar_pop", "partitioned"): (0.0, 0.0, 0.0, 0.0, 0.0, -1.0, -1.0, -2.625),
    ("covar_pop", "range_frame"): (0.0, 0.0, 0.0, 0.0, -1.5, -0.6666666666666666, -1.0, -2.625),
    ("covar_pop", "rows_both"): (
        0.0,
        0.0,
        0.375,
        -1.5,
        -0.6666666666666666,
        -0.6666666666666666,
        -1.7777777777777777,
        -2.625,
    ),
    ("covar_samp", "all_null"): (None, None, None, None, None, None, None, None),
    ("covar_samp", "empty_frame"): (None, None, None, None, None, None, None, None),
    ("covar_samp", "partitioned"): (None, None, None, None, None, -2.0, -2.0, -5.25),
    ("covar_samp", "range_frame"): (None, None, None, None, -3.0, -1.0, -2.0, -5.25),
    ("covar_samp", "rows_both"): (None, None, 0.75, -3.0, -1.0, -1.0, -2.6666666666666665, -5.25),
    ("percentile_approx", "all_null"): (None, None, None, None, None, None, None, None),
    ("percentile_approx", "empty_frame"): (None, None, None, None, 1.0, 1.0, 3.5, -2.0),
    ("percentile_approx", "partitioned"): (1.0, 1.0, 1.0, -2.0, 4.0, 2.0, 2.0, 2.0),
    ("percentile_approx", "range_frame"): (1.0, 1.0, 1.0, -2.0, 3.5, 2.0, 2.0, 2.0),
    ("percentile_approx", "rows_both"): (1.0, 1.0, 1.0, 3.5, 2.0, 2.0, 4.0, 2.0),
    ("try_sum", "all_null"): (None, None, None, None, None, None, None, None),
    ("try_sum", "empty_frame"): (None, None, None, None, 1.0, 1.0, 3.5, 1.5),
    ("try_sum", "partitioned"): (1.0, 1.0, 4.5, 1.5, 4.0, 6.0, 6.0, 7.0),
    ("try_sum", "range_frame"): (1.0, 1.0, 4.5, 1.5, 5.5, 4.0, 6.0, 7.0),
    ("try_sum", "rows_both"): (1.0, 4.5, 2.5, 5.5, 7.5, 4.0, 11.0, 7.0),
}

SKETCH_SAMPLE_INDICES: Final[tuple[int, ...]] = (0, 49, 99, 149, 199)

SPARK_EXTRA_GOLDEN: Final[dict[str, tuple[Any, ...]]] = {
    "try_sum_overflow": (9223372036854775806, 9223372036854775807, 3, 5, None, None, 3, 5),
    "collect_list_tail": (
        [1.0, 3.5, -2.0, 4.0, 2.0, 5.0],
        [3.5, -2.0, 4.0, 2.0, 5.0],
        [3.5, -2.0, 4.0, 2.0, 5.0],
        [-2.0, 4.0, 2.0, 5.0],
        [4.0, 2.0, 5.0],
        [2.0, 5.0],
        [5.0],
        [5.0],
    ),
    "collect_list_rows": (
        [1.0],
        [1.0],
        [1.0, 3.5],
        [3.5, -2.0],
        [3.5, -2.0, 4.0],
        [-2.0, 4.0, 2.0],
        [4.0, 2.0],
        [2.0, 5.0],
    ),
    "collect_set_rows": (
        [1.0],
        [1.0],
        [3.5, 1.0],
        [3.5, -2.0],
        [3.5, -2.0, 4.0],
        [-2.0, 2.0, 4.0],
        [2.0, 4.0],
        [2.0, 5.0],
    ),
    "bit_and_tail": (0, 0, 0, 0, 0, 0, 0, 4),
    "corr_tail": (
        -0.657951694959769,
        -0.6400553612788729,
        -0.6400553612788729,
        -0.6400553612788729,
        -0.9941916256019201,
        -1.0,
        None,
        None,
    ),
    "approx_count_distinct_tail": (7, 6, 5, 5, 4, 3, 2, 1),
    "percentile_approx_rows": (1.0, 1.0, 1.0, 1.0, 3.5, 2.0, 2.0, 4.0),
    "sketch_default": (1.0, 25.0, 50.0, 100.0, 150.0),
    "sketch_acc2": (1.0, 1.0, 1.0, 51.0, 101.0),
}


FLOAT_CANCEL_ROWS: Final[tuple[tuple[Any, ...], ...]] = (
    (1, 1e16),
    (2, 1.0),
    (3, 1.0),
    (4, 1e16),
    (5, 1.0),
    (6, 1.0),
)

CANCEL_FRAME: Final[str] = "ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"

REPARK_CANCEL_GOLDEN: Final[dict[str, tuple[Any, ...]]] = {
    "sum": (1e16, 1e16, 0.0, 1e16, 1e16, 0.0),
    "avg": (1e16, 5000000000000000.0, 0.0, 5000000000000000.0, 5000000000000000.0, 0.0),
}

SPARK_CANCEL_GOLDEN: Final[dict[str, tuple[Any, ...]]] = {
    "sum": (1e16, 1e16, 2.0, 1e16, 1e16, 2.0),
    "avg": (1e16, 5000000000000000.0, 1.0, 5000000000000000.0, 5000000000000000.0, 1.0),
}

CANCEL_EQUAL_GOLDEN: Final[dict[str, tuple[Any, ...]]] = {
    "min": (1e16, 1.0, 1.0, 1.0, 1.0, 1.0),
    "max": (1e16, 1e16, 1.0, 1e16, 1e16, 1.0),
    "count": (1, 2, 2, 2, 2, 2),
    "stddev_pop": (0.0, 5000000000000000.0, 0.0, 5000000000000000.0, 5000000000000000.0, 0.0),
    "stddev_samp": (None, 7071067811865475.0, 0.0, 7071067811865475.0, 7071067811865475.0, 0.0),
    "bit_and": (10000000000000000, 0, 1, 0, 0, 1),
    "bit_xor": (10000000000000000, 10000000000000001, 0, 10000000000000001, 10000000000000001, 0),
    "covar_pop": (
        0.0,
        2.4999999999999997e31,
        0.0,
        2.4999999999999997e31,
        2.4999999999999997e31,
        0.0,
    ),
}

CANCEL_EQUAL_EXPRESSIONS: Final[dict[str, str]] = {
    "min": "min(v)",
    "max": "max(v)",
    "count": "count(v)",
    "stddev_pop": "stddev_pop(v)",
    "stddev_samp": "stddev_samp(v)",
    "bit_and": "bit_and(CAST(v AS BIGINT))",
    "bit_xor": "bit_xor(CAST(v AS BIGINT))",
    "covar_pop": "covar_pop(v, v)",
}

CANCEL_DRIFT_EXPRESSIONS: Final[dict[str, str]] = {
    "var_pop": "var_pop(v)",
    "var_samp": "var_samp(v)",
    "regr_avgx": "regr_avgx(v, v)",
}

SPARK_CANCEL_DRIFT_GOLDEN: Final[dict[str, tuple[Any, ...]]] = {
    "var_pop": (0.0, 2.5e31, 0.0, 2.5e31, 2.5e31, 0.0),
    "var_samp": (None, 5e31, 0.0, 5e31, 5e31, 0.0),
    "regr_avgx": (1e16, 5000000000000000.0, 1.0, 5000000000000000.0, 5000000000000000.0, 1.0),
}

DATE_RANGE_ROWS: Final[int] = 6
DATE_RANGE_SUM: Final[tuple[float, ...]] = (1.0, 3.0, 6.0, 9.0, 12.0, 15.0)
REPARK_DATE_DOOR_ERROR: Final[str] = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"
SPARK_DATE_DOOR_ERROR: Final[str] = "DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE"


def _cancel_fixture(engine: Any) -> Any:
    """The catastrophic-cancellation seed: 1e16 next to 1.0, so a lost 1.0 is visible."""
    types = engine.types
    schema = types.StructType(
        [
            types.StructField("id", types.IntegerType(), False),
            types.StructField("v", types.DoubleType(), True),
        ]
    )
    return engine.session.createDataFrame(list(FLOAT_CANCEL_ROWS), schema)


def _date_range_fixture(engine: Any) -> Any:
    """Six consecutive days with a matching timestamp, for the RANGE order-key rows."""
    types = engine.types
    schema = types.StructType(
        [
            types.StructField("d", types.DateType(), True),
            types.StructField("ts", types.TimestampType(), True),
            types.StructField("v", types.DoubleType(), True),
        ]
    )
    base = dt.date(2026, 1, 1)
    rows = [
        (
            base + dt.timedelta(days=index),
            dt.datetime(2026, 1, 1 + index, 12, 0, 0),
            float(index + 1),
        )
        for index in range(DATE_RANGE_ROWS)
    ]
    return engine.session.createDataFrame(rows, schema)


def _fixture_schema(types: Any) -> Any:
    """The seven typed seed columns shared by both engines."""
    return types.StructType(
        [
            types.StructField("id", types.IntegerType(), False),
            types.StructField("g", types.StringType(), True),
            types.StructField("v", types.DoubleType(), True),
            types.StructField("v2", types.DoubleType(), True),
            types.StructField("vi", types.LongType(), True),
            types.StructField("b", types.BooleanType(), True),
            types.StructField("ov", types.LongType(), True),
        ]
    )


def _fixture(engine: Any) -> Any:
    """The seed frame plus the three all-NULL probe columns."""
    fns = engine.functions
    frame = engine.session.createDataFrame(list(FIXTURE_ROWS), _fixture_schema(engine.types))
    frame = frame.withColumn("vn", fns.lit(None).cast("double"))
    frame = frame.withColumn("vin", fns.lit(None).cast("bigint"))
    return frame.withColumn("bn", fns.lit(None).cast("boolean"))


def _sql_window_values(engine: Any, view: str, expression: str, frame_clause: str) -> tuple:
    """Collect the window column of one SQL-door sliding-frame query, ordered by id."""
    query = f"SELECT id, {expression} OVER ({frame_clause}) AS w FROM {view} ORDER BY id"
    return tuple(row[1] for row in engine.session.sql(query).collect())


def _query_column(engine: Any, query: str) -> tuple:
    """Collect the single projected column of one query, in the query's own order."""
    return tuple(row[0] for row in engine.session.sql(query).collect())


def _door_window_values(engine: Any, frame: Any, aggregate: str, shape: str) -> tuple:
    """Collect the window column of one DataFrame-door sliding-frame projection."""
    columns = next(cols for name, _, cols in FRAME_SHAPES if name == shape)
    spec = WINDOW_SPECS[shape](engine.window)
    column = DOOR_BUILDERS[aggregate](engine.functions, columns)
    projected = frame.select(engine.functions.col("id"), column.over(spec).alias("w"))
    return tuple(row[1] for row in projected.orderBy("id").collect())


def _sql_case(aggregate: str, shape: str) -> tuple[str, str]:
    """The SQL expression and frame clause for one (aggregate, shape) cell."""
    template = next(text for name, text in SLIDING_AGGREGATES if name == aggregate)
    frame_clause, columns = next(
        (clause, cols) for name, clause, cols in FRAME_SHAPES if name == shape
    )
    return template.format(**columns), frame_clause


def _normalize(aggregate: str, value: Any) -> Any:
    """Order-normalise one answer: ``collect_set`` is a multiset, everything else is ordered."""
    if aggregate == "collect_set" and value is not None:
        return sorted(value)
    return value


def _scalar_same(left: Any, right: Any) -> bool:
    """Compare two scalars, floats to a 1e-9 relative tolerance."""
    if isinstance(left, float) and isinstance(right, float):
        if math.isnan(left) and math.isnan(right):
            return True
        return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)
    return bool(left == right) and type(left) is type(right)


def _same(left: Any, right: Any) -> bool:
    """Compare two answers, descending one list level and applying the float tolerance."""
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, list) != isinstance(right, list):
        return False
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _scalar_same(one, other) for one, other in zip(left, right, strict=True)
        )
    return _scalar_same(left, right)


def _rows_same(left: tuple, right: tuple, aggregate: str) -> bool:
    """Compare two ordered answer columns cell by cell."""
    if len(left) != len(right):
        return False
    return all(
        _same(_normalize(aggregate, one), _normalize(aggregate, other))
        for one, other in zip(left, right, strict=True)
    )


@pytest.fixture
def repark_engine() -> Any:
    """A fresh repark engine per test (the suite clears the active session between tests)."""
    return lp.build_repark_engine()


@pytest.fixture
def repark_fixture(repark_engine: Any) -> Any:
    """The seed frame on repark, registered as the module-private SQL view."""
    frame = _fixture(repark_engine)
    frame.createOrReplaceTempView("win_slide_1_t")
    return frame


@pytest.mark.parametrize("aggregate", AGGREGATE_NAMES)
@pytest.mark.parametrize("shape", SHAPE_NAMES)
def test_sql_door_matches_the_spark_pin(repark_engine, repark_fixture, aggregate, shape) -> None:
    """C-001: every SQL-door sliding frame answers the recorded Spark 4.1.2 column."""
    expression, frame_clause = _sql_case(aggregate, shape)
    live = _sql_window_values(repark_engine, "win_slide_1_t", expression, frame_clause)
    pinned = SPARK_GOLDEN[(aggregate, shape)]
    assert _rows_same(live, pinned, aggregate), f"{aggregate}/{shape}: {live} != {pinned}"


@pytest.mark.parametrize("aggregate", AGGREGATE_NAMES)
@pytest.mark.parametrize("shape", SHAPE_NAMES)
def test_dataframe_door_matches_the_spark_pin(
    repark_engine, repark_fixture, aggregate, shape
) -> None:
    """C-002: every DataFrame-door `over(rowsBetween/rangeBetween)` answers the same column."""
    live = _door_window_values(repark_engine, repark_fixture, aggregate, shape)
    pinned = SPARK_GOLDEN[(aggregate, shape)]
    assert _rows_same(live, pinned, aggregate), f"{aggregate}/{shape}: {live} != {pinned}"


def test_collect_list_preserves_the_frame_row_order(repark_engine, repark_fixture) -> None:
    """C-003: `collect_list` over a frame keeps the frame's row order on both doors."""
    clause = "ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
    pinned = SPARK_EXTRA_GOLDEN["collect_list_rows"]
    sql_rows = _sql_window_values(repark_engine, "win_slide_1_t", "collect_list(v)", clause)
    assert list(sql_rows) == [list(item) for item in pinned]
    tail = _sql_window_values(repark_engine, "win_slide_1_t", "collect_list(v)", TAIL_FRAME)
    assert list(tail) == [list(item) for item in SPARK_EXTRA_GOLDEN["collect_list_tail"]]


def test_collect_set_over_a_frame_is_the_frame_multiset(repark_engine, repark_fixture) -> None:
    """C-003: `collect_set` order is Spark-unspecified; the sorted set is the pin."""
    clause = "ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
    rows = _sql_window_values(repark_engine, "win_slide_1_t", "collect_set(v)", clause)
    pinned = SPARK_EXTRA_GOLDEN["collect_set_rows"]
    assert [sorted(item) for item in rows] == [sorted(item) for item in pinned]


def test_try_sum_overflow_inside_the_frame_is_null(repark_engine, repark_fixture) -> None:
    """C-004: a frame whose sum overflows BIGINT answers NULL for that row, Spark-equal."""
    rows = _sql_window_values(repark_engine, "win_slide_1_t", "try_sum(ov)", OVERFLOW_FRAME)
    assert list(rows) == list(SPARK_EXTRA_GOLDEN["try_sum_overflow"])


@pytest.mark.parametrize(
    ("expression", "label"),
    [
        ("bit_and(vi)", "bit_and_tail"),
        ("corr(v, v2)", "corr_tail"),
        ("approx_count_distinct(vi)", "approx_count_distinct_tail"),
    ],
)
def test_current_row_to_unbounded_following_answers(
    repark_engine, repark_fixture, expression, label
) -> None:
    """C-005: the ever-receding frame is a sliding frame too and answers Spark-equal."""
    rows = _sql_window_values(repark_engine, "win_slide_1_t", expression, TAIL_FRAME)
    assert _rows_same(rows, SPARK_EXTRA_GOLDEN[label], label)


def test_percentile_approx_over_a_frame_ignores_the_accuracy_knob(repark_engine) -> None:
    """C-006: repark answers the discrete p50 per frame; Spark's accuracy-2 sketch diverges."""
    frame = repark_engine.session.range(1, 201).selectExpr("CAST(id AS DOUBLE) AS x", "id AS k")
    frame.createOrReplaceTempView("win_slide_1_sketch")
    clause = "ORDER BY k ROWS BETWEEN 99 PRECEDING AND CURRENT ROW"
    default_rows = _query_column(
        repark_engine,
        f"SELECT percentile_approx(x, 0.5) OVER ({clause}) AS w FROM win_slide_1_sketch ORDER BY k",
    )
    accuracy_rows = _query_column(
        repark_engine,
        f"SELECT percentile_approx(x, 0.5, 2) OVER ({clause}) AS w "
        "FROM win_slide_1_sketch ORDER BY k",
    )
    sampled = tuple(default_rows[index] for index in SKETCH_SAMPLE_INDICES)
    accurate = tuple(accuracy_rows[index] for index in SKETCH_SAMPLE_INDICES)
    assert sampled == SPARK_EXTRA_GOLDEN["sketch_default"]
    assert accurate == sampled
    assert accurate != SPARK_EXTRA_GOLDEN["sketch_acc2"]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_reproduces_every_pinned_answer(spark_engine) -> None:
    """C-007: live PySpark 4.1.2 still answers every pinned (aggregate, shape) column."""
    frame = _fixture(spark_engine)
    frame.createOrReplaceTempView("win_slide_1_oracle")
    for aggregate in AGGREGATE_NAMES:
        for shape in SHAPE_NAMES:
            expression, frame_clause = _sql_case(aggregate, shape)
            rows = _sql_window_values(spark_engine, "win_slide_1_oracle", expression, frame_clause)
            pinned = SPARK_GOLDEN[(aggregate, shape)]
            assert _rows_same(rows, pinned, aggregate), f"{aggregate}/{shape}"
            door = _door_window_values(spark_engine, frame, aggregate, shape)
            assert _rows_same(door, pinned, aggregate), f"door {aggregate}/{shape}"


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_reproduces_the_frame_shape_pins(spark_engine) -> None:
    """C-007: live PySpark still answers the order, overflow, tail and sketch pins."""
    frame = _fixture(spark_engine)
    frame.createOrReplaceTempView("win_slide_1_oracle")
    clause = "ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
    checks = (
        ("collect_list(v)", clause, "collect_list_rows"),
        ("collect_list(v)", TAIL_FRAME, "collect_list_tail"),
        ("collect_set(v)", clause, "collect_set_rows"),
        ("try_sum(ov)", OVERFLOW_FRAME, "try_sum_overflow"),
        ("bit_and(vi)", TAIL_FRAME, "bit_and_tail"),
        ("corr(v, v2)", TAIL_FRAME, "corr_tail"),
        ("approx_count_distinct(vi)", TAIL_FRAME, "approx_count_distinct_tail"),
    )
    for expression, frame_clause, label in checks:
        rows = _sql_window_values(spark_engine, "win_slide_1_oracle", expression, frame_clause)
        assert _rows_same(rows, SPARK_EXTRA_GOLDEN[label], label), label
    sketch = spark_engine.session.range(1, 201).selectExpr("CAST(id AS DOUBLE) AS x", "id AS k")
    sketch.createOrReplaceTempView("win_slide_1_sketch_oracle")
    sketch_clause = "ORDER BY k ROWS BETWEEN 99 PRECEDING AND CURRENT ROW"
    for expression, label in (
        ("percentile_approx(x, 0.5)", "sketch_default"),
        ("percentile_approx(x, 0.5, 2)", "sketch_acc2"),
    ):
        column = _query_column(
            spark_engine,
            f"SELECT {expression} OVER ({sketch_clause}) AS w "
            "FROM win_slide_1_sketch_oracle ORDER BY k",
        )
        sampled = tuple(column[index] for index in SKETCH_SAMPLE_INDICES)
        assert sampled == SPARK_EXTRA_GOLDEN[label], label


@pytest.mark.parametrize("aggregate", ["sum", "avg"])
def test_sliding_sum_and_avg_retract_where_spark_rescans(repark_engine, aggregate) -> None:
    """WIN-SLIDE-FLOAT-1: repark's retracting sum/avg lose a summand Spark's re-scan keeps."""
    _cancel_fixture(repark_engine).createOrReplaceTempView("win_slide_1_cancel")
    rows = _sql_window_values(repark_engine, "win_slide_1_cancel", f"{aggregate}(v)", CANCEL_FRAME)
    assert _rows_same(rows, REPARK_CANCEL_GOLDEN[aggregate], aggregate)
    assert not _rows_same(rows, SPARK_CANCEL_GOLDEN[aggregate], aggregate)


@pytest.mark.parametrize("aggregate", sorted(CANCEL_EQUAL_EXPRESSIONS))
def test_the_cancellation_fixture_is_spark_equal_off_the_sum_path(repark_engine, aggregate) -> None:
    """WIN-SLIDE-FLOAT-1 control: the divergence is the running-sum path, not the frame."""
    _cancel_fixture(repark_engine).createOrReplaceTempView("win_slide_1_cancel")
    rows = _sql_window_values(
        repark_engine, "win_slide_1_cancel", CANCEL_EQUAL_EXPRESSIONS[aggregate], CANCEL_FRAME
    )
    assert _rows_same(rows, CANCEL_EQUAL_GOLDEN[aggregate], aggregate)


@pytest.mark.parametrize("aggregate", sorted(CANCEL_DRIFT_EXPRESSIONS))
def test_the_variance_family_drifts_within_one_ulp_on_the_cancellation_fixture(
    repark_engine, aggregate
) -> None:
    """WIN-SLIDE-FLOAT-1 control: var/regr retract too, but bounded, not answer-destroying."""
    _cancel_fixture(repark_engine).createOrReplaceTempView("win_slide_1_cancel")
    rows = _sql_window_values(
        repark_engine,
        "win_slide_1_cancel",
        CANCEL_DRIFT_EXPRESSIONS[aggregate],
        CANCEL_FRAME,
    )
    pinned = SPARK_CANCEL_DRIFT_GOLDEN[aggregate]
    assert len(rows) == len(pinned)
    for live, spark in zip(rows, pinned, strict=True):
        if live is None or spark is None:
            assert live is None and spark is None
            continue
        assert math.isclose(live, spark, rel_tol=1e-15, abs_tol=1e-12)


def test_dataframe_door_range_over_a_date_key_refuses_with_reparks_own_error_class(
    repark_engine,
) -> None:
    """WIN-RANGE-ERRCLASS-1: both engines refuse; the class differs (pre-existing G2 guard)."""
    from repark.errors import AnalysisException

    frame = _date_range_fixture(repark_engine)
    functions = repark_engine.functions
    for key in ("d", "ts"):
        spec = repark_engine.window.orderBy(key).rangeBetween(-2, 0)
        with pytest.raises(AnalysisException, match=REPARK_DATE_DOOR_ERROR.split(".")[1]):
            frame.select(functions.sum("v").over(spec).alias("w")).collect()


def test_sql_door_range_over_a_date_key_is_spark_equal(repark_engine) -> None:
    """WIN-RANGE-DF-1 scope: the SQL door answers Spark's column over DATE and TIMESTAMP keys."""
    _date_range_fixture(repark_engine).createOrReplaceTempView("win_slide_1_dates")
    for clause in (
        "ORDER BY d RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW",
        "ORDER BY d RANGE BETWEEN 2 PRECEDING AND CURRENT ROW",
        "ORDER BY ts RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW",
    ):
        rows = _query_column(
            repark_engine,
            f"SELECT sum(v) OVER ({clause}) AS w FROM win_slide_1_dates ORDER BY d",
        )
        assert _rows_same(rows, DATE_RANGE_SUM, "sum"), clause
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException, match="RANGE_FRAME_INVALID_TYPE"):
        _query_column(
            repark_engine,
            "SELECT sum(v) OVER (ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS w "
            "FROM win_slide_1_dates ORDER BY d",
        )


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_rescans_the_cancellation_fixture(spark_engine) -> None:
    """WIN-SLIDE-FLOAT-1 oracle: Spark re-scans, so its sum/avg keep the small summand."""
    _cancel_fixture(spark_engine).createOrReplaceTempView("win_slide_1_cancel_oracle")
    for aggregate in ("sum", "avg"):
        rows = _sql_window_values(
            spark_engine, "win_slide_1_cancel_oracle", f"{aggregate}(v)", CANCEL_FRAME
        )
        assert _rows_same(rows, SPARK_CANCEL_GOLDEN[aggregate], aggregate), aggregate
    for aggregate, expression in CANCEL_EQUAL_EXPRESSIONS.items():
        rows = _sql_window_values(
            spark_engine, "win_slide_1_cancel_oracle", expression, CANCEL_FRAME
        )
        assert _rows_same(rows, CANCEL_EQUAL_GOLDEN[aggregate], aggregate), aggregate


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_refuses_the_date_key_range_frame_with_its_own_class(spark_engine) -> None:
    """WIN-RANGE-ERRCLASS-1 oracle: Spark refuses the same frame under a different class."""
    frame = _date_range_fixture(spark_engine)
    functions = spark_engine.functions
    for key in ("d", "ts"):
        spec = spark_engine.window.orderBy(key).rangeBetween(-2, 0)
        with pytest.raises(Exception, match=SPARK_DATE_DOOR_ERROR.split(".")[1]):
            frame.select(functions.sum("v").over(spec).alias("w")).collect()
    _date_range_fixture(spark_engine).createOrReplaceTempView("win_slide_1_dates_oracle")
    rows = _query_column(
        spark_engine,
        "SELECT sum(v) OVER (ORDER BY d RANGE BETWEEN INTERVAL 2 DAYS PRECEDING "
        "AND CURRENT ROW) AS w FROM win_slide_1_dates_oracle ORDER BY d",
    )
    assert _rows_same(rows, DATE_RANGE_SUM, "sum")
