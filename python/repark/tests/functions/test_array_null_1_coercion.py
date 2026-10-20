"""ARRAY-NULL-1 round-2 — element coercion oracle cells and temporal pins.

Spark's recursive ``findTightestCommonType`` for ``array_append`` /
``array_prepend`` (run 14b, oracle-l5l11): numeric ladder, float32 over ints,
date/NTZ/LTZ timestamp resolution at microsecond precision in the session
zone, recursive array/map/struct widening, and the refusal pair tokens.
Pins: array-null-1/L-2, L-5..L-11, P3-1.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-array-null-1-coercion").getOrCreate()
    yield session
    session.stop()


def _check(table: pa.Table, expected: list, value_type: str) -> None:
    column = table.column("r")
    assert column.to_pylist() == expected
    field_type = table.schema.field("r").type
    assert pa.types.is_list(field_type)
    assert str(field_type.value_type) == value_type


_COERCE_REFUSAL = "DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"
_UTC = ZoneInfo("UTC")
_TS_ARROW = "timestamp[us, tz=UTC]"

_COERCION_CELLS: list[tuple] = [
    (
        "str_num+int",
        "a array<string>",
        [(["1", "2"],)],
        F.lit(4),
        "4",
        "refuse",
        {"facade": ("ARRAY<STRING>", "INT"), "sql": ("ARRAY<STRING>", "BIGINT")},
    ),
    (
        "str_alpha+int",
        "a array<string>",
        [(["x", "y"],)],
        F.lit(4),
        "4",
        "refuse",
        {"facade": ("ARRAY<STRING>", "INT"), "sql": ("ARRAY<STRING>", "BIGINT")},
    ),
    (
        "str+double",
        "a array<string>",
        [(["1", "2"],)],
        F.lit(1.5),
        "CAST(1.5 AS DOUBLE)",
        "refuse",
        ("ARRAY<STRING>", "DOUBLE"),
    ),
    (
        "int+double",
        "a array<int>",
        [([1, 2],)],
        F.lit(1.5),
        "CAST(1.5 AS DOUBLE)",
        ([[1.0, 2.0, 1.5]], [[1.5, 1.0, 2.0]]),
        "double",
    ),
    (
        "int+dec104",
        "a array<int>",
        [([1, 2],)],
        F.lit("1.2345").cast("decimal(10,4)"),
        "CAST(1.2345 AS DECIMAL(10,4))",
        "refuse",
        ("ARRAY<INT>", "DECIMAL(10,4)"),
    ),
    (
        "int+bigint",
        "a array<int>",
        [([1, 2],)],
        F.lit(4).cast("bigint"),
        "CAST(4 AS BIGINT)",
        ([[1, 2, 4]], [[4, 1, 2]]),
        "int64",
    ),
    (
        "int+string",
        "a array<int>",
        [([1, 2],)],
        F.lit("5"),
        "'5'",
        "refuse",
        ("ARRAY<INT>", "STRING"),
    ),
    (
        "bigint+double",
        "a array<bigint>",
        [([1, 2],)],
        F.lit(1.5),
        "CAST(1.5 AS DOUBLE)",
        ([[1.0, 2.0, 1.5]], [[1.5, 1.0, 2.0]]),
        "double",
    ),
    (
        "float+double",
        "a array<float>",
        [([1.0, 2.0],)],
        F.lit(1.5),
        "CAST(1.5 AS DOUBLE)",
        ([[1.0, 2.0, 1.5]], [[1.5, 1.0, 2.0]]),
        "double",
    ),
    (
        "double+int",
        "a array<double>",
        [([1.0, 2.0],)],
        F.lit(4),
        "4",
        ([[1.0, 2.0, 4.0]], [[4.0, 1.0, 2.0]]),
        "double",
    ),
    (
        "dec102+dec104",
        "a array<decimal(10,2)>",
        [([Decimal("1.25"), Decimal("2.50")],)],
        F.lit("1.2345").cast("decimal(10,4)"),
        "CAST(1.2345 AS DECIMAL(10,4))",
        "refuse",
        ("ARRAY<DECIMAL(10,2)>", "DECIMAL(10,4)"),
    ),
    (
        "dec102+int",
        "a array<decimal(10,2)>",
        [([Decimal("1.25")],)],
        F.lit(4),
        "4",
        "refuse",
        {"facade": ("ARRAY<DECIMAL(10,2)>", "INT"), "sql": ("ARRAY<DECIMAL(10,2)>", "BIGINT")},
    ),
    (
        "dec102+double",
        "a array<decimal(10,2)>",
        [([Decimal("1.25")],)],
        F.lit(1.5),
        "CAST(1.5 AS DOUBLE)",
        "refuse",
        ("ARRAY<DECIMAL(10,2)>", "DOUBLE"),
    ),
    (
        "ts+date",
        "a array<timestamp>",
        [([datetime(2024, 1, 2, 3, 4, 5)],)],
        F.lit("2024-05-06").cast("date"),
        "DATE'2024-05-06'",
        (
            [[datetime(2024, 1, 2, 3, 4, 5, tzinfo=_UTC), datetime(2024, 5, 6, tzinfo=_UTC)]],
            [[datetime(2024, 5, 6, tzinfo=_UTC), datetime(2024, 1, 2, 3, 4, 5, tzinfo=_UTC)]],
        ),
        _TS_ARROW,
    ),
    (
        "date+ts",
        "a array<date>",
        [([date(2024, 1, 2)],)],
        F.lit("2024-05-06 07:08:09").cast("timestamp"),
        "TIMESTAMP'2024-05-06 07:08:09'",
        (
            [
                [
                    datetime(2024, 1, 2, tzinfo=_UTC),
                    datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC),
                ]
            ],
            [
                [
                    datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC),
                    datetime(2024, 1, 2, tzinfo=_UTC),
                ]
            ],
        ),
        _TS_ARROW,
    ),
    (
        "float+int",
        "a array<float>",
        [([1.0, 2.0],)],
        F.lit(4),
        "4",
        ([[1.0, 2.0, 4.0]], [[4.0, 1.0, 2.0]]),
        "float",
    ),
    (
        "float+int_over24",
        "a array<float>",
        [([1.0, 2.0],)],
        F.lit(16777217),
        "16777217",
        ([[1.0, 2.0, 16777216.0]], [[16777216.0, 1.0, 2.0]]),
        "float",
    ),
    (
        "nested+array9",
        "a array<array<int>>",
        [([[1], [2]],)],
        F.array(F.lit(9)),
        "array(9)",
        ([[[1], [2], [9]]], [[[9], [1], [2]]]),
        "list<item: int32>",
    ),
    (
        "nested+bigint9",
        "a array<array<int>>",
        [([[1], [2]],)],
        F.array(F.lit(9).cast("bigint")),
        "array(CAST(9 AS BIGINT))",
        ([[[1], [2], [9]]], [[[9], [1], [2]]]),
        "list<item: int64>",
    ),
    (
        "nested_float+int",
        "a array<array<float>>",
        [([[1.5]],)],
        F.array(F.lit(2)),
        "array(2)",
        ([[[1.5], [2.0]]], [[[2.0], [1.5]]]),
        "list<item: float>",
    ),
    (
        "nested_float+double",
        "a array<array<float>>",
        [([[1.5]],)],
        F.array(F.lit(2.0)),
        "array(CAST(2 AS DOUBLE))",
        ([[[1.5], [2.0]]], [[[2.0], [1.5]]]),
        "list<item: double>",
    ),
    (
        "nested_int+string",
        "a array<array<int>>",
        [([[1]],)],
        F.array(F.lit("x")),
        "array('x')",
        "refuse",
        ("ARRAY<ARRAY<INT>>", "ARRAY<STRING>"),
    ),
    (
        "nested_date+ts",
        "a array<array<date>>",
        [([[date(2024, 1, 2)]],)],
        F.array(F.lit("2024-05-06 07:08:09").cast("timestamp")),
        "array(TIMESTAMP'2024-05-06 07:08:09')",
        (
            [
                [
                    [datetime(2024, 1, 2, tzinfo=_UTC)],
                    [datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC)],
                ]
            ],
            [
                [
                    [datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC)],
                    [datetime(2024, 1, 2, tzinfo=_UTC)],
                ]
            ],
        ),
        "list<item: timestamp[us, tz=UTC]>",
    ),
    (
        "map_strint+bigint",
        "a array<map<string,int>>",
        [([{"k": 1}],)],
        F.create_map(F.lit("k"), F.lit(2).cast("bigint")),
        "map('k', CAST(2 AS BIGINT))",
        ([[[("k", 1)], [("k", 2)]]], [[[("k", 2)], [("k", 1)]]]),
        "map<string, int64>",
    ),
    (
        "map_strint+int",
        "a array<map<string,int>>",
        [([{"k": 1}],)],
        F.create_map(F.lit(1), F.lit(2)),
        "map(1, 2)",
        "refuse",
        {
            "facade": ("ARRAY<MAP<STRING, INT>>", "MAP<INT, INT>"),
            "sql": ("ARRAY<MAP<STRING, INT>>", "MAP<BIGINT, BIGINT>"),
        },
    ),
    (
        "map_strint+strstr",
        "a array<map<string,int>>",
        [([{"k": 1}],)],
        F.create_map(F.lit("k"), F.lit("v")),
        "map('k', 'v')",
        "refuse",
        ("ARRAY<MAP<STRING, INT>>", "MAP<STRING, STRING>"),
    ),
    (
        "struct+bigint",
        "a array<struct<x:int>>",
        [([(1,)],)],
        F.struct(F.lit(1).cast("bigint").alias("x")),
        "named_struct('x', CAST(1 AS BIGINT))",
        ([[{"x": 1}, {"x": 1}]], [[{"x": 1}, {"x": 1}]]),
        "struct<x: int64>",
    ),
    (
        "struct_case",
        "a array<struct<x:int,y:string>>",
        [([(1, "a")],)],
        F.struct(F.lit(2).alias("X"), F.lit("b").alias("y")),
        "named_struct('X', CAST(2 AS INT), 'y', 'b')",
        (
            [[{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]],
            [[{"x": 2, "y": "b"}, {"x": 1, "y": "a"}]],
        ),
        "struct<x: int32, y: string>",
    ),
    (
        "struct_name",
        "a array<struct<x:int,y:string>>",
        [([(1, "a")],)],
        F.struct(F.lit(2).alias("z"), F.lit("b").alias("y")),
        "named_struct('z', CAST(2 AS INT), 'y', 'b')",
        "refuse",
        ("ARRAY<STRUCT<x: INT, y: STRING>>", "STRUCT<z: INT, y: STRING>"),
    ),
    (
        "struct_count",
        "a array<struct<x:int,y:string>>",
        [([(1, "a")],)],
        F.struct(F.lit(2).alias("x")),
        "named_struct('x', CAST(2 AS INT))",
        "refuse",
        ("ARRAY<STRUCT<x: INT, y: STRING>>", "STRUCT<x: INT>"),
    ),
    (
        "struct_order",
        "a array<struct<x:int,y:string>>",
        [([(1, "a")],)],
        F.struct(F.lit("b").alias("y"), F.lit(2).alias("x")),
        "named_struct('y', 'b', 'x', CAST(2 AS INT))",
        "refuse",
        ("ARRAY<STRUCT<x: INT, y: STRING>>", "STRUCT<y: STRING, x: INT>"),
    ),
    (
        "ntz+ts",
        "a array<timestamp_ntz>",
        [([datetime(2024, 1, 2, 3, 4, 5)],)],
        F.lit("2024-05-06 07:08:09").cast("timestamp"),
        "TIMESTAMP'2024-05-06 07:08:09'",
        (
            [
                [
                    datetime(2024, 1, 2, 3, 4, 5, tzinfo=_UTC),
                    datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC),
                ]
            ],
            [
                [
                    datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC),
                    datetime(2024, 1, 2, 3, 4, 5, tzinfo=_UTC),
                ]
            ],
        ),
        _TS_ARROW,
    ),
    (
        "ntz+date",
        "a array<timestamp_ntz>",
        [([datetime(2024, 1, 2, 3, 4, 5)],)],
        F.lit("2024-05-06").cast("date"),
        "DATE'2024-05-06'",
        (
            [[datetime(2024, 1, 2, 3, 4, 5), datetime(2024, 5, 6)]],
            [[datetime(2024, 5, 6), datetime(2024, 1, 2, 3, 4, 5)]],
        ),
        "timestamp[us]",
    ),
    (
        "ts+ntz",
        "a array<timestamp>, e timestamp_ntz",
        [([datetime(2024, 1, 2, 3, 4, 5)], datetime(2024, 5, 6, 7, 8, 9))],
        F.col("e"),
        "e",
        (
            [
                [
                    datetime(2024, 1, 2, 3, 4, 5, tzinfo=_UTC),
                    datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC),
                ]
            ],
            [
                [
                    datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC),
                    datetime(2024, 1, 2, 3, 4, 5, tzinfo=_UTC),
                ]
            ],
        ),
        _TS_ARROW,
    ),
    (
        "ntz+ntz",
        "a array<timestamp_ntz>, e timestamp_ntz",
        [([datetime(2024, 1, 2, 3, 4, 5)], datetime(2024, 5, 6, 7, 8, 9))],
        F.col("e"),
        "e",
        (
            [[datetime(2024, 1, 2, 3, 4, 5), datetime(2024, 5, 6, 7, 8, 9)]],
            [[datetime(2024, 5, 6, 7, 8, 9), datetime(2024, 1, 2, 3, 4, 5)]],
        ),
        "timestamp[us]",
    ),
]

_DOOR_ONLY_COERCION_CELLS: list[tuple] = [
    (
        "int+bare1.5",
        "a array<int>",
        [([1, 2],)],
        "1.5",
        ("ARRAY<INT>", "DECIMAL(2,1)"),
    ),
    (
        "double+bare1.5",
        "a array<double>",
        [([1.0, 2.0],)],
        "1.5",
        ("ARRAY<DOUBLE>", "DECIMAL(2,1)"),
    ),
    (
        "dec102+bare1.5",
        "a array<decimal(10,2)>",
        [([Decimal("1.25")],)],
        "1.5",
        ("ARRAY<DECIMAL(10,2)>", "DECIMAL(2,1)"),
    ),
]


def _coercion_door_table(
    spark: ReparkSession, ddl: str, rows: list, func: str, element: str
) -> pa.Table:
    spark.createDataFrame(rows, ddl).createOrReplaceTempView("v")
    return spark.sql(f"SELECT {func}(a, {element}) AS r FROM v").to_arrow()


def _refusal_pair_token(expected: tuple | dict, door: str) -> str:
    pair = expected[door] if isinstance(expected, dict) else expected
    return f'["{pair[0]}", "{pair[1]}"]'


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
@pytest.mark.parametrize("cell", _COERCION_CELLS, ids=[str(cell[0]) for cell in _COERCION_CELLS])
def test_array_element_coercion_cells(
    spark: ReparkSession, cell: tuple, door: str, func: str
) -> None:
    """pins: array-null-1/L-2, L-7..L-11 — every oracle coercion cell, both doors."""
    _, ddl, rows, facade_element, sql_element, want, expected = cell
    arrow_type = expected.get(door) if isinstance(expected, dict) else expected
    want_value = want[0 if func == "array_append" else 1]
    if isinstance(want_value, dict):
        want_value = want_value[door]
    if door == "facade":
        frame = spark.createDataFrame(rows, ddl)
        column = getattr(F, func)("a", facade_element).alias("r")
        if want == "refuse":
            with pytest.raises(PySparkException) as raised:
                frame.select(column).to_arrow()
            message = str(raised.value)
            assert _COERCE_REFUSAL in message, message
            assert _refusal_pair_token(expected, door) in message, message
            return
        _check(frame.select(column).to_arrow(), want_value, arrow_type)
        return
    if want == "refuse":
        with pytest.raises(PySparkException) as raised:
            _coercion_door_table(spark, ddl, rows, func, sql_element)
        message = str(raised.value)
        assert _COERCE_REFUSAL in message, message
        assert _refusal_pair_token(expected, door) in message, message
        return
    _check(
        _coercion_door_table(spark, ddl, rows, func, sql_element),
        want_value,
        arrow_type,
    )


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize(
    "cell",
    _DOOR_ONLY_COERCION_CELLS,
    ids=[str(cell[0]) for cell in _DOOR_ONLY_COERCION_CELLS],
)
def test_array_element_coercion_door_only_cells(
    spark: ReparkSession, cell: tuple, func: str
) -> None:
    """pins: array-null-1/L-2 — bare SQL decimal literals refuse like Spark (door only)."""
    _, ddl, rows, sql_element, expected = cell
    with pytest.raises(PySparkException) as raised:
        _coercion_door_table(spark, ddl, rows, func, sql_element)
    message = str(raised.value)
    assert _COERCE_REFUSAL in message, message
    assert _refusal_pair_token(expected, "sql") in message, message


_EPOCH_NAIVE = datetime(1970, 1, 1)
_LA_ZONE = "America/Los_Angeles"


def _micros(value: datetime) -> int:
    """Unix microseconds of an instant (tz-aware) or wall clock (naive)."""
    if value.tzinfo is not None:
        return int(value.timestamp() * 1_000_000)
    return int((value - _EPOCH_NAIVE).total_seconds() * 1_000_000)


def _micros_rows(table: pa.Table) -> list:
    return [[_micros(v) for v in row] for row in table.column("r").to_pylist()]


def _la_session() -> ReparkSession:
    return (
        ReparkSession.builder.appName("array-null-1-la")
        .config("spark.sql.session.timeZone", _LA_ZONE)
        .getOrCreate()
    )


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
def test_date_array_elements_localize_in_session_zone(door: str, func: str) -> None:
    """pins: array-null-1/L-5, L-6 — date-array elements widened to timestamp are
    midnight in the session zone at µs precision on both doors; year 1 and 9999
    survive (oracle unix micros under America/Los_Angeles)."""
    spark = _la_session()
    la = ZoneInfo(_LA_ZONE)
    dates = [date(2024, 1, 2), date(1, 1, 1), date(9999, 12, 31)]
    rows = [([d],) for d in dates]
    date_micros = [_micros(datetime(d.year, d.month, d.day, tzinfo=la)) for d in dates]
    ts_micros = _micros(datetime(2024, 5, 6, 7, 8, 9, tzinfo=la))
    if door == "facade":
        element = F.lit("2024-05-06 07:08:09").cast("timestamp")
        table = (
            spark.createDataFrame(rows, "a array<date>")
            .select(getattr(F, func)("a", element).alias("r"))
            .to_arrow()
        )
    else:
        table = _coercion_door_table(
            spark,
            "a array<date>",
            rows,
            func,
            "TIMESTAMP'2024-05-06 07:08:09'",
        )
    want = [
        [date_micros[i], ts_micros] if func == "array_append" else [ts_micros, date_micros[i]]
        for i in range(len(rows))
    ]
    assert str(table.schema.field("r").type.value_type) == _TS_ARROW
    assert _micros_rows(table) == want


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
def test_timestamp_array_plus_date_element_localizes_in_session_zone(door: str, func: str) -> None:
    """pins: array-null-1/L-6 — the scalar date element path already localized in the
    session zone; the widened date-element-into-timestamp-array arm must agree."""
    spark = _la_session()
    la = ZoneInfo(_LA_ZONE)
    rows = [([datetime(2024, 1, 2, 3, 4, 5)],)]
    frame = spark.createDataFrame(rows, "a array<timestamp>")
    input_micros = _micros(frame.to_arrow().column("a").to_pylist()[0][0])
    date_micros = _micros(datetime(2024, 5, 6, tzinfo=la))
    if door == "facade":
        element = F.lit("2024-05-06").cast("date")
        table = frame.select(getattr(F, func)("a", element).alias("r")).to_arrow()
    else:
        frame.createOrReplaceTempView("v")
        table = spark.sql(f"SELECT {func}(a, DATE'2024-05-06') AS r FROM v").to_arrow()
    want = [[input_micros, date_micros] if func == "array_append" else [date_micros, input_micros]]
    assert str(table.schema.field("r").type.value_type) == _TS_ARROW
    assert _micros_rows(table) == want


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
def test_ntz_array_plus_timestamp_localizes_in_session_zone(door: str, func: str) -> None:
    """pins: array-null-1/L-9 — ntz + timestamp widens to timestamp with the NTZ wall
    read in the session zone (oracle: [-62135568422000000, 1715004489000000])."""
    spark = _la_session()
    rows = [([datetime(1, 1, 1)],)]
    if door == "facade":
        element = F.lit("2024-05-06 07:08:09").cast("timestamp")
        table = (
            spark.createDataFrame(rows, "a array<timestamp_ntz>")
            .select(getattr(F, func)("a", element).alias("r"))
            .to_arrow()
        )
    else:
        table = _coercion_door_table(
            spark,
            "a array<timestamp_ntz>",
            rows,
            func,
            "TIMESTAMP'2024-05-06 07:08:09'",
        )
    ntz_micros = _micros(datetime(1, 1, 1, tzinfo=ZoneInfo(_LA_ZONE)))
    ts_micros = _micros(datetime(2024, 5, 6, 7, 8, 9, tzinfo=ZoneInfo(_LA_ZONE)))
    want = [[ntz_micros, ts_micros] if func == "array_append" else [ts_micros, ntz_micros]]
    assert str(table.schema.field("r").type.value_type) == _TS_ARROW
    assert _micros_rows(table) == want


def test_timestamp_array_plus_timestamp_plans_without_array_cast(spark: ReparkSession) -> None:
    """pins: array-null-1/L-5 — the common timestamp is µs; an array<timestamp[us]> +
    timestamp plan carries no CAST of the array argument."""
    spark.createDataFrame(
        [([datetime(2024, 1, 2, 3, 4, 5)],)], "a array<timestamp>"
    ).createOrReplaceTempView("v")
    plan = spark.sql(
        "SELECT array_append(a, TIMESTAMP'2024-05-06 07:08:09') AS r FROM v"
    )._explain_text()
    assert "CAST" not in plan, plan


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
def test_array_all_null_input_returns_typed_null(
    spark: ReparkSession, door: str, func: str
) -> None:
    """pins: array-null-1/P3-1 — an all-null array column returns typed NULLs, both doors."""
    rows = [(None,), (None,)]
    if door == "facade":
        table = (
            spark.createDataFrame(rows, "a array<int>")
            .select(getattr(F, func)("a", F.lit(9)).alias("r"))
            .to_arrow()
        )
        want_type = "int32"
    else:
        table = _coercion_door_table(spark, "a array<int>", rows, func, "9")
        want_type = "int32"
    _check(table, [None, None], want_type)


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
def test_float16_array_plus_int_resolves_as_float32(spark: ReparkSession, func: str) -> None:
    """pins: array-null-1/L-12 — Spark has no half-float; float16 takes part on the
    numeric ladder as FLOAT (float32), so 100000 stores 100000.0, never Inf."""
    arrow = pa.array([[1.0, 2.0], [None]], type=pa.list_(pa.float16()))
    frame = spark.createDataFrame(pl.from_arrow(pa.table({"a": arrow})))
    table = frame.select(getattr(F, func)("a", F.lit(100000)).alias("r")).to_arrow()
    assert str(table.schema.field("r").type.value_type) == "float"
    want = (
        [[1.0, 2.0, 100000.0], [None, 100000.0]]
        if func == "array_append"
        else [[100000.0, 1.0, 2.0], [100000.0, None]]
    )
    assert table.column("r").to_pylist() == want


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
def test_struct_field_matching_ignores_case_sensitive(door: str, func: str) -> None:
    """pins: array-null-1/L-13 — struct field names match case-insensitively even
    with spark.sql.caseSensitive=true (the product rule; the oracle's default is
    false and this UDF does not read the conf)."""
    spark = (
        ReparkSession.builder.appName("pytest-array-null-1-casesensitive")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )
    rows = [([(1, "a")],)]
    if door == "facade":
        element = F.struct(F.lit(2).alias("X"), F.lit("b").alias("y"))
        table = (
            spark.createDataFrame(rows, "a array<struct<x:int,y:string>>")
            .select(getattr(F, func)("a", element).alias("r"))
            .to_arrow()
        )
    else:
        table = _coercion_door_table(
            spark,
            "a array<struct<x:int,y:string>>",
            rows,
            func,
            "named_struct('X', CAST(2 AS INT), 'y', 'b')",
        )
    want = (
        [[{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]]
        if func == "array_append"
        else [[{"x": 2, "y": "b"}, {"x": 1, "y": "a"}]]
    )
    _check(table, want, "struct<x: int32, y: string>")


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
def test_dst_transition_day_midnights(door: str, func: str) -> None:
    """pins: array-null-1/P2-1 — spring-forward and fall-back midnights keep the
    answers localize_wall_micros_in_zone produced before the span cache."""
    spark = _la_session()
    rows = [([date(2024, 3, 10)],), ([date(2024, 11, 3)],)]
    ts_micros = _micros(datetime(2024, 5, 6, 7, 8, 9, tzinfo=ZoneInfo(_LA_ZONE)))
    if door == "facade":
        element = F.lit("2024-05-06 07:08:09").cast("timestamp")
        table = (
            spark.createDataFrame(rows, "a array<date>")
            .select(getattr(F, func)("a", element).alias("r"))
            .to_arrow()
        )
    else:
        table = _coercion_door_table(
            spark,
            "a array<date>",
            rows,
            func,
            "TIMESTAMP'2024-05-06 07:08:09'",
        )
    midnights = [1710057600000000, 1730617200000000]
    want = [
        [midnights[i], ts_micros] if func == "array_append" else [ts_micros, midnights[i]]
        for i in range(len(rows))
    ]
    assert str(table.schema.field("r").type.value_type) == _TS_ARROW
    assert _micros_rows(table) == want


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
def test_ntz_walls_in_skipped_and_repeated_hours(door: str, func: str) -> None:
    """pins: array-null-1/P2-1 — the 02:30 wall inside the spring-forward gap keeps
    the pre-gap offset; the 01:30 wall inside the fall-back overlap keeps the
    earliest (pre-transition) offset, both from the pre-change answers."""
    spark = _la_session()
    rows = [([datetime(2024, 3, 10, 2, 30)],), ([datetime(2024, 11, 3, 1, 30)],)]
    ts_micros = _micros(datetime(2024, 5, 6, 7, 8, 9, tzinfo=ZoneInfo(_LA_ZONE)))
    if door == "facade":
        element = F.lit("2024-05-06 07:08:09").cast("timestamp")
        table = (
            spark.createDataFrame(rows, "a array<timestamp_ntz>")
            .select(getattr(F, func)("a", element).alias("r"))
            .to_arrow()
        )
    else:
        table = _coercion_door_table(
            spark,
            "a array<timestamp_ntz>",
            rows,
            func,
            "TIMESTAMP'2024-05-06 07:08:09'",
        )
    walls = [1710066600000000, 1730622600000000]
    want = [
        [walls[i], ts_micros] if func == "array_append" else [ts_micros, walls[i]]
        for i in range(len(rows))
    ]
    assert str(table.schema.field("r").type.value_type) == _TS_ARROW
    assert _micros_rows(table) == want


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
def test_timestamp_ns_unit_rescale_truncates_like_arrow_cast(
    spark: ReparkSession, door: str, func: str
) -> None:
    """pins: array-null-1/P2-2 — ns -> us rescale divides by 1000 with Arrow's
    truncation toward zero for negative pre-epoch values."""
    inner = pa.array(
        [-1_500_000_001, -1_500_000_000, -999, 999],
        type=pa.timestamp("ns", tz="UTC"),
    )
    column = pa.ListArray.from_arrays(pa.array([0, 4], type=pa.int32()), inner)
    spark._ensure_alive().register_arrow_stream_as_temp_view("ns_v", pa.table({"a": column}))
    if door == "facade":
        table = (
            spark.sql("SELECT a FROM ns_v")
            .select(
                getattr(F, func)("a", F.lit("2024-05-06 07:08:09").cast("timestamp")).alias("r")
            )
            .to_arrow()
        )
    else:
        table = spark.sql(
            f"SELECT {func}(a, TIMESTAMP'2024-05-06 07:08:09') AS r FROM ns_v"
        ).to_arrow()
    ts_micros = _micros(datetime(2024, 5, 6, 7, 8, 9, tzinfo=ZoneInfo("UTC")))
    rescaled = [-1_500_000, -1_500_000, 0, 0]
    want = [[*rescaled, ts_micros] if func == "array_append" else [ts_micros, *rescaled]]
    assert str(table.schema.field("r").type.value_type) == _TS_ARROW
    assert _micros_rows(table) == want
