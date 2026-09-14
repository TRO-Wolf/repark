"""ARRAY-NULL-1 — ``F.array_append`` / ``F.array_prepend`` oracle cells + memory pin.

pins: array-null-1/C-001, C-002, C-003, C-004, C-005, L-1, L-2, L-3, P2-1, P3-1
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.column import Column
from repark.spark.types import ArrayType, IntegerType, StructField, StructType

_DEPTH = 40
_HEADROOM = 3 * 8 * 1024**3

_WORKER = """
import json
import resource
import sys

depth = int(sys.argv[1])
headroom = int(sys.argv[2])
bound = int(sys.argv[3])
mode = sys.argv[4]

import numpy
import pyarrow
from repark import ReparkSession
from repark.spark import functions as F

spark = ReparkSession.builder.appName("array-null-1-mem").getOrCreate()
frame = spark.createDataFrame([([1, 2],)], "a array<int>")
frame.select(F.col("a").alias("warm")).collect()


def vm_size_bytes():
    for line in open("/proc/self/status"):
        if line.startswith("VmSize:"):
            return int(line.split()[1]) * 1024
    return 0


def peak_rss_bytes():
    for line in open("/proc/self/status"):
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    return 0


resource.setrlimit(resource.RLIMIT_AS, (vm_size_bytes() + headroom,) * 2)

before = peak_rss_bytes()
if mode == "flat":
    frame.select(
        *[F.array_append(F.col("a"), F.lit(i)).alias(f"c{i}") for i in range(depth)]
    ).collect()
else:
    builder = F.array_append if mode == "append" else F.array_prepend
    expression = F.col("a")
    for level in range(1, depth + 1):
        expression = builder(expression, F.lit(level))
        delta = peak_rss_bytes() - before
        if bound and delta > bound:
            print("JSON" + json.dumps({"crossed": level, "delta": delta}), flush=True)
            sys.exit(2)
    frame.select(expression.alias("c")).collect()
print("JSON" + json.dumps({"delta": peak_rss_bytes() - before}), flush=True)
"""


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-array-null-1").getOrCreate()
    yield session
    session.stop()


def _run_worker(depth: int, bound: int, mode: str) -> dict:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _WORKER,
            str(depth),
            str(_HEADROOM),
            str(bound),
            mode,
        ],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    tail = completed.stdout.strip().splitlines()
    assert tail, (
        f"{mode} worker produced no output: rc={completed.returncode}\n"
        f"stderr tail: {completed.stderr[-1200:]}"
    )
    marker = tail[-1]
    assert marker.startswith("JSON"), f"{mode} worker bad tail: {marker[:200]}"
    result = json.loads(marker[len("JSON") :])
    result["returncode"] = completed.returncode
    result["stderr_tail"] = completed.stderr[-600:]
    return result


def _result(spark: ReparkSession, ddl: str, rows: list, column: Column) -> pa.Table:
    return spark.createDataFrame(rows, ddl).select(column.alias("r")).to_arrow()


def _check(table: pa.Table, expected: list, value_type: str) -> None:
    column = table.column("r")
    assert column.to_pylist() == expected
    field_type = table.schema.field("r").type
    assert pa.types.is_list(field_type)
    assert str(field_type.value_type) == value_type
    assert field_type.value_field.nullable


def test_array_append_oracle_cells(spark: ReparkSession) -> None:
    """pins: array-null-1/C-001 — append answers equal the live PySpark 4.1.2 oracle."""
    _check(
        _result(spark, "a array<int>, e int", [(None, 4)], F.array_append("a", F.col("e"))),
        [None],
        "int32",
    )
    _check(
        _result(spark, "a array<int>, e int", [([1, 2], None)], F.array_append("a", F.col("e"))),
        [[1, 2, None]],
        "int32",
    )
    _check(
        _result(spark, "a array<int>, e int", [([], 4)], F.array_append("a", F.col("e"))),
        [[4]],
        "int32",
    )
    _check(
        _result(
            spark,
            "a array<array<int>>, e array<int>",
            [([[1], [2, 3]], [9])],
            F.array_append("a", F.col("e")),
        ),
        [[[1], [2, 3], [9]]],
        "list<item: int32>",
    )
    _check(
        _result(spark, "a array<bigint>", [([1, 2],)], F.array_append("a", F.lit(4))),
        [[1, 2, 4]],
        "int64",
    )
    _check(
        _result(spark, "a array<double>", [([1.0],)], F.array_append("a", F.lit(4))),
        [[1.0, 4.0]],
        "double",
    )
    with pytest.raises(PySparkException):
        spark.createDataFrame([([1, 2],)], "a array<int>").select(
            F.array_append("a", F.lit("x")).alias("r")
        ).to_arrow()
    nonnullable = StructType([StructField("a", ArrayType(IntegerType(), containsNull=False))])
    table = (
        spark.createDataFrame([([1, 2],)], nonnullable)
        .select(F.array_append("a", F.lit(None).cast("int")).alias("r"))
        .to_arrow()
    )
    _check(table, [[1, 2, None]], "int32")
    _check(
        spark.range(1)
        .select(F.array_append(F.array(F.lit(1), F.lit(2)), F.lit(3)).alias("r"))
        .to_arrow(),
        [[1, 2, 3]],
        "int32",
    )


def test_array_prepend_oracle_cells(spark: ReparkSession) -> None:
    """pins: array-null-1/C-001 — prepend answers equal the live PySpark 4.1.2 oracle."""
    _check(
        _result(spark, "a array<int>, e int", [(None, 4)], F.array_prepend("a", F.col("e"))),
        [None],
        "int32",
    )
    _check(
        _result(spark, "a array<int>, e int", [([1, 2], None)], F.array_prepend("a", F.col("e"))),
        [[None, 1, 2]],
        "int32",
    )
    _check(
        _result(spark, "a array<int>, e int", [([], 4)], F.array_prepend("a", F.col("e"))),
        [[4]],
        "int32",
    )
    _check(
        _result(
            spark,
            "a array<array<int>>, e array<int>",
            [([[1], [2, 3]], [9])],
            F.array_prepend("a", F.col("e")),
        ),
        [[[9], [1], [2, 3]]],
        "list<item: int32>",
    )
    _check(
        _result(spark, "a array<bigint>", [([1, 2],)], F.array_prepend("a", F.lit(4))),
        [[4, 1, 2]],
        "int64",
    )
    _check(
        _result(spark, "a array<double>", [([1.0],)], F.array_prepend("a", F.lit(4))),
        [[4.0, 1.0]],
        "double",
    )
    with pytest.raises(PySparkException):
        spark.createDataFrame([([1, 2],)], "a array<int>").select(
            F.array_prepend("a", F.lit("x")).alias("r")
        ).to_arrow()
    nonnullable = StructType([StructField("a", ArrayType(IntegerType(), containsNull=False))])
    table = (
        spark.createDataFrame([([1, 2],)], nonnullable)
        .select(F.array_prepend("a", F.lit(None).cast("int")).alias("r"))
        .to_arrow()
    )
    _check(table, [[None, 1, 2]], "int32")
    _check(
        spark.range(1)
        .select(F.array_prepend(F.array(F.lit(1), F.lit(2)), F.lit(3)).alias("r"))
        .to_arrow(),
        [[3, 1, 2]],
        "int32",
    )


def _door_result(spark: ReparkSession, ddl: str | StructType, rows: list, sql: str) -> pa.Table:
    spark.createDataFrame(rows, ddl).createOrReplaceTempView("v")
    return spark.sql(sql).to_arrow()


def test_array_append_door_oracle_cells(spark: ReparkSession) -> None:
    """pins: array-null-1/C-004, C-005 — the SQL door answers append like the facade."""
    _check(
        _door_result(
            spark, "a array<int>, e int", [(None, 4)], "SELECT array_append(a, e) AS r FROM v"
        ),
        [None],
        "int32",
    )
    _check(
        _door_result(
            spark, "a array<int>, e int", [([1, 2], None)], "SELECT array_append(a, e) AS r FROM v"
        ),
        [[1, 2, None]],
        "int32",
    )
    _check(
        _door_result(
            spark, "a array<int>, e int", [([], 4)], "SELECT array_append(a, e) AS r FROM v"
        ),
        [[4]],
        "int32",
    )
    _check(
        _door_result(
            spark,
            "a array<array<int>>, e array<int>",
            [([[1], [2, 3]], [9])],
            "SELECT array_append(a, e) AS r FROM v",
        ),
        [[[1], [2, 3], [9]]],
        "list<item: int32>",
    )
    _check(
        _door_result(
            spark, "a array<bigint>, e int", [([1, 2], 4)], "SELECT array_append(a, e) AS r FROM v"
        ),
        [[1, 2, 4]],
        "int64",
    )
    _check(
        _door_result(
            spark, "a array<double>, e int", [([1.0], 4)], "SELECT array_append(a, e) AS r FROM v"
        ),
        [[1.0, 4.0]],
        "double",
    )
    with pytest.raises(PySparkException):
        _door_result(
            spark,
            "a array<int>, e string",
            [([1, 2], "x")],
            "SELECT array_append(a, e) AS r FROM v",
        )
    nonnullable = StructType([StructField("a", ArrayType(IntegerType(), containsNull=False))])
    _check(
        _door_result(
            spark,
            nonnullable,
            [([1, 2],)],
            "SELECT array_append(a, CAST(NULL AS INT)) AS r FROM v",
        ),
        [[1, 2, None]],
        "int32",
    )
    _check(
        spark.sql("SELECT array_append(array(1, 2), 3) AS r").to_arrow(),
        [[1, 2, 3]],
        "int64",
    )


def test_array_prepend_door_oracle_cells(spark: ReparkSession) -> None:
    """pins: array-null-1/C-004, C-005 — the SQL door answers prepend like the facade."""
    _check(
        _door_result(
            spark, "a array<int>, e int", [(None, 4)], "SELECT array_prepend(a, e) AS r FROM v"
        ),
        [None],
        "int32",
    )
    _check(
        _door_result(
            spark, "a array<int>, e int", [([1, 2], None)], "SELECT array_prepend(a, e) AS r FROM v"
        ),
        [[None, 1, 2]],
        "int32",
    )
    _check(
        _door_result(
            spark, "a array<int>, e int", [([], 4)], "SELECT array_prepend(a, e) AS r FROM v"
        ),
        [[4]],
        "int32",
    )
    _check(
        _door_result(
            spark,
            "a array<array<int>>, e array<int>",
            [([[1], [2, 3]], [9])],
            "SELECT array_prepend(a, e) AS r FROM v",
        ),
        [[[9], [1], [2, 3]]],
        "list<item: int32>",
    )
    _check(
        _door_result(
            spark, "a array<bigint>, e int", [([1, 2], 4)], "SELECT array_prepend(a, e) AS r FROM v"
        ),
        [[4, 1, 2]],
        "int64",
    )
    _check(
        _door_result(
            spark, "a array<double>, e int", [([1.0], 4)], "SELECT array_prepend(a, e) AS r FROM v"
        ),
        [[4.0, 1.0]],
        "double",
    )
    with pytest.raises(PySparkException):
        _door_result(
            spark,
            "a array<int>, e string",
            [([1, 2], "x")],
            "SELECT array_prepend(a, e) AS r FROM v",
        )
    nonnullable = StructType([StructField("a", ArrayType(IntegerType(), containsNull=False))])
    _check(
        _door_result(
            spark,
            nonnullable,
            [([1, 2],)],
            "SELECT array_prepend(a, CAST(NULL AS INT)) AS r FROM v",
        ),
        [[None, 1, 2]],
        "int32",
    )
    _check(
        spark.sql("SELECT array_prepend(array(1, 2), 3) AS r").to_arrow(),
        [[3, 1, 2]],
        "int64",
    )


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
        ("ARRAY<STRING>", "INT"),
    ),
    (
        "str_alpha+int",
        "a array<string>",
        [(["x", "y"],)],
        F.lit(4),
        "4",
        "refuse",
        ("ARRAY<STRING>", "INT"),
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
        ("ARRAY<DECIMAL(10,2)>", "INT"),
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
            {
                "facade": [
                    [
                        datetime(2024, 1, 2, tzinfo=_UTC),
                        datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC),
                    ]
                ],
                "sql": [[datetime(2024, 1, 2), datetime(2024, 5, 6, 7, 8, 9)]],
            },
            {
                "facade": [
                    [
                        datetime(2024, 5, 6, 7, 8, 9, tzinfo=_UTC),
                        datetime(2024, 1, 2, tzinfo=_UTC),
                    ]
                ],
                "sql": [[datetime(2024, 5, 6, 7, 8, 9), datetime(2024, 1, 2)]],
            },
        ),
        (_TS_ARROW, "timestamp[ns]"),
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


@pytest.mark.parametrize("func", ["array_append", "array_prepend"])
@pytest.mark.parametrize("door", ["facade", "sql"])
@pytest.mark.parametrize("cell", _COERCION_CELLS, ids=[str(cell[0]) for cell in _COERCION_CELLS])
def test_array_element_coercion_cells(
    spark: ReparkSession, cell: tuple, door: str, func: str
) -> None:
    """pins: array-null-1/L-2 — every oracle coercion cell, both doors and functions."""
    _, ddl, rows, facade_element, sql_element, want, expected = cell
    arrow_type = expected if isinstance(expected, str) else expected[0 if door == "facade" else 1]
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
            for name in expected:
                assert name in message, message
            return
        _check(frame.select(column).to_arrow(), want_value, arrow_type)
        return
    if want == "refuse":
        with pytest.raises(PySparkException) as raised:
            _coercion_door_table(spark, ddl, rows, func, sql_element)
        message = str(raised.value)
        assert _COERCE_REFUSAL in message, message
        for name in expected:
            assert name in message, message
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
    for name in expected:
        assert name in message, message


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
        want_type = "int64"
    _check(table, [None, None], want_type)


def test_array_append_depth3_plan_shape(spark: ReparkSession) -> None:
    """pins: array-null-1/L-3 — one UDF call per level, no CASE in the physical plan."""
    frame = spark.createDataFrame([([1, 2],)], "a array<int>")
    expression = F.array_append(
        F.array_append(F.array_append(F.col("a"), F.lit(1)), F.lit(2)), F.lit(3)
    )
    plan = frame.select(expression.alias("r"))._explain_text()
    assert plan.count("array_append") == 3, plan
    assert "CASE" not in plan.upper(), plan


@pytest.mark.parametrize("mode", ["append", "prepend"])
def test_array_append_depth40_memory_linear(mode: str) -> None:
    """pins: array-null-1/C-002, P2-1 — depth-40 stays under 2x flat or 2x depth-4 chain."""
    control = _run_worker(_DEPTH, 0, "flat")
    assert control["returncode"] == 0, f"flat select control failed: {control['stderr_tail']}"
    shallow = _run_worker(4, 0, mode)
    assert shallow["returncode"] == 0, f"depth-4 {mode} control failed: {shallow['stderr_tail']}"
    bound = max(2 * control["delta"], 2 * shallow["delta"])
    result = _run_worker(_DEPTH, bound, mode)
    print(
        f"DELTAS {mode}: flat={control['delta']} depth4={shallow['delta']} "
        f"depth40={result.get('delta', '?')} bound={bound}",
        flush=True,
    )
    assert result["returncode"] == 0, (
        f"F.array_{mode} depth-{_DEPTH} crossed the bound {bound} B "
        f"(2x flat delta {control['delta']} B, 2x depth-4 chain delta "
        f"{shallow['delta']} B): {result.get('crossed', 'died')} "
        f"at delta {result.get('delta', '?')} B; {result['stderr_tail']}"
    )
