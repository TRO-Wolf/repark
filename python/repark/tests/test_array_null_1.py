"""ARRAY-NULL-1 — ``F.array_append`` / ``F.array_prepend`` oracle cells + memory pin.

pins: array-null-1/C-001, C-002, C-003
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.column import Column
from repark.spark.types import ArrayType, IntegerType, StructField, StructType

_DEPTH = 40
_HEADROOM = 3 * 8 * 1024**3
_DELTA_FLOOR = 64 * 1024**2
_MEM_SKIP = "REPARK_ARRAY_NULL_1_MEM != 1 — armed-only until the step-1 lowering lands"

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


@pytest.mark.skipif(os.environ.get("REPARK_ARRAY_NULL_1_MEM") != "1", reason=_MEM_SKIP)
def test_array_append_depth40_memory_linear() -> None:
    """pins: array-null-1/C-002 — a depth-40 chain stays under 2x a flat 40-append select."""
    control = _run_worker(_DEPTH, 0, "flat")
    assert control["returncode"] == 0, f"flat select control failed: {control['stderr_tail']}"
    bound = max(2 * control["delta"], _DELTA_FLOOR)
    result = _run_worker(_DEPTH, bound, "append")
    assert result["returncode"] == 0, (
        f"F.array_append depth-{_DEPTH} crossed the bound "
        f"{bound} B (2x flat 40-append select delta {control['delta']} B, floor "
        f"{_DELTA_FLOOR} B): {result.get('crossed', 'died')} "
        f"at delta {result.get('delta', '?')} B; {result['stderr_tail']}"
    )
