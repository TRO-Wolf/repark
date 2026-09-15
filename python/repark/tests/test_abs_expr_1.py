"""ABS-EXPR-1 — ``F.abs`` / ``F.cbrt`` / ``F.nullif`` lower to one native scalar call each.

pins: abs-expr-1/C-001, abs-expr-1/C-002, abs-expr-1/C-003, abs-expr-1/C-004
"""

from __future__ import annotations

import decimal
import json
import math
import subprocess
import sys

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

_DEPTH = 40
_ADDRESS_CAP = 12 * 1024**3
_DELTA_FLOOR = 64 * 1024**2

_WORKER = """
import json
import resource
import sys

depth = int(sys.argv[1])
cap = int(sys.argv[2])
bound = int(sys.argv[3])

import numpy
import pyarrow
from repark import ReparkSession
from repark.spark import functions as F

spark = ReparkSession.builder.appName("abs-expr-1-mem").getOrCreate()
frame = spark.createDataFrame([(-3,)], "x int")
frame.select(F.col("x").alias("warm")).collect()

resource.setrlimit(resource.RLIMIT_AS, (cap, cap))


def peak_rss_bytes():
    for line in open("/proc/self/status"):
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    return 0


builders = {
    "sqrt": lambda e: F.sqrt(e),
    "abs": lambda e: F.abs(e),
    "cbrt": lambda e: F.cbrt(e),
    "nullif": lambda e: F.nullif(e, F.lit(1)),
}

before = peak_rss_bytes()
expression = F.col("x")
for level in range(1, depth + 1):
    expression = builders[sys.argv[4]](expression)
    delta = peak_rss_bytes() - before
    if bound and delta > bound:
        print("JSON" + json.dumps({"crossed": level, "delta": delta}), flush=True)
        sys.exit(2)
frame.select(expression.alias("c")).collect()
print("JSON" + json.dumps({"delta": peak_rss_bytes() - before}), flush=True)
"""


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-abs-expr-1").getOrCreate()
    yield session
    session.stop()


def _run_chain(name: str, bound: int) -> dict:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _WORKER,
            str(_DEPTH),
            str(_ADDRESS_CAP),
            str(bound),
            name,
        ],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    tail = completed.stdout.strip().splitlines()
    assert tail, (
        f"F.{name} worker produced no output: rc={completed.returncode}\n"
        f"stderr tail: {completed.stderr[-1200:]}"
    )
    marker = tail[-1]
    assert marker.startswith("JSON"), f"F.{name} worker bad tail: {marker[:200]}"
    result = json.loads(marker[len("JSON") :])
    result["returncode"] = completed.returncode
    result["stderr_tail"] = completed.stderr[-600:]
    return result


def test_abs_chain_depth40_memory_linear() -> None:
    """pins: abs-expr-1/C-003 — depth-40 ``F.abs`` RSS bound = flat ``F.sqrt`` chain x2."""
    control = _run_chain("sqrt", 0)
    assert control["returncode"] == 0, f"flat F.sqrt control failed: {control['stderr_tail']}"
    bound = max(2 * control["delta"], _DELTA_FLOOR)
    for name in ("abs", "cbrt", "nullif"):
        result = _run_chain(name, bound)
        assert result["returncode"] == 0, (
            f"F.{name} depth-{_DEPTH} crossed the bound "
            f"{bound} B (2x flat F.sqrt delta {control['delta']} B, floor "
            f"{_DELTA_FLOOR} B): {result.get('crossed', 'died')} "
            f"at delta {result.get('delta', '?')} B; {result['stderr_tail']}"
        )


def test_abs_oracle_answer_cells(spark: ReparkSession) -> None:
    """pins: abs-expr-1/C-001, C-002 — ``F.abs`` matches the PySpark oracle, value AND type."""
    table = (
        spark.createDataFrame([(-3,), (None,), (5,)], "x int")
        .select(F.abs("x").alias("a"))
        .to_arrow()
    )
    assert table.column("a").to_pylist() == [3, None, 5]
    assert str(table.schema.field("a").type) == "int32"

    table = (
        spark.createDataFrame([(-1.5,), (-0.0,), (0.5,)], "x double")
        .select(F.abs("x").alias("a"))
        .to_arrow()
    )
    assert table.column("a").to_pylist() == [1.5, 0.0, 0.5]
    assert str(table.schema.field("a").type) == "double"
    assert math.copysign(1.0, table.column("a").to_pylist()[1]) > 0

    table = spark.createDataFrame([(-1.5,)], "x float").select(F.abs("x").alias("a")).to_arrow()
    assert table.column("a").to_pylist() == [1.5]
    assert str(table.schema.field("a").type) == "float"

    table = spark.createDataFrame([(-5,)], "x tinyint").select(F.abs("x").alias("a")).to_arrow()
    assert table.column("a").to_pylist() == [5]
    assert str(table.schema.field("a").type) == "int8"

    table = (
        spark.createDataFrame([(decimal.Decimal("-12.345"),)], "x decimal(10,3)")
        .select(F.abs("x").alias("a"))
        .to_arrow()
    )
    assert table.column("a").to_pylist() == [decimal.Decimal("12.345")]
    assert str(table.schema.field("a").type) == "decimal128(10, 3)"


def test_abs_integer_min_raises(spark: ReparkSession) -> None:
    """pins: abs-expr-1/C-001, C-002 — integer-min ``F.abs`` raises (Spark ARITHMETIC_OVERFLOW)."""
    with pytest.raises(PySparkException, match="overflow"):
        spark.createDataFrame([(-2147483648,)], "x int").select(F.abs("x")).to_arrow()
    with pytest.raises(PySparkException, match="overflow"):
        spark.createDataFrame([(-9223372036854775808,)], "x bigint").select(F.abs("x")).to_arrow()
    with pytest.raises(PySparkException, match="overflow"):
        spark.createDataFrame([(-128,)], "x tinyint").select(F.abs("x")).to_arrow()
    with pytest.raises(PySparkException, match="overflow"):
        spark.createDataFrame([(-32768,)], "x smallint").select(F.abs("x")).to_arrow()


def test_abs_non_numeric_refuses(spark: ReparkSession) -> None:
    """pins: abs-expr-1/C-001, C-002 — ``F.abs`` on boolean and string columns raises."""
    with pytest.raises(AnalysisException):
        spark.createDataFrame([(True,)], "x boolean").select(F.abs("x")).to_arrow()
    with pytest.raises(AnalysisException):
        spark.createDataFrame([("a",)], "x string").select(F.abs("x")).to_arrow()


def test_cbrt_oracle_answer_cells(spark: ReparkSession) -> None:
    """pins: abs-expr-1/C-001, C-002 — ``F.cbrt`` returns the exact signed real root."""
    table = (
        spark.createDataFrame([(-8.0,), (27.0,), (-1.0,), (None,)], "x double")
        .select(F.cbrt("x").alias("c"))
        .to_arrow()
    )
    assert table.column("c").to_pylist() == [-2.0, 3.0, -1.0, None]
    assert str(table.schema.field("c").type) == "double"

    table = spark.createDataFrame([(-8,), (64,)], "x int").select(F.cbrt("x").alias("c")).to_arrow()
    assert table.column("c").to_pylist() == [-2.0, 4.0]
    assert str(table.schema.field("c").type) == "double"

    table = spark.createDataFrame([(-0.0,)], "x double").select(F.cbrt("x").alias("c")).to_arrow()
    assert math.copysign(1.0, table.column("c").to_pylist()[0]) < 0

    table = (
        spark.createDataFrame([(decimal.Decimal("-8"),)], "x decimal(10,0)")
        .select(F.cbrt("x").alias("c"))
        .to_arrow()
    )
    assert table.column("c").to_pylist() == [-2.0]
    assert str(table.schema.field("c").type) == "double"


def test_cbrt_returns_double_for_every_numeric_input(spark: ReparkSession) -> None:
    """pins: abs-expr-1/C-001, C-002 — ``F.cbrt`` is double for every numeric input."""
    cases = [
        ("x float", [(27.5,)]),
        ("x tinyint", [(27,)]),
        ("x int", [(64,)]),
        ("x bigint", [(1000,)]),
        ("x decimal(10,0)", [(decimal.Decimal("-8"),)]),
    ]
    for ddl, rows in cases:
        table = spark.createDataFrame(rows, ddl).select(F.cbrt("x").alias("c")).to_arrow()
        assert str(table.schema.field("c").type) == "double", ddl
    table = spark.createDataFrame([(27.5,)], "x float").select(F.cbrt("x").alias("c")).to_arrow()
    assert table.column("c").to_pylist() == [3.018405368398843]


def test_cbrt_non_numeric_refuses(spark: ReparkSession) -> None:
    """pins: abs-expr-1/C-001, C-002 — ``F.cbrt`` on boolean and string columns raises."""
    with pytest.raises(AnalysisException):
        spark.createDataFrame([(True,)], "x boolean").select(F.cbrt("x")).to_arrow()
    with pytest.raises(AnalysisException):
        spark.createDataFrame([("a",)], "x string").select(F.cbrt("x")).to_arrow()


def test_abs_door_parity_integer_min(spark: ReparkSession) -> None:
    """pins: abs-expr-1/C-002, door-converge-1/C-003 — both doors raise at int-min."""
    for ddl, minimum in (
        ("tinyint", -128),
        ("smallint", -32768),
        ("int", -2147483648),
        ("bigint", -9223372036854775808),
    ):
        with pytest.raises(PySparkException, match="overflow"):
            spark.createDataFrame([(minimum,)], f"x {ddl}").select(F.abs("x")).to_arrow()
        spark.createDataFrame([(minimum,)], f"x {ddl}").createOrReplaceTempView("v")
        with pytest.raises(PySparkException, match="ARITHMETIC_OVERFLOW"):
            spark.sql("SELECT abs(x) AS a FROM v").collect()
    with pytest.raises(PySparkException, match="ARITHMETIC_OVERFLOW"):
        spark.sql("SELECT abs(CAST(-2147483648 AS INT)) AS a").collect()


def test_nullif_oracle_answer_cells(spark: ReparkSession) -> None:
    """pins: abs-expr-1/C-002, C-004 — ``F.nullif`` keeps its measured answers natively."""
    table = (
        spark.createDataFrame([(1,), (2,), (None,)], "x int")
        .select(F.nullif("x", F.lit(1)).alias("n"))
        .to_arrow()
    )
    assert table.column("n").to_pylist() == [None, 2, None]
    assert str(table.schema.field("n").type) == "int32"

    table = (
        spark.createDataFrame([(0,), (5,)], "x int").select(F.nullifzero("x").alias("z")).to_arrow()
    )
    assert table.column("z").to_pylist() == [None, 5]
    assert str(table.schema.field("z").type) == "int32"

    table = (
        spark.createDataFrame([("a",), ("b",)], "x string")
        .select(F.nullif("x", F.lit("a")).alias("n"))
        .to_arrow()
    )
    assert table.column("n").to_pylist() == [None, "b"]

    table = (
        spark.createDataFrame([(1.5,), (2.0,)], "x double")
        .select(F.nullif("x", F.lit(2)).alias("n"))
        .to_arrow()
    )
    assert table.column("n").to_pylist() == [1.5, None]
    assert str(table.schema.field("n").type) == "double"
