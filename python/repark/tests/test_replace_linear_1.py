"""REPLACE-LINEAR-1 — ``DataFrame.replace`` oracle cells and the exponential memory pin.

pins: replace-linear-1/C-001, C-002, C-005
"""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

_DEPTH = 40
_HEADROOM = 3 * 8 * 1024**3
_DELTA_FLOOR = 64 * 1024**2
_MEM_SKIP = "REPARK_REPLACE_LINEAR_1_MEM != 1 — armed-only until the step-1 rewrite lands"

_WORKER = """
import json
import resource
import sys

entries = int(sys.argv[1])
headroom = int(sys.argv[2])
bound = int(sys.argv[3])
mode = sys.argv[4]

import numpy
import pyarrow
from repark import ReparkSession
from repark.spark import functions as F

spark = ReparkSession.builder.appName("replace-linear-1-mem").getOrCreate()
frame = spark.createDataFrame([(i,) for i in range(10)], "x int")
frame.select(F.col("x").alias("warm")).collect()


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
    frame.select(*[(F.col("x") + i).alias(f"c{i}") for i in range(entries)]).collect()
else:
    frame.replace({i: i + 1000 for i in range(entries)}, subset=["x"]).collect()
delta = peak_rss_bytes() - before
if bound and delta > bound:
    print("JSON" + json.dumps({"crossed": entries, "delta": delta}), flush=True)
    sys.exit(2)
print("JSON" + json.dumps({"delta": delta}), flush=True)
"""


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-replace-linear-1").getOrCreate()
    yield session
    session.stop()


def _run_worker(entries: int, bound: int, mode: str) -> dict:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _WORKER,
            str(entries),
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


@pytest.mark.skipif(os.environ.get("REPARK_REPLACE_LINEAR_1_MEM") != "1", reason=_MEM_SKIP)
def test_replace_dict_depth40_memory_linear() -> None:
    """pins: replace-linear-1/C-002 — a 40-entry dict stays under 2x a flat 40-col select."""
    control = _run_worker(_DEPTH, 0, "flat")
    assert control["returncode"] == 0, f"flat select control failed: {control['stderr_tail']}"
    bound = max(2 * control["delta"], _DELTA_FLOOR)
    result = _run_worker(_DEPTH, bound, "replace")
    assert result["returncode"] == 0, (
        f"DataFrame.replace depth-{_DEPTH} crossed the bound "
        f"{bound} B (2x flat 40-col select delta {control['delta']} B, floor "
        f"{_DELTA_FLOOR} B): {result.get('crossed', 'died')} "
        f"at delta {result.get('delta', '?')} B; {result['stderr_tail']}"
    )


def test_replace_oracle_cells_matching(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-001 — cells where repark already answers like PySpark 4.1.2."""
    table = (
        spark.createDataFrame([(1.0,), (2.0,), (None,)], "x double")
        .replace({1.0: None}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [None, 2.0, None]
    assert str(table.schema.field("x").type) == "double"

    table = (
        spark.createDataFrame([(1,), (2,), (None,)], "x int")
        .replace(1, None, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [None, 2, None]
    assert str(table.schema.field("x").type) == "int32"

    table = (
        spark.createDataFrame([(1,), (2,), (3,)], "x int")
        .replace({1: None, 2: 3}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [None, 3, 3]
    assert str(table.schema.field("x").type) == "int32"

    table = spark.createDataFrame([(1,), (2,)], "x int").replace({1: None}, subset=["x"]).to_arrow()
    assert table.column("x").to_pylist() == [None, 2]

    table = (
        spark.createDataFrame([(True,), (False,), (None,)], "x boolean")
        .replace(True, False, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [False, False, None]
    assert str(table.schema.field("x").type) == "bool"

    table = (
        spark.createDataFrame([(1, 1), (2, 2)], "x int, y int")
        .replace(1, 9, subset=("x",))
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [9, 2]
    assert table.column("y").to_pylist() == [1, 2]

    table = (
        spark.createDataFrame([(1, 1), (2, 2)], "x int, y int").replace(1, 9, subset="x").to_arrow()
    )
    assert table.column("x").to_pylist() == [9, 2]
    assert table.column("y").to_pylist() == [1, 2]

    table = spark.createDataFrame([(1, 1), (2, 2)], "x int, y int").replace(1, 9).to_arrow()
    assert table.column("x").to_pylist() == [9, 2]
    assert table.column("y").to_pylist() == [9, 2]

    table = (
        spark.createDataFrame([(math.nan,), (1.0,), (None,)], "x double")
        .replace({math.nan: 0.0}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [0.0, 1.0, None]
    assert str(table.schema.field("x").type) == "double"

    table = (
        spark.createDataFrame([(1.0,), (2.0,), (None,)], "x double")
        .replace(1, 9, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [9.0, 2.0, None]
    assert str(table.schema.field("x").type) == "double"

    table = spark.createDataFrame([(1,), (2,)], "x int").replace({}).to_arrow()
    assert table.column("x").to_pylist() == [1, 2]
    assert str(table.schema.field("x").type) == "int32"

    table = spark.createDataFrame([(1,), (2,)], ["a b"]).replace(1, 9, subset=["a b"]).to_arrow()
    assert table.column("a b").to_pylist() == [9, 2]
    assert str(table.schema.field("a b").type) == "int64"


def test_replace_divergent_cells_today(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-001 — today's answers on the cells that differ from the oracle."""
    table = (
        spark.createDataFrame([(1,), (2,), (3,), (None,)], "x int")
        .replace({1: 2, 2: 3}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [3, 3, 3, None]
    assert str(table.schema.field("x").type) == "int32"

    table = (
        spark.createDataFrame([(1,), (None,), (5,)], "x int")
        .replace({None: 5}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [1, None, 5]

    table = (
        spark.createDataFrame([(1,), (None,)], "x int").replace(None, 5, subset=["x"]).to_arrow()
    )
    assert table.column("x").to_pylist() == [1, None]

    with pytest.raises(TypeError, match="unhashable"):
        spark.createDataFrame([(1,), (2,), (3,)], "x int").replace([1, 2], [3, 4], subset=["x"])

    with pytest.raises(TypeError, match="unhashable"):
        spark.createDataFrame([(1,), (2,), (3,)], "x int").replace([1, 2], 9, subset=["x"])

    with pytest.raises(PySparkException, match="Cannot cast string"):
        spark.createDataFrame([("a", 1), ("b", 2)], "s string, y int").replace("a", "b").collect()

    with pytest.raises(AnalysisException, match="type_coercion"):
        spark.createDataFrame([(True,), (False,)], "x boolean").replace(
            1, 2, subset=["x"]
        ).collect()

    with pytest.raises(AnalysisException, match="type_coercion"):
        spark.createDataFrame([(1,), (0,), (2,)], "x int").replace(True, 9, subset=["x"]).collect()

    table = (
        spark.createDataFrame([(1,), (2,), (None,)], "x int")
        .replace(1, 2.5, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [2.5, 2.0, None]
    assert str(table.schema.field("x").type) == "double"

    with pytest.raises(PySparkException, match="Cannot cast string"):
        spark.createDataFrame([(1, "a"), (2, "b")], "x int, s string").replace({"a": 1}).collect()

    table = spark.createDataFrame([(1,), (2,)], "x int").replace(1, 9, subset="missing").to_arrow()
    assert table.column("x").to_pylist() == [1, 2]

    with pytest.raises(PySparkException, match="Cannot cast string"):
        spark.createDataFrame([(1,), (2,)], "x int").replace("a", "b", subset=["x"]).collect()

    with pytest.raises(TypeError, match="unhashable"):
        spark.createDataFrame([(1,), (2,), (3,)], "x int").replace([1, 2], [3], subset=["x"])
