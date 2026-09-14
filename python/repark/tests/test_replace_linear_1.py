"""REPLACE-LINEAR-1 — ``DataFrame.replace`` oracle cells and the exponential memory pin.

pins: replace-linear-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import json
import math
import subprocess
import sys

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
)

_DEPTH = 40
_HEADROOM = 3 * 8 * 1024**3
_DELTA_FLOOR = 8 * 1024**2

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


def test_replace_divergent_cells_match_spark(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-001, C-003 — the ruled cells now answer like PySpark 4.1.2."""
    table = (
        spark.createDataFrame([(1,), (2,), (3,), (None,)], "x int")
        .replace({1: 2, 2: 3}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [2, 3, 3, None]
    assert str(table.schema.field("x").type) == "int32"

    with pytest.raises(PySparkValueError) as null_key:
        spark.createDataFrame([(1,), (None,), (5,)], "x int").replace({None: 5}, subset=["x"])
    assert null_key.value.getCondition() == "MIXED_TYPE_REPLACEMENT"

    with pytest.raises(PySparkTypeError) as none_scalar:
        spark.createDataFrame([(1,), (None,)], "x int").replace(None, 5, subset=["x"])
    assert (
        none_scalar.value.getCondition()
        == "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE"
    )

    table = (
        spark.createDataFrame([(1,), (2,), (3,)], "x int")
        .replace([1, 2], [3, 4], subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [3, 4, 3]
    assert str(table.schema.field("x").type) == "int32"

    table = (
        spark.createDataFrame([(1,), (2,), (3,)], "x int")
        .replace([1, 2], 9, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [9, 9, 3]
    assert str(table.schema.field("x").type) == "int32"

    with pytest.raises(PySparkValueError) as length_mismatch:
        spark.createDataFrame([(1,), (2,), (3,)], "x int").replace([1, 2], [3], subset=["x"])
    assert length_mismatch.value.getCondition() == "LENGTH_SHOULD_BE_THE_SAME"

    rows = (
        spark.createDataFrame([("a", 1), ("b", 2)], "s string, y int").replace("a", "b").collect()
    )
    assert rows == [("b", 1), ("b", 2)]

    table = (
        spark.createDataFrame([(True,), (False,), (None,)], "x boolean")
        .replace(1, 2, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [True, False, None]
    assert str(table.schema.field("x").type) == "bool"

    with pytest.raises(IllegalArgumentException, match="Unsupported value type"):
        spark.createDataFrame([(1,), (0,), (2,)], "x int").replace(True, 9, subset=["x"])

    table = (
        spark.createDataFrame([(1,), (2,), (None,)], "x int")
        .replace(1, 2.5, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [2, 2, None]
    assert str(table.schema.field("x").type) == "int32"

    with pytest.raises(PySparkValueError) as mixed_map:
        spark.createDataFrame([(1, "a"), (2, "b")], "x int, s string").replace({"a": 1})
    assert mixed_map.value.getCondition() == "MIXED_TYPE_REPLACEMENT"

    with pytest.raises(AnalysisException, match="cannot be resolved"):
        spark.createDataFrame([(1,), (2,)], "x int").replace(1, 9, subset="missing")

    table = spark.createDataFrame([(1,), (2,)], "x int").replace("a", "b", subset=["x"]).to_arrow()
    assert table.column("x").to_pylist() == [1, 2]
    assert str(table.schema.field("x").type) == "int32"


def test_replace_oracle_extra_cells(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-001 — promoted edge cells from the C-001 probe rows."""
    with pytest.raises(AnalysisException, match="cannot be resolved"):
        spark.createDataFrame([(1,), (2,)], "x int").replace({}, subset="missing")

    table = (
        spark.createDataFrame([(1,), (0,), (2,)], "x int")
        .replace({2: True}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [1, 0, 1]
    assert str(table.schema.field("x").type) == "int32"

    table = (
        spark.createDataFrame([(True,), (False,)], "x boolean")
        .replace({True: None}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [None, False]
    assert str(table.schema.field("x").type) == "bool"

    table = (
        spark.createDataFrame([(1,), (2,)], "x int").replace({True: None}, subset=["x"]).to_arrow()
    )
    assert table.column("x").to_pylist() == [1, 2]
    assert str(table.schema.field("x").type) == "int32"

    table = spark.createDataFrame([(1,), (2,)], "x int").replace({1.5: 9}, subset=["x"]).to_arrow()
    assert table.column("x").to_pylist() == [1, 2]
    assert str(table.schema.field("x").type) == "int32"

    table = (
        spark.createDataFrame([(1.5,), (2.0,), (None,)], "x double")
        .replace({1.5: 9}, subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [9.0, 2.0, None]
    assert str(table.schema.field("x").type) == "double"

    table = spark.createDataFrame([(1,), (2,)], "x int").replace({1: 2}, subset="X").to_arrow()
    assert table.column("x").to_pylist() == [1, 2]
    assert str(table.schema.field("x").type) == "int32"

    table = (
        spark.createDataFrame([(1,), (2,)], "x int")
        .replace((1, 2), (9, 8), subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [9, 8]
    assert str(table.schema.field("x").type) == "int32"

    table = (
        spark.createDataFrame([(True, 1), (False, 2)], "b boolean, x int").replace(1, 2).to_arrow()
    )
    assert table.column("b").to_pylist() == [True, False]
    assert str(table.schema.field("b").type) == "bool"
    assert table.column("x").to_pylist() == [2, 2]
    assert str(table.schema.field("x").type) == "int32"


def test_replace_binary_column_not_string_family(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-001 — string keys never touch a binary column (P1-1)."""
    frame = spark.createDataFrame([(b"a",), (b"a",), (b"ab",)], ["x"])
    table = frame.replace("a", "b").to_arrow()
    assert table.column("x").to_pylist() == [b"a", b"a", b"ab"]
    assert str(table.schema.field("x").type) == "binary"

    table = frame.replace("a", "b", subset=["x"]).to_arrow()
    assert table.column("x").to_pylist() == [b"a", b"a", b"ab"]
    assert str(table.schema.field("x").type) == "binary"


def test_replace_last_wins_duplicate_keys(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-001 — duplicate keys keep the last arm (P2-2)."""
    table = (
        spark.createDataFrame([(1,), (2,)], "x int")
        .replace([1, 1], [2, 3], subset=["x"])
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [3, 2]
    assert str(table.schema.field("x").type) == "int32"

    table = (
        spark.createDataFrame([(1,), (2,)], "x int")
        .replace({1: 5, 1.0: 6}, subset=["x"])  # noqa: F601
        .to_arrow()
    )
    assert table.column("x").to_pylist() == [6, 2]
    assert str(table.schema.field("x").type) == "int32"


def test_replace_overflow_collect_disclosed(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-003 — out-of-range cast refuses at collect today (P2-1)."""
    frame = spark.createDataFrame([(1,), (2,)], "x int").replace(1, 3000000000, subset=["x"])
    with pytest.raises(PySparkException, match="cast"):
        frame.collect()


def test_replace_struct_subset_disclosed(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-003 — a nested-field subset refuses (P2-4)."""
    frame = spark.createDataFrame([((1, "a"),)], ["s"])
    with pytest.raises(AnalysisException, match="cannot be resolved"):
        frame.replace(1, 9, ["s.x"])


def test_replace_duplicate_name_join_columns(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-004 — multi-name join output replaces by field (P2-3)."""
    left = spark.createDataFrame([(1, 10), (2, 20)], "k int, x int")
    right = spark.createDataFrame([(1, 10), (2, 30)], "k int, x int")

    replaced = left.join(right, "k").replace(10, 99)
    assert replaced.columns == ["k", "x", "x"]
    rows = replaced.collect()
    assert [(row[0], row[1], row[2]) for row in rows] == [(1, 99, 99), (2, 20, 30)]

    replaced = left.alias("df1").join(right.alias("df2"), "k").replace(10, 99)
    assert replaced.columns == ["k", "x", "x"]
    rows = replaced.collect()
    assert [(row[0], row[1], row[2]) for row in rows] == [(1, 99, 99), (2, 20, 30)]

    with pytest.raises(AnalysisException, match="ambiguous"):
        left.join(right, "k").replace(10, 99, subset=["x"])


def test_replace_projection_metadata_plain_frame(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-004 — a plain frame keeps the projected name and rebinds."""
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "x int, s string")
    replaced = frame.replace(1, 9, subset=["x"])
    assert replaced.columns == ["x", "s"]
    assert replaced.select("x").to_arrow().column("x").to_pylist() == [9, 2]
    assert replaced.select("s").to_arrow().column("s").to_pylist() == ["a", "b"]


def test_replace_projection_metadata_join_frame(spark: ReparkSession) -> None:
    """pins: replace-linear-1/C-004 — a join-origin frame keeps names and rebinds by name."""
    left = spark.createDataFrame([(1, 10), (2, 20)], "k int, x int")
    right = spark.createDataFrame([(1, "a"), (2, "b")], "k int, s string")
    joined = left.join(right, "k")
    assert joined.columns == ["k", "x", "s"]
    replaced = joined.replace(10, 99, subset=["x"])
    assert replaced.columns == ["k", "x", "s"]
    assert replaced.select("x").to_arrow().column("x").to_pylist() == [99, 20]
    assert replaced.select("s").to_arrow().column("s").to_pylist() == ["a", "b"]
    assert replaced.select("k").to_arrow().column("k").to_pylist() == [1, 2]
