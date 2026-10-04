"""GROWN-STACK-GATE-1 pins — the 34 gated sites answer on small stacks.

P1 is a must-not-change neighbour (it equals main). P2, P4 and P6 pin the
frame-door verdicts; P3 is the regression pin for the write-options door's
deep-view mark; P5 pins long SQL text through the write-options door. Each
shape runs in an isolated interpreter with its deep work on an 8 MiB thread.

pins: grown-stack-gate-1/C-002, C-003, C-004
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

_WORKER = """
import json
import tempfile
import threading
from pathlib import Path

from repark import ReparkSession
from repark import _native
from repark.spark import functions as F
from repark.spark.ml.feature import VectorAssembler
from repark.spark.ml.regression import LinearRegression

DEPTH = 2000
out = {}
session = ReparkSession.builder.appName("grown-stack-gate-1").getOrCreate()
base = session.createDataFrame([(i,) for i in range(20)], "a INT")


def deep_chain(source):
    chained = source
    for i in range(DEPTH):
        chained = chained.filter(F.col("a") >= -1000000 - (i % 7))
    return chained


def run_on_small_stack(work):
    worker = threading.Thread(target=work)
    worker.start()
    worker.join()


def shape_p1():
    try:
        deep_chain(base).createOrReplaceTempView("gsg1_deep_v")
        out["p1"] = session.sql("SELECT count(*) FROM gsg1_deep_v").collect()[0][0]
    except BaseException as raised:
        out["p1"] = type(raised).__name__ + ": " + str(raised)[:150]
    print("p1-done", flush=True)


def shape_p2():
    try:
        out["p2"] = deep_chain(base).cache().count()
    except BaseException as raised:
        out["p2"] = type(raised).__name__ + ": " + str(raised)[:150]
    print("p2-done", flush=True)


def shape_p3():
    try:
        with tempfile.TemporaryDirectory(prefix="gsg1-p3-") as tmp:
            deep_chain(base).write.mode("overwrite").csv(tmp + "/out")
            print("write-done", flush=True)
            out["p3"] = session.read.csv(tmp + "/out").count()
    except BaseException as raised:
        out["p3"] = type(raised).__name__ + ": " + str(raised)[:150]
    print("p3-done", flush=True)


def shape_p4():
    try:
        with tempfile.TemporaryDirectory(prefix="gsg1-p4-") as tmp:
            base.write.mode("overwrite").parquet(tmp + "/pq")
            back = session.read.parquet(tmp + "/pq")
            out["p4"] = sorted(deep_chain(back).inputFiles())
    except BaseException as raised:
        out["p4"] = type(raised).__name__ + ": " + str(raised)[:150]
    print("p4-done", flush=True)


def shape_p5():
    try:
        tmp = tempfile.mkdtemp(prefix="gsg1-p5-")
        session.register_memory_catalog("gsg1mem", tmp)
        session.sql("CREATE NAMESPACE IF NOT EXISTS gsg1mem.ns")
        seed = session.createDataFrame([(i,) for i in range(20)], "a INT")
        seed.write.mode("overwrite").saveAsTable("gsg1mem.ns.gsg1_p5_t")
        seed.createOrReplaceTempView("gsg1_p5_src")
        predicate = " OR ".join(f"a = {i}" for i in range(DEPTH))
        query = f"INSERT INTO gsg1mem.ns.gsg1_p5_t SELECT * FROM gsg1_p5_src WHERE {predicate}"
        out["p5_len"] = len(query)
        _native.session_sql_with_write_options(seed._session, query, {})
        out["p5"] = session.sql("SELECT count(*) FROM gsg1mem.ns.gsg1_p5_t").collect()[0][0]
    except BaseException as raised:
        out["p5"] = type(raised).__name__ + ": " + str(raised)[:300]
    print("p5-done", flush=True)


def shape_p6():
    try:
        out["p6_declare"] = base.declare_sorted("a").count()
    except BaseException as raised:
        out["p6_declare"] = type(raised).__name__ + ": " + str(raised)[:150]
    try:
        out["p6_mat_temp"] = base.localCheckpoint(eager=True).count()
    except BaseException as raised:
        out["p6_mat_temp"] = type(raised).__name__ + ": " + str(raised)[:150]
    try:
        out["p6_mat_cache"] = base.cache().count()
    except BaseException as raised:
        out["p6_mat_cache"] = type(raised).__name__ + ": " + str(raised)[:150]
    try:
        with tempfile.TemporaryDirectory(prefix="gsg1-p6t-") as tmp:
            words = session.createDataFrame([(f"w{i}",) for i in range(20)], "value STRING")
            words.write.mode("overwrite").text(tmp + "/txt")
            out["p6_text"] = session.read.text(tmp + "/txt").count()
    except BaseException as raised:
        out["p6_text"] = type(raised).__name__ + ": " + str(raised)[:150]
    try:
        with tempfile.TemporaryDirectory(prefix="gsg1-p6p-") as tmp:
            parted = session.createDataFrame(
                [(f"w{i}", f"p{i % 2}") for i in range(20)], "value STRING, p STRING"
            )
            parted.write.mode("overwrite").partitionBy("p").text(tmp + "/ptxt")
            entries = Path(tmp + "/ptxt").rglob("*")
            parts = [p for p in entries if p.is_file() and p.name != "_SUCCESS"]
            out["p6_text_part"] = len(parts)
    except BaseException as raised:
        out["p6_text_part"] = type(raised).__name__ + ": " + str(raised)[:150]
    try:
        _native.session_write_path(
            base._session, base._inner, "s3://gsg1-nope/key", "zzz", "error", {}, []
        )
        out["p6_write_path"] = "answered"
    except BaseException as raised:
        out["p6_write_path"] = type(raised).__name__ + ": " + str(raised)[:150]
    try:
        rows = [(float(x), 2.0 + 3.0 * float(x)) for x in range(10)]
        frame = session.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(frame)
        model = LinearRegression(featuresCol="features", labelCol="label").fit(assembled)
        out["p6_ml"] = [model.intercept, list(model.coefficients)]
    except BaseException as raised:
        out["p6_ml"] = type(raised).__name__ + ": " + str(raised)[:150]
    try:
        with tempfile.TemporaryDirectory(prefix="gsg1-p6i-") as tmp:
            base.write.mode("overwrite").parquet(tmp + "/pq")
            out["p6_input_files"] = sorted(session.read.parquet(tmp + "/pq").inputFiles())
    except BaseException as raised:
        out["p6_input_files"] = type(raised).__name__ + ": " + str(raised)[:150]
    try:
        grid = session.createDataFrame([(2, 1, 5), (1, 3, 6)], "k int, a int, b int")
        turned = grid.transpose()
        out["p6_transpose"] = [turned.count(), len(turned.columns)]
    except BaseException as raised:
        out["p6_transpose"] = type(raised).__name__ + ": " + str(raised)[:150]
    print("p6-done", flush=True)


threading.stack_size(8 * 1024 * 1024)
for shape in (shape_p1, shape_p2, shape_p3, shape_p4, shape_p5, shape_p6):
    run_on_small_stack(shape)
print(json.dumps(out))
"""

_TIMEOUT_SECONDS = 900


@pytest.fixture(scope="module")
def worker_results() -> dict[str, object]:
    """Drive the gated-site battery once in an isolated interpreter."""
    proc = subprocess.run(
        [sys.executable, "-c", _WORKER],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"the gated-site worker must survive every shape (rc={proc.returncode}): "
        f"{proc.stderr[-2000:]}"
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_temp_view_count_matches_main(worker_results: dict[str, object]) -> None:
    """P1 neighbour: a 2,000-deep view answers its count as on main."""
    assert worker_results["p1"] == 20


def test_cached_deep_frame_counts(worker_results: dict[str, object]) -> None:
    """P2: a 2,000-deep frame answers through cache and count."""
    assert worker_results["p2"] == 20


def test_deep_frame_csv_roundtrip_counts(worker_results: dict[str, object]) -> None:
    """P3: a 2,000-deep frame answers through write.csv and read-back."""
    assert worker_results["p3"] == 21


def test_deep_frame_input_files_lists_one_parquet(
    worker_results: dict[str, object],
) -> None:
    """P4: a 2,000-deep frame answers inputFiles over its parquet read."""
    files = worker_results["p4"]
    assert isinstance(files, list) and len(files) == 1
    assert files[0].endswith(".parquet")


def test_long_or_insert_answers(worker_results: dict[str, object]) -> None:
    """P5: a 2,000-term OR INSERT answers through the write-options door."""
    assert worker_results["p5_len"] == 22951
    assert worker_results["p5"] == 40


def test_shallow_declare_sorted_counts(worker_results: dict[str, object]) -> None:
    """P6: a shallow frame answers through declare_sorted."""
    assert worker_results["p6_declare"] == 20


def test_shallow_checkpoint_counts(worker_results: dict[str, object]) -> None:
    """P6: a shallow frame answers through eager localCheckpoint."""
    assert worker_results["p6_mat_temp"] == 20


def test_shallow_cache_counts(worker_results: dict[str, object]) -> None:
    """P6: a shallow frame answers through cache and count."""
    assert worker_results["p6_mat_cache"] == 20


def test_shallow_text_roundtrip_counts(worker_results: dict[str, object]) -> None:
    """P6: a shallow frame answers through write.text and read-back."""
    assert worker_results["p6_text"] == 20


def test_shallow_partitioned_text_writes_parts(
    worker_results: dict[str, object],
) -> None:
    """P6: a shallow frame answers through partitioned write.text."""
    assert worker_results["p6_text_part"] == 2


def test_shallow_write_path_refusal_matches_main(
    worker_results: dict[str, object],
) -> None:
    """P6: an unknown write format refuses exactly as on main."""
    assert (
        worker_results["p6_write_path"]
        == "AnalysisException: unknown path write format 'zzz' (expected parquet, csv or json)"
    )


def test_shallow_linear_regression_fits(worker_results: dict[str, object]) -> None:
    """P6: a shallow frame answers through the native ML fit."""
    fitted = worker_results["p6_ml"]
    assert isinstance(fitted, list)
    assert abs(fitted[0] - 2.0) < 1e-6
    assert len(fitted[1]) == 1
    assert abs(fitted[1][0] - 3.0) < 1e-6


def test_shallow_input_files_lists_one_parquet(
    worker_results: dict[str, object],
) -> None:
    """P6: a shallow frame answers inputFiles over its parquet read."""
    files = worker_results["p6_input_files"]
    assert isinstance(files, list) and len(files) == 1
    assert files[0].endswith(".parquet")


def test_shallow_transpose_counts(worker_results: dict[str, object]) -> None:
    """P6: a shallow frame answers through transpose."""
    assert worker_results["p6_transpose"] == [2, 3]
