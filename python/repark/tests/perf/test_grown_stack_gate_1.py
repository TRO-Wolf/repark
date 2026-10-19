"""GROWN-STACK-GATE-1 pins — the 34 gated sites answer on small stacks.

P1 is a must-not-change neighbour (it equals main). P2, P4 and P6 pin the
frame-door verdicts; P3 is the regression pin for the write-options door's
deep-view mark; P5 pins long SQL text through the write-options door. The
D-pins cover the five sized sites the verifier found unpinned: deep
write.text, partitioned text, localCheckpoint, transpose and the ML fit.
Each shape runs in its own isolated interpreter with its deep work on an
8 MiB thread, so a crash fails only that shape's test and names it.

pins: grown-stack-gate-1/C-002, C-003, C-004
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

_WORKER = """
import json
import sys
import tempfile
import threading
from pathlib import Path

from repark import ReparkSession
from repark import _native
from repark.spark import functions as F
from repark.spark.ml.feature import VectorAssembler
from repark.spark.ml.regression import LinearRegression

DEPTH = 2000
SHAPE = sys.argv[1]
out = {}
session = ReparkSession.builder.appName("grown-stack-gate-1").getOrCreate()
base = session.createDataFrame([(i,) for i in range(20)], "a INT")
wide = session.createDataFrame(
    [(i, f"w{i}", f"p{i % 2}", float(i)) for i in range(20)],
    "a INT, s STRING, p STRING, x DOUBLE",
)


def deep_chain(source):
    chained = source
    for i in range(DEPTH):
        chained = chained.filter(F.col("a") >= -1000000 - (i % 7))
    return chained


def shape_p1():
    deep_chain(base).createOrReplaceTempView("gsg1_deep_v")
    return session.sql("SELECT count(*) FROM gsg1_deep_v").collect()[0][0]


def shape_p2():
    return deep_chain(base).cache().count()


def shape_p3():
    with tempfile.TemporaryDirectory(prefix="gsg1-p3-") as tmp:
        deep_chain(base).write.mode("overwrite").csv(tmp + "/out")
        return session.read.csv(tmp + "/out").count()


def shape_p4():
    with tempfile.TemporaryDirectory(prefix="gsg1-p4-") as tmp:
        base.write.mode("overwrite").parquet(tmp + "/pq")
        back = session.read.parquet(tmp + "/pq")
        return sorted(deep_chain(back).inputFiles())


def shape_p5():
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
    return session.sql("SELECT count(*) FROM gsg1mem.ns.gsg1_p5_t").collect()[0][0]


def shape_p6():
    found = {}
    found["declare"] = base.declare_sorted("a").count()
    found["mat_temp"] = base.localCheckpoint(eager=True).count()
    found["mat_cache"] = base.cache().count()
    with tempfile.TemporaryDirectory(prefix="gsg1-p6t-") as tmp:
        words = session.createDataFrame([(f"w{i}",) for i in range(20)], "value STRING")
        words.write.mode("overwrite").text(tmp + "/txt")
        found["text"] = session.read.text(tmp + "/txt").count()
    with tempfile.TemporaryDirectory(prefix="gsg1-p6p-") as tmp:
        parted = session.createDataFrame(
            [(f"w{i}", f"p{i % 2}") for i in range(20)], "value STRING, p STRING"
        )
        parted.write.mode("overwrite").partitionBy("p").text(tmp + "/ptxt")
        entries = Path(tmp + "/ptxt").rglob("*")
        found["text_part"] = len([p for p in entries if p.is_file() and p.name != "_SUCCESS"])
    try:
        _native.session_write_path(
            base._session, base._inner, "s3://gsg1-nope/key", "zzz", "error", {}, []
        )
        found["write_path"] = "answered"
    except BaseException as raised:
        found["write_path"] = type(raised).__name__ + ": " + str(raised)[:150]
    rows = [(float(x), 2.0 + 3.0 * float(x)) for x in range(10)]
    frame = session.createDataFrame(rows, ["x", "label"])
    assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(frame)
    model = LinearRegression(featuresCol="features", labelCol="label").fit(assembled)
    found["ml"] = [model.intercept, list(model.coefficients)]
    with tempfile.TemporaryDirectory(prefix="gsg1-p6i-") as tmp:
        base.write.mode("overwrite").parquet(tmp + "/pq")
        found["input_files"] = sorted(session.read.parquet(tmp + "/pq").inputFiles())
    grid = session.createDataFrame([(2, 1, 5), (1, 3, 6)], "k int, a int, b int")
    turned = grid.transpose()
    found["transpose"] = [turned.count(), len(turned.columns)]
    return found


def shape_d_text():
    with tempfile.TemporaryDirectory(prefix="gsg1-dt-") as tmp:
        deep_chain(wide).select("s").write.mode("overwrite").text(tmp + "/t")
        return session.read.text(tmp + "/t").count()


def shape_d_text_part():
    with tempfile.TemporaryDirectory(prefix="gsg1-dp-") as tmp:
        deep_chain(wide).select("s", "p").write.mode("overwrite").partitionBy("p").text(tmp + "/t")
        return len(
            [p for p in Path(tmp + "/t").rglob("*.txt")]
            + [p for p in Path(tmp + "/t").rglob("part*")]
        )


def shape_d_ckpt():
    return deep_chain(wide).localCheckpoint(eager=True).count()


def shape_d_transpose():
    return deep_chain(wide).select("a", "x").transpose().count()


def shape_d_ml():
    labeled = deep_chain(wide).withColumn("label", F.col("x") * 3 + 2)
    assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(labeled)
    model = LinearRegression(featuresCol="features", labelCol="label").fit(assembled)
    return [model.intercept, list(model.coefficients)]


SHAPES = {
    "p1": shape_p1,
    "p2": shape_p2,
    "p3": shape_p3,
    "p4": shape_p4,
    "p5": shape_p5,
    "p6": shape_p6,
    "d_text": shape_d_text,
    "d_text_part": shape_d_text_part,
    "d_ckpt": shape_d_ckpt,
    "d_transpose": shape_d_transpose,
    "d_ml": shape_d_ml,
}


def work():
    try:
        out[SHAPE] = SHAPES[SHAPE]()
    except BaseException as raised:
        out[SHAPE] = type(raised).__name__ + ": " + str(raised)[:300]


threading.stack_size(8 * 1024 * 1024)
worker = threading.Thread(target=work)
worker.start()
worker.join()
print(json.dumps(out))
"""

_TIMEOUT_SECONDS = 600


def _run_shape(shape: str) -> dict[str, object]:
    """Drive one pin shape in its own isolated interpreter."""
    proc = subprocess.run(
        [sys.executable, "-c", _WORKER, shape],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"shape {shape} went red (rc={proc.returncode}): {proc.stderr[-2000:]}"
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


@pytest.fixture(scope="module")
def result_p1() -> dict[str, object]:
    """The P1 neighbour shape's answer."""
    return _run_shape("p1")


@pytest.fixture(scope="module")
def result_p2() -> dict[str, object]:
    """The P2 cache shape's answer."""
    return _run_shape("p2")


@pytest.fixture(scope="module")
def result_p3() -> dict[str, object]:
    """The P3 write.csv shape's answer."""
    return _run_shape("p3")


@pytest.fixture(scope="module")
def result_p4() -> dict[str, object]:
    """The P4 inputFiles shape's answer."""
    return _run_shape("p4")


@pytest.fixture(scope="module")
def result_p5() -> dict[str, object]:
    """The P5 long-OR INSERT shape's answer."""
    return _run_shape("p5")


@pytest.fixture(scope="module")
def result_p6() -> dict[str, object]:
    """The P6 shallow-shape group's answers."""
    return _run_shape("p6")


@pytest.fixture(scope="module")
def result_d_text() -> dict[str, object]:
    """The deep write.text shape's answer."""
    return _run_shape("d_text")


@pytest.fixture(scope="module")
def result_d_text_part() -> dict[str, object]:
    """The deep partitioned-text shape's answer."""
    return _run_shape("d_text_part")


@pytest.fixture(scope="module")
def result_d_ckpt() -> dict[str, object]:
    """The deep checkpoint shape's answer."""
    return _run_shape("d_ckpt")


@pytest.fixture(scope="module")
def result_d_transpose() -> dict[str, object]:
    """The deep transpose shape's answer."""
    return _run_shape("d_transpose")


@pytest.fixture(scope="module")
def result_d_ml() -> dict[str, object]:
    """The deep ML-fit shape's answer."""
    return _run_shape("d_ml")


def test_temp_view_count_matches_main(result_p1: dict[str, object]) -> None:
    """P1 neighbour: a 2,000-deep view answers its count as on main."""
    assert result_p1["p1"] == 20


def test_cached_deep_frame_counts(result_p2: dict[str, object]) -> None:
    """P2: a 2,000-deep frame answers through cache and count."""
    assert result_p2["p2"] == 20


def test_deep_frame_csv_roundtrip_counts(result_p3: dict[str, object]) -> None:
    """P3: a 2,000-deep frame answers through write.csv and read-back."""
    assert result_p3["p3"] == 21


def test_deep_frame_input_files_lists_one_parquet(
    result_p4: dict[str, object],
) -> None:
    """P4: a 2,000-deep frame answers inputFiles over its parquet read."""
    files = result_p4["p4"]
    assert isinstance(files, list) and len(files) == 1
    assert files[0].endswith(".parquet")


def test_long_or_insert_answers(result_p5: dict[str, object]) -> None:
    """P5: a 2,000-term OR INSERT answers through the write-options door."""
    assert result_p5["p5_len"] == 22951
    assert result_p5["p5"] == 40


def test_shallow_declare_sorted_counts(result_p6: dict[str, object]) -> None:
    """P6: a shallow frame answers through declare_sorted."""
    found = result_p6["p6"]
    assert isinstance(found, dict) and found["declare"] == 20


def test_shallow_checkpoint_counts(result_p6: dict[str, object]) -> None:
    """P6: a shallow frame answers through eager localCheckpoint."""
    found = result_p6["p6"]
    assert isinstance(found, dict) and found["mat_temp"] == 20


def test_shallow_cache_counts(result_p6: dict[str, object]) -> None:
    """P6: a shallow frame answers through cache and count."""
    found = result_p6["p6"]
    assert isinstance(found, dict) and found["mat_cache"] == 20


def test_shallow_text_roundtrip_counts(result_p6: dict[str, object]) -> None:
    """P6: a shallow frame answers through write.text and read-back."""
    found = result_p6["p6"]
    assert isinstance(found, dict) and found["text"] == 20


def test_shallow_partitioned_text_writes_parts(
    result_p6: dict[str, object],
) -> None:
    """P6: a shallow frame answers through partitioned write.text."""
    found = result_p6["p6"]
    assert isinstance(found, dict) and found["text_part"] == 2


def test_shallow_write_path_refusal_matches_main(
    result_p6: dict[str, object],
) -> None:
    """P6: an unknown write format refuses exactly as on main."""
    found = result_p6["p6"]
    assert isinstance(found, dict)
    assert (
        found["write_path"]
        == "AnalysisException: unknown path write format 'zzz' (expected parquet, csv or json)"
    )


def test_shallow_linear_regression_fits(result_p6: dict[str, object]) -> None:
    """P6: a shallow frame answers through the native ML fit."""
    found = result_p6["p6"]
    assert isinstance(found, dict)
    fitted = found["ml"]
    assert isinstance(fitted, list)
    assert abs(fitted[0] - 2.0) < 1e-6
    assert len(fitted[1]) == 1
    assert abs(fitted[1][0] - 3.0) < 1e-6


def test_shallow_input_files_lists_one_parquet(
    result_p6: dict[str, object],
) -> None:
    """P6: a shallow frame answers inputFiles over its parquet read."""
    files = result_p6["p6"]
    assert isinstance(files, dict)
    listed = files["input_files"]
    assert isinstance(listed, list) and len(listed) == 1
    assert listed[0].endswith(".parquet")


def test_shallow_transpose_counts(result_p6: dict[str, object]) -> None:
    """P6: a shallow frame answers through transpose."""
    found = result_p6["p6"]
    assert isinstance(found, dict) and found["transpose"] == [2, 3]


def test_deep_text_roundtrip_counts(result_d_text: dict[str, object]) -> None:
    """D-text: a 2,000-deep frame answers through write.text and read-back."""
    assert result_d_text["d_text"] == 20


def test_deep_partitioned_text_writes_parts(
    result_d_text_part: dict[str, object],
) -> None:
    """D-text-part: a 2,000-deep frame answers through partitioned text."""
    assert result_d_text_part["d_text_part"] == 4


def test_deep_checkpoint_counts(result_d_ckpt: dict[str, object]) -> None:
    """D-ckpt: a 2,000-deep frame answers through eager localCheckpoint."""
    assert result_d_ckpt["d_ckpt"] == 20


def test_deep_transpose_counts(result_d_transpose: dict[str, object]) -> None:
    """D-transpose: a 2,000-deep frame answers through transpose."""
    assert result_d_transpose["d_transpose"] == 1


def test_deep_linear_regression_fits(result_d_ml: dict[str, object]) -> None:
    """D-ml: a 2,000-deep frame answers through the native ML fit."""
    fitted = result_d_ml["d_ml"]
    assert isinstance(fitted, list)
    assert abs(fitted[0] - 2.0) < 1e-6
    assert len(fitted[1]) == 1
    assert abs(fitted[1][0] - 3.0) < 1e-6
