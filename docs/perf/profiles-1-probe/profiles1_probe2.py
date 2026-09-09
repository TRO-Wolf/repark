from __future__ import annotations

import contextlib
import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from repark.spark import SparkSession

WORK = Path(__file__).resolve().parent / "data2"
ROWS = 160_000
FILES = 8


def build_source_dir() -> None:
    directory = WORK / "scan_src"
    directory.mkdir(parents=True, exist_ok=True)
    for index in range(FILES):
        k = pa.array(range(index * ROWS, (index + 1) * ROWS), type=pa.int64())
        v = pa.array(
            [float(i) * 1.5 for i in range(index * ROWS, (index + 1) * ROWS)], type=pa.float64()
        )
        s = pa.array([f"s-{i}" for i in range(index * ROWS, (index + 1) * ROWS)], type=pa.string())
        pq.write_table(pa.table({"k": k, "v": v, "s": s}), directory / f"part_{index}.parquet")
    small = WORK / "small_src"
    small.mkdir(parents=True, exist_ok=True)
    k = pa.array(range(2_000), type=pa.int64())
    v = pa.array([float(i) for i in range(2_000)], type=pa.float64())
    pq.write_table(
        pa.table({"k": k, "v": v}),
        small / "tiny.parquet",
        row_group_size=100,
    )


def capture_explain(frame: object) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain(mode="simple")
    return buffer.getvalue()


def scan_dir_plan(spark: SparkSession, predicate: str) -> str:
    frame = spark.read.parquet(str(WORK / "scan_src"))
    return capture_explain(frame.filter(predicate))


def small_repartition_plan(spark: SparkSession) -> str:
    frame = spark.read.parquet(str(WORK / "small_src"))
    return capture_explain(frame.filter("v > 100").select("v").repartition(32))


def open_session(configs: list[tuple[str, str]]) -> SparkSession:
    builder = SparkSession.builder
    for key, value in configs:
        builder = builder.config(key, value)
    return builder.getOrCreate()


def main() -> None:
    WORK.mkdir(parents=True, exist_ok=True)
    build_source_dir()

    results: dict[str, object] = {}
    cases: list[tuple[str, list[tuple[str, str]]]] = [
        ("baseline", []),
        ("pushdown_filters_false", [("datafusion.execution.parquet.pushdown_filters", "false")]),
        (
            "repartition_file_scans_false",
            [("datafusion.optimizer.repartition_file_scans", "false")],
        ),
        ("coalesce_batches_false", [("datafusion.execution.coalesce_batches", "false")]),
        (
            "coalesce_batches_false_smallrp",
            [
                ("datafusion.execution.coalesce_batches", "false"),
                ("datafusion.execution.target_partitions", "1"),
            ],
        ),
    ]
    for name, configs in cases:
        session = open_session(configs)
        results[name] = {
            "numeric_filter": scan_dir_plan(session, "v > 1000000"),
            "string_filter": scan_dir_plan(session, "s LIKE '%7-1%'"),
            "small_repartition": small_repartition_plan(session),
        }
        session.stop()

    for name, plans in results.items():
        print(f"===== {name}")
        for label, plan in plans.items():
            print(f"--- {label}")
            print(plan)


if __name__ == "__main__":
    main()
