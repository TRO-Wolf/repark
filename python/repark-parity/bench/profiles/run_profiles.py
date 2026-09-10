"""PROFILES-1 sweep runner: build the bed, time knob cells, write the CSV."""

from __future__ import annotations

import argparse
import functools
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from profiles import datasets, harness, queries


def open_session(knob: str, value: str) -> object:
    """Open one session with a single knob set, or the baseline when default."""
    from repark.spark import SparkSession

    builder = SparkSession.builder.appName("profiles-bed")
    if value != harness.BASELINE_VALUE:
        builder = builder.config(knob, value)
    return builder.getOrCreate()


def run_read_cell(spark: object, query: str, dataset: str, repeats: int) -> list[float]:
    """Time one read query on one dataset."""
    func, _ = queries.READ_QUERIES[query]
    return harness.measure(functools.partial(func, spark, dataset), repeats)


def run_write_cell(
    spark: object,
    query: str,
    warehouse: Path,
    files: int,
    rows_per_file: int,
    scale: queries.WriteScale,
    repeats: int,
) -> list[float]:
    """Rebuild the bed table per repetition, then time one write shape."""
    func, _ = queries.WRITE_QUERIES[query]
    seconds: list[float] = []
    for _ in range(repeats):
        harness.require_quiet_box()
        table = datasets.build_iceberg_table(spark, files, rows_per_file)
        start = time.perf_counter()
        func(spark, table, scale)
        seconds.append(time.perf_counter() - start)
    return seconds


def report_cell(
    out: Path, dataset: str, query: str, knob: str, value: str, seconds: list[float]
) -> None:
    """Write one cell's rows and print its median."""
    harness.write_cell_rows(out, dataset, query, knob, value, seconds)
    median = harness.median_seconds(seconds)
    line = f"{dataset:8s} {query:19s} {knob}={value} n={len(seconds)} median={median:.3f}s"
    print(line, flush=True)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse the runner command line."""
    parser = argparse.ArgumentParser(description="PROFILES-1 bed runner (timing harness)")
    parser.add_argument("--scratch", type=Path, required=True, help="writable scratch root")
    parser.add_argument("--out", type=Path, default=None, help="CSV path (default cells.csv)")
    parser.add_argument("--knob", type=str, default="", help="knob to sweep (empty is baseline)")
    parser.add_argument(
        "--values", type=str, default=harness.BASELINE_VALUE, help="comma-separated values"
    )
    parser.add_argument(
        "--repeats", type=int, default=harness.DEFAULT_REPEATS, help="repetitions (sweeps use 3)"
    )
    parser.add_argument("--smoke", action="store_true", help="tiny one-repetition proof run")
    parser.add_argument("--futures", type=Path, default=None, help="owner futures parquet override")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Build the bed, run every cell, write the CSV."""
    args = parse_args(argv)
    values = [item.strip() for item in args.values.split(",") if item.strip()]
    if args.smoke:
        repeats = 1
        smoke = harness.smoke_config()
        tpch_sf = smoke.tpch_sf
        iceberg_files = smoke.iceberg_files
        rows_per_file = datasets.SMOKE_ROWS_PER_FILE
        scale = queries.WriteScale(
            append_files=smoke.append_files, merge_fraction=queries.MERGE_UPDATE_FRACTION
        )
        use_tiny_futures = True
    else:
        if args.repeats != harness.DEFAULT_REPEATS:
            print(f"refusing: sweeps run {harness.DEFAULT_REPEATS} repetitions, got {args.repeats}")
            return 2
        repeats = args.repeats
        tpch_sf = datasets.FULL_TPCH_SF
        iceberg_files = datasets.FULL_ICEBERG_FILES
        rows_per_file = datasets.FULL_ROWS_PER_FILE
        scale = queries.FULL_WRITE_SCALE
        use_tiny_futures = False
    if not args.knob and values != [harness.BASELINE_VALUE]:
        print("refusing: values without a knob are meaningless; pass one --knob")
        return 2
    knob = args.knob if args.knob else "(baseline)"
    scratch: Path = args.scratch
    scratch.mkdir(parents=True, exist_ok=True)
    out: Path = args.out if args.out is not None else scratch / "cells.csv"
    warehouse = scratch / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    if args.futures is not None:
        futures_path = args.futures
    else:
        futures_path = datasets.futures_file(scratch, smoke=use_tiny_futures)
    tpch_path = datasets.tpch_dir(scratch, tpch_sf)
    for value in values:
        spark = open_session(args.knob, value)
        try:
            datasets.ensure_catalog(spark, warehouse)
            datasets.register_views(spark, futures_path, tpch_path)
            for query, (_, query_datasets) in queries.READ_QUERIES.items():
                for dataset in query_datasets:
                    seconds = run_read_cell(spark, query, dataset, repeats)
                    report_cell(out, dataset, query, knob, value, seconds)
            for query in queries.WRITE_QUERIES:
                seconds = run_write_cell(
                    spark, query, warehouse, iceberg_files, rows_per_file, scale, repeats
                )
                report_cell(out, "iceberg", query, knob, value, seconds)
        finally:
            spark.stop()  # type: ignore[attr-defined, union-attr]
    files = datasets.count_iceberg_files(warehouse)
    print(f"cells={out} iceberg_parquet_files={files}")
    print("SMOKE OK" if args.smoke else "SWEEP DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
