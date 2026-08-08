#!/usr/bin/env python3
"""N4 R-PERF-MEASURE — phase-decomposed timing of the "+1s coalesce" chain (LOCAL only).

Synthetic ~N rows x progressive prefixes of:
  TA window UDFs (if registered) → withColumns(coalesce/when) → sort → show

Hypothesis: each action re-executes the full lazy plan (no .cache()), so a second
withColumns appears to add ~full-chain cost rather than incremental work.

Usage (from repo root, wheel installed)::

    python python/repark-parity/bench/bench_coalesce_chain.py [--rows 1000000]
"""

from __future__ import annotations

import argparse
import statistics
import tempfile
import time
from collections.abc import Callable


def _median(samples: list[float]) -> float:
    return float(statistics.median(samples))


def _time(fn: Callable[[], object], repeats: int = 3) -> tuple[float, list[float]]:
    samples: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return _median(samples), samples


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rows", type=int, default=200_000, help="row count (default 200k for CI-ish)"
    )
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    from repark import ReparkSession
    from repark import functions as F  # noqa: N812 — PySpark convention

    rows = args.rows
    with tempfile.TemporaryDirectory():
        spark = ReparkSession.builder.appName("bench-coalesce").getOrCreate()
        # Build a synthetic frame without AWS.
        data = [(index, float(index % 100), float((index * 3) % 50)) for index in range(rows)]
        base = spark.createDataFrame(data, schema=["id", "close", "volume"])

        # Progressive prefixes — measure wall of each terminal action.
        results: list[tuple[str, float, list[float]]] = []

        def collect_base() -> None:
            base.limit(1).collect()

        median, samples = _time(collect_base, args.repeats)
        results.append(("base_limit1_collect", median, samples))

        # withColumns coalesce/when chain (user-shaped, no TA if UDF slow)
        c1 = base.withColumn(
            "c1",
            F.when(F.col("close") > 50, F.col("close")).otherwise(F.lit(0.0)),
        )
        c2 = c1.withColumn(
            "c2",
            F.coalesce(F.col("c1"), F.col("volume"), F.lit(0.0)),
        )
        sorted_frame = c2.sort(F.col("id"))

        def action_c1() -> None:
            c1.limit(1).collect()

        def action_c2() -> None:
            c2.limit(1).collect()

        def action_sort() -> None:
            sorted_frame.limit(1).collect()

        def action_show() -> None:
            # show prints; still exercises the plan
            import io
            from contextlib import redirect_stdout

            with redirect_stdout(io.StringIO()):
                sorted_frame.show(5)

        for label, fn in [
            ("after_withColumn_c1_limit1", action_c1),
            ("after_withColumn_c2_limit1", action_c2),
            ("after_sort_limit1", action_sort),
            ("show_5", action_show),
        ]:
            median, samples = _time(fn, args.repeats)
            results.append((label, median, samples))

        # Re-run c2 after c1 to detect full recompute (if c2 ≈ c1+c2 work, lazy recompute).
        print(f"rows={rows} repeats={args.repeats}")
        print(f"{'phase':40s}  median_s   samples")
        for label, median, samples in results:
            print(f"{label:40s}  {median:8.4f}   {[round(s, 4) for s in samples]}")

        # Ratio: c2 / c1 — if ~2x, second withColumn re-executes prior work.
        by_label = {label: median for label, median, _ in results}
        if by_label.get("after_withColumn_c1_limit1", 0) > 0:
            ratio = by_label["after_withColumn_c2_limit1"] / by_label["after_withColumn_c1_limit1"]
            print(f"ratio c2/c1 limit1 collect = {ratio:.2f}  (>1.5 suggests full-chain recompute)")
        spark.stop()


if __name__ == "__main__":
    main()
