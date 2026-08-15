#!/usr/bin/env python3
"""§8.4 — ``batch_size`` sweep, one symbol, ``target_partitions=1``.

``evaluate_all`` concatenates every batch in the partition before the kernel.
Default DataFusion ``batch_size`` is 8192; a 2M-bar symbol is then ~244
batches. This sweep records wall time vs that knob.

Usage::

    python python/repark-parity/bench/ta/bench_batch_size.py [--quick]
"""

from __future__ import annotations

import argparse

import harness

FULL_SWEEP = (1_024, 8_192, 65_536, 262_144, 2_097_152)
QUICK_SWEEP = (1_024, 8_192, 65_536)


def _ema_work(seed: object) -> object:
    """Single-symbol EMA via fused ``over_columns``; Arrow sink."""
    from repark import Window
    from repark.spark import ta

    window = Window.orderBy("ts")
    columns = {"ema21": ta.ema("close", timeperiod=21)}
    return seed.withColumns(ta.over_columns(window, columns)).to_arrow()  # type: ignore[attr-defined]


def main() -> None:
    """Run the §8.4 batch-size sweep and print ``TA_PIPELINE`` lines."""
    parser = argparse.ArgumentParser(description=__doc__)
    harness.add_timing_args(parser)
    parser.add_argument("--n-rows", type=int, default=None)
    parser.add_argument(
        "--batch-sizes",
        type=str,
        default=None,
        help="comma-separated batch_size values (default: charter sweep)",
    )
    args = parser.parse_args()
    warmup, iterations = harness.resolve_timing(args)
    n_rows = harness.resolve_n_rows(
        args, full=harness.DEFAULT_BATCH_SWEEP_ROWS, quick=harness.QUICK_BATCH_SWEEP_ROWS
    )
    if args.batch_sizes:
        sweep = tuple(int(part) for part in args.batch_sizes.split(",") if part)
    else:
        sweep = QUICK_SWEEP if args.quick else FULL_SWEEP
    harness.emit_hardware(script="bench_batch_size")
    seed = harness.one_symbol_polars(n_rows)

    for batch_size in sweep:
        spark = harness.make_session(
            app_name=f"bench-ta-batch-size-{batch_size}",
            target_partitions=1,
            batch_size=batch_size,
        )
        try:
            repark_seed = harness.seed_repark_frame(spark, seed)
            from repark import Window
            from repark.spark import ta

            planned = repark_seed.withColumns(
                ta.over_columns(Window.orderBy("ts"), {"ema21": ta.ema("close", timeperiod=21)})
            )
            plan = harness.physical_plan_text(planned)
            median_s, table = harness.time_median(
                lambda framed=repark_seed: _ema_work(framed),
                warmup=warmup,
                iterations=iterations,
            )
            harness.emit_line(
                "bench_batch_size",
                impl="repark_engine",
                kernel="ema21",
                n=n_rows,
                target_partitions=1,
                batch_size=batch_size,
                warmup=warmup,
                iterations=iterations,
                median_s=median_s,
                ns_per_row=harness.ns_per_row(median_s, n_rows),
                window_agg_exec=harness.count_window_agg_exec(plan),
                rows_out=harness.sink_arrow(table),
            )
        finally:
            harness.stop_session(spark)


if __name__ == "__main__":
    main()
