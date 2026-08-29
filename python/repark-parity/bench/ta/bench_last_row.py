#!/usr/bin/env python3
"""``with_indicators(last_row=True)`` vs full-table collect.

Times Arrow (``to_arrow``) and Spark-Row (``collect``) sinks for the last bar per symbol
and the full TA window; serving usually wants ``N_symbols`` rows, not the whole history.

Usage::

    python python/repark-parity/bench/ta/bench_last_row.py [--quick]
"""

from __future__ import annotations

import argparse

import harness
from target_partition_contract import emit_target_partition_fields


def _indicators(seed: object, *, last_row: bool) -> object:
    """EMA+RSI via the serving helper."""
    from repark.spark import ta

    return ta.with_indicators(
        seed,
        partition="symbol",
        order="ts",
        columns={
            "ema21": ta.ema("close", timeperiod=21),
            "rsi14": ta.rsi("close", timeperiod=14),
        },
        last_row=last_row,
    )


def main() -> None:
    """Run the last-row vs full-table matrix and print ``TA_PIPELINE`` lines."""
    parser = argparse.ArgumentParser(description=__doc__)
    harness.add_timing_args(parser)
    parser.add_argument("--n-symbols", type=int, default=None)
    parser.add_argument("--bars", type=int, default=None)
    parser.add_argument(
        "--skip-row-collect",
        action="store_true",
        help="Arrow only (skip Spark Row collect; documented cut)",
    )
    args = parser.parse_args()
    warmup, iterations = harness.resolve_timing(args)
    n_symbols = args.n_symbols
    if n_symbols is None:
        n_symbols = harness.QUICK_N_SYMBOLS if args.quick else harness.DEFAULT_N_SYMBOLS
    bars = args.bars
    if bars is None:
        bars = harness.QUICK_BARS_PER_SYMBOL if args.quick else harness.DEFAULT_BARS_PER_SYMBOL
    n_rows = n_symbols * bars
    harness.emit_hardware(script="bench_last_row")
    seed = harness.many_symbols_polars(n_symbols, bars)
    spark = harness.make_session(app_name="bench-ta-last-row")
    try:
        repark_seed = harness.seed_repark_frame(spark, seed)
        from repark.spark import ta

        for last_row, shape in ((False, "full_table"), (True, "last_row")):
            planned = ta.with_indicators(
                repark_seed,
                partition="symbol",
                order="ts",
                columns={
                    "ema21": ta.ema("close", timeperiod=21),
                    "rsi14": ta.rsi("close", timeperiod=14),
                },
                last_row=last_row,
            )
            plan = harness.physical_plan_text(planned)

            def arrow_work(flag: bool = last_row) -> object:
                return _indicators(repark_seed, last_row=flag).to_arrow()

            median_s, table = harness.time_median(arrow_work, warmup=warmup, iterations=iterations)
            harness.emit_line(
                "bench_last_row",
                impl="repark_engine",
                shape=shape,
                sink="to_arrow",
                n_symbols=n_symbols,
                bars=bars,
                n=n_rows,
                **emit_target_partition_fields(isolation=False),
                warmup=warmup,
                iterations=iterations,
                median_s=median_s,
                ns_per_row=harness.ns_per_row(median_s, n_rows),
                window_agg_exec=harness.count_window_agg_exec(plan),
                window_fn_tokens=harness.count_window_fn_tokens(plan),
                rows_out=harness.sink_arrow(table),
            )
            if args.skip_row_collect:
                harness.emit_line(
                    "bench_last_row",
                    impl="repark_engine",
                    shape=shape,
                    sink="collect",
                    n=n_rows,
                    skipped=True,
                    reason="skip_row_collect",
                )
                continue

            def row_work(flag: bool = last_row) -> object:
                return _indicators(repark_seed, last_row=flag).collect()

            median_s, rows = harness.time_median(row_work, warmup=warmup, iterations=iterations)
            harness.emit_line(
                "bench_last_row",
                impl="repark_engine",
                shape=shape,
                sink="collect",
                n_symbols=n_symbols,
                bars=bars,
                n=n_rows,
                **emit_target_partition_fields(isolation=False),
                warmup=warmup,
                iterations=iterations,
                median_s=median_s,
                ns_per_row=harness.ns_per_row(median_s, n_rows),
                window_agg_exec=harness.count_window_agg_exec(plan),
                rows_out=harness.sink_rows(rows),
            )
    finally:
        harness.stop_session(spark)


if __name__ == "__main__":
    main()
