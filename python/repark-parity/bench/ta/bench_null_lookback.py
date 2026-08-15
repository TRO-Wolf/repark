#!/usr/bin/env python3
"""§8.5 — ``null_lookback=True`` x 10 columns vs the default NaN-prefix path.

Records ``WindowAggExec`` count and window-fn tokens (``ta_*`` + ``row_number``)
alongside wall time. The opt-in rewrite is a Python ``when(row_number() > L,
ta.over(w))`` per column — extra window work, same Arrow sink.

Usage::

    python python/repark-parity/bench/ta/bench_null_lookback.py [--quick]
"""

from __future__ import annotations

import argparse

import harness


def _work(seed: object, *, null_lookback: bool) -> object:
    """Ten TA columns through ``with_indicators``; Arrow sink."""
    from repark.spark import ta

    return ta.with_indicators(
        seed,
        partition="symbol",
        order="ts",
        columns=harness.ten_lookback_columns(ta),
        null_lookback=null_lookback,
    ).to_arrow()


def main() -> None:
    """Run the §8.5 null-lookback pair and print ``TA_PIPELINE`` lines."""
    parser = argparse.ArgumentParser(description=__doc__)
    harness.add_timing_args(parser)
    parser.add_argument("--n-rows", type=int, default=None)
    args = parser.parse_args()
    warmup, iterations = harness.resolve_timing(args)
    n_rows = harness.resolve_n_rows(args, full=harness.DEFAULT_N_ROWS, quick=harness.QUICK_N_ROWS)
    harness.emit_hardware(script="bench_null_lookback")
    seed = harness.one_symbol_polars(n_rows)
    spark = harness.make_session(
        app_name="bench-ta-null-lookback",
        target_partitions=1,
    )
    try:
        repark_seed = harness.seed_repark_frame(spark, seed)
        from repark.spark import ta

        for null_lookback, shape in ((False, "default_nan_prefix"), (True, "null_lookback")):
            planned = ta.with_indicators(
                repark_seed,
                partition="symbol",
                order="ts",
                columns=harness.ten_lookback_columns(ta),
                null_lookback=null_lookback,
            )
            plan = harness.physical_plan_text(planned)
            median_s, table = harness.time_median(
                lambda framed=repark_seed, flag=null_lookback: _work(framed, null_lookback=flag),
                warmup=warmup,
                iterations=iterations,
            )
            harness.emit_line(
                "bench_null_lookback",
                impl="repark_engine",
                shape=shape,
                n_columns=10,
                n=n_rows,
                target_partitions=1,
                warmup=warmup,
                iterations=iterations,
                median_s=median_s,
                ns_per_row=harness.ns_per_row(median_s, n_rows),
                window_agg_exec=harness.count_window_agg_exec(plan),
                window_fn_tokens=harness.count_window_fn_tokens(plan),
                rows_out=harness.sink_arrow(table),
            )
    finally:
        harness.stop_session(spark)


if __name__ == "__main__":
    main()
