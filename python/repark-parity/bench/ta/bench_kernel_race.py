#!/usr/bin/env python3
"""Kernel race, one symbol, null-free, already sorted, Arrow only.

Compares RePark DataFrame ``evaluate_all`` (``over_columns`` + ``to_arrow``) against
``polars_talib`` (C TA-Lib 0.4.0) and, where reachable, raw ``repark_ta``. Raw kernels
are a Rust crate with no Python import; the script records that SKIP and cites P-1
criterion (#132).

Usage::

    # record env already has polars_talib 0.1.5 — prepend its site-packages, or:
    # uv run --with polars-talib==0.1.5 python …/bench_kernel_race.py
    python python/repark-parity/bench/ta/bench_kernel_race.py [--quick]
"""

from __future__ import annotations

import argparse
from typing import Any

import harness
from target_partition_contract import emit_target_partition_fields


def _repark_kernel_work(
    spark: object,
    seed: object,
    kernel: str,
) -> object:
    """One fused ``over_columns`` plan for ``kernel``; materialize via Arrow."""
    from repark import Window
    from repark.spark import ta

    window = Window.orderBy("ts")
    if kernel == "sma":
        columns: dict[str, object] = {"out": ta.sma("close", timeperiod=20)}
    elif kernel == "ema":
        columns = {"out": ta.ema("close", timeperiod=21)}
    elif kernel == "rsi":
        columns = {"out": ta.rsi("close", timeperiod=14)}
    elif kernel == "bbands":
        columns = {
            "upper": ta.bbands_upper("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0),
            "middle": ta.bbands_middle("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0),
            "lower": ta.bbands_lower("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0),
        }
    else:
        raise ValueError(f"unknown kernel {kernel!r}")
    framed = seed.withColumns(ta.over_columns(window, columns))  # type: ignore[attr-defined]
    return framed.to_arrow()  # type: ignore[attr-defined]


def _polars_kernel_work(frame: Any, plta: Any, kernel: str) -> object:
    """Eager Polars + ``polars_talib``; Arrow export for an apples-to-apples sink."""
    import polars as pl

    close = pl.col("close")
    if kernel == "sma":
        exprs = [plta.sma(close, timeperiod=20).alias("out")]
    elif kernel == "ema":
        exprs = [plta.ema(close, timeperiod=21).alias("out")]
    elif kernel == "rsi":
        exprs = [plta.rsi(close, timeperiod=14).alias("out")]
    elif kernel == "bbands":
        exprs = [plta.bbands(close, timeperiod=20, nbdevup=2.0, nbdevdn=2.0).alias("out")]
    else:
        raise ValueError(f"unknown kernel {kernel!r}")
    return frame.with_columns(exprs).to_arrow()


def main() -> None:
    """Run the kernel race and print ``TA_PIPELINE`` lines."""
    parser = argparse.ArgumentParser(description=__doc__)
    harness.add_timing_args(parser)
    parser.add_argument("--n-rows", type=int, default=None)
    parser.add_argument(
        "--impl",
        choices=("all", "repark", "polars", "raw"),
        default="all",
        help="which legs to run (default all)",
    )
    args = parser.parse_args()
    warmup, iterations = harness.resolve_timing(args)
    n_rows = harness.resolve_n_rows(args, full=harness.DEFAULT_N_ROWS, quick=harness.QUICK_N_ROWS)
    harness.emit_hardware(script="bench_kernel_race")

    kernels = ("sma", "ema", "rsi", "bbands")
    seed = harness.one_symbol_polars(n_rows)

    if args.impl in ("all", "raw"):
        reachable, reason = harness.raw_repark_ta_status()
        for kernel in kernels:
            harness.emit_line(
                "bench_kernel_race",
                impl="repark_ta_raw",
                kernel=kernel,
                n=n_rows,
                reachable=reachable,
                reason=reason,
            )

    if args.impl in ("all", "polars"):
        plta = harness.try_polars_talib()
        if plta is None:
            harness.emit_line(
                "bench_kernel_race",
                impl="polars_talib",
                n=n_rows,
                reachable=False,
                reason="polars_talib_not_importable_use_record_env",
            )
        else:
            for kernel in kernels:
                median_s, table = harness.time_median(
                    lambda k=kernel: _polars_kernel_work(seed, plta, k),
                    warmup=warmup,
                    iterations=iterations,
                )
                harness.emit_line(
                    "bench_kernel_race",
                    impl="polars_talib",
                    kernel=kernel,
                    n=n_rows,
                    warmup=warmup,
                    iterations=iterations,
                    median_s=median_s,
                    ns_per_row=harness.ns_per_row(median_s, n_rows),
                    rows_out=harness.sink_arrow(table),
                )

    if args.impl in ("all", "repark"):
        spark = harness.make_session(app_name="bench-ta-kernel-race")
        try:
            repark_seed = harness.seed_repark_frame(spark, seed)
            for kernel in kernels:
                plan_frame = _repark_kernel_work(spark, repark_seed, kernel)
                # Plan shape from a fresh lazy rebuild; the timed path rebuilds too.
                from repark import Window
                from repark.spark import ta

                window = Window.orderBy("ts")
                if kernel == "bbands":
                    columns: dict[str, object] = {
                        "upper": ta.bbands_upper("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0),
                        "middle": ta.bbands_middle(
                            "close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0
                        ),
                        "lower": ta.bbands_lower("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0),
                    }
                elif kernel == "sma":
                    columns = {"out": ta.sma("close", timeperiod=20)}
                elif kernel == "ema":
                    columns = {"out": ta.ema("close", timeperiod=21)}
                else:
                    columns = {"out": ta.rsi("close", timeperiod=14)}
                planned = repark_seed.withColumns(ta.over_columns(window, columns))
                plan = harness.physical_plan_text(planned)
                median_s, table = harness.time_median(
                    lambda k=kernel, framed=repark_seed: _repark_kernel_work(spark, framed, k),
                    warmup=warmup,
                    iterations=iterations,
                )
                harness.emit_line(
                    "bench_kernel_race",
                    impl="repark_engine",
                    kernel=kernel,
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
                del plan_frame
        finally:
            harness.stop_session(spark)


if __name__ == "__main__":
    main()
