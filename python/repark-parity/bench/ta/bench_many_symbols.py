#!/usr/bin/env python3
"""§8.2 — many symbols x M bars: partitionBy vs Polars ``.over("symbol")``.

Rows:
  * RePark ``partitionBy("symbol").orderBy("ts")`` at
    ``target_partitions ∈ {1, cores}``
  * the **no-partitionBy** cliff (``Window.orderBy("ts")`` only)
  * Polars ``plta.ema(...).over("symbol")`` vs the same expr with no ``.over``

Usage::

    python python/repark-parity/bench/ta/bench_many_symbols.py [--quick]
"""

from __future__ import annotations

import argparse
from typing import Any

import harness


def _repark_work(seed: object, *, partition: bool) -> object:
    """EMA+RSI fused window; Arrow sink."""
    from repark import Window
    from repark.spark import ta

    window = Window.partitionBy("symbol").orderBy("ts") if partition else Window.orderBy("ts")
    columns = {
        "ema21": ta.ema("close", timeperiod=21),
        "rsi14": ta.rsi("close", timeperiod=14),
    }
    return seed.withColumns(ta.over_columns(window, columns)).to_arrow()  # type: ignore[attr-defined]


def _polars_work(frame: Any, plta: Any, *, over_symbol: bool) -> object:
    """Polars group-plugin path (``.over("symbol")``) or the global-series cliff."""
    import polars as pl

    close = pl.col("close")
    ema = plta.ema(close, timeperiod=21)
    rsi = plta.rsi(close, timeperiod=14)
    if over_symbol:
        ema = ema.over("symbol")
        rsi = rsi.over("symbol")
    return frame.with_columns(ema.alias("ema21"), rsi.alias("rsi14")).to_arrow()


def main() -> None:
    """Run the §8.2 partition matrix and print ``TA_PIPELINE`` lines."""
    parser = argparse.ArgumentParser(description=__doc__)
    harness.add_timing_args(parser)
    parser.add_argument("--n-symbols", type=int, default=None)
    parser.add_argument("--bars", type=int, default=None)
    parser.add_argument(
        "--impl",
        choices=("all", "repark", "polars"),
        default="all",
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
    cores = harness.cpu_core_count()
    harness.emit_hardware(script="bench_many_symbols")
    seed = harness.many_symbols_polars(n_symbols, bars)

    if args.impl in ("all", "polars"):
        plta = harness.try_polars_talib()
        if plta is None:
            harness.emit_line(
                "bench_many_symbols",
                impl="polars_talib",
                n=n_rows,
                reachable=False,
                reason="polars_talib_not_importable_use_record_env",
            )
        else:
            for over_symbol, shape in (
                (True, "over_symbol"),
                (False, "no_over_cliff"),
            ):
                median_s, table = harness.time_median(
                    lambda flag=over_symbol: _polars_work(seed, plta, over_symbol=flag),
                    warmup=warmup,
                    iterations=iterations,
                )
                harness.emit_line(
                    "bench_many_symbols",
                    impl="polars_talib",
                    shape=shape,
                    n_symbols=n_symbols,
                    bars=bars,
                    n=n_rows,
                    warmup=warmup,
                    iterations=iterations,
                    median_s=median_s,
                    ns_per_row=harness.ns_per_row(median_s, n_rows),
                    rows_out=harness.sink_arrow(table),
                )

    if args.impl in ("all", "repark"):
        for target_partitions, partition, shape in (
            (1, True, "partition_by_symbol"),
            (cores, True, "partition_by_symbol"),
            (1, False, "no_partition_by_cliff"),
        ):
            spark = harness.make_session(
                app_name=f"bench-ta-many-symbols-p{target_partitions}-{shape}",
                target_partitions=target_partitions,
            )
            try:
                repark_seed = harness.seed_repark_frame(spark, seed)
                from repark import Window
                from repark.spark import ta

                window = (
                    Window.partitionBy("symbol").orderBy("ts")
                    if partition
                    else Window.orderBy("ts")
                )
                columns = {
                    "ema21": ta.ema("close", timeperiod=21),
                    "rsi14": ta.rsi("close", timeperiod=14),
                }
                planned = repark_seed.withColumns(ta.over_columns(window, columns))
                plan = harness.physical_plan_text(planned)
                median_s, table = harness.time_median(
                    lambda framed=repark_seed, flag=partition: _repark_work(framed, partition=flag),
                    warmup=warmup,
                    iterations=iterations,
                )
                harness.emit_line(
                    "bench_many_symbols",
                    impl="repark_engine",
                    shape=shape,
                    n_symbols=n_symbols,
                    bars=bars,
                    n=n_rows,
                    target_partitions=target_partitions,
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
