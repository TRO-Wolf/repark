#!/usr/bin/env python3
"""§8.3 — wide serving SELECT (3xBBANDS + 3xMACD + 2xSTOCH + EMA/RSI/ATR).

Times one fused ``over_columns`` / ``with_indicators`` plan, then the same
set with an intervening ``filter`` (stacked ``WindowAggExec``). Plan-shape
(``WindowAggExec`` count + window-fn tokens) is recorded next to wall time.

§8.6 SQL same-OVER EXPLAIN already shipped as #116 — this script does not
rebuild that pin.

Usage::

    python python/repark-parity/bench/ta/bench_wide_serving.py [--quick]
"""

from __future__ import annotations

import argparse

import harness
from target_partition_contract import emit_target_partition_fields


def _fused_over_columns(seed: object) -> object:
    """One ``over_columns`` + ``withColumns`` (N2 fused shape)."""
    from repark import Window
    from repark.spark import ta

    window = Window.partitionBy("symbol").orderBy("ts")
    return seed.withColumns(  # type: ignore[attr-defined]
        ta.over_columns(window, harness.wide_serving_columns(ta))
    ).to_arrow()


def _fused_with_indicators(seed: object) -> object:
    """Same columns via the TA-2 serving helper."""
    from repark.spark import ta

    return ta.with_indicators(
        seed,
        partition="symbol",
        order="ts",
        columns=harness.wide_serving_columns(ta),
    ).to_arrow()


def _stacked_filter(seed: object) -> object:
    """First-half window → ``filter`` → second-half window (both outputs live)."""
    from repark import Window
    from repark.spark import functions as F  # noqa: N812 — PySpark idiom
    from repark.spark import ta

    window = Window.partitionBy("symbol").orderBy("ts")
    wide = harness.wide_serving_columns(ta)
    first = {key: wide[key] for key in ("bb_upper", "bb_middle", "bb_lower", "macd")}
    second = {
        key: wide[key]
        for key in (
            "macd_signal",
            "macd_hist",
            "stoch_slowk",
            "stoch_slowd",
            "ema21",
            "rsi14",
            "atr14",
        )
    }
    mid = seed.withColumns(ta.over_columns(window, first))  # type: ignore[attr-defined]
    # Always-true on the walk (close ≈ 100) so the row count is unchanged, but
    # the filter is a live plan barrier (TA-1 stacked-window truth).
    filtered = mid.filter(F.col("close") > 0)
    return filtered.withColumns(ta.over_columns(window, second)).to_arrow()


def _plan_for(seed: object, shape: str) -> str:
    """Rebuild the lazy plan for ``shape`` and return the physical-plan text."""
    from repark import Window
    from repark.spark import functions as F  # noqa: N812 — PySpark idiom
    from repark.spark import ta

    window = Window.partitionBy("symbol").orderBy("ts")
    wide = harness.wide_serving_columns(ta)
    if shape == "over_columns":
        framed = seed.withColumns(ta.over_columns(window, wide))  # type: ignore[attr-defined]
    elif shape == "with_indicators":
        framed = ta.with_indicators(seed, partition="symbol", order="ts", columns=wide)
    elif shape == "stacked_filter":
        first = {key: wide[key] for key in ("bb_upper", "bb_middle", "bb_lower", "macd")}
        second = {
            key: wide[key]
            for key in (
                "macd_signal",
                "macd_hist",
                "stoch_slowk",
                "stoch_slowd",
                "ema21",
                "rsi14",
                "atr14",
            )
        }
        mid = seed.withColumns(ta.over_columns(window, first))  # type: ignore[attr-defined]
        framed = mid.filter(F.col("close") > 0).withColumns(ta.over_columns(window, second))
    else:
        raise ValueError(f"unknown shape {shape!r}")
    return harness.physical_plan_text(framed)


def main() -> None:
    """Run the §8.3 wide-serving shapes and print ``TA_PIPELINE`` lines."""
    parser = argparse.ArgumentParser(description=__doc__)
    harness.add_timing_args(parser)
    parser.add_argument("--n-rows", type=int, default=None)
    args = parser.parse_args()
    warmup, iterations = harness.resolve_timing(args)
    n_rows = harness.resolve_n_rows(args, full=harness.DEFAULT_N_ROWS, quick=harness.QUICK_N_ROWS)
    harness.emit_hardware(script="bench_wide_serving")
    seed = harness.one_symbol_polars(n_rows)
    spark = harness.make_session(app_name="bench-ta-wide-serving")
    workers = {
        "over_columns": _fused_over_columns,
        "with_indicators": _fused_with_indicators,
        "stacked_filter": _stacked_filter,
    }
    try:
        repark_seed = harness.seed_repark_frame(spark, seed)
        for shape, worker in workers.items():
            plan = _plan_for(repark_seed, shape)
            median_s, table = harness.time_median(
                lambda work=worker, framed=repark_seed: work(framed),
                warmup=warmup,
                iterations=iterations,
            )
            harness.emit_line(
                "bench_wide_serving",
                impl="repark_engine",
                shape=shape,
                n=n_rows,
                **emit_target_partition_fields(isolation=False),
                warmup=warmup,
                iterations=iterations,
                median_s=median_s,
                ns_per_row=harness.ns_per_row(median_s, n_rows),
                window_agg_exec=harness.count_window_agg_exec(plan),
                window_fn_tokens=harness.count_window_fn_tokens(plan),
                rows_out=harness.sink_arrow(table),
                sql_same_over_cite="PR116_ta1",
            )
    finally:
        harness.stop_session(spark)


if __name__ == "__main__":
    main()
