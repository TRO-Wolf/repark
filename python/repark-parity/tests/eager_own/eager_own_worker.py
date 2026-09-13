"""EAGER-OWN-1 step-0 worker: N bare ``eager()`` calls on the TA fixture, metrics to JSON."""

from __future__ import annotations

import argparse
import gc
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

_CACHE_VIEW_PREFIX = "__repark_cache_"
_SEED = 20260913
_TS_EPOCH_START = 1_767_225_600
_SOURCE_COLUMNS = 6
_ADDED_COLUMNS = 19


def read_status_bytes(field: str) -> int:
    """Read one kB field of ``/proc/self/status`` as bytes."""
    for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{field}:"):
            return int(line.split()[1]) * 1024
    raise OSError(f"/proc/self/status has no {field} line")


def read_mem_total_bytes() -> int | None:
    """Read ``MemTotal`` from ``/proc/meminfo`` as bytes, or ``None`` when absent."""
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def cache_registration_count(session: Any) -> int:
    """Count ``__repark_cache_*`` names in ``catalog.listTables()``."""
    return sum(
        1 for table in session.catalog.listTables() if table.name.startswith(_CACHE_VIEW_PREFIX)
    )


def temp_view_registration_count(session: Any) -> int:
    """Cross-check the count over ``session.list_temp_view_names()``."""
    return sum(1 for name in session.list_temp_view_names() if name.startswith(_CACHE_VIEW_PREFIX))


def build_source_frame(session: Any, rows: int) -> Any:
    """Build the deterministic one-series OHLCV source frame through the polars Arrow path."""
    import numpy as np
    import polars as pl

    rng = np.random.default_rng(_SEED)
    ts_epoch = _TS_EPOCH_START + np.arange(rows, dtype=np.int64) * 60
    log_returns = rng.normal(0.0, 0.0008, rows)
    close_price = 100.0 * np.exp(np.cumsum(log_returns))
    open_price = np.concatenate((close_price[:1] * 0.999, close_price[:-1]))
    spread = np.abs(rng.normal(0.0, 0.0005, rows))
    high_price = np.maximum(open_price, close_price) * (1.0 + spread)
    low_price = np.minimum(open_price, close_price) * (1.0 - spread)
    volume = rng.integers(1_000, 50_000, rows).astype(np.float64)
    frame = pl.DataFrame(
        {
            "ts": pl.Series(ts_epoch).cast(pl.Datetime("us")),
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
        }
    )
    return session.createDataFrame(frame)


def build_ta_frame(source: Any) -> Any:
    """The card's 19-column ``withColumns`` TA chain over the ts-ordered window."""
    from repark import Window
    from repark.spark import functions, ta

    window = Window.orderBy("ts")
    enriched = source.withColumns(
        ta.over_columns(
            window,
            {
                "ema_5": ta.ema("close", timeperiod=5),
                "ema_9": ta.ema("close", timeperiod=9),
                "ema_21": ta.ema("close", timeperiod=21),
                "sma_10": ta.sma("close", timeperiod=10),
                "sma_20": ta.sma("close", timeperiod=20),
                "sma_50": ta.sma("close", timeperiod=50),
                "rsi_7": ta.rsi("close", timeperiod=7),
                "rsi_14": ta.rsi("close", timeperiod=14),
                "adx_14": ta.adx("high", "low", "close", timeperiod=14),
                "linreg_14": ta.linearreg("close", timeperiod=14),
                "linreg_slope_14": ta.linearreg_slope("close", timeperiod=14),
                "tr": ta.trange("high", "low", "close"),
                "atr_14": ta.atr("high", "low", "close", timeperiod=14),
                "mom_10": ta.mom("close", timeperiod=10),
            },
        )
    )
    return enriched.withColumns(
        {
            "ema_tr_14": ta.ema("tr", timeperiod=14).over(window),
            "close_open_ratio": functions.col("close") / functions.col("open"),
            "close_rounded": functions.round(functions.col("close"), 2),
            "rsi_filled": functions.coalesce(
                functions.when(functions.col("rsi_14") > 0, functions.col("rsi_14")),
                functions.lit(50.0),
            ),
            "regime": functions.when(
                functions.col("close") > functions.col("ema_21"), functions.lit("up")
            ).otherwise(functions.lit("down")),
        }
    )


def snapshot(session: Any) -> dict[str, Any]:
    """One observation point: both registration counts, VmRSS, and VmHWM."""
    return {
        "registrations": cache_registration_count(session),
        "temp_view_registrations": temp_view_registration_count(session),
        "rss_bytes": read_status_bytes("VmRSS"),
        "peak_rss_bytes": read_status_bytes("VmHWM"),
    }


def measure(session: Any, frame: Any, iterations: int) -> dict[str, Any]:
    """Run ``iterations`` bare ``frame.eager()`` calls (results not assigned)."""
    records: list[dict[str, Any]] = []
    for iteration in range(iterations):
        gc.collect()
        started = time.perf_counter()
        frame.eager()
        seconds = time.perf_counter() - started
        after_call = snapshot(session)
        gc.collect()
        records.append(
            {
                "iteration": iteration,
                "seconds": seconds,
                "registrations_after_call": after_call["registrations"],
                "temp_view_registrations_after_call": after_call["temp_view_registrations"],
                "rss_after_call_bytes": after_call["rss_bytes"],
                "rss_after_gc_bytes": read_status_bytes("VmRSS"),
                "peak_rss_bytes": after_call["peak_rss_bytes"],
            }
        )
    post_loop = snapshot(session)
    gc.collect()
    post_gc = snapshot(session)
    session.catalog.clearCache()
    post_clear_cache = snapshot(session)
    return {
        "iterations": records,
        "post_loop": post_loop,
        "post_gc": post_gc,
        "post_clear_cache": post_clear_cache,
    }


def environment() -> dict[str, Any]:
    """The machine and module header every committed measurement carries."""
    import repark._native as native

    return {
        "python": sys.version.split()[0],
        "cpu_count": os.cpu_count(),
        "mem_total_bytes": read_mem_total_bytes(),
        "native_debug_assertions": bool(native.__debug_assertions__),
    }


def run(args: argparse.Namespace) -> int:
    """Build the fixture, run the bare-eager loop, and write the JSON payload."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("eager-own-1-step0").getOrCreate()
    try:
        source = build_source_frame(session, args.rows)
        frame = build_ta_frame(source)
        total_columns = len(frame.columns)
        started = time.perf_counter()
        outcome = measure(session, frame, args.iterations)
        total_seconds = time.perf_counter() - started
    finally:
        session.stop()
    payload = {
        "fixture": {
            "rows": args.rows,
            "seed": _SEED,
            "source_columns": _SOURCE_COLUMNS,
            "added_columns": _ADDED_COLUMNS,
            "total_columns": total_columns,
        },
        "environment": environment(),
        "total_seconds": total_seconds,
        **outcome,
    }
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the worker's argv."""
    parser = argparse.ArgumentParser(description="eager-own-1 step-0 measurement worker")
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--iterations", type=int, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI for one isolated bare-eager measurement."""
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
