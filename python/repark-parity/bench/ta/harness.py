#!/usr/bin/env python3
"""Shared fixtures for the P-2 TA pipeline benches.

Measurement only — no engine edits. Deterministic generators (never wall clock
in the seed path). Warm-up + N-iteration median. Machine-readable one-liners.

Reuse the parity ``record`` / PEP-723 env that already pins ``polars_talib==0.1.5``.
Do not add that package to the main workspace.
"""

from __future__ import annotations

import argparse
import os
import re
import statistics
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

RESULT_PREFIX = "TA_PIPELINE"
HARDWARE_PREFIX = "TA_PIPELINE_HW"
DEFAULT_N_ROWS = 1_000_000
QUICK_N_ROWS = 100_000
DEFAULT_BATCH_SWEEP_ROWS = 2_000_000
QUICK_BATCH_SWEEP_ROWS = 200_000
DEFAULT_WARMUP = 2
DEFAULT_ITERATIONS = 5
QUICK_WARMUP = 1
QUICK_ITERATIONS = 3
# §8.2 default: 256 symbols x 4096 bars = 1_048_576 rows (one-million class).
DEFAULT_N_SYMBOLS = 256
DEFAULT_BARS_PER_SYMBOL = 4_096
QUICK_N_SYMBOLS = 32
QUICK_BARS_PER_SYMBOL = 1_024
P1C_WALK_MODULUS = 10_000
P1C_WALK_SCALE = 0.002
NATIVE_DEBUG_SIZE_BYTES = 200_000_000
WINDOW_FN_TOKEN = re.compile(r"\b(?:ta_[a-z0-9_]+|row_number)\b", re.IGNORECASE)

_POLARS_TALIB: Any | None = None
_POLARS_TALIB_TRIED = False


def cpu_core_count() -> int:
    """Logical CPU count (``os.cpu_count``); at least 1."""
    counted = os.cpu_count()
    return counted if counted is not None and counted > 0 else 1


def read_text_or_none(path: Path) -> str | None:
    """Read a small sysfs/proc file; return None if it is absent."""
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def hardware_fields() -> dict[str, str]:
    """Best-effort hardware snapshot for the one-line header."""
    model = "unknown"
    cpuinfo = read_text_or_none(Path("/proc/cpuinfo"))
    if cpuinfo is not None:
        for line in cpuinfo.splitlines():
            if line.lower().startswith("model name"):
                _, _, model = line.partition(":")
                model = "_".join(model.strip().split())
                break
    governor = read_text_or_none(Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"))
    mem_kib = None
    meminfo = read_text_or_none(Path("/proc/meminfo"))
    if meminfo is not None:
        for line in meminfo.splitlines():
            if line.startswith("MemTotal:"):
                mem_kib = line.split()[1]
                break
    ram_gib = "unknown"
    if mem_kib is not None:
        ram_gib = f"{int(mem_kib) / (1024 * 1024):.1f}"
    return {
        "cpu": model,
        "cores": str(cpu_core_count()),
        "governor": governor if governor is not None else "unknown",
        "ram_gib": ram_gib,
    }


def native_build_flavor() -> str:
    """Heuristic: the debug cdylib is ~600 MiB; a release develop is much smaller."""
    try:
        from repark import _native
    except ImportError:
        return "repark_not_importable"
    path = getattr(_native, "__file__", None)
    if not isinstance(path, str):
        return "unknown"
    try:
        size = Path(path).stat().st_size
    except OSError:
        return "unknown"
    if size >= NATIVE_DEBUG_SIZE_BYTES:
        return f"debug_or_unstripped size_bytes={size}"
    return f"release_or_stripped size_bytes={size}"


def walk_close(n_rows: int, *, start: float = 100.0) -> np.ndarray:
    """P-1 / p1c deterministic walk: null-free ``float64``, no wall clock."""
    close = np.empty(n_rows, dtype=np.float64)
    price = start
    for index in range(n_rows):
        phase_steps = index % P1C_WALK_MODULUS
        phase = float(phase_steps) / float(P1C_WALK_MODULUS)
        price *= 1.0 + (phase - 0.5) * P1C_WALK_SCALE
        close[index] = price
    return close


def ohlcv_from_close(close: np.ndarray) -> dict[str, np.ndarray]:
    """Derive OHLC+V so ``high > close > low`` and volume is positive."""
    high = close * 1.001
    low = close * 0.999
    open_price = close.copy()
    volume = 1_000.0 + (np.arange(close.shape[0], dtype=np.float64) % 100.0)
    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def one_symbol_polars(n_rows: int, *, symbol: str = "AAA") -> pl.DataFrame:
    """One pre-sorted symbol: ``ts`` is the bar index (not wall clock)."""
    close = walk_close(n_rows)
    ohlcv = ohlcv_from_close(close)
    return pl.DataFrame(
        {
            "symbol": [symbol] * n_rows,
            "ts": np.arange(n_rows, dtype=np.int64),
            **ohlcv,
        }
    )


def many_symbols_polars(n_symbols: int, bars_per_symbol: int) -> pl.DataFrame:
    """``n_symbols`` instruments, **same** timestamps — the partitionBy fixture.

    Shared ``ts`` is the cross-symbol RSI footgun: a missing ``partitionBy``
    folds every symbol into one series (correctness + perf cliff).
    """
    frames: list[pl.DataFrame] = []
    for symbol_index in range(n_symbols):
        symbol = f"S{symbol_index:04d}"
        close = walk_close(bars_per_symbol, start=100.0 + float(symbol_index))
        ohlcv = ohlcv_from_close(close)
        frames.append(
            pl.DataFrame(
                {
                    "symbol": [symbol] * bars_per_symbol,
                    "ts": np.arange(bars_per_symbol, dtype=np.int64),
                    **ohlcv,
                }
            )
        )
    return pl.concat(frames, how="vertical")


def try_polars_talib() -> Any | None:
    """Import ``polars_talib`` 0.1.5 if the record env (or ``--with``) is on the path."""
    global _POLARS_TALIB, _POLARS_TALIB_TRIED
    if _POLARS_TALIB_TRIED:
        return _POLARS_TALIB
    _POLARS_TALIB_TRIED = True
    try:
        import polars_talib as plta
    except ImportError:
        _POLARS_TALIB = None
        return None
    _POLARS_TALIB = plta
    return plta


def raw_repark_ta_status() -> tuple[bool, str]:
    """Raw ``repark_ta`` kernels are a Rust crate — not a Python import.

    P-1 (#132) already times them via criterion. This path stays SKIP here.
    """
    try:
        import repark_ta  # noqa: F401
    except ImportError:
        return False, "no_python_module_cite_PR132_criterion"
    return True, "imported"


def time_median(
    work: Callable[[], object],
    *,
    warmup: int,
    iterations: int,
) -> tuple[float, object]:
    """Warm up, then return ``(median_seconds, last_result)`` over ``iterations``."""
    if warmup < 0:
        raise ValueError(f"warmup must be >= 0, got {warmup}")
    if iterations < 1:
        raise ValueError(f"iterations must be >= 1, got {iterations}")
    last: object = None
    for _ in range(warmup):
        last = work()
    samples: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        last = work()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples), last


def ns_per_row(median_s: float, n_rows: int) -> float:
    """Convert a median wall time to nanoseconds per input row."""
    if n_rows <= 0:
        raise ValueError(f"n_rows must be > 0, got {n_rows}")
    return (median_s * 1e9) / float(n_rows)


def emit_line(script: str, **fields: object) -> None:
    """Print one machine-readable ``TA_PIPELINE`` result line."""
    parts = [f"{RESULT_PREFIX} script={script}"]
    for key, value in fields.items():
        if value is None:
            rendered = "none"
        elif isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, float):
            rendered = f"{value:.3f}" if key == "ns_per_row" else f"{value:.6f}"
        else:
            rendered = str(value).replace(" ", "_")
        parts.append(f"{key}={rendered}")
    print(" ".join(parts), flush=True)


def emit_hardware(*, script: str) -> None:
    """Print the hardware header (once per process is enough; scripts call it)."""
    fields = hardware_fields()
    flavor = native_build_flavor()
    parts = [f"{HARDWARE_PREFIX} script={script}", f"native={flavor.replace(' ', '_')}"]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def make_session(
    *,
    app_name: str,
    target_partitions: int | None = None,
    batch_size: int | None = None,
) -> object:
    """Build a fresh ``ReparkSession``. Caller must ``stop`` it.

    Engine knobs are fixed at build — do not reuse a session across a sweep.
    ``target_partitions=None`` (the default) omits ``repark.target.partitions``
    so DataFusion's ``num_cpus`` default plans the query — BH-1 primary cells.
    """
    from repark import ReparkSession

    builder = ReparkSession.builder.appName(app_name)
    if target_partitions is not None:
        builder = builder.config("repark.target.partitions", str(target_partitions))
    if batch_size is not None:
        builder = builder.config("repark.batch.size", str(batch_size))
    return builder.getOrCreate()


def stop_session(spark: object) -> None:
    """Idempotent session stop."""
    stopper = getattr(spark, "stop", None)
    if callable(stopper):
        stopper()


def seed_repark_frame(spark: Any, frame: pl.DataFrame) -> Any:
    """Materialize a Polars seed as a RePark DataFrame (Arrow path; not VALUES)."""
    return spark.createDataFrame(frame)


def physical_plan_text(frame: Any) -> str:
    """Capture ``DataFrame.explain()`` physical-plan body (N2 / TA-1 mechanic)."""
    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain()
    text = buffer.getvalue()
    match = re.search(
        r"plan_type='physical_plan', plan='((?:\\'|[^'])*)'",
        text,
    )
    if match is None:
        return text
    return match.group(1).replace("\\n", "\n").replace("\\'", "'")


def count_window_agg_exec(plan: str) -> int:
    """Count ``WindowAggExec`` nodes in a physical plan string."""
    return plan.count("WindowAggExec")


def count_window_fn_tokens(plan: str) -> int:
    """Count live TA / ``row_number`` window-fn tokens in a physical plan string."""
    return len(WINDOW_FN_TOKEN.findall(plan))


def add_timing_args(parser: argparse.ArgumentParser) -> None:
    """Shared ``--quick`` / warm-up / iteration flags."""
    parser.add_argument(
        "--quick",
        action="store_true",
        help="n=1e5 (or the script's short path), warmup=1, iterations=3",
    )
    parser.add_argument("--warmup", type=int, default=None, help="warm-up iterations")
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="timed iterations (median)",
    )


def resolve_timing(args: argparse.Namespace) -> tuple[int, int]:
    """Return ``(warmup, iterations)`` honoring ``--quick`` defaults."""
    warmup = args.warmup
    if warmup is None:
        warmup = QUICK_WARMUP if args.quick else DEFAULT_WARMUP
    iterations = args.iterations
    if iterations is None:
        iterations = QUICK_ITERATIONS if args.quick else DEFAULT_ITERATIONS
    return warmup, iterations


def resolve_n_rows(args: argparse.Namespace, *, full: int, quick: int) -> int:
    """Row count: explicit ``--n-rows`` wins, else ``--quick`` selects the short path."""
    explicit = getattr(args, "n_rows", None)
    if explicit is not None:
        return int(explicit)
    return quick if args.quick else full


def sink_arrow(table: Any) -> int:
    """Keep an Arrow table live; return ``num_rows``."""
    return int(table.num_rows)


def sink_rows(rows: object) -> int:
    """Keep a ``collect()`` list live; return ``len``."""
    length = getattr(rows, "__len__", None)
    if callable(length):
        return int(length())
    raise TypeError(f"collect sink is not sized: {type(rows).__name__}")


def wide_serving_columns(ta: Any) -> dict[str, object]:
    """§8.3 set: 3xBBANDS + 3xMACD + 2xSTOCH + EMA/RSI/ATR."""
    return {
        "bb_upper": ta.bbands_upper("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0),
        "bb_middle": ta.bbands_middle("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0),
        "bb_lower": ta.bbands_lower("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0),
        "macd": ta.macd("close", fastperiod=12, slowperiod=26, signalperiod=9),
        "macd_signal": ta.macd_signal("close", fastperiod=12, slowperiod=26, signalperiod=9),
        "macd_hist": ta.macd_hist("close", fastperiod=12, slowperiod=26, signalperiod=9),
        "stoch_slowk": ta.stoch_slowk("high", "low", "close"),
        "stoch_slowd": ta.stoch_slowd("high", "low", "close"),
        "ema21": ta.ema("close", timeperiod=21),
        "rsi14": ta.rsi("close", timeperiod=14),
        "atr14": ta.atr("high", "low", "close", timeperiod=14),
    }


def ten_lookback_columns(ta: Any) -> dict[str, object]:
    """§8.5: ten single-output TA columns for the ``null_lookback`` plan tax."""
    return {
        "ema5": ta.ema("close", timeperiod=5),
        "ema10": ta.ema("close", timeperiod=10),
        "ema21": ta.ema("close", timeperiod=21),
        "sma10": ta.sma("close", timeperiod=10),
        "sma20": ta.sma("close", timeperiod=20),
        "rsi7": ta.rsi("close", timeperiod=7),
        "rsi14": ta.rsi("close", timeperiod=14),
        "mom10": ta.mom("close", timeperiod=10),
        "roc10": ta.roc("close", timeperiod=10),
        "wma10": ta.wma("close", timeperiod=10),
    }
