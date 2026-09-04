"""Demonstrate the composition helpers ``ta.over_columns`` and ``ta.with_indicators``.

Every column the helpers produce is asserted bit-for-bit against a golden read from
``crates/repark-ta/tests/goldens`` at run time.

pins: ex-24-ta-b/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = ["ta.over_columns", "ta.with_indicators"]

GOLDENS = Path(__file__).resolve().parents[3] / "crates" / "repark-ta" / "tests" / "goldens"


def golden(name: str) -> np.ndarray:
    return np.frombuffer((GOLDENS / f"{name}.bin").read_bytes(), dtype="<f8")


def column(rows: list[Row], name: str) -> np.ndarray:
    return np.ascontiguousarray([row[name] for row in rows], dtype=np.float64)


def expect_bit_exact(name: str, got: np.ndarray, golden: np.ndarray) -> None:
    if got.size != golden.size:
        raise SystemExit(f"{name}: length {got.size} != golden length {golden.size}")
    both_nan = np.isnan(got) & np.isnan(golden)
    mismatching = np.flatnonzero((got.view("uint64") != golden.view("uint64")) & ~both_nan)
    if mismatching.size == 0:
        return
    row = int(mismatching[0])
    raise SystemExit(
        f"{name}: bit mismatch at row {row}: got {float(got[row])!r} vs golden"
        f" {float(golden[row])!r}"
    )


def main() -> None:
    """Fuse several ta kernels through each helper and check every column bit-for-bit."""
    repark = ReparkSession.builder.appName("ex-ta-composition").master("local[1]").getOrCreate()
    try:
        close = golden("fixture_close")
        frame = repark.createDataFrame(
            list(enumerate(close)),
            ["ts", "close"],
        )
        symbols = repark.createDataFrame(
            [(index, "TEST", value) for index, value in enumerate(close)],
            ["ts", "symbol", "close"],
        )
        window = Window.orderBy("ts")
        fused = ta.over_columns(
            window,
            {
                "ema5": ta.ema("close", timeperiod=5),
                "trima5": ta.trima("close", timeperiod=5),
            },
        )
        rows = frame.withColumns(fused).collect()
        expect_bit_exact("ta.ema via ta.over_columns", column(rows, "ema5"), golden("ema_5"))
        expect_bit_exact("ta.trima via ta.over_columns", column(rows, "trima5"), golden("trima_5"))
        rows = ta.with_indicators(
            symbols,
            partition="symbol",
            order="ts",
            columns={
                "rsi3": ta.rsi("close", timeperiod=3),
                "min34": ta.min("close", timeperiod=34),
            },
        ).collect()
        expect_bit_exact("ta.rsi via ta.with_indicators", column(rows, "rsi3"), golden("rsi_3"))
        expect_bit_exact("ta.min via ta.with_indicators", column(rows, "min34"), golden("min_34"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
