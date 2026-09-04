"""Demonstrate the rolling-extreme ``ta`` kernels and their TA-Lib-name aliases.

Every asserted value is read from ``crates/repark-ta/tests/goldens`` at run time.

pins: ex-23-ta-a/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = ["ta.MAX", "ta.MIN", "ta.SUM"]

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
    """Run ``ta.MAX`` / ``ta.MIN`` / ``ta.SUM`` over the fixture window and check bit-for-bit."""
    repark = ReparkSession.builder.appName("ex-ta-extremes").master("local[1]").getOrCreate()
    try:
        close = golden("fixture_close")
        frame = repark.createDataFrame(
            list(
                zip(
                    range(close.size),
                    golden("fixture_open"),
                    golden("fixture_high"),
                    golden("fixture_low"),
                    close,
                    golden("fixture_volume"),
                    strict=True,
                )
            ),
            ["ts", "open", "high", "low", "close", "volume"],
        )
        window = Window.orderBy("ts")
        rows = frame.select(ta.MAX("close", timeperiod=21).over(window).alias("max21")).collect()
        expect_bit_exact("ta.MAX", column(rows, "max21"), golden("max_21"))
        rows = frame.select(ta.MIN("close", timeperiod=21).over(window).alias("min21")).collect()
        expect_bit_exact("ta.MIN", column(rows, "min21"), golden("min_21"))
        rows = frame.select(ta.SUM("close", timeperiod=21).over(window).alias("sum21")).collect()
        expect_bit_exact("ta.SUM", column(rows, "sum21"), golden("sum_21"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
