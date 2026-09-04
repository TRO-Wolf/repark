"""Demonstrate the dispersion and forecast ``ta`` kernels against the recorded goldens.

Every asserted value is read from ``crates/repark-ta/tests/goldens`` at run time.

pins: ex-24-ta-b/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = ["ta.stddev", "ta.tsf", "ta.var"]

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
    """Run each dispersion and forecast kernel over the fixture window and check bit-for-bit."""
    repark = ReparkSession.builder.appName("ex-ta-statistics").master("local[1]").getOrCreate()
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
        rows = frame.select(
            ta.stddev("close", timeperiod=5, nbdev=1.0).over(window).alias("stddev")
        ).collect()
        expect_bit_exact("ta.stddev", column(rows, "stddev"), golden("stddev_5_nbdev1"))
        rows = frame.select(ta.tsf("close", timeperiod=5).over(window).alias("tsf")).collect()
        expect_bit_exact("ta.tsf", column(rows, "tsf"), golden("tsf_5"))
        rows = frame.select(
            ta.var("close", timeperiod=5, nbdev=1.0).over(window).alias("var")
        ).collect()
        expect_bit_exact("ta.var", column(rows, "var"), golden("var_5"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
