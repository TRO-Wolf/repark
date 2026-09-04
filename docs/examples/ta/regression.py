"""Demonstrate the regression and statistic ``ta`` kernels against the recorded goldens.

Every asserted value is read from ``crates/repark-ta/tests/goldens`` at run time.

pins: ex-23-ta-a/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = [
    "ta.linearreg",
    "ta.linearreg_angle",
    "ta.linearreg_intercept",
    "ta.linearreg_slope",
    "ta.beta",
    "ta.correl",
]

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
    """Run each regression kernel over the fixture window and check the golden bit-for-bit."""
    repark = ReparkSession.builder.appName("ex-ta-regression").master("local[1]").getOrCreate()
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
            ta.linearreg("close", timeperiod=5).over(window).alias("linearreg")
        ).collect()
        expect_bit_exact("ta.linearreg", column(rows, "linearreg"), golden("linearreg_5"))
        rows = frame.select(
            ta.linearreg_slope("close", timeperiod=5).over(window).alias("slope")
        ).collect()
        expect_bit_exact("ta.linearreg_slope", column(rows, "slope"), golden("linearreg_slope_5"))
        rows = frame.select(
            ta.linearreg_intercept("close", timeperiod=5).over(window).alias("intercept")
        ).collect()
        expect_bit_exact(
            "ta.linearreg_intercept", column(rows, "intercept"), golden("linearreg_intercept_5")
        )
        rows = frame.select(
            ta.linearreg_angle("close", timeperiod=14).over(window).alias("angle")
        ).collect()
        expect_bit_exact("ta.linearreg_angle", column(rows, "angle"), golden("linearreg_angle_14"))
        rows = frame.select(
            ta.beta("high", "low", timeperiod=5).over(window).alias("beta")
        ).collect()
        expect_bit_exact("ta.beta", column(rows, "beta"), golden("beta_5"))
        rows = frame.select(
            ta.correl("high", "low", timeperiod=14).over(window).alias("correl")
        ).collect()
        expect_bit_exact("ta.correl", column(rows, "correl"), golden("correl_14"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
