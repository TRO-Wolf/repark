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
    """Load a recorded golden or fixture ``.bin`` (little-endian ``f64``)."""
    return np.frombuffer((GOLDENS / f"{name}.bin").read_bytes(), dtype="<f8")


def column(rows: list[Row], name: str) -> np.ndarray:
    """Materialize one collected column as an ``f64`` array in ``ts`` order."""
    return np.ascontiguousarray([row[name] for row in rows], dtype=np.float64)


def expect_tail(label: str, output: np.ndarray, expected: np.ndarray) -> None:
    """Raise SystemExit unless the last five non-NaN values match the golden within 1e-9."""
    tail = np.flatnonzero(~np.isnan(expected))[-5:]
    got = output[tail]
    want = expected[tail]
    if not np.allclose(got, want, rtol=0.0, atol=1e-9):
        raise SystemExit(f"{label} tail {got.tolist()!r} != golden tail {want.tolist()!r}")


def main() -> None:
    """Run each regression kernel over the ordered fixture window and check the golden tail."""
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
        expect_tail("ta.linearreg", column(rows, "linearreg"), golden("linearreg_5"))
        rows = frame.select(
            ta.linearreg_slope("close", timeperiod=5).over(window).alias("slope")
        ).collect()
        expect_tail("ta.linearreg_slope", column(rows, "slope"), golden("linearreg_slope_5"))
        rows = frame.select(
            ta.linearreg_intercept("close", timeperiod=5).over(window).alias("intercept")
        ).collect()
        expect_tail(
            "ta.linearreg_intercept", column(rows, "intercept"), golden("linearreg_intercept_5")
        )
        rows = frame.select(
            ta.linearreg_angle("close", timeperiod=14).over(window).alias("angle")
        ).collect()
        expect_tail("ta.linearreg_angle", column(rows, "angle"), golden("linearreg_angle_14"))
        rows = frame.select(
            ta.beta("high", "low", timeperiod=5).over(window).alias("beta")
        ).collect()
        expect_tail("ta.beta", column(rows, "beta"), golden("beta_5"))
        rows = frame.select(
            ta.correl("high", "low", timeperiod=14).over(window).alias("correl")
        ).collect()
        expect_tail("ta.correl", column(rows, "correl"), golden("correl_14"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
