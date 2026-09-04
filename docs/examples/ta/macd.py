"""Demonstrate the MACD-family ``ta`` kernels against the recorded TA-Lib goldens.

Each engine call selects the kernel's split outputs in one pass over the window.

pins: ex-23-ta-a/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = [
    "ta.macd",
    "ta.macd_signal",
    "ta.macd_hist",
    "ta.macdext",
    "ta.macdext_signal",
    "ta.macdext_hist",
    "ta.macdfix",
    "ta.macdfix_signal",
    "ta.macdfix_hist",
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
    """Run each MACD variant over the ordered fixture window and check the golden tails."""
    repark = ReparkSession.builder.appName("ex-ta-macd").master("local[1]").getOrCreate()
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
            ta.macd("close").over(window).alias("macd"),
            ta.macd_signal("close").over(window).alias("signal"),
            ta.macd_hist("close").over(window).alias("hist"),
        ).collect()
        expect_tail("ta.macd", column(rows, "macd"), golden("macd_12_26_9_macd"))
        expect_tail("ta.macd_signal", column(rows, "signal"), golden("macd_12_26_9_signal"))
        expect_tail("ta.macd_hist", column(rows, "hist"), golden("macd_12_26_9_hist"))
        rows = frame.select(
            ta.macdext("close").over(window).alias("macd"),
            ta.macdext_signal("close").over(window).alias("signal"),
            ta.macdext_hist("close").over(window).alias("hist"),
        ).collect()
        expect_tail("ta.macdext", column(rows, "macd"), golden("macdext_12_26_9_macd"))
        expect_tail("ta.macdext_signal", column(rows, "signal"), golden("macdext_12_26_9_signal"))
        expect_tail("ta.macdext_hist", column(rows, "hist"), golden("macdext_12_26_9_hist"))
        rows = frame.select(
            ta.macdfix("close").over(window).alias("macd"),
            ta.macdfix_signal("close").over(window).alias("signal"),
            ta.macdfix_hist("close").over(window).alias("hist"),
        ).collect()
        expect_tail("ta.macdfix", column(rows, "macd"), golden("macdfix_9_macd"))
        expect_tail("ta.macdfix_signal", column(rows, "signal"), golden("macdfix_9_signal"))
        expect_tail("ta.macdfix_hist", column(rows, "hist"), golden("macdfix_9_hist"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
