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
    """Run ``ta.MAX`` / ``ta.MIN`` / ``ta.SUM`` over the fixture window and check the tails."""
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
        expect_tail("ta.MAX", column(rows, "max21"), golden("max_21"))
        rows = frame.select(ta.MIN("close", timeperiod=21).over(window).alias("min21")).collect()
        expect_tail("ta.MIN", column(rows, "min21"), golden("min_21"))
        rows = frame.select(ta.SUM("close", timeperiod=21).over(window).alias("sum21")).collect()
        expect_tail("ta.SUM", column(rows, "sum21"), golden("sum_21"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
