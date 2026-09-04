"""Demonstrate the oscillator ``ta`` kernels against the recorded TA-Lib goldens.

Every asserted value is read from ``crates/repark-ta/tests/goldens`` at run time.

pins: ex-24-ta-b/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = ["ta.ppo", "ta.rsi", "ta.trix", "ta.ultosc", "ta.willr"]

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
    """Run each oscillator over the ordered fixture window and check the golden bit-for-bit."""
    repark = ReparkSession.builder.appName("ex-ta-oscillators").master("local[1]").getOrCreate()
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
            ta.ppo("close", fastperiod=12, slowperiod=26, matype=0).over(window).alias("ppo")
        ).collect()
        expect_bit_exact("ta.ppo", column(rows, "ppo"), golden("ppo_12_26"))
        rows = frame.select(ta.rsi("close", timeperiod=14).over(window).alias("rsi")).collect()
        expect_bit_exact("ta.rsi", column(rows, "rsi"), golden("rsi_14"))
        rows = frame.select(ta.trix("close", timeperiod=30).over(window).alias("trix")).collect()
        expect_bit_exact("ta.trix", column(rows, "trix"), golden("trix_30"))
        rows = frame.select(
            ta.ultosc("high", "low", "close", timeperiod1=7, timeperiod2=14, timeperiod3=28)
            .over(window)
            .alias("ultosc")
        ).collect()
        expect_bit_exact("ta.ultosc", column(rows, "ultosc"), golden("ultosc_7_14_28"))
        rows = frame.select(
            ta.willr("high", "low", "close", timeperiod=14).over(window).alias("willr")
        ).collect()
        expect_bit_exact("ta.willr", column(rows, "willr"), golden("willr_14"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
