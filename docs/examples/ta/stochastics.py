"""Demonstrate the stochastic ``ta`` kernels against the recorded TA-Lib goldens.

Each variant selects its two split outputs in one pass over the ordered window.

pins: ex-24-ta-b/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = [
    "ta.stoch_slowd",
    "ta.stoch_slowk",
    "ta.stochf_fastd",
    "ta.stochf_fastk",
    "ta.stochrsi_fastd",
    "ta.stochrsi_fastk",
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
    """Run each stochastic variant over the ordered fixture window and check bit-for-bit."""
    repark = ReparkSession.builder.appName("ex-ta-stochastics").master("local[1]").getOrCreate()
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
            ta.stoch_slowk(
                "high",
                "low",
                "close",
                fastk_period=5,
                slowk_period=3,
                slowk_matype=0,
                slowd_period=3,
                slowd_matype=0,
            )
            .over(window)
            .alias("slowk"),
            ta.stoch_slowd(
                "high",
                "low",
                "close",
                fastk_period=5,
                slowk_period=3,
                slowk_matype=0,
                slowd_period=3,
                slowd_matype=0,
            )
            .over(window)
            .alias("slowd"),
        ).collect()
        expect_bit_exact("ta.stoch_slowk", column(rows, "slowk"), golden("stoch_slowk"))
        expect_bit_exact("ta.stoch_slowd", column(rows, "slowd"), golden("stoch_slowd"))
        rows = frame.select(
            ta.stochf_fastk("high", "low", "close", fastk_period=5, fastd_period=3, fastd_matype=0)
            .over(window)
            .alias("fastk"),
            ta.stochf_fastd("high", "low", "close", fastk_period=5, fastd_period=3, fastd_matype=0)
            .over(window)
            .alias("fastd"),
        ).collect()
        expect_bit_exact("ta.stochf_fastk", column(rows, "fastk"), golden("stochf_fastk"))
        expect_bit_exact("ta.stochf_fastd", column(rows, "fastd"), golden("stochf_fastd"))
        rows = frame.select(
            ta.stochrsi_fastk(
                "close", timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0
            )
            .over(window)
            .alias("fastk"),
            ta.stochrsi_fastd(
                "close", timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0
            )
            .over(window)
            .alias("fastd"),
        ).collect()
        expect_bit_exact("ta.stochrsi_fastk", column(rows, "fastk"), golden("stochrsi_fastk"))
        expect_bit_exact("ta.stochrsi_fastd", column(rows, "fastd"), golden("stochrsi_fastd"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
