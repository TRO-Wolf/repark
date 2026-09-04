"""Demonstrate the remaining overlap-study ``ta`` kernels against the recorded goldens.

Every asserted value is read from ``crates/repark-ta/tests/goldens`` at run time.

pins: ex-24-ta-b/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = [
    "ta.mama",
    "ta.mavp",
    "ta.midpoint",
    "ta.midprice",
    "ta.t3",
    "ta.tema",
    "ta.trima",
    "ta.wma",
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
    """Run each remaining overlap study over the ordered fixture window and check bit-for-bit."""
    repark = ReparkSession.builder.appName("ex-ta-ma-variants").master("local[1]").getOrCreate()
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
                    golden("fixture_periods"),
                    strict=True,
                )
            ),
            ["ts", "open", "high", "low", "close", "volume", "periods"],
        )
        window = Window.orderBy("ts")
        rows = frame.select(
            ta.mama("close", fastlimit=0.5, slowlimit=0.05).over(window).alias("mama")
        ).collect()
        expect_bit_exact("ta.mama", column(rows, "mama"), golden("mama_mama"))
        rows = frame.select(
            ta.mavp("close", "periods", minperiod=5, maxperiod=20, matype=0)
            .over(window)
            .alias("mavp")
        ).collect()
        expect_bit_exact("ta.mavp", column(rows, "mavp"), golden("mavp"))
        rows = frame.select(
            ta.midpoint("close", timeperiod=10).over(window).alias("midpoint")
        ).collect()
        expect_bit_exact("ta.midpoint", column(rows, "midpoint"), golden("midpoint_10"))
        rows = frame.select(
            ta.midprice("high", "low", timeperiod=10).over(window).alias("midprice")
        ).collect()
        expect_bit_exact("ta.midprice", column(rows, "midprice"), golden("midprice_10"))
        rows = frame.select(
            ta.t3("close", timeperiod=5, vfactor=0.7).over(window).alias("t3")
        ).collect()
        expect_bit_exact("ta.t3", column(rows, "t3"), golden("t3_5"))
        rows = frame.select(ta.tema("close", timeperiod=10).over(window).alias("tema")).collect()
        expect_bit_exact("ta.tema", column(rows, "tema"), golden("tema_10"))
        rows = frame.select(ta.trima("close", timeperiod=10).over(window).alias("trima")).collect()
        expect_bit_exact("ta.trima", column(rows, "trima"), golden("trima_10"))
        rows = frame.select(ta.wma("close", timeperiod=10).over(window).alias("wma")).collect()
        expect_bit_exact("ta.wma", column(rows, "wma"), golden("wma_10"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
