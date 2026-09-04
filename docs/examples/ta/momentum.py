"""Demonstrate the momentum and directional-movement ``ta`` kernels against the goldens.

Every asserted value is read from ``crates/repark-ta/tests/goldens`` at run time.

pins: ex-23-ta-a/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = [
    "ta.adx",
    "ta.adxr",
    "ta.apo",
    "ta.aroon_down",
    "ta.aroon_up",
    "ta.aroonosc",
    "ta.bop",
    "ta.cci",
    "ta.cmo",
    "ta.dx",
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
    """Run each momentum kernel over the ordered fixture window and check the golden tail."""
    repark = ReparkSession.builder.appName("ex-ta-momentum").master("local[1]").getOrCreate()
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
            ta.apo("close", fastperiod=12, slowperiod=26, matype=0).over(window).alias("apo")
        ).collect()
        expect_tail("ta.apo", column(rows, "apo"), golden("apo_12_26"))
        rows = frame.select(
            ta.aroon_down("high", "low", timeperiod=14).over(window).alias("down")
        ).collect()
        expect_tail("ta.aroon_down", column(rows, "down"), golden("aroon_14_down"))
        rows = frame.select(
            ta.aroon_up("high", "low", timeperiod=14).over(window).alias("up")
        ).collect()
        expect_tail("ta.aroon_up", column(rows, "up"), golden("aroon_14_up"))
        rows = frame.select(
            ta.aroonosc("high", "low", timeperiod=14).over(window).alias("osc")
        ).collect()
        expect_tail("ta.aroonosc", column(rows, "osc"), golden("aroonosc_14"))
        rows = frame.select(
            ta.bop("open", "high", "low", "close").over(window).alias("bop")
        ).collect()
        expect_tail("ta.bop", column(rows, "bop"), golden("bop"))
        rows = frame.select(
            ta.cci("high", "low", "close", timeperiod=14).over(window).alias("cci")
        ).collect()
        expect_tail("ta.cci", column(rows, "cci"), golden("cci_14"))
        rows = frame.select(ta.cmo("close", timeperiod=14).over(window).alias("cmo")).collect()
        expect_tail("ta.cmo", column(rows, "cmo"), golden("cmo_14"))
        rows = frame.select(
            ta.adx("high", "low", "close", timeperiod=14).over(window).alias("adx")
        ).collect()
        expect_tail("ta.adx", column(rows, "adx"), golden("adx_14"))
        rows = frame.select(
            ta.adxr("high", "low", "close", timeperiod=14).over(window).alias("adxr")
        ).collect()
        expect_tail("ta.adxr", column(rows, "adxr"), golden("adxr_14"))
        rows = frame.select(
            ta.dx("high", "low", "close", timeperiod=14).over(window).alias("dx")
        ).collect()
        expect_tail("ta.dx", column(rows, "dx"), golden("dx_14"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
