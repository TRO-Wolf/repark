"""Demonstrate the overlap-study ``ta`` kernels against the recorded TA-Lib goldens.

Every asserted value is read from ``crates/repark-ta/tests/goldens`` at run time.

pins: ex-23-ta-a/C-001
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from repark import Row
from repark.spark import ReparkSession, Window, ta

COVERS: list[str] = [
    "ta.ema",
    "ta.dema",
    "ta.kama",
    "ta.ma",
    "ta.bbands_upper",
    "ta.bbands_middle",
    "ta.bbands_lower",
    "ta.fama",
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
    """Run each overlap study over the ordered fixture window and check the golden tail."""
    repark = ReparkSession.builder.appName("ex-ta-overlap").master("local[1]").getOrCreate()
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
        rows = frame.select(ta.ema("close", timeperiod=21).over(window).alias("ema21")).collect()
        expect_tail("ta.ema", column(rows, "ema21"), golden("ema_21"))
        rows = frame.select(ta.dema("close", timeperiod=10).over(window).alias("dema10")).collect()
        expect_tail("ta.dema", column(rows, "dema10"), golden("dema_10"))
        rows = frame.select(ta.kama("close", timeperiod=10).over(window).alias("kama10")).collect()
        expect_tail("ta.kama", column(rows, "kama10"), golden("kama_10"))
        rows = frame.select(
            ta.ma("close", timeperiod=30, matype=0).over(window).alias("ma30")
        ).collect()
        expect_tail("ta.ma", column(rows, "ma30"), golden("ma_30_type0"))
        rows = frame.select(
            ta.bbands_upper("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)
            .over(window)
            .alias("upper")
        ).collect()
        expect_tail("ta.bbands_upper", column(rows, "upper"), golden("bbands_20_upper"))
        rows = frame.select(
            ta.bbands_middle("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)
            .over(window)
            .alias("middle")
        ).collect()
        expect_tail("ta.bbands_middle", column(rows, "middle"), golden("bbands_20_middle"))
        rows = frame.select(
            ta.bbands_lower("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)
            .over(window)
            .alias("lower")
        ).collect()
        expect_tail("ta.bbands_lower", column(rows, "lower"), golden("bbands_20_lower"))
        rows = frame.select(
            ta.fama("close", fastlimit=0.5, slowlimit=0.05).over(window).alias("fama")
        ).collect()
        expect_tail("ta.fama", column(rows, "fama"), golden("mama_fama"))
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
