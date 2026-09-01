"""Demonstrate the ``ta.sma`` kernel over a local ordered window.

pins: ex-0-example-drift-gate/C-002, C-008, C-010
"""

from __future__ import annotations

from repark.spark import SparkSession, Window, ta

COVERS: list[str] = ["ta.sma"]


def main() -> None:
    """Compute SMA(2) on four ordered close prices and check the last value."""
    spark = SparkSession.builder.appName("ex-sma").master("local[1]").getOrCreate()
    try:
        frame = spark.createDataFrame(
            [(1, 10.0), (2, 11.0), (3, 12.0), (4, 13.0)],
            ["ts", "close"],
        )
        window = Window.orderBy("ts")
        rows = frame.select(ta.sma("close", timeperiod=2).over(window).alias("sma2")).collect()
        last = rows[-1]["sma2"]
        if last is None or abs(float(last) - 12.5) > 1e-9:
            raise SystemExit(f"ta.sma last value {last!r} != 12.5")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
