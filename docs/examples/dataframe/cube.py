"""Cube-group one frame over two keys and aggregate every grouping set.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.cube"]


def main() -> None:
    """Run the measured cube answers, including the grand total and per-key subtotal rows."""
    repark = ReparkSession.builder.appName("ex-df-cube").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        rolled = frame.cube("g", "k").agg(F.sum("v"))
        assert rolled.columns == ["g", "k", "sum(v)"]
        assert set(rolled.collect()) == {
            ("a", 1, 10.0),
            ("a", 2, 50.0),
            ("a", 3, 40.0),
            ("a", None, 100.0),
            ("b", 1, 50.0),
            ("b", 2, None),
            ("b", None, 50.0),
            (None, 1, 60.0),
            (None, 2, 50.0),
            (None, 3, 40.0),
            (None, None, 150.0),
        }
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
