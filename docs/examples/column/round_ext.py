"""Round values with the repark-only ``Column.round`` extension.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["Column.round"]


def main() -> None:
    """Run the measured rounding answers on one local frame."""
    repark = ReparkSession.builder.appName("ex-col-round").master("local[1]").getOrCreate()
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
        rounded = frame.select(frame.v.round(1))
        round_columns = ["round(v, 1)"]
        if rounded.columns != round_columns:
            raise SystemExit(f"Column.round columns {rounded.columns!r} != {round_columns!r}")
        rounded_rows = set(rounded.collect())
        rounded_expected = {(10.0,), (20.0,), (30.0,), (40.0,), (50.0,), (None,)}
        if rounded_rows != rounded_expected:
            raise SystemExit(f"Column.round rows {rounded_rows!r} != {rounded_expected!r}")

        half = F.lit(2.5)
        bumped = frame.select(half.round(0))
        bump_rows = set(bumped.collect())
        bump_expected = {(3.0,)}
        if bump_rows != bump_expected:
            raise SystemExit(f"Column.round rows {bump_rows!r} != {bump_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
