"""Demonstrate the ``F.percentile_approx`` / ``F.approx_percentile`` alias pair."""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.percentile_approx",
    "F.approx_percentile",
]


def main() -> None:
    """Run the measured percentile-approx alias-pair arms."""
    repark = ReparkSession.builder.appName("ex-stats").master("local[1]").getOrCreate()
    try:
        ranks = repark.createDataFrame([(value,) for value in [*range(1, 101), None]], "x INT")
        rows = ranks.select(
            F.percentile_approx("x", 0.5).alias("median"),
            F.approx_percentile("x", 0.5).alias("median_alias"),
            F.percentile_approx("x", [0.0, 0.5, 1.0]).alias("quartiles"),
        ).collect()
        checked = (
            ("median", [50]),
            ("median_alias", [50]),
            ("quartiles", [[1, 50, 100]]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
