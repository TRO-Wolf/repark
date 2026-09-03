"""Demonstrate the ``F.*`` SQL wildcard names on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.like", "F.ilike", "F.col", "F.lit"]


def main() -> None:
    """Check the percent and underscore wildcards and the backslash escape, case-folded in ilike."""
    repark = ReparkSession.builder.appName("ex-like").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("Spark SQL",), ("aaa",), ("abc123",), (None,)], ["s"])
        rows = frame.select(
            F.like(F.col("s"), F.lit("S%")).alias("s_pct"),
            F.like(F.col("s"), F.lit("_aa")).alias("under"),
            F.like(F.col("s"), F.lit("%123")).alias("pct_123"),
            F.ilike(F.col("s"), F.lit("s%")).alias("ilike_s"),
            F.ilike(F.col("s"), F.lit("%sql")).alias("ilike_sql"),
        ).collect()
        checked = (
            ("s_pct", [True, False, False, None]),
            ("under", [False, True, False, None]),
            ("pct_123", [False, False, True, None]),
            ("ilike_s", [True, False, False, None]),
            ("ilike_sql", [True, False, False, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        escapes = repark.createDataFrame([("100%",), ("100x",), ("a_b",), ("axb",), (None,)], ["s"])
        rows = escapes.select(
            F.like(F.col("s"), F.lit("100\\%")).alias("esc_pct"),
            F.like(F.col("s"), F.lit("a\\_b")).alias("esc_us"),
            F.ilike(F.col("s"), F.lit("100\\%")).alias("ilike_esc"),
        ).collect()
        checked = (
            ("esc_pct", [True, False, False, False, None]),
            ("esc_us", [False, False, True, False, None]),
            ("ilike_esc", [True, False, False, False, None]),
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
