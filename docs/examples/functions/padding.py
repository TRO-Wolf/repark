"""Demonstrate the ``F.*`` padding and strip names on a small local frame.

pins: ex-4-functions-strings-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.lpad",
    "F.rpad",
    "F.ltrim",
    "F.rtrim",
    "F.trim",
    "F.btrim",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Check pad-to-width with truncation, the space strips, and the named-character strip."""
    repark = ReparkSession.builder.appName("ex-padding").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark", " xx "), ("Apache", "aXa"), ("aPACHE", "  yy  "), ("", ""), (None, None)],
            ["s", "t"],
        )
        rows = frame.select(
            F.lpad(F.col("s"), F.lit(8), F.lit("-")).alias("lpad_8"),
            F.lpad(F.col("s"), F.lit(3), F.lit("-")).alias("lpad_3"),
            F.rpad(F.col("s"), F.lit(8), F.lit("-")).alias("rpad_8"),
            F.rpad(F.col("s"), F.lit(3), F.lit("-")).alias("rpad_3"),
            F.ltrim(F.col("t")).alias("ltrim_t"),
            F.rtrim(F.col("t")).alias("rtrim_t"),
            F.trim(F.col("t")).alias("trim_t"),
            F.btrim(F.col("t")).alias("btrim_t"),
            F.btrim(F.lit("xxSparkxx"), F.lit("x")).alias("btrim_x"),
        ).collect()
        checked = (
            ("lpad_8", ["---Spark", "--Apache", "--aPACHE", "--------", None]),
            ("lpad_3", ["Spa", "Apa", "aPA", "---", None]),
            ("rpad_8", ["Spark---", "Apache--", "aPACHE--", "--------", None]),
            ("rpad_3", ["Spa", "Apa", "aPA", "---", None]),
            ("ltrim_t", ["xx ", "aXa", "yy  ", "", None]),
            ("rtrim_t", [" xx", "aXa", "  yy", "", None]),
            ("trim_t", ["xx", "aXa", "yy", "", None]),
            ("btrim_t", ["xx", "aXa", "yy", "", None]),
            ("btrim_x", ["Spark"] * 5),
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
