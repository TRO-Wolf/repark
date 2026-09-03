"""Demonstrate the ``F.*`` hex and binary encoding names on a small local frame.

pins: ex-11-functions-hash-url-random/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.hex", "F.unhex", "F.bin", "F.try_to_binary", "F.col", "F.lit"]


def main() -> None:
    """Check hex and base-two spelling of integers, and the try-to-binary fallback."""
    repark = ReparkSession.builder.appName("ex-hex-binary").master("local[1]").getOrCreate()
    try:
        ints = repark.createDataFrame([(13,), (255,), (0,), (-1,), (None,)], ["n"])
        rows = ints.select(
            F.col("n"),
            F.hex(F.col("n")).alias("hexed"),
            F.bin(F.col("n")).alias("binned"),
        ).collect()
        checked = (
            ("hexed", ["D", "FF", "0", "FFFFFFFFFFFFFFFF", None]),
            ("binned", ["1101", "11111111", "0", "1" * 64, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")

        strings = repark.createDataFrame([("hello",), (None,)], ["s"])
        rows = strings.select(F.hex(F.col("s")).alias("hexed")).collect()
        values = [row["hexed"] for row in rows]
        print(f"F.hex on strings: {values!r}")
        if values != ["68656C6C6F", None]:
            raise SystemExit(f"F.hex values {values!r} != ['68656C6C6F', None]")

        ones = repark.createDataFrame([(1,)], ["i"])
        row = ones.select(
            F.unhex(F.lit("68656C6C6F")).alias("unhexed"),
            F.unhex(F.lit("nothex")).alias("unhex_bad"),
            F.unhex(F.lit(None)).alias("unhex_null"),
            F.try_to_binary(F.lit("hello"), F.lit("utf-8")).alias("as_utf8"),
            F.try_to_binary(F.lit("hello"), F.lit("bad-cs")).alias("as_bad_cs"),
            F.try_to_binary(F.lit(None), F.lit("utf-8")).alias("as_null"),
            F.hex(F.try_to_binary(F.lit("hello"), F.lit("utf-8"))).alias("round_trip"),
        ).collect()[0]
        print(f"unhex and try_to_binary row: {row!r}")
        expected = {
            "unhexed": b"hello",
            "unhex_bad": None,
            "unhex_null": None,
            "as_utf8": b"hello",
            "as_bad_cs": None,
            "as_null": None,
            "round_trip": "68656C6C6F",
        }
        for name, want in expected.items():
            if row[name] != want:
                raise SystemExit(f"{name} gave {row[name]!r}, expected {want!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
