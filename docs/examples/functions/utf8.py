"""Demonstrate the ``F.*`` UTF-8 byte-view names on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.bit_length",
    "F.octet_length",
    "F.is_valid_utf8",
    "F.make_valid_utf8",
    "F.try_validate_utf8",
    "F.col",
]


def main() -> None:
    """Check the byte counts and the three invalid-sequence behaviours on one binary frame."""
    repark = ReparkSession.builder.appName("ex-utf8").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                (b"abc",),
                (b"\xff",),
                ("Café".encode(),),
                (b"a\xffb",),
                (b"",),
                (None,),
            ],
            ["b"],
        )
        rows = frame.select(
            F.bit_length(F.col("b")).alias("bits"),
            F.octet_length(F.col("b")).alias("octets"),
            F.is_valid_utf8(F.col("b")).alias("valid"),
            F.make_valid_utf8(F.col("b")).alias("repaired"),
            F.try_validate_utf8(F.col("b")).alias("tolerant"),
        ).collect()
        checked = (
            ("bits", [24, 8, 40, 24, 0, None]),
            ("octets", [3, 1, 5, 3, 0, None]),
            ("valid", [True, False, True, False, True, None]),
            ("repaired", ["abc", "\ufffd", "Café", "a\ufffdb", "", None]),
            ("tolerant", ["abc", None, "Café", None, "", None]),
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
