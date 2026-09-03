"""Demonstrate the ``F.*`` length names and the first code point ``F.ascii``.

pins: ex-4-functions-strings-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.length",
    "F.char_length",
    "F.character_length",
    "F.ascii",
    "F.col",
]


def main() -> None:
    """Check the three length spellings and the first code point, ASCII and Unicode."""
    repark = ReparkSession.builder.appName("ex-length").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark",), ("Apache",), ("aPACHE",), ("",), (None,)], ["s"]
        )
        rows = frame.select(
            F.length(F.col("s")).alias("length"),
            F.char_length(F.col("s")).alias("char_length"),
            F.character_length(F.col("s")).alias("character_length"),
            F.ascii(F.col("s")).alias("ascii"),
        ).collect()
        checked = (
            ("length", [5, 6, 6, 0, None]),
            ("char_length", [5, 6, 6, 0, None]),
            ("character_length", [5, 6, 6, 0, None]),
            ("ascii", [83, 65, 97, 0, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        if [row["char_length"] for row in rows] != [row["length"] for row in rows]:
            raise SystemExit("F.char_length is F.length and must agree exactly")
        if [row["character_length"] for row in rows] != [row["char_length"] for row in rows]:
            raise SystemExit("F.character_length is F.char_length and must agree exactly")
        unicode_frame = repark.createDataFrame(
            [("héllo",), ("日本語",), ("𝄞ab",), ("straße",), ("İstanbul",)], ["s"]
        )
        unicode_rows = unicode_frame.select(
            F.length(F.col("s")).alias("length_u"),
            F.char_length(F.col("s")).alias("char_length_u"),
            F.character_length(F.col("s")).alias("character_length_u"),
            F.ascii(F.col("s")).alias("ascii_u"),
        ).collect()
        unicode_lengths = [5, 3, 3, 6, 8]
        unicode_checked = (
            ("length_u", unicode_lengths),
            ("char_length_u", unicode_lengths),
            ("character_length_u", unicode_lengths),
            ("ascii_u", [104, 26085, 119070, 115, 304]),
        )
        for name, expected in unicode_checked:
            values = [row[name] for row in unicode_rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
