"""Demonstrate the ``F.*`` length names and the code-point pair ``F.ascii`` / ``F.chr``.

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
    "F.char",
    "F.chr",
    "F.col",
]


def main() -> None:
    """Check the length spellings, the first code point, and the inverse chr/char pair."""
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
        code_frame = repark.createDataFrame([(65,), (97,), (0,), (32,), (None,)], ["n"])
        code_rows = code_frame.select(
            F.chr(F.col("n")).alias("chr"),
            F.char(F.col("n")).alias("char"),
        ).collect()
        code_checked = (
            ("chr", ["A", "a", "\x00", " ", None]),
            ("char", ["A", "a", "\x00", " ", None]),
        )
        for name, expected in code_checked:
            values = [row[name] for row in code_rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        if [row["chr"] for row in code_rows] != [row["char"] for row in code_rows]:
            raise SystemExit("F.char is F.chr and must agree exactly")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
