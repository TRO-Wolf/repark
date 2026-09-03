"""Demonstrate the ``F.*`` string-shape helper names on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.repeat", "F.reverse", "F.soundex", "F.quote", "F.col"]


def main() -> None:
    """Check the repeat counts, the reversed spellings, the soundex codes, and the quoting."""
    repark = ReparkSession.builder.appName("ex-words").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark",), ("SQL",), ("hello world",), ("café",), ("",), (None,)], ["s"]
        )
        rows = frame.select(
            F.repeat(F.col("s"), 2).alias("twice"),
            F.repeat(F.col("s"), 0).alias("zero"),
            F.repeat(F.col("s"), -1).alias("negative"),
            F.reverse(F.col("s")).alias("rev"),
            F.soundex(F.col("s")).alias("code"),
        ).collect()
        checked = (
            ("twice", ["SparkSpark", "SQLSQL", "hello worldhello world", "cafécafé", "", None]),
            ("zero", ["", "", "", "", "", None]),
            ("negative", ["", "", "", "", "", None]),
            ("rev", ["krapS", "LQS", "dlrow olleh", "éfac", "", None]),
            ("code", ["S162", "S400", "H464", "C100", "", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        literals = repark.createDataFrame([("Spark",), ("a`b",), ("",), (None,)], ["s"])
        quoted = literals.select(F.quote(F.col("s")).alias("quoted")).collect()
        values = [row["quoted"] for row in quoted]
        print(f"F.quote: {values!r}")
        if values != ["'Spark'", "'a`b'", "''", None]:
            raise SystemExit(f"F.quote values {values!r} != [\"'Spark'\", \"'a`b'\", \"''\", None]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
