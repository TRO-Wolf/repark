"""Demonstrate the printf-style ``F.format_string`` and its sibling ``F.printf``.

pins: ex-4-functions-strings-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.format_string", "F.printf", "F.col", "F.lit"]


def main() -> None:
    """Check the template spellings, including how a NULL argument renders."""
    repark = ReparkSession.builder.appName("ex-format").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark", 4), ("Apache", -3), ("aPACHE", 0), ("", 6), (None, None)], ["s", "n"]
        )
        rows = frame.select(
            F.format_string("%s=%d", F.col("s"), F.col("n")).alias("formatted"),
            F.printf(F.lit("%d apples"), F.col("n")).alias("printf_apples"),
        ).collect()
        checked = (
            ("formatted", ["Spark=4", "Apache=-3", "aPACHE=0", "=6", "null=null"]),
            ("printf_apples", ["4 apples", "-3 apples", "0 apples", "6 apples", "null apples"]),
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
