"""Demonstrate the ``F.*`` per-character text-rewriting name on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.translate", "F.col"]


def main() -> None:
    """Check the per-character translate map, including deleting characters with an empty map."""
    repark = ReparkSession.builder.appName("ex-translate").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark",), ("SQL",), ("hello world",), ("café",), ("",), (None,)], ["s"]
        )
        mapped = frame.select(
            F.translate(F.col("s"), "Sl", "76").alias("mapped"),
            F.translate(F.col("s"), "o", "").alias("deleted"),
        ).collect()
        checked = (
            ("mapped", ["7park", "7QL", "he66o wor6d", "café", "", None]),
            ("deleted", ["Spark", "SQL", "hell wrld", "café", "", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in mapped]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
